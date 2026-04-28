/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.search;

import static org.apache.solr.search.DocSetUtil.copyBitRange;
import static org.apache.solr.search.OrdMapRegenerator.getRegenKeepAliveNanos;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.IntBuffer;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.ExitableDirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TimeLimitingCollector;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.MapWriter;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.search.SolrCache.MetaEntry;
import org.apache.solr.util.IOFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A regenerator capable of lazily warming entries where possible (passing stale entries along
 * unmodified, with metadata to allow using the stale entry to reconstruct new entries on-demand,
 * without actually needing to run the query again over segments that still exist in the index).
 *
 * <p>Cache entries with cross-doc/cross-segment dependencies (e.g., {@link JoinQuery}) cannot be
 * warmed lazily, but this regenerator may still be configured to eagerly warm such queries.
 *
 * <p>Especially because this regenerator is designed to be used in conjunction with {@code
 * autowarm="100%"}, it can be important to prevent unused queries from being carried forward
 * indefinitely. Accordingly, this class extends {@link MetaCacheRegenerator}, wrapping entries in
 * the associated cache to record (and update) each entry's last access time, allowing to drop (not
 * warm) entries that haven't been accessed recently enough.
 *
 * <p>There are 4 configuration parameters (configured by attributes in the element configuring the
 * associated cache):
 *
 * <ul>
 *   <li>{@code regenKeepAlive}: max time since last access for cache entries to warm lazily
 *   <li>{@code eagerKeepAlive}: max time since last access for cache entries to warm eagerly
 *   <li>{@code preferLazy}: determines warming behavior for cache entries that are capable of being
 *       warmed lazily, but that also meet the criteria configured for {@code eagerKeepAlive}. If
 *       {@code false} (the default), such entries will be warmed eagerly (though still leveraging
 *       the stale cache entry to make warming more efficient). If set to {@code "true"}, no eager
 *       evaluation will be performed at warming time; the stale entry will be carried forward to
 *       the new cache as-is, and used lazily to reconstruct a new cache entry on-demand, if needed.
 *   <li>{@code overlapThreshold}: (default {@code 0.5}) determines the minimum segment overlap
 *       threshold that allows cache entries to be carried forward lazily. Below this threshold, the
 *       costs (heap consumed) are considered to outweigh the potential benefits of partial cache
 *       entry reconstruction.
 * </ul>
 */
public class KeepAliveRegenerator<M extends MetaEntry<Query, DocSet, M>>
    extends MetaCacheRegenerator<Query, DocSet, M> {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int DEFAULT_REGEN_KEEPALIVE_MINUTES = 2;
  private static final long DEFAULT_REGEN_KEEPALIVE_NANOS =
      TimeUnit.MINUTES.toNanos(DEFAULT_REGEN_KEEPALIVE_MINUTES);

  private static final double DEFAULT_OVERLAP_THRESHOLD = 0.5;

  private final long regenKeepAliveNanos;
  private final long eagerKeepAliveNanos;
  private final boolean preferLazy;
  private final double overlapThreshold;

  public KeepAliveRegenerator() {
    this(DEFAULT_REGEN_KEEPALIVE_NANOS, 0, false);
    // default ctor in case someone specifies this class via standard `"regen"=[className]` syntax
  }

  static boolean autowarmOn(CacheConfig config) {
    return new SolrCacheBase.AutoWarmCountRef(
            (String) config.toMap(new HashMap<>()).get("autowarmCount"))
        .isAutoWarmingOn();
  }

  public KeepAliveRegenerator(SolrConfig solrConfig, CacheConfig cacheConfig) {
    this(solrConfig, cacheConfig, new LongAdder(), new DoubleAdder());
  }

  private KeepAliveRegenerator(
      SolrConfig solrConfig,
      CacheConfig cacheConfig,
      LongAdder partialHits,
      DoubleAdder partialHitsRatio) {
    super(autowarmOn(cacheConfig), getWrapFunction(partialHits, partialHitsRatio));
    Map<String, Object> cacheConfigArgs = cacheConfig.toMap(Collections.emptyMap());
    this.regenKeepAliveNanos =
        getRegenKeepAliveNanos("regenKeepAlive", solrConfig, cacheConfigArgs, null);
    this.eagerKeepAliveNanos =
        getRegenKeepAliveNanos("eagerKeepAlive", solrConfig, cacheConfigArgs, "0");
    this.preferLazy = "true".equals(cacheConfigArgs.get("preferLazy"));
    this.partialHits = partialHits;
    this.partialHitsRatio = partialHitsRatio;
    String tmp = (String) cacheConfigArgs.get("overlapThreshold");
    this.overlapThreshold = tmp == null ? DEFAULT_OVERLAP_THRESHOLD : Double.parseDouble(tmp);
  }

  private KeepAliveRegenerator(
      long regenKeepAliveNanos, long eagerKeepAliveNanos, boolean preferLazy) {
    this(regenKeepAliveNanos, eagerKeepAliveNanos, preferLazy, new LongAdder(), new DoubleAdder());
  }

  private KeepAliveRegenerator(
      long regenKeepAliveNanos,
      long eagerKeepAliveNanos,
      boolean preferLazy,
      LongAdder partialHits,
      DoubleAdder partialHitsRatio) {
    super(true, getWrapFunction(partialHits, partialHitsRatio));
    this.regenKeepAliveNanos = regenKeepAliveNanos;
    this.eagerKeepAliveNanos = eagerKeepAliveNanos;
    this.preferLazy = preferLazy;
    this.partialHits = partialHits;
    this.partialHitsRatio = partialHitsRatio;
    this.overlapThreshold = DEFAULT_OVERLAP_THRESHOLD;
  }

  private static <M> BiFunction<SegmentMap, DocSet, M> getWrapFunction(
      LongAdder partialHits, DoubleAdder partialHitsRatio) {
    // noinspection Convert2Lambda
    return new BiFunction<SegmentMap, DocSet, M>() {
      @Override
      @SuppressWarnings("unchecked")
      public M apply(SegmentMap segMap, DocSet v) {
        return (M) new KeepAliveSegAwareValue(segMap, v, partialHits, partialHitsRatio);
      }
    };
  }

  static boolean isCrossDoc(Query q) {
    boolean[] ret = new boolean[1];
    q.visit(
        new QueryVisitor() {
          @Override
          public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
            return isCrossDoc(parent);
          }

          @Override
          public void visitLeaf(Query query) {
            isCrossDoc(query);
          }

          private QueryVisitor isCrossDoc(Query q) {
            if (ret[0]) {
              return EMPTY_VISITOR;
            } else if (q.getClass().getSimpleName().endsWith("JoinQuery")) {
              ret[0] = true;
              return EMPTY_VISITOR;
            } else {
              return this;
            }
          }

          @Override
          public void consumeTerms(Query query, Term... terms) {
            isCrossDoc(query);
          }

          @Override
          public void consumeTermsMatching(
              Query query, String field, Supplier<ByteRunAutomaton> automaton) {
            isCrossDoc(query);
          }

          @Override
          public boolean acceptField(String field) {
            return !ret[0];
          }
        });
    return ret[0];
  }

  private static class KeepAliveSegAwareValue
      implements MetaEntry<Query, DocSet, KeepAliveSegAwareValue> {

    private static final long BASE_RAM_BYTES_USED =
        RamUsageEstimator.shallowSizeOfInstance(KeepAliveSegAwareValue.class)
            + RamUsageEstimator.shallowSizeOfInstance(AtomicReference.class)
            + RamUsageEstimator.shallowSizeOfInstance(AbstractMap.SimpleImmutableEntry.class)
            + RamUsageEstimator.shallowSizeOfInstance(CompletableFuture.class);

    private final AtomicReference<
            AbstractMap.SimpleImmutableEntry<SegmentMap, CompletableFuture<DocSet>>>
        ref;
    private final LongAdder partialHits;
    private final DoubleAdder partialHitsRatio;
    private final long ramBytesUsed;
    private long accessTimestampNanos;
    // non-null only for entries created via the template constructor (lazy warming path); holds a
    // refcount on the stale DocSet to keep it alive for reconstruction, released after
    // reconstruction completes or when this entry is evicted without ever being accessed.
    private final AtomicReference<Closeable> staleHold = new AtomicReference<>(UNINITIALIZED);

    public KeepAliveSegAwareValue(
        SegmentMap segMap, DocSet val, LongAdder partialHits, DoubleAdder partialHitsRatio) {
      CompletableFuture<DocSet> f = new CompletableFuture<>();
      f.complete(val);
      ref = new AtomicReference<>(new AbstractMap.SimpleImmutableEntry<>(segMap, f));
      ramBytesUsed = BASE_RAM_BYTES_USED + val.ramBytesUsed();
      this.accessTimestampNanos = System.nanoTime();
      this.partialHits = partialHits;
      this.partialHitsRatio = partialHitsRatio;
    }

    public KeepAliveSegAwareValue(
        SegmentMap segMap,
        DocSet val,
        long accessTimestampNanos,
        LongAdder partialHits,
        DoubleAdder partialHitsRatio) {
      CompletableFuture<DocSet> f = new CompletableFuture<>();
      f.complete(val);
      ref = new AtomicReference<>(new AbstractMap.SimpleImmutableEntry<>(segMap, f));
      ramBytesUsed = BASE_RAM_BYTES_USED + val.ramBytesUsed();
      this.accessTimestampNanos = accessTimestampNanos;
      this.partialHits = partialHits;
      this.partialHitsRatio = partialHitsRatio;
    }

    public KeepAliveSegAwareValue(
        KeepAliveSegAwareValue template, LongAdder partialHits, DoubleAdder partialHitsRatio)
        throws AlreadyClosedException {
      AbstractMap.SimpleImmutableEntry<SegmentMap, CompletableFuture<DocSet>> entry =
          template.ref.get();
      ref = new AtomicReference<>(template.ref.get());
      DocSet val = entry.getValue().getNow(null);
      if (val == null) {
        // warming a value that's not yet loaded; just use the old `ramBytesUsed`
        ramBytesUsed = template.ramBytesUsed();
      } else {
        ramBytesUsed = BASE_RAM_BYTES_USED + val.ramBytesUsed();
        // If we already have a DocSet, opportunistically eagerly acquire it here so that
        // it stays alive until we've used it for reconstruction (or until we're evicted
        // without being accessed). If we fail to acquire, that's probably because the old
        // entry was concurrently evicted from its cache. This should be rare, but in such
        // cases we throw `AlreadyClosedException` to prevent the entry pointlessly consuming
        // space in entry count and ramBytesAccounting.
        Closeable release = val.acquire();
        if (release == null) {
          throw new AlreadyClosedException("stale docset already closed");
        }
        staleHold.set(release);
      }
      this.accessTimestampNanos = template.accessTimestampNanos;
      this.partialHits = partialHits;
      this.partialHitsRatio = partialHitsRatio;
    }

    @Override
    public KeepAliveSegAwareValue metaClone(DocSet val) {
      throw new UnsupportedOperationException();
    }

    private static final Closeable UNINITIALIZED = () -> {};

    @Override
    @SuppressWarnings("try")
    public void close() throws IOException {
      log.warn("should instead call close(SegmentMap)");
      try (Closeable hold = staleHold.getAndSet(null)) {
        // we can at least close stale, though
      }
    }

    @Override
    @SuppressWarnings("try")
    public void close(SegmentMap segMap) throws IOException {
      AbstractMap.SimpleImmutableEntry<SegmentMap, CompletableFuture<DocSet>> ref = this.ref.get();
      try (Closeable hold = staleHold.getAndSet(null);
          Closeable fresh = segMap == ref.getKey() ? ref.getValue().getNow(null) : null) {
        // NOTE: there's a race condition here, where a value in the process of computing may not
        // get closed. This is acceptable. DocSets usage is varied enough that it's impractical to
        // try to completely control the lifecycle, so we won't ever be able to get away without
        // ReferenceQueue as backup; so we knowingly let this "leak" for the sake of simplicity.
      }
    }

    @Override
    @SuppressWarnings("ReferenceEquality")
    public DocSet get(
        SegmentMap segMap, Query key, IOFunction<? super Query, ? extends DocSet> mappingFunction)
        throws IOException {
      AbstractMap.SimpleImmutableEntry<SegmentMap, CompletableFuture<DocSet>> ref = this.ref.get();
      accessTimestampNanos = System.nanoTime();
      DocSet[] weComputed = new DocSet[1];
      IOFunction<? super Query, ? extends DocSet> wrappedMappingFunction =
          (k) -> {
            if (mappingFunction == null) {
              return null;
            }
            DocSet ret = mappingFunction.apply(k);
            weComputed[0] = ret;
            return ret;
          };
      try {
        if (ref.getKey() == segMap) {
          return CaffeineCache.getV(key, wrappedMappingFunction, ref.getValue(), NOOP);
        } else if (mappingFunction == null) {
          return null;
        }
        CompletableFuture<DocSet> f = new CompletableFuture<>();
        AbstractMap.SimpleImmutableEntry<SegmentMap, CompletableFuture<DocSet>> newRef =
            new AbstractMap.SimpleImmutableEntry<>(segMap, f);
        AbstractMap.SimpleImmutableEntry<SegmentMap, CompletableFuture<DocSet>> witness =
            this.ref.compareAndExchange(ref, newRef);
        if (witness == ref) {
          // we compute
          try {
            DocSet stale = CaffeineCache.getV(key, (k) -> null, ref.getValue(), NOOP);
            DocSet computed;
            if (stale == null) {
              // nothing to reconstruct from (stale DocSet was null, or could not be acquired at
              // warming time because it was already closed); just run the query from scratch.
              computed = mappingFunction.apply(key);
              f.complete(computed);
            } else {
              try (Closeable release = acquireLazily(stale)) {
                if (release == null) {
                  computed = mappingFunction.apply(key);
                } else {
                  computed =
                      mappingFunction.apply(
                          new FrankensteinQuery(key, ref.getKey().segments, stale));
                  partialHits.increment();
                  partialHitsRatio.add(ref.getKey().getOverlap(segMap.key));
                }
              }
              f.complete(computed); // asap
            }
            return computed;
          } catch (TimeLimitingCollector.TimeExceededException
              | CancellableCollector.QueryCancelledException
              | ExitableDirectoryReader.ExitingReaderException e) {
            // These exceptions are related to the calling thread, so are "recoverable" from other
            // threads
            // that might be waiting for our computation to complete.
            f.completeExceptionally(CaffeineCache.REQUEST_SCOPED_EXCEPTION);
            throw e;
          } catch (Error | RuntimeException | IOException e) {
            f.completeExceptionally(e); // This will remove the future from the cache
            throw e;
          }
        } else if (witness.getKey() == segMap) {
          return CaffeineCache.getV(key, wrappedMappingFunction, witness.getValue(), NOOP);
        } else {
          return wrappedMappingFunction.apply(key);
        }
      } finally {
        // Here we attempt to avoid a situation where we have successfully computed a result, but
        // somehow have a stored exceptional result. We already have the `KeepAliveSegAwareValue`
        // cache entry, and a valid value. Replace the extant value if it's stale, or completed
        // exceptionally.
        DocSet outOfBandComputed = weComputed[0];
        if (outOfBandComputed != null) {
          this.ref.updateAndGet(
              (extant) -> {
                if (extant.getKey() != segMap || extant.getValue().isCompletedExceptionally()) {
                  CompletableFuture<DocSet> opportunistic = new CompletableFuture<>();
                  opportunistic.complete(outOfBandComputed);
                  return new AbstractMap.SimpleImmutableEntry<>(segMap, opportunistic);
                } else {
                  return extant;
                }
              });
        }
      }
    }

    private Closeable acquireLazily(DocSet stale) {
      Closeable extant = staleHold.get();
      if (extant == null) {
        // probably closing, but it doesn't hurt to try to acquire
        return stale.acquire();
      } else if (staleHold.compareAndSet(extant, null)) {
        if (extant == UNINITIALIZED) {
          return stale.acquire();
        } else {
          return extant;
        }
      } else {
        // probably closing, but it doesn't hurt to try to acquire
        return stale.acquire();
      }
    }

    @Override
    public long ramBytesUsed() {
      return ramBytesUsed;
    }
  }

  private static void doNothing() {}

  private static final Runnable NOOP = KeepAliveRegenerator::doNothing;

  private final LongAdder partialHits;
  private final DoubleAdder partialHitsRatio;
  private final LongAdder priorPartialHits = new LongAdder();
  private final DoubleAdder priorPartialHitsRatio = new DoubleAdder();

  @Override
  public void postWarm() {
    priorPartialHits.add(partialHits.sumThenReset());
    priorPartialHitsRatio.add(partialHitsRatio.sumThenReset());
  }

  @Override
  public void appendMetrics(MapWriter.EntryWriter map) throws IOException {
    final long partialHitCount = partialHits.sum();
    final double currentPartialHitRatio = partialHitsRatio.sum();
    final long cumPartialHitCount = priorPartialHits.sum() + partialHitCount;
    final double cumCurrentPartialHitsRatio = priorPartialHitsRatio.sum() + currentPartialHitRatio;
    map.put("partialHits", partialHitCount);
    map.put("partialHitsRatio", currentPartialHitRatio);
    map.put(
        "partialRatioPerHit",
        partialHitCount == 0 ? 1.0 : (currentPartialHitRatio / partialHitCount));
    map.put("cumulative_partialHits", cumPartialHitCount);
    map.put("cumulative_partialHitsRatio", cumCurrentPartialHitsRatio);
    map.put(
        "cumulative_partialRatioPerHit",
        cumPartialHitCount == 0 ? 1.0 : (cumCurrentPartialHitsRatio / cumPartialHitCount));
  }

  @Override
  public <K, V> boolean regenerateItem(
      SolrIndexSearcher newSearcher,
      SolrCache<K, V> newCache,
      SolrCache<K, V> oldCache,
      K oldKey,
      V oldVal)
      throws IOException {
    DirectoryReader in = newSearcher.getIndexReader();
    IndexReader.CacheHelper cacheHelper = in.getReaderCacheHelper();
    if (cacheHelper == null) {
      return false;
    }

    final List<LeafReaderContext> leaves = in.leaves();
    final int size = leaves.size();

    if (size < 2) {
      // we don't need OrdinalMaps for these trivial cases
      return false;
    }

    KeepAliveSegAwareValue metaEntry = (KeepAliveSegAwareValue) oldVal;
    final long extantTimestamp = metaEntry.accessTimestampNanos;
    long lastAccessAgo = System.nanoTime() - extantTimestamp;
    if (lastAccessAgo > regenKeepAliveNanos) {
      // it has been long enough since this was last accessed that we don't want to carry it forward
      return true;
    }

    final Query query = (Query) oldKey;

    @SuppressWarnings("unchecked")
    SolrCache<Query, KeepAliveSegAwareValue> c =
        (SolrCache<Query, KeepAliveSegAwareValue>) newCache;

    if (metaEntry.ref.get().getKey().registerOverlap(newSearcher.getSegmentMap()) < overlapThreshold
        || isCrossDoc(query)) {
      if (lastAccessAgo < eagerKeepAliveNanos) {
        // if we meet the criterion for eager warming, do it here (not leveraging segment-aware)
        c.computeIfAbsent(
            query,
            (q) -> {
              SegmentMap segMap = newSearcher.getSegmentMap();
              DocSet docSet = newSearcher.getDocSetNC(query, null);
              return new KeepAliveSegAwareValue(
                  segMap, docSet, extantTimestamp, partialHits, partialHitsRatio);
            });
      }
      return true;
    }

    if (!preferLazy && lastAccessAgo < eagerKeepAliveNanos) {
      // if we're doing eager warming, do it here even for segment-aware entries. We can
      // still benefit from the old cache entry.
      c.computeIfAbsent(
          query,
          (q) -> {
            AbstractMap.SimpleImmutableEntry<SegmentMap, CompletableFuture<DocSet>> ref =
                metaEntry.ref.get();
            DocSet stale;
            try {
              stale = ref.getValue().get();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new RuntimeException(e);
            } catch (ExecutionException e) {
              Throwable cause = e.getCause();
              if (cause instanceof IOException) {
                throw (IOException) cause;
              } else {
                throw new RuntimeException(e);
              }
            }
            DocSet docSet;
            try (Closeable release = stale.acquire()) {
              if (release == null) {
                docSet = newSearcher.getDocSetNC(query, null);
              } else {
                docSet =
                    newSearcher.getDocSetNC(
                        new FrankensteinQuery(query, ref.getKey().segments, stale), null);
              }
            }
            return new KeepAliveSegAwareValue(
                newSearcher.getSegmentMap(),
                docSet,
                extantTimestamp,
                partialHits,
                partialHitsRatio);
          });
      return true;
    }

    // the lazy way. Just pass the stale cache entry along in case it's queried later
    try {
      c.computeIfAbsent(
          query, (q) -> new KeepAliveSegAwareValue(metaEntry, partialHits, partialHitsRatio));
    } catch (AlreadyClosedException ignore) {
      // ignore
    }
    return true;
  }

  private static class FrankensteinQuery extends Query implements DocSetProducer {

    private final Query backing;
    private final Map<IndexReader.CacheKey, SegmentMap.Segment> segs;
    private final DocSet stale;

    FrankensteinQuery(
        Query backing, Map<IndexReader.CacheKey, SegmentMap.Segment> segs, DocSet stale) {
      this.backing = backing;
      this.segs = segs;
      this.stale = stale;
    }

    @Override
    public DocSet createDocSet(SolrIndexSearcher searcher) throws IOException {
      int maxDoc = searcher.maxDoc();
      int smallSetSize = DocSetUtil.smallSetSize(maxDoc);
      if (stale.size() < smallSetSize) {
        // TODO: it's possible that for small-set TermQuery specifically, it may actually be
        //  more efficient to use the below optimized `createDocSet()` method, instead of
        //  reconstructing from stale cache values:
        //  if (backing instanceof TermQuery) {
        //    return DocSetUtil.createDocSet(searcher, ((TermQuery) backing).getTerm());
        //  }
        // for small set sizes just use `createDocSetGeneric()`. This will work
        // via `createWeight()`, and will properly handle choosing DocSet type, etc.
        // for smaller sets it's likely that.
        // TODO: it would be possible to directly implement optimized DocSet creation here;
        //  but for smaller sets the impact is probably not significant enough to warrant
        //  the effort (incl. having to add special handling for choosing DocSet type, etc.)
        return DocSetUtil.createDocSetGeneric(searcher, this);
      } else {
        final Weight backingWeight =
            searcher
                .rewrite(new ConstantScoreQuery(backing))
                .createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
        final FixedBitSets staleBits;
        if (stale instanceof BitDocSet) {
          staleBits = ((BitDocSet) stale).getBits();
        } else {
          staleBits = null;
        }
        int size = 0;
        final FixedBitSets bits = new FixedBitSets(maxDoc);
        for (LeafReaderContext context : searcher.getLeafContexts()) {
          final Bits liveDocs = context.reader().getLiveDocs();
          final int newDocBase = context.docBase;
          final SegmentMap.Segment segment =
              segs.get(context.reader().getCoreCacheHelper().getKey());
          if (segment == null) {
            Scorer scorer = backingWeight.scorer(context);
            TwoPhaseIterator tpi;
            if (scorer == null) {
              continue; // nothing to do for this segment
            } else if ((tpi = scorer.twoPhaseIterator()) == null) {
              DocIdSetIterator iter = scorer.iterator();
              int doc;
              while ((doc = iter.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (liveDocs == null || liveDocs.get(doc)) {
                  bits.set(newDocBase + doc);
                  size++;
                }
              }
            } else {
              DocIdSetIterator iter = tpi.approximation();
              int doc;
              while ((doc = iter.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (tpi.matches() && (liveDocs == null || liveDocs.get(doc))) {
                  bits.set(newDocBase + doc);
                  size++;
                }
              }
            }
          } else {
            // backed by existing (partially stale) DocSet
            final int docBase = segment.docBase;
            final DocIdSetIterator disi;
            if (staleBits == null) {
              assert stale instanceof SortedIntDocSet;
              SortedIntDocSet staleSorted = (SortedIntDocSet) stale;
              int capacity = staleSorted.capacity;
              final IntBuffer[] docs = staleSorted.getDocs();
              final int first =
                  SortedIntDocSet.binarySearch(docs, 0, staleSorted.capacity, docBase);
              disi =
                  new DocIdSetIterator() {
                    final int limit = segment.maxDoc + docBase;
                    int idx = (first < 0 ? ~first : first) - 1;
                    int id = -1;

                    @Override
                    public int docID() {
                      return id == NO_MORE_DOCS ? NO_MORE_DOCS : id - docBase;
                    }

                    @Override
                    public int nextDoc() {
                      if (++idx >= capacity
                          || (id =
                                  docs[idx >> SortedIntDocSet.WORDS_SHIFT].get(
                                      idx & SortedIntDocSet.ARR_MASK))
                              >= limit) {
                        return id = NO_MORE_DOCS;
                      } else {
                        return id - docBase;
                      }
                    }

                    @Override
                    public int advance(int target) {
                      while (nextDoc() < target) {
                        // advance
                      }
                      return id == NO_MORE_DOCS ? NO_MORE_DOCS : id - docBase;
                    }

                    @Override
                    public long cost() {
                      return 0;
                    }
                  };
              int doc;
              while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (liveDocs == null || liveDocs.get(doc)) {
                  bits.set(newDocBase + doc);
                  size++;
                }
              }
            } else {
              copyBitRange(staleBits, docBase, bits, newDocBase, segment.maxDoc);
            }
          }
        }
        BitDocSet ret;
        if (staleBits == null) {
          ret = new BitDocSet(bits, size);
        } else {
          // we don't know the size upfront, and we still have to handle live docs
          bits.and(searcher.getLiveDocSet().getBits());
          ret = new BitDocSet(bits);
        }
        return ret;
      }
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
      final Weight backingWeight = backing.createWeight(searcher, scoreMode, boost);
      return new Weight(this) {
        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
          return null;
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          final SegmentMap.Segment segment =
              segs.get(context.reader().getCoreCacheHelper().getKey());
          if (segment == null) {
            return backingWeight.scorer(context);
          } else {
            // backed by existing (partially stale) DocSet
            final int docBase = segment.docBase;
            final DocIdSetIterator disi;
            if (stale instanceof SortedIntDocSet) {
              SortedIntDocSet staleSorted = (SortedIntDocSet) stale;
              int capacity = staleSorted.capacity;
              final IntBuffer[] docs = staleSorted.getDocs();
              final int first =
                  SortedIntDocSet.binarySearch(docs, 0, staleSorted.capacity, docBase);
              disi =
                  new DocIdSetIterator() {
                    final int limit = segment.maxDoc + docBase;
                    int idx = (first < 0 ? ~first : first) - 1;
                    int id = -1;

                    @Override
                    public int docID() {
                      return id == NO_MORE_DOCS ? NO_MORE_DOCS : id - docBase;
                    }

                    @Override
                    public int nextDoc() {
                      if (++idx >= capacity
                          || (id =
                                  docs[idx >> SortedIntDocSet.WORDS_SHIFT].get(
                                      idx & SortedIntDocSet.ARR_MASK))
                              >= limit) {
                        return id = NO_MORE_DOCS;
                      } else {
                        return id - docBase;
                      }
                    }

                    @Override
                    public int advance(int target) {
                      while (nextDoc() < target) {
                        // advance
                      }
                      return id == NO_MORE_DOCS ? NO_MORE_DOCS : id - docBase;
                    }

                    @Override
                    public long cost() {
                      return 0;
                    }
                  };
            } else if (stale instanceof BitDocSet) {
              final FixedBitSets docs = ((BitDocSet) stale).getBits();
              disi =
                  new DocIdSetIterator() {
                    final int limit = segment.maxDoc + docBase;
                    int id = docBase - 1;

                    @Override
                    public int docID() {
                      return id == NO_MORE_DOCS ? NO_MORE_DOCS : id - docBase;
                    }

                    @Override
                    public int nextDoc() {
                      if (++id >= limit || (id = docs.nextSetBit(id)) >= limit) {
                        return id = NO_MORE_DOCS;
                      } else {
                        return id - docBase;
                      }
                    }

                    @Override
                    public int advance(int target) {
                      if (target >= limit || (id = docs.nextSetBit(target)) >= limit) {
                        return id = NO_MORE_DOCS;
                      } else {
                        return id - docBase;
                      }
                    }

                    @Override
                    public long cost() {
                      return 0;
                    }
                  };
            } else {
              throw new IllegalStateException();
            }
            return new ConstantScoreScorer(this, 1f, scoreMode, disi);
          }
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
          return true;
        }
      };
    }

    @Override
    @SuppressWarnings("ReferenceEquality")
    public Query rewrite(IndexReader reader) throws IOException {
      Query rewrittenBacking = backing.rewrite(reader);
      return rewrittenBacking == backing
          ? this
          : new FrankensteinQuery(rewrittenBacking, segs, stale);
    }

    @Override
    public String toString(String field) {
      return FrankensteinQuery.class.getSimpleName() + "{" + backing + "}";
    }

    @Override
    public void visit(QueryVisitor visitor) {
      backing.visit(visitor);
    }

    @Override
    public boolean equals(Object obj) {
      return obj == this
          || (obj instanceof FrankensteinQuery
              && ((FrankensteinQuery) obj).backing.equals(backing));
    }

    @Override
    public int hashCode() {
      return classHash() ^ backing.hashCode();
    }
  }
}
