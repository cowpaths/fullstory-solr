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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.search.SolrCache.MetaEntry;
import org.apache.solr.search.OrdMapRegenerator.KeepAliveValue;
import org.apache.solr.util.IOFunction;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.apache.solr.search.OrdMapRegenerator.getRegenKeepAliveNanos;

/** Cache regenerator that builds OrdinalMap instances against the new searcher. */
public class KeepAliveRegenerator<M extends MetaEntry<Query, DocSet, M>>
    extends MetaCacheRegenerator<Query, DocSet, M> {

  private static final int DEFAULT_REGEN_KEEPALIVE_MINUTES = 2;
  private static final long DEFAULT_REGEN_KEEPALIVE_NANOS =
      TimeUnit.MINUTES.toNanos(DEFAULT_REGEN_KEEPALIVE_MINUTES);

  private final long regenKeepAliveNanos;
  private final boolean warmEagerly;

  public KeepAliveRegenerator() {
    this(DEFAULT_REGEN_KEEPALIVE_NANOS, false);
    // default ctor in case someone specifies this class via standard `"regen"=[className]` syntax
  }

  static boolean autowarmOn(CacheConfig config) {
    return new SolrCacheBase.AutoWarmCountRef(
        (String) config.toMap(new HashMap<>()).get("autowarmCount"))
        .isAutoWarmingOn();
  }

  public KeepAliveRegenerator(SolrConfig solrConfig, CacheConfig cacheConfig) {
    super(autowarmOn(cacheConfig), getWrapFunction());
    this.regenKeepAliveNanos = getRegenKeepAliveNanos(solrConfig, cacheConfig);

    // default to false
    this.warmEagerly = "true".equals(cacheConfig.toMap(null).get("warmEagerly"));
  }

  private KeepAliveRegenerator(long regenKeepAliveNanos, boolean warmEagerly) {
    super(true, getWrapFunction());
    this.regenKeepAliveNanos = regenKeepAliveNanos;
    this.warmEagerly = warmEagerly;
  }

  @SuppressWarnings({"unchecked", "UnnecessaryLambda"})
  private static <M> BiFunction<SegmentMap, DocSet, M> getWrapFunction() {
    return (segMap, v) -> (M) new KeepAliveSegAwareValue(segMap, v);
  }

  private static boolean isCrossDoc(Query q) {
    boolean[] ret = new boolean[1];
    q.visit(new QueryVisitor() {
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
      public void consumeTermsMatching(Query query, String field, Supplier<ByteRunAutomaton> automaton) {
        isCrossDoc(query);
      }

      @Override
      public boolean acceptField(String field) {
        return !ret[0];
      }
    });
    return ret[0];
  }

  private static class KeepAliveSegAwareValue implements MetaEntry<Query, DocSet, KeepAliveValue<Query, DocSet>> {

    private static final long BASE_RAM_BYTES_USED =
        RamUsageEstimator.shallowSizeOfInstance(KeepAliveSegAwareValue.class) +
            RamUsageEstimator.shallowSizeOfInstance(AtomicReference.class) +
            RamUsageEstimator.shallowSizeOfInstance(AbstractMap.SimpleImmutableEntry.class) +
            RamUsageEstimator.shallowSizeOfInstance(CompletableFuture.class);

    private final AtomicReference<AbstractMap.SimpleImmutableEntry<SegmentMap, CompletableFuture<DocSet>>> ref;
    private final long ramBytesUsed;
    private long accessTimestampNanos;

    public KeepAliveSegAwareValue(SegmentMap segMap, DocSet val) {
      CompletableFuture<DocSet> f = new CompletableFuture<>();
      f.complete(val);
      ref = new AtomicReference<>(new AbstractMap.SimpleImmutableEntry<>(segMap, f));
      ramBytesUsed = BASE_RAM_BYTES_USED + val.ramBytesUsed();
      this.accessTimestampNanos = System.nanoTime();
    }

    public KeepAliveSegAwareValue(SegmentMap segMap, DocSet val, long accessTimestampNanos) {
      CompletableFuture<DocSet> f = new CompletableFuture<>();
      f.complete(val);
      ref = new AtomicReference<>(new AbstractMap.SimpleImmutableEntry<>(segMap, f));
      ramBytesUsed = BASE_RAM_BYTES_USED + val.ramBytesUsed();
      this.accessTimestampNanos = accessTimestampNanos;
    }

    public KeepAliveSegAwareValue(KeepAliveSegAwareValue template) {
      AbstractMap.SimpleImmutableEntry<SegmentMap, CompletableFuture<DocSet>> entry = template.ref.get();
      ref = new AtomicReference<>(template.ref.get());
      ramBytesUsed = BASE_RAM_BYTES_USED + entry.getValue().getNow(null).ramBytesUsed();
      this.accessTimestampNanos = template.accessTimestampNanos;
    }

    @Override
    public KeepAliveValue<Query, DocSet> metaClone(DocSet val) {
      throw new UnsupportedOperationException();
    }

    @Override
    public DocSet get(SegmentMap segMap, Query key, IOFunction<? super Query, ? extends DocSet> mappingFunction) throws IOException {
      AbstractMap.SimpleImmutableEntry<SegmentMap, CompletableFuture<DocSet>> ref = this.ref.get();
      accessTimestampNanos = System.nanoTime();
      try {
        if (ref.getKey() == segMap) {
          return ref.getValue().get();
        }
        CompletableFuture<DocSet> f = new CompletableFuture<>();
        AbstractMap.SimpleImmutableEntry<SegmentMap, CompletableFuture<DocSet>> newRef = new AbstractMap.SimpleImmutableEntry<>(segMap, f);
        AbstractMap.SimpleImmutableEntry<SegmentMap, CompletableFuture<DocSet>> actualNewRef = this.ref.compareAndExchange(ref, newRef);
        if (actualNewRef == ref) {
          // we compute
          Query frankenstein = new SegAwareDocSetCache.FrankensteinQuery(key, ref.getKey().segments, ref.getValue().get());
          DocSet computed = mappingFunction.apply(frankenstein);
          f.complete(computed);
          return computed;
        } else if (actualNewRef.getKey() == segMap) {
          return actualNewRef.getValue().get();
        } else {
          return mappingFunction.apply(key);
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(ex);
      } catch (ExecutionException ex) {
        Throwable cause = ex.getCause();
        if (cause instanceof IOException) {
          throw (IOException) cause;
        } else {
          throw new RuntimeException(ex);
        }
      }
    }

    @Override
    public long ramBytesUsed() {
      return ramBytesUsed;
    }
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
    if (System.nanoTime() - extantTimestamp > regenKeepAliveNanos) {
      // it has been long enough since this was last accessed that we don't want to carry it forward
      return true;
    }

    final Query query = (Query) oldKey;

    @SuppressWarnings("unchecked")
    SolrCache<Query, KeepAliveSegAwareValue> c = (SolrCache<Query, KeepAliveSegAwareValue>) newCache;

    if (isCrossDoc(query)) {
      if (warmEagerly) {
        c.computeIfAbsent(query, (q) -> {
          SegmentMap segMap = newSearcher.getSegmentMap();
          DocSet docSet = newSearcher.getDocSetNC(query, null);
          return new KeepAliveSegAwareValue(segMap, docSet, extantTimestamp);
        });
      }
      return true;
    }

    c.computeIfAbsent(query, (q) -> new KeepAliveSegAwareValue(metaEntry));
    return true;
  }
}
