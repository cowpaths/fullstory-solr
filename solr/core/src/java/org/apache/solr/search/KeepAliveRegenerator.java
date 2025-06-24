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
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.index.SlowCompositeReaderWrapper;
import org.apache.solr.search.SolrCache.MetaEntry;
import org.apache.solr.search.OrdMapRegenerator.KeepAliveValue;
import org.apache.solr.util.IOFunction;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.LongFunction;
import java.util.function.Supplier;

import static org.apache.solr.search.OrdMapRegenerator.configureRegenerator0;

/** Cache regenerator that builds OrdinalMap instances against the new searcher. */
public class KeepAliveRegenerator<M extends MetaEntry<Query, DocSet, M>>
    extends MetaCacheRegenerator<Query, DocSet, M> {

  private static final int DEFAULT_REGEN_KEEPALIVE_MINUTES = 2;
  private static final long DEFAULT_REGEN_KEEPALIVE_NANOS =
      TimeUnit.MINUTES.toNanos(DEFAULT_REGEN_KEEPALIVE_MINUTES);
  private static final KeepAliveRegenerator<KeepAliveValue<Query, DocSet>> DEFAULT_INSTANCE =
      new KeepAliveRegenerator<>(DEFAULT_REGEN_KEEPALIVE_NANOS);

  private final long regenKeepAliveNanos;

  public KeepAliveRegenerator() {
    this(DEFAULT_REGEN_KEEPALIVE_NANOS);
    // default ctor in case someone specifies this class via standard `"regen"=[className]` syntax
  }

  private KeepAliveRegenerator(long regenKeepAliveNanos) {
    super(getWrapFunction());
    this.regenKeepAliveNanos = regenKeepAliveNanos;
  }

  @SuppressWarnings({"unchecked", "UnnecessaryLambda"})
  private static <M> BiFunction<SegmentMap, DocSet, M> getWrapFunction() {
    return (segMap, v) -> (M) new KeepAliveSegAwareValue(segMap, v);
  }

  public static void configureRegenerator(SolrConfig solrConfig, CacheConfig config) {
    configureRegenerator0(solrConfig, config, DOCSET_REGEN_SUPPLIER);
  }

  private static final LongFunction<KeepAliveRegenerator<KeepAliveValue<Query, DocSet>>> DOCSET_REGEN_SUPPLIER = (regenKeepAliveNanos) -> {
    if (regenKeepAliveNanos == DEFAULT_REGEN_KEEPALIVE_NANOS) {
      return DEFAULT_INSTANCE;
    } else {
      return new KeepAliveRegenerator<>(regenKeepAliveNanos);
    }
  };

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

    public KeepAliveSegAwareValue(SegmentMap segMap, DocSet val) {
      CompletableFuture<DocSet> f = new CompletableFuture<>();
      f.complete(val);
      ref = new AtomicReference<>(new AbstractMap.SimpleImmutableEntry<>(segMap, f));
      ramBytesUsed = BASE_RAM_BYTES_USED + val.ramBytesUsed();
    }

    public KeepAliveSegAwareValue(AbstractMap.SimpleImmutableEntry<SegmentMap, CompletableFuture<DocSet>> entry, long accessTimestampNanos) {
      ref = new AtomicReference<>(entry);
      ramBytesUsed = BASE_RAM_BYTES_USED + entry.getValue().getNow(null).ramBytesUsed();
    }

    @Override
    public KeepAliveValue<Query, DocSet> metaClone(DocSet val) {
      throw new UnsupportedOperationException();
    }

    @Override
    public DocSet get(SegmentMap segMap, Query key, IOFunction<? super Query, ? extends DocSet> mappingFunction) throws IOException {
      AbstractMap.SimpleImmutableEntry<SegmentMap, CompletableFuture<DocSet>> ref = this.ref.get();
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

    @SuppressWarnings("unchecked")
    KeepAliveValue<String, OrdinalMap> ordinalMapValue = (KeepAliveValue<String, OrdinalMap>) oldVal;
    final long extantTimestamp = ordinalMapValue.extantTimestamp();
    if (System.nanoTime() - extantTimestamp > regenKeepAliveNanos) {
      // it has been long enough since this was last accessed that we don't want to carry it forward
      return true;
    }

    final Query query = (Query) oldKey;
    final IndexReader.CacheKey readerKey = cacheHelper.getKey();
    final IOFunction<? super Query, ? extends KeepAliveValue<Query, OrdinalMap>> producer;
    //TODO: adjust this for Query->DocSet (not OrdinalMap)
    DocIdSetIterator[] dvs = SlowCompositeReaderWrapper.getLeafDocValues(leaves, null);
    if (dvs == null) {
      // All empty for this field, but should still warm others
      return true;
    } else if (dvs instanceof SortedDocValues[]) {
      producer =
          (notUsed) ->
              new KeepAliveValue<>(
                  OrdinalMap.build(readerKey, (SortedDocValues[]) dvs, PackedInts.DEFAULT),
                  extantTimestamp);
    } else if (dvs instanceof SortedSetDocValues[]) {
      producer =
          (notUsed) ->
              new KeepAliveValue<>(
                  OrdinalMap.build(readerKey, (SortedSetDocValues[]) dvs, PackedInts.DEFAULT),
                  extantTimestamp);
    } else {
      throw new IllegalStateException();
    }

    @SuppressWarnings("unchecked")
    SolrCache<Query, KeepAliveValue<Query, OrdinalMap>> c = (SolrCache<Query, KeepAliveValue<Query, OrdinalMap>>) newCache;
    c.computeIfAbsent(query, producer);
    return true;
  }
}
