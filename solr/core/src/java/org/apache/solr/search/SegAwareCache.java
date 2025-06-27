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

import com.codahale.metrics.Gauge;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.Accountable;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.util.IOFunction;

/**
 * A specialization of {@link CaffeineCache} that is capable of partially recreating cache entries
 * in a segment-aware way.
 */
public class SegAwareCache<K, V> extends SolrCacheBase implements SolrCache<K, V>, Accountable {
  private static class InternalMappingInput<K, V> {
    private final IOFunction<? super K, ? extends V> externalMappingFunction;
    private final IndexReader.CacheKey topLevelReaderKey;
    private final K key;
    private final HashMap<IndexReader.CacheKey, SegmentMap> oldSegMaps;
    private final LongAdder partialHits;
    private final DoubleAdder partialHitsRatio;

    private InternalMappingInput(
        IOFunction<? super K, ? extends V> externalMappingFunction,
        IndexReader.CacheKey topLevelReaderKey,
        K key,
        HashMap<IndexReader.CacheKey, SegmentMap> oldSegMaps,
        LongAdder partialHits,
        DoubleAdder partialHitsRatio) {
      this.externalMappingFunction = externalMappingFunction;
      this.topLevelReaderKey = topLevelReaderKey;
      this.key = key;
      this.oldSegMaps = oldSegMaps;
      this.partialHits = partialHits;
      this.partialHitsRatio = partialHitsRatio;
    }
  }

  private static class InternalMappingFunction<K, V>
      implements IOFunction<InternalMappingInput<K, V>, FutureTask<V>> {
    private IndexReader.CacheKey topLevelReaderKey; // not final, we need to update this
    private FutureTask<V> cached;
    private final ReconstructorShim<K, V> reconstructorShim;

    private InternalMappingFunction(
        IndexReader.CacheKey topLevelReaderKey,
        V externalValue,
        ReconstructorShim<K, V> reconstructorShim) {
      this.topLevelReaderKey = topLevelReaderKey;
      this.cached = new FutureTask<>(() -> externalValue);
      this.cached.run(); // run inline; should return immediately
      this.reconstructorShim = reconstructorShim;
    }

    @Override
    public FutureTask<V> apply(InternalMappingInput<K, V> internalMappingInput) throws IOException {
      synchronized (this) {
        if (!topLevelReaderKey.equals(internalMappingInput.topLevelReaderKey)) {
          final SegmentMap oldSegMap = internalMappingInput.oldSegMaps.get(topLevelReaderKey);
          if (oldSegMap != null) {
            internalMappingInput.partialHits.increment();
            internalMappingInput.partialHitsRatio.add(
                oldSegMap.getOverlap(internalMappingInput.topLevelReaderKey));
          }
          final V stale;
          try {
            stale = cached.get();
          } catch (InterruptedException | ExecutionException ex) {
            throw new RuntimeException(ex); // should be impossible
          }
          final K possiblyShimmed =
              reconstructorShim.getKeyOrShimKey(internalMappingInput.key, oldSegMap, stale);
          topLevelReaderKey = internalMappingInput.topLevelReaderKey;
          cached =
              new FutureTask<>(
                  () -> internalMappingInput.externalMappingFunction.apply(possiblyShimmed));
          cached.run(); // run inline
        }
        return cached;
      }
    }
  }

  private final ReconstructorShim<K, V> reconstructorShim;
  private final CaffeineCache<K, InternalMappingFunction<K, V>> backing;
  private static final int DEFAULT_INDEX_GENERATION_LIMIT = 10;
  private static final double DEFAULT_OVERLAP_THRESHOLD = 0.5;
  private static final String INDEX_GENERATION_LIMIT_PROPNAME = "indexGenerationLimit";
  private static final String OVERLAP_THRESHOLD_PROPNAME = "overlapThreshold";
  private int indexGenerationLimit = DEFAULT_INDEX_GENERATION_LIMIT;
  private double overlapThreshold = DEFAULT_OVERLAP_THRESHOLD;
  private LinkedHashMap<IndexReader.CacheKey, SegmentMap> oldSegMaps;
  private SegmentMap segMap;

  protected SegAwareCache(ReconstructorShim<K, V> reconstructorShim) {
    this.backing = new CaffeineCache<>();
    this.reconstructorShim = reconstructorShim;
  }

  @SuppressWarnings("unchecked")
  private K unwrapShimKey(K key) {
    if (key instanceof ShimKey) {
      return ((ShimKey<K>) key).getUnshimmedKey();
    } else {
      return key;
    }
  }

  private V unwrap(InternalMappingFunction<K, V> internalMappingFunction) {
    try {
      if (internalMappingFunction == null
          || !internalMappingFunction.topLevelReaderKey.equals(segMap.key)) {
        return null;
      } else {
        final FutureTask<V> cached = internalMappingFunction.cached;
        return cached.isDone() ? cached.get() : null;
      }
    } catch (ExecutionException | InterruptedException ex) {
      throw new RuntimeException(ex); // should never happen
    }
  }

  @Override
  public V put(K key, V value) {
    assert !(key instanceof ShimKey);
    return unwrap(
        backing.put(key, new InternalMappingFunction<>(segMap.key, value, reconstructorShim)));
  }

  @Override
  public V get(K key) {
    assert !(key instanceof ShimKey);
    return unwrap(backing.get(key));
  }

  @Override
  public V remove(K key) {
    assert !(key instanceof ShimKey);
    return unwrap(backing.remove(key));
  }

  private final LongAdder partialHits = new LongAdder();
  private final DoubleAdder partialHitsRatio = new DoubleAdder();
  private long priorPartialHits;
  private double priorPartialHitsRatio;

  @Override
  public V computeIfAbsent(K key, IOFunction<? super K, ? extends V> externalMappingFunction)
      throws IOException {
    final K unshimmed = unwrapShimKey(key);
    FutureTask<V> ret =
        backing
            .computeIfAbsent(
                unshimmed,
                (ignore) -> {
                  final V val =
                      externalMappingFunction.apply(
                          key); // always pass the original input key to externalMappingFunction
                  return new InternalMappingFunction<>(segMap.key, val, reconstructorShim);
                })
            .apply(
                new InternalMappingInput<>(
                    externalMappingFunction,
                    segMap.key,
                    unshimmed,
                    oldSegMaps,
                    partialHits,
                    partialHitsRatio));
    try {
      return ret.get();
    } catch (InterruptedException | ExecutionException ex) {
      throw new RuntimeException(ex);
    }
  }

  private static class PartialRegenerator<Kx, Vx> implements CacheRegenerator {
    private final int externalAutowarmCount;
    private final Map<IndexReader.CacheKey, SegmentMap> oldSegMaps;
    private final CacheRegenerator externalRegenerator;
    private final ReconstructorShim<Kx, Vx> reconstructorShim;
    private final SolrCache<Kx, Vx> newCacheExternal;
    private final SolrCache<Kx, Vx> oldCacheExternal;
    int count = 0;

    private PartialRegenerator(
        int externalAutowarmCount,
        Map<IndexReader.CacheKey, SegmentMap> oldSegMaps,
        CacheRegenerator externalRegenerator,
        ReconstructorShim<Kx, Vx> reconstructorShim,
        SolrCache<Kx, Vx> newCacheExternal,
        SolrCache<Kx, Vx> oldCacheExternal) {
      this.externalAutowarmCount = externalAutowarmCount;
      this.oldSegMaps = oldSegMaps;
      this.externalRegenerator = externalRegenerator;
      this.reconstructorShim = reconstructorShim;
      this.newCacheExternal = newCacheExternal;
      this.oldCacheExternal = oldCacheExternal;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> boolean regenerateItem(
        SolrIndexSearcher newSearcher,
        SolrCache<K, V> newCache,
        SolrCache<K, V> oldCache,
        K oldKey,
        V oldVal)
        throws IOException {
      final InternalMappingFunction<Kx, Vx> val = (InternalMappingFunction<Kx, Vx>) oldVal;
      if (count++ < externalAutowarmCount) {
        assert val.reconstructorShim == reconstructorShim;
        final IndexReader.CacheKey oldTopLevelReaderKey;
        final Vx oldValExternal;
        try {
          synchronized (val) {
            // we need to ensure that the reader key corresponds to the cached value upon which
            // reconstruction is based
            oldTopLevelReaderKey = val.topLevelReaderKey;
            oldValExternal = val.cached.get();
          }
        } catch (InterruptedException | ExecutionException ex) {
          throw new RuntimeException(ex);
        }
        // Kx should always be compatible with K
        final Kx keyOrShimKey =
            reconstructorShim.getKeyOrShimKey(
                (Kx) oldKey, oldSegMaps.get(oldTopLevelReaderKey), oldValExternal);
        externalRegenerator.regenerateItem(
            newSearcher, newCacheExternal, oldCacheExternal, keyOrShimKey, oldValExternal);
      } else if (oldSegMaps.isEmpty()) {
        return false;
      } else if (oldSegMaps.containsKey(
          val.topLevelReaderKey)) { // otherwise we have nothing to reconstruct from
        newCache.put(oldKey, oldVal);
      }
      return true;
    }
  }

  public abstract static class ReconstructorShim<K, V> {
    private K getKeyOrShimKey(K key, SegmentMap staleSegs, V staleVal) {
      if (staleSegs == null) {
        return key;
      } else {
        return getShimKey(key, staleSegs, staleVal);
      }
    }

    public abstract K getShimKey(K key, SegmentMap staleSegs, V staleVal);
  }

  public interface ShimKey<K> {
    K getUnshimmedKey();
  }

  @Override
  public void initialSearcher(SolrIndexSearcher initialSearcher) {
    this.segMap = initialSearcher.getSegmentMap();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void warm(SolrIndexSearcher searcher, SolrCache<K, V> old) {
    segMap = searcher.getSegmentMap();
    SegAwareCache<K, V> other = (SegAwareCache<K, V>) old;
    IndexReader.CacheKey purge = null;
    if (other.oldSegMaps == null) {
      oldSegMaps = new LinkedHashMap<>();
    } else if (!(oldSegMaps = other.oldSegMaps).isEmpty()) {
      Iterator<Map.Entry<IndexReader.CacheKey, SegmentMap>> iter = oldSegMaps.entrySet().iterator();
      Map.Entry<IndexReader.CacheKey, SegmentMap> e = iter.next();
      if (oldSegMaps.size() >= indexGenerationLimit) {
        // we've kept it this long already; defer actually purging until _after_ warming
        purge = e.getKey();
      }
      for (; ; ) {
        if (e.getValue().registerOverlap(segMap) < overlapThreshold) {
          iter.remove();
        }
        if (!iter.hasNext()) {
          break;
        }
        e = iter.next();
      }
    }
    final SegmentMap oldSegMap = other.segMap;
    if (oldSegMap.registerOverlap(segMap) >= overlapThreshold) {
      this.oldSegMaps.put(oldSegMap.key, oldSegMap);
    }
    assert !oldSegMaps.containsKey(segMap.key);

    final int externalAutowarmCount = autowarm.getWarmCount(old.size());
    backing.regenerator =
        new PartialRegenerator<>(
            externalAutowarmCount, oldSegMaps, regenerator, reconstructorShim, this, old);
    backing.warm(searcher, other.backing);
    partialHits.reset();
    partialHitsRatio.reset();
    priorPartialHits = other.priorPartialHits + other.partialHits.sum();
    priorPartialHitsRatio = other.priorPartialHitsRatio + other.partialHitsRatio.sum();
    if (purge != null) {
      oldSegMaps.remove(purge);
    }
  }

  @Override
  public long ramBytesUsed() {
    return backing.ramBytesUsed();
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return backing.getChildResources();
  }

  @Override
  public String getName() {
    return backing.getName();
  }

  @Override
  public String getDescription() {
    return backing.getDescription();
  }

  private MetricsMap cacheMap;
  private SolrMetricsContext solrMetricsContext;

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    final MetricsMap[] wrappedMap = new MetricsMap[1];
    final SolrMetricsContext tmp =
        new SolrMetricsContext(null, null, null) {
          @Override
          public SolrMetricsContext getChildContext(Object child) {
            return new SolrMetricsContext(null, null, null) {
              @Override
              public void unregister() {
                // no-op
              }

              @Override
              public void gauge(
                  Gauge<?> gauge, boolean force, String metricName, String... metricPath) {
                wrappedMap[0] = (MetricsMap) gauge;
              }
            };
          }
        };
    backing.initializeMetrics(tmp, null);
    solrMetricsContext = parentContext.getChildContext(this);
    final MetricsMap backingMetrics = wrappedMap[0];
    cacheMap =
        new MetricsMap(
            (map) -> {
              backingMetrics.writeMap(map);
              final long partialHitCount = partialHits.sum();
              final double currentPartialHitRatio = partialHitsRatio.sum();
              final long cumPartialHitCount = priorPartialHits + partialHitCount;
              final double cumCurrentPartialHitsRatio =
                  priorPartialHitsRatio + currentPartialHitRatio;
              map.put("partialHits", partialHitCount);
              map.put("partialHitsRatio", currentPartialHitRatio);
              map.put(
                  "partialRatioPerHit",
                  partialHitCount == 0 ? 1.0 : (currentPartialHitRatio / partialHitCount));
              map.put("cumulative_partialHits", cumPartialHitCount);
              map.put("cumulative_partialHitsRatio", cumCurrentPartialHitsRatio);
              map.put(
                  "cumulative_partialRatioPerHit",
                  cumPartialHitCount == 0
                      ? 1.0
                      : (cumCurrentPartialHitsRatio / cumPartialHitCount));
            });
    solrMetricsContext.gauge(cacheMap, true, scope, getCategory().toString());
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }

  @Override
  public Object init(Map<String, String> args, Object persistence, CacheRegenerator regenerator) {
    super.init(args, regenerator);
    final String overlapThresholdSpec = args.get(OVERLAP_THRESHOLD_PROPNAME);
    if (overlapThresholdSpec != null) {
      try {
        this.overlapThreshold = Double.parseDouble(overlapThresholdSpec);
      } catch (NumberFormatException ex) {
        this.overlapThreshold = Double.NEGATIVE_INFINITY;
      }
      if (this.overlapThreshold < 0 || this.overlapThreshold > 1.0) {
        throw new IllegalArgumentException(
            OVERLAP_THRESHOLD_PROPNAME
                + " must be a value between 0 and 1, inclusive; found: "
                + overlapThresholdSpec);
      }
    }
    final String indexGenerationLimitSpec = args.get(INDEX_GENERATION_LIMIT_PROPNAME);
    if (indexGenerationLimitSpec != null) {
      try {
        this.indexGenerationLimit = Integer.parseInt(indexGenerationLimitSpec);
      } catch (NumberFormatException ex) {
        this.indexGenerationLimit = -1;
      }
      if (this.indexGenerationLimit < 0) {
        throw new IllegalArgumentException(
            INDEX_GENERATION_LIMIT_PROPNAME
                + " must be a non-negative integer; found: "
                + indexGenerationLimitSpec);
      }
    }
    Map<String, String> backingArgs = new HashMap<>(args);
    backingArgs.put("autowarmCount", "100%"); // we control via the regenerator
    return backing.init(backingArgs, persistence, regenerator);
  }

  @Override
  public int size() {
    return backing.size();
  }

  @Override
  public void clear() {
    backing.clear();
  }

  @Override
  public void close() throws IOException {
    SolrCache.super.close();
    backing.close();
  }

  @Override
  public int getMaxSize() {
    return backing.getMaxSize();
  }

  @Override
  public void setMaxSize(int maxSize) {
    backing.setMaxSize(maxSize);
  }

  @Override
  public int getMaxRamMB() {
    return backing.getMaxRamMB();
  }

  @Override
  public void setMaxRamMB(int maxRamMB) {
    backing.setMaxRamMB(maxRamMB);
  }

  @Override
  public boolean isRecursionSupported() {
    return backing.isRecursionSupported();
  }
}
