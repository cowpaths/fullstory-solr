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

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.common.SolrException;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.util.IOFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RootCacheSolr<K, V> extends SolrCacheBase
    implements SolrCache<K, V>, RemovalListener<K, V>, Accountable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(RootCacheSolr.class);
  private RootCache<K, V> rootCache = null;

  private String tierScope;
  private RootCacheSolr<K, V> parent;
  private int maxSize;
  private long maxRamBytes;
  private int initialSize;
  private int maxIdleTimeSec;

  private long initialRamBytes = 0;

  // SolrCache
  @Override
  public Object init(Map args, Object persistence, CacheRegenerator regenerator) {
    tierScope = null; // TODO: e.g., coreName
    parent = null; // parent cache; TODO: lookup by name
    String str = (String) args.get(SIZE_PARAM);
    maxSize = (str == null) ? 1024 : Integer.parseInt(str);
    str = (String) args.get(INITIAL_SIZE_PARAM);
    initialSize = Math.min((str == null) ? 1024 : Integer.parseInt(str), maxSize);
    str = (String) args.get(MAX_IDLE_TIME_PARAM);
    if (str == null) {
      maxIdleTimeSec = -1;
    } else {
      maxIdleTimeSec = Integer.parseInt(str);
    }
    str = (String) args.get(MAX_RAM_MB_PARAM);
    int maxRamMB = str == null ? -1 : Double.valueOf(str).intValue();
    maxRamBytes = maxRamMB < 0 ? Long.MAX_VALUE : maxRamMB * 1024L * 1024L;
    description = generateDescription(maxSize, initialSize);
    initialRamBytes = RamUsageEstimator.sizeOfObject(description);
    rootCache =
        new RootCache<>(
            maxSize,
            maxRamBytes,
            initialSize,
            maxIdleTimeSec,
            parent == null ? null : parent.rootCache,
            tierScope,
            this);
    return persistence;
  }

  /** Returns the description of this cache. */
  private String generateDescription(int limit, int initialSize) {
    return String.format(
        Locale.ROOT,
        "Root Cache Solr(maxSize=%d, initialSize=%d%s)",
        limit,
        initialSize,
        isAutowarmingOn() ? (", " + getAutowarmDescription()) : "");
  }

  @Override
  public int size() {
    return (int) rootCache.size();
  }

  @Override
  public V put(K key, V value) {
    return rootCache.put(key, value);
  }

  @Override
  public V get(K key) {
    return rootCache.get(key);
  }

  @Override
  public V remove(K key) {
    return rootCache.remove(key);
  }

  @Override
  public V computeIfAbsent(K key, IOFunction<? super K, ? extends V> mappingFunction)
      throws IOException {
    return rootCache.computeIfAbsent(key, mappingFunction);
  }

  @Override
  public void clear() {
    rootCache.clear();
  }

  @Override
  public void warm(SolrIndexSearcher searcher, SolrCache<K, V> old) {
    if (regenerator == null) {
      return;
    }

    long warmingStartTime = System.nanoTime();
    RootCacheSolr<K, V> other = (RootCacheSolr<K, V>) old;

    // warm entries
    if (isAutowarmingOn()) {
      int size = autowarm.getWarmCount(other.size());
      Map.Entry<K, Exception> ex =
          other.rootCache.forEachTopEntry(
              size,
              (entry) -> {
                return regenerator.regenerateItem(
                    searcher, this, old, entry.getKey(), entry.getValue());
              });
      if (ex != null) {
        SolrException.log(log, "Error during auto-warming of key:" + ex.getKey(), ex.getValue());
      }
    }

    rootCache.resetStats();
    CacheStats oldStats = other.rootCache.stats();
    priorStats = oldStats.plus(other.priorStats);
    priorHits = oldStats.hitCount() + other.rootCache.asyncHits() + other.priorHits;
    priorInserts = other.rootCache.inserts() + other.priorInserts;
    priorLookups = oldStats.requestCount() + other.rootCache.asyncLookups() + other.priorLookups;
    warmupTime =
        TimeUnit.MILLISECONDS.convert(System.nanoTime() - warmingStartTime, TimeUnit.NANOSECONDS);
  }

  @Override
  public void close() throws IOException {
    rootCache.close();
    SolrCache.super.close();
  }

  @Override
  public int getMaxSize() {
    return maxSize;
  }

  @Override
  public void setMaxSize(int maxSize) {
    if (this.maxSize == maxSize) {
      return;
    }
    this.maxSize = maxSize;
    int adjustInitialSize = rootCache.setMaxSize(maxSize);
    if (adjustInitialSize != -1) {
      initialSize = adjustInitialSize;
      description = generateDescription(this.maxSize, initialSize);
    }
  }

  @Override
  public int getMaxRamMB() {
    return maxRamBytes != Long.MAX_VALUE ? (int) (maxRamBytes / 1024L / 1024L) : -1;
  }

  @Override
  public void setMaxRamMB(int maxRamMB) {
    long newMaxRamBytes = maxRamMB < 0 ? Long.MAX_VALUE : maxRamMB * 1024L * 1024L;
    if (newMaxRamBytes != maxRamBytes) {
      maxRamBytes = newMaxRamBytes;
      if (rootCache.setMaxRamMB(newMaxRamBytes)) {
        description = generateDescription(this.maxSize, initialSize);
        initialRamBytes = RamUsageEstimator.sizeOfObject(description);
      }
    }
  }

  //////////////////////// SolrInfoBean methods //////////////////////

  private String description = "Root Cache Solr";
  private Set<String> metricNames = ConcurrentHashMap.newKeySet();
  private MetricsMap cacheMap;
  private SolrMetricsContext solrMetricsContext;

  @Override
  public String getName() {
    return RootCacheSolr.class.getName();
  }

  @Override
  public String getDescription() {
    return description;
  }

  // for unit tests only
  @VisibleForTesting
  MetricsMap getMetricsMap() {
    return cacheMap;
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }

  @Override
  public String toString() {
    return name() + (cacheMap != null ? cacheMap.getValue().toString() : "");
  }

  private CacheStats priorStats;
  private long priorLookups;
  private long priorHits;
  private long priorInserts;
  private long warmupTime;

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    solrMetricsContext = parentContext.getChildContext(this);
    cacheMap =
        new MetricsMap(
            map -> {
              if (rootCache != null) {
                CacheStats stats = rootCache.stats();
                long hitCount = stats.hitCount() + rootCache.asyncHits();
                long insertCount = rootCache.inserts();
                long lookupCount = stats.requestCount() + rootCache.asyncLookups();

                map.put(LOOKUPS_PARAM, lookupCount);
                map.put(HITS_PARAM, hitCount);
                map.put(HIT_RATIO_PARAM, hitRate(hitCount, lookupCount));
                map.put(INSERTS_PARAM, insertCount);
                map.put(EVICTIONS_PARAM, stats.evictionCount());
                map.put(SIZE_PARAM, rootCache.size());
                map.put("warmupTime", warmupTime);
                map.put(RAM_BYTES_USED_PARAM, ramBytesUsed());
                map.put(MAX_RAM_MB_PARAM, getMaxRamMB());

                CacheStats cumulativeStats = priorStats.plus(stats);
                long cumLookups = priorLookups + lookupCount;
                long cumHits = priorHits + hitCount;
                map.put("cumulative_lookups", cumLookups);
                map.put("cumulative_hits", cumHits);
                map.put("cumulative_hitratio", hitRate(cumHits, cumLookups));
                map.put("cumulative_inserts", priorInserts + insertCount);
                map.put("cumulative_evictions", cumulativeStats.evictionCount());
              }
            });
    solrMetricsContext.gauge(cacheMap, true, scope, getCategory().toString());
  }

  private static double hitRate(long hitCount, long lookupCount) {
    return lookupCount == 0 ? 1.0 : (double) hitCount / lookupCount;
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED
        + initialRamBytes
        + (rootCache == null ? 0 : rootCache.ramBytesUsed());
  }

  @Override
  public void onRemoval(K key, V value, RemovalCause cause) {
    // no-op default impl
  }
}
