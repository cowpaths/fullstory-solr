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

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.metrics.SolrMetricManager;
import org.junit.Test;

/** Test for {@link org.apache.solr.search.CaffeineCache}. */
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "")
public class TestTieredCache extends SolrTestCase {

  SolrMetricManager metricManager = new SolrMetricManager();
  String registry = TestUtil.randomSimpleString(random(), 2, 10);
  String scope = TestUtil.randomSimpleString(random(), 2, 10);

  private static int[] getRandomPerTier(Random r, int maxTiers, int maxPerTier) {
    int tiers = r.nextInt(maxTiers) + 1;
    int[] perTier = new int[tiers];
    perTier[0] = 1;
    for (int i = 1; i < tiers; i++) {
      perTier[i] = r.nextInt(maxPerTier) + 1;
    }
    Arrays.sort(perTier);
    return perTier;
  }

  private static final class CacheStruct {
    final RootCache<Integer, String> cache;
    final RootCache<Integer, String> parent;

    private CacheStruct(RootCache<Integer, String> cache, RootCache<Integer, String> parent) {
      this.cache = cache;
      this.parent = parent;
    }
  }

  private CacheStruct[][] getCaches(Random r, int[] perTier, int minSize, int maxSizeExclusive) {
    CacheStruct[][] caches = new CacheStruct[perTier.length + 1][];
    boolean realisticSizing = r.nextBoolean();
    int rootSize =
        realisticSizing ? maxSizeExclusive - 1 : r.nextInt(maxSizeExclusive - minSize) + minSize;
    int nonRootMaxSizeExclusive =
        realisticSizing ? Math.max(minSize + 1, maxSizeExclusive / 10) : maxSizeExclusive;
    RootCache<Integer, String> root = new RootCache<>(rootSize, null, null);
    CacheStruct[] cachesPreviousTier = new CacheStruct[] {new CacheStruct(root, null)};
    caches[0] = cachesPreviousTier;
    ArrayList<CacheStruct> leafNodes = new ArrayList<>();
    for (int i = 1; i < perTier.length; i++) {
      final int atTier = perTier[i];
      final boolean[] previousTierHasChildren = new boolean[cachesPreviousTier.length];
      CacheStruct[] cachesAtTier = new CacheStruct[atTier];
      caches[i] = cachesAtTier;
      boolean ensureBalanced = false;
      int j = 0;
      if (ensureBalanced) {
        do {
          // ensure that each parent has at least one child
          RootCache<Integer, String> parentCache = cachesPreviousTier[j].cache;
          previousTierHasChildren[j] = true;
          String tierScope = atTier <= Long.SIZE ? null : i + "-" + j;
          RootCache<Integer, String> cache =
              new RootCache<>(
                  r.nextInt(nonRootMaxSizeExclusive - minSize) + minSize, parentCache, tierScope);
          cachesAtTier[j] = new CacheStruct(cache, parentCache);
        } while (++j < cachesPreviousTier.length);
      }
      if (j < atTier) {
        do {
          // randomly assign a parent to each remaining child cache
          final int parentIdx = r.nextInt(cachesPreviousTier.length);
          RootCache<Integer, String> parentCache = cachesPreviousTier[parentIdx].cache;
          previousTierHasChildren[parentIdx] = true;
          String tierScope = atTier <= Long.SIZE ? null : i + "-" + j;
          RootCache<Integer, String> cache =
              new RootCache<>(
                  r.nextInt(nonRootMaxSizeExclusive - minSize) + minSize, parentCache, tierScope);
          cachesAtTier[j] = new CacheStruct(cache, parentCache);
        } while (++j < atTier);
      }
      for (j = 0; j < previousTierHasChildren.length; j++) {
        if (!previousTierHasChildren[j]) {
          leafNodes.add(cachesPreviousTier[j]);
        }
      }
      cachesPreviousTier = cachesAtTier;
    }
    leafNodes.addAll(List.of(caches[perTier.length - 1])); // all of the last tier are leaves
    caches[caches.length - 1] = leafNodes.toArray(new CacheStruct[0]);
    return caches;
  }

  @Test
  @ThreadLeakLingering(linger = 1000) // even proper threadpool shutdown can leak transient threads
  @Monster("because of fanout, this test may require up to `-Ptests.heapsize=1g`")
  public void test2() throws IOException, ExecutionException, InterruptedException {
    final int MAX_STD_DEV = 500;
    final int N_THREADS = 100;
    final int ct = 50000;
    final int MAX_TIERS = 5;
    final int MAX_PER_TIER = 100000;
    Random r = random();
    int[] perTier = getRandomPerTier(r, MAX_TIERS, r.nextBoolean() ? Long.SIZE : MAX_PER_TIER);
    // perTier = new int[] {1,100000};
    System.err.println("perTier: " + Arrays.toString(perTier));
    int minSize = r.nextInt(1000) + 1;
    int maxSizeExclusive = r.nextInt(10000 - minSize) + minSize + 1; // +1 for exclusive
    CacheStruct[][] caches = getCaches(r, perTier, minSize, maxSizeExclusive);
    CacheStruct[] leafCaches = caches[caches.length - 1];
    ExecutorService executor =
        new ExecutorUtil.MDCAwareThreadPoolExecutor(
            N_THREADS,
            N_THREADS,
            1,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new ThreadPoolExecutor.CallerRunsPolicy());
    try {
      @SuppressWarnings({"unchecked", "rawtypes"})
      Future<Integer>[] futures = new Future[N_THREADS];
      // pre-start all the threads
      for (int i = 0; i < N_THREADS; i++) {
        futures[i] =
            executor.submit(
                () -> {
                  Thread.sleep(50);
                  return 1;
                });
      }
      for (Future<Integer> f : futures) {
        // block before proceeding
        f.get();
      }
      long start = System.nanoTime();
      final int stdDev = r.nextInt(MAX_STD_DEV) + 1;
      System.err.println("stdDev: " + stdDev);
      // sometimes spread requests over entire domain; sometimes restrict to ensure that evictions
      // happen at both shared and local levels
      final int selectCacheLimit =
          r.nextBoolean()
              ? leafCaches.length
              : Math.min(leafCaches.length, Math.max(1, (maxSizeExclusive / minSize) - 2));
      AtomicBoolean shortcircuit = new AtomicBoolean(false);
      for (int i = 0; i < N_THREADS; i++) {
        final Random threadRandom = new Random(r.nextInt());
        futures[i] =
            executor.submit(
                () -> {
                  for (int j = 0; j < ct && !shortcircuit.get(); j++) {
                    int cacheIdx = j % selectCacheLimit;
                    RootCache<Integer, String> c = leafCaches[cacheIdx].cache;
                    int key = (int) (threadRandom.nextGaussian() * stdDev);
                    switch (threadRandom.nextInt(4)) {
                      case 0:
                        c.computeIfAbsent(key, (k) -> k + "-" + threadRandom.nextInt());
                        break;
                      case 1:
                        // System.err.println("X "+key);
                        c.put(key, key + "-" + threadRandom.nextInt());
                        break;
                      case 2:
                        c.get(key);
                        break;
                      case 3:
                        c.remove(key);
                        break;
                    }
                  }
                  shortcircuit.set(true);
                  return 0;
                });
      }
      for (Future<Integer> f : futures) {
        // allow any exceptions to be propagated
        f.get();
      }
      System.err.println("duration: " + Duration.ofNanos(System.nanoTime() - start));
      caches[0][0].cache.validate(
          "", new int[1], System.err); // pass in e.g. `System.err` for debug output
    } finally {
      ExecutorUtil.shutdownAndAwaitTermination(executor);
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static final Comparator<Map.Entry<? extends Comparable, ? extends Comparable>>
      entryComparator =
          (a, b) -> {
            int primary = a.getKey().compareTo(b.getKey());
            if (primary != 0) {
              return primary;
            } else {
              return a.getValue().compareTo(b.getValue());
            }
          };

  @Test
  @ThreadLeakLingering(linger = 1000) // even proper threadpool shutdown can leak transient threads
  public void test() throws IOException, ExecutionException, InterruptedException {
    // seed B856D5E822A2585C
    final int MAX_STD_DEV = 500;
    final int N_THREADS = 100;
    final int ct = 50000;
    final int MAX_TIERS = 5;
    final int MAX_LEAF_CACHES = 10;
    Random r = random();
    RootCache<Integer, String> root = new RootCache<>(20, null, null);
    RootCache<Integer, String> leaf1 = new RootCache<>(18, root, "one");
    RootCache<Integer, String> leaf2 = new RootCache<>(4, root, "two");
    @SuppressWarnings({"unchecked", "rawtypes"})
    RootCache<Integer, String>[] leafCaches = new RootCache[] {leaf1, leaf2};
    ExecutorService executor =
        new ExecutorUtil.MDCAwareThreadPoolExecutor(
            N_THREADS,
            N_THREADS,
            1,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new ThreadPoolExecutor.CallerRunsPolicy());
    try {
      @SuppressWarnings({"unchecked", "rawtypes"})
      Future<Integer>[] futures = new Future[N_THREADS];
      // pre-start all the threads
      for (int i = 0; i < N_THREADS; i++) {
        futures[i] =
            executor.submit(
                () -> {
                  Thread.sleep(50);
                  return 1;
                });
      }
      for (Future<Integer> f : futures) {
        // block before proceeding
        f.get();
      }
      final int stdDev = 10; // r.nextInt(MAX_STD_DEV) + 1;
      System.err.println("stdDev: " + stdDev);
      AtomicBoolean shortcircuit = new AtomicBoolean(false);
      for (int i = 0; i < N_THREADS; i++) {
        final Random threadRandom = new Random(r.nextInt());
        futures[i] =
            executor.submit(
                () -> {
                  for (int j = 0; j < ct && !shortcircuit.get(); j++) {
                    int cacheIdx = j % leafCaches.length;
                    RootCache<Integer, String> c = leafCaches[cacheIdx];
                    int key = (int) (threadRandom.nextGaussian() * stdDev);
                    c.computeIfAbsent(key, (k) -> k + "-" + threadRandom.nextInt());
                  }
                  shortcircuit.set(true);
                  return 0;
                });
      }
      for (Future<Integer> f : futures) {
        // allow any exceptions to be propagated
        f.get();
      }
      Set<Map.Entry<Integer, String>> rootKeySet = root.keySet();
      Set<Map.Entry<Integer, String>> merged = new HashSet<>(rootKeySet.size());
      merged.addAll(leaf1.keySet());
      merged.addAll(leaf2.keySet());
      print(root, leafCaches);
      List<Map.Entry<Integer, String>> sortedRootKeys =
          rootKeySet.stream().sorted(entryComparator).collect(Collectors.toList());
      List<Map.Entry<Integer, String>> sortedMergedKeys =
          merged.stream().sorted(entryComparator).collect(Collectors.toList());
      System.err.println("root:   " + sortedRootKeys);
      System.err.println("merged: " + sortedMergedKeys);
      assertTrue(sortedMergedKeys.containsAll(sortedRootKeys));
      assertEquals(sortedRootKeys, sortedMergedKeys);
    } finally {
      ExecutorUtil.shutdownAndAwaitTermination(executor);
    }
  }

  @Test
  @ThreadLeakLingering(linger = 1000) // even proper threadpool shutdown can leak transient threads
  @SuppressWarnings("rawtypes")
  public void test3() throws IOException, ExecutionException, InterruptedException {
    // seed B856D5E822A2585C
    final int MAX_STD_DEV = 500;
    final int N_THREADS = 100;
    final int ct = 50000;
    final int MAX_TIERS = 5;
    final int MAX_LEAF_CACHES = 10;
    Random r = random();
    RootCache<Integer, String> root = new RootCache<>(100, null, null);
    RootCache<Integer, String> node = new RootCache<>(100, root, null);
    RootCache<Integer, String> leaf = new RootCache<>(100, node, null);
    @SuppressWarnings("unchecked")
    RootCache<Integer, String>[] leafCaches = new RootCache[] {leaf};
    ExecutorService executor =
        new ExecutorUtil.MDCAwareThreadPoolExecutor(
            N_THREADS,
            N_THREADS,
            1,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new ThreadPoolExecutor.CallerRunsPolicy());
    try {
      @SuppressWarnings("unchecked")
      Future<Integer>[] futures = new Future[N_THREADS];
      // pre-start all the threads
      for (int i = 0; i < N_THREADS; i++) {
        futures[i] =
            executor.submit(
                () -> {
                  Thread.sleep(50);
                  return 1;
                });
      }
      for (Future<Integer> f : futures) {
        // block before proceeding
        f.get();
      }
      final int stdDev = r.nextInt(MAX_STD_DEV) + 1;
      System.err.println("stdDev: " + stdDev);
      AtomicBoolean shortcircuit = new AtomicBoolean(false);
      for (int i = 0; i < N_THREADS; i++) {
        final Random threadRandom = new Random(r.nextInt());
        futures[i] =
            executor.submit(
                () -> {
                  for (int j = 0; j < ct && !shortcircuit.get(); j++) {
                    int cacheIdx = j % leafCaches.length;
                    RootCache<Integer, String> c = leafCaches[cacheIdx];
                    int key = (int) (threadRandom.nextGaussian() * stdDev);
                    c.computeIfAbsent(key, (k) -> k + "-" + threadRandom.nextInt());
                  }
                  shortcircuit.set(true);
                  return 0;
                });
      }
      for (Future<Integer> f : futures) {
        // allow any exceptions to be propagated
        f.get();
      }
      Set<Integer> rootKeySet =
          root.keySet().stream().map(Map.Entry::getKey).collect(Collectors.toSet());
      Set<Integer> merged = new HashSet<>(rootKeySet.size());
      merged.addAll(leaf.keySet().stream().map(Map.Entry::getKey).collect(Collectors.toSet()));
      print(root, new RootCache[] {node, leafCaches[0]});
      List<Integer> sortedRootKeys = rootKeySet.stream().sorted().collect(Collectors.toList());
      List<Integer> sortedMergedKeys = merged.stream().sorted().collect(Collectors.toList());
      System.err.println("root:   " + sortedRootKeys);
      System.err.println("merged: " + sortedMergedKeys);
      assertTrue(sortedMergedKeys.containsAll(sortedRootKeys));
      assertEquals(sortedRootKeys, sortedMergedKeys);
    } finally {
      ExecutorUtil.shutdownAndAwaitTermination(executor);
    }
  }

  private static void print(RootCache<?, ?> root, RootCache<?, ?>[] leaves) {
    System.err.println("deferred removals remaining: " + root.deferredRemaining());
    Set<?> rootKeys = root.keySet().stream().map(Map.Entry::getKey).collect(Collectors.toSet());
    assertTrue(root.verifyRemovalCounts());
    System.err.println(
        "root size "
            + rootKeys.size()
            + " "
            + rootKeys.stream().sorted().collect(Collectors.toList()));
    for (int i = 0; i < leaves.length; i++) {
      Set<?> leafKeys =
          leaves[i].keySet().stream().map(Map.Entry::getKey).collect(Collectors.toSet());
      assertTrue(leaves[i].verifyRemovalCounts());
      System.err.println(
          "leaf "
              + i
              + ": "
              + leafKeys.size()
              + " "
              + leafKeys.stream().sorted().collect(Collectors.toList()));
    }
  }

  @SuppressWarnings("unchecked")
  private Map<String, ?> getMetricsMap(String scopeSuffix) {
    return ((SolrMetricManager.GaugeWrapper<Map<String, ?>>)
            metricManager.registry(registry).getMetrics().get("CACHE." + scope + scopeSuffix))
        .getValue();
  }
}
