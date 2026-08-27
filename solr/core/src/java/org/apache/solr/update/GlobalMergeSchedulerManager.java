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
package org.apache.solr.update;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.GlobalConcurrentMergeScheduler;
import org.apache.lucene.index.GlobalConcurrentMergeScheduler.MergeConcurrencyGate;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.ZkStateReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JVM singleton that owns a shared {@link MergeConcurrencyGate} and a ZK clusterprops watcher for
 * live {@code ext.globalMergeScheduler} overrides.
 *
 * <p>Each Solr core that configures {@link GlobalConcurrentMergeScheduler} gets its own CMS
 * instance via {@link #createScheduler()}, all sharing this manager's gate. The watcher only
 * updates the gate ({@code maxThreadCount} → node-wide max <em>running</em> merges); per-core
 * {@code maxThreadCount}/{@code maxMergeCount} stay in solrconfig.
 *
 * <p>Example:
 *
 * <pre>
 * {
 *   "ext.globalMergeScheduler": {
 *     "maxThreadCount": 2,
 *     "nodes": ["solr-c92-8:8986_solr"]
 *   }
 * }
 * </pre>
 */
final class GlobalMergeSchedulerManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Object INSTANCE_LOCK = new Object();
  private static GlobalMergeSchedulerManager INSTANCE;

  private final Gate gate;
  private final Watcher watcher;

  static GlobalMergeSchedulerManager getInstance(ZkController zkController) {
    synchronized (INSTANCE_LOCK) {
      if (INSTANCE == null) {
        INSTANCE = new GlobalMergeSchedulerManager(zkController);
      }
      return INSTANCE;
    }
  }

  private GlobalMergeSchedulerManager(ZkController zkController) {
    this.gate = new Gate();
    this.watcher = new Watcher(zkController.getNodeName(), gate);
    ZkStateReader zkStateReader = zkController.getZkStateReader();
    if (zkStateReader != null) {
      watcher.startListening(zkStateReader);
    }
  }

  /** Creates a new per-core scheduler bound to this manager's shared gate. */
  GlobalConcurrentMergeScheduler createScheduler() {
    return new GlobalConcurrentMergeScheduler(gate);
  }

  Gate getGate() {
    return gate;
  }

  /** Clears static state for unit tests. */
  static void resetForTesting() {
    synchronized (INSTANCE_LOCK) {
      if (INSTANCE != null) {
        INSTANCE.gate.shutdown();
      }
      INSTANCE = null;
    }
  }

  // package-visible for tests
  Watcher getWatcher() {
    return watcher;
  }

  /**
   * Node-wide merge concurrency gate: sticky permits for merge threads that are allowed to run;
   * excess merges are paused via rate {@code 0.0}.
   */
  static final class Gate implements MergeConcurrencyGate {
    private final Object lock = new Object();
    private int maxRunning = Integer.MAX_VALUE;
    private final Map<Object, ConcurrentMergeScheduler> permits = new ConcurrentHashMap<>();
    private final Set<ConcurrentMergeScheduler> schedulers = ConcurrentHashMap.newKeySet();
    private final ExecutorService rebalanceExecutor;
    private final AtomicInteger rebalanceThreadId = new AtomicInteger();

    Gate() {
      ThreadFactory tf =
          r -> {
            Thread t =
                new Thread(r, "merge-concurrency-gate-rebalance-" + rebalanceThreadId.incrementAndGet());
            t.setDaemon(true);
            return t;
          };
      this.rebalanceExecutor = Executors.newSingleThreadExecutor(tf);
    }

    int getMaxRunning() {
      synchronized (lock) {
        return maxRunning;
      }
    }

    int getPermitCount() {
      synchronized (lock) {
        purgeDeadThreads();
        return permits.size();
      }
    }

    void setMaxRunning(int maxRunning) {
      if (maxRunning < 1) {
        throw new IllegalArgumentException("maxRunning must be >= 1, got " + maxRunning);
      }
      synchronized (lock) {
        this.maxRunning = maxRunning;
        // Revoke excess permits; peers re-acquire (or stay paused) on rebalance.
        while (permits.size() > this.maxRunning) {
          Iterator<Object> it = permits.keySet().iterator();
          if (!it.hasNext()) {
            break;
          }
          it.next();
          it.remove();
        }
      }
      scheduleRebalance(null);
    }

    void clearMaxRunning() {
      synchronized (lock) {
        this.maxRunning = Integer.MAX_VALUE;
      }
      scheduleRebalance(null);
    }

    @Override
    public double adjustRate(
        ConcurrentMergeScheduler scheduler, Object mergeKey, double proposedMBPerSec) {
      Objects.requireNonNull(scheduler, "scheduler");
      Objects.requireNonNull(mergeKey, "mergeKey");
      synchronized (lock) {
        purgeDeadThreads();
        if (proposedMBPerSec == 0.0) {
          permits.remove(mergeKey);
          return 0.0;
        }
        ConcurrentMergeScheduler holder = permits.get(mergeKey);
        if (holder == scheduler) {
          return proposedMBPerSec;
        }
        if (holder != null) {
          // Another scheduler somehow owns this key; do not steal.
          return 0.0;
        }
        if (permits.size() < maxRunning) {
          permits.put(mergeKey, scheduler);
          return proposedMBPerSec;
        }
        return 0.0;
      }
    }

    @Override
    public void register(ConcurrentMergeScheduler scheduler) {
      schedulers.add(Objects.requireNonNull(scheduler));
    }

    @Override
    public void unregister(ConcurrentMergeScheduler scheduler) {
      schedulers.remove(scheduler);
      synchronized (lock) {
        permits.entrySet().removeIf(e -> e.getValue() == scheduler);
      }
      scheduleRebalance(scheduler);
    }

    @Override
    public void afterUpdate(ConcurrentMergeScheduler scheduler) {
      scheduleRebalance(scheduler);
    }

    private void purgeDeadThreads() {
      assert Thread.holdsLock(lock);
      permits
          .entrySet()
          .removeIf(
              e -> {
                Object key = e.getKey();
                return key instanceof Thread && !((Thread) key).isAlive();
              });
    }

    private void scheduleRebalance(ConcurrentMergeScheduler except) {
      if (rebalanceExecutor.isShutdown()) {
        return;
      }
      for (ConcurrentMergeScheduler s : schedulers) {
        if (s == except) {
          continue;
        }
        final ConcurrentMergeScheduler target = s;
        try {
          rebalanceExecutor.execute(() -> rebalanceOne(target));
        } catch (RuntimeException e) {
          // executor shut down between check and execute
          log.debug("Skipping merge gate rebalance; executor unavailable", e);
        }
      }
    }

    private static void rebalanceOne(ConcurrentMergeScheduler scheduler) {
      try {
        if (scheduler instanceof GlobalConcurrentMergeScheduler) {
          ((GlobalConcurrentMergeScheduler) scheduler).rebalanceMergeThreads();
        }
      } catch (RuntimeException e) {
        log.warn("Merge concurrency gate rebalance failed for {}", scheduler, e);
      }
    }

    void shutdown() {
      rebalanceExecutor.shutdownNow();
      synchronized (lock) {
        permits.clear();
        schedulers.clear();
        maxRunning = Integer.MAX_VALUE;
      }
    }
  }

  /**
   * Watches {@code ext.globalMergeScheduler} and applies {@code maxThreadCount} as the node-wide
   * max running merges on the shared gate.
   */
  final class Watcher {
    static final String GLOBAL_MERGE_SCHEDULER_KEY =
        ClusterProperties.EXT_PROPRTTY_PREFIX + "globalMergeScheduler";

    private final String nodeName;
    private final Gate gate;

    private volatile Integer overrideMaxThreadCount;
    /** null means apply to all nodes; empty list means apply to no nodes. */
    private volatile List<String> overrideNodes;

    private Watcher(String nodeName, Gate gate) {
      this.nodeName = nodeName;
      this.gate = gate;
    }

    void startListening(ZkStateReader zkStateReader) {
      zkStateReader.registerClusterPropertiesListener(
          (Map<String, Object> properties) -> {
            processOverride(properties.get(GLOBAL_MERGE_SCHEDULER_KEY));
            return false;
          });
    }

    boolean hasActiveOverride() {
      return isOverrideActiveForThisNode();
    }

    @SuppressWarnings("unchecked")
    private void processOverride(Object raw) {
      if (raw == null) {
        clearOverrideAndApply("key absent");
        return;
      }
      if (!(raw instanceof Map)) {
        log.warn(
            "{} value must be a map, got {}; ignoring",
            GLOBAL_MERGE_SCHEDULER_KEY,
            raw.getClass().getName());
        return;
      }
      Map<String, Object> map = (Map<String, Object>) raw;
      Integer maxThreadCount = asInteger(map.get("maxThreadCount"));
      if (maxThreadCount == null) {
        log.warn(
            "{} requires maxThreadCount; got {}; ignoring", GLOBAL_MERGE_SCHEDULER_KEY, map);
        return;
      }
      if (maxThreadCount < 1) {
        log.warn(
            "{} invalid maxThreadCount={}; ignoring", GLOBAL_MERGE_SCHEDULER_KEY, maxThreadCount);
        return;
      }
      if (map.containsKey("maxMergeCount")) {
        log.debug(
            "{} maxMergeCount is ignored for the merge concurrency gate (per-core maxMergeCount stays in solrconfig)",
            GLOBAL_MERGE_SCHEDULER_KEY);
      }

      List<String> nodes = null;
      Object nodesRaw = map.get("nodes");
      if (nodesRaw != null) {
        if (!(nodesRaw instanceof List)) {
          log.warn(
              "{} nodes must be a list, got {}; ignoring override",
              GLOBAL_MERGE_SCHEDULER_KEY,
              nodesRaw.getClass().getName());
          return;
        }
        nodes = (List<String>) nodesRaw;
      }

      overrideMaxThreadCount = maxThreadCount;
      overrideNodes = nodes == null ? null : List.copyOf(nodes);

      log.info(
          "{} change detected node={} nodesFilter={} maxThreadCount(maxRunning)={}",
          GLOBAL_MERGE_SCHEDULER_KEY,
          nodeName,
          overrideNodes == null ? "(all)" : overrideNodes,
          maxThreadCount);
      applyLimits();
    }

    private void clearOverrideAndApply(String reason) {
      boolean hadOverride = overrideMaxThreadCount != null || overrideNodes != null;
      overrideMaxThreadCount = null;
      overrideNodes = null;
      if (hadOverride) {
        log.info("{} cleared ({}); removing node-wide running-merge cap", GLOBAL_MERGE_SCHEDULER_KEY, reason);
      }
      applyLimits();
    }

    private void applyLimits() {
      if (isOverrideActiveForThisNode()) {
        int maxRunning = overrideMaxThreadCount;
        log.info(
            "Applying {} override: maxRunning={}", GLOBAL_MERGE_SCHEDULER_KEY, maxRunning);
        gate.setMaxRunning(maxRunning);
        return;
      }
      gate.clearMaxRunning();
    }

    private boolean isOverrideActiveForThisNode() {
      if (overrideMaxThreadCount == null) {
        return false;
      }
      List<String> nodes = overrideNodes;
      if (nodes == null) {
        return true;
      }
      if (nodeName == null) {
        return false;
      }
      return nodes.contains(nodeName);
    }

    private Integer asInteger(Object value) {
      if (value == null) {
        return null;
      }
      if (value instanceof Integer) {
        return (Integer) value;
      }
      if (value instanceof Number) {
        return ((Number) value).intValue();
      }
      if (value instanceof String) {
        try {
          return Integer.parseInt((String) value);
        } catch (NumberFormatException e) {
          return null;
        }
      }
      return null;
    }

    Integer getOverrideMaxThreadCount() {
      return overrideMaxThreadCount;
    }

    List<String> getOverrideNodes() {
      List<String> nodes = overrideNodes;
      return nodes == null ? null : Collections.unmodifiableList(nodes);
    }
  }
}
