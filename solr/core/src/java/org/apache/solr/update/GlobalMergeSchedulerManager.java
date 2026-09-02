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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;

import org.apache.lucene.index.GlobalConcurrentMergeScheduler;
import org.apache.lucene.index.MergePolicy;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


final class GlobalMergeSchedulerManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Object INSTANCE_LOCK = new Object();
  private static GlobalMergeSchedulerManager INSTANCE;

  private final ConcurrentSemaphore semaphore;
  private final Watcher watcher;

  static GlobalMergeSchedulerManager getInstance(ZkController zkController, NamedList<Object> initArgs) {
    synchronized (INSTANCE_LOCK) {
      if (INSTANCE == null) {
        INSTANCE = new GlobalMergeSchedulerManager(zkController, initArgs);
      }
      return INSTANCE;
    }
  }

  private GlobalMergeSchedulerManager(ZkController zkController, NamedList<Object> initArgs) {
    Integer maxThreadCount = (Integer) initArgs.get("maxGlobalThreadCount");
    if (maxThreadCount == null) {
      throw new IllegalArgumentException("maxGlobalThreadCount is required for GlobalConcurrentMergeScheduler");
    }
    this.semaphore = new ConcurrentSemaphore(maxThreadCount);
    this.watcher = new Watcher(zkController.getNodeName(), semaphore);
    ZkStateReader zkStateReader = zkController.getZkStateReader();
    if (zkStateReader != null) {
      watcher.startListening(zkStateReader);
    }
  }

  GlobalConcurrentMergeScheduler getScheduler() {
    return new GlobalConcurrentMergeScheduler(semaphore);
  }

  /** Clears static state for unit tests. */
  static void resetForTesting() {
    synchronized (INSTANCE_LOCK) {
      INSTANCE = null;
    }
  }

  // package-visible for tests
  ConcurrentSemaphore getSemaphore() {
    return semaphore;
  }

  // package-visible for tests
  Watcher getWatcher() {
    return watcher;
  }

  /**
   * Node-wide merge concurrency semaphore: sticky permits keyed by {@link MergePolicy.OneMerge}.
   * Excess merge spawning stalls in {@link GlobalConcurrentMergeScheduler#maybeStall}.
   */
  static final class ConcurrentSemaphore extends Semaphore
      implements GlobalConcurrentMergeScheduler.MergeConcurrencySemaphore {
    private final int defaultMaxRunning;
    private final Set<MergePolicy.OneMerge> activeMerges = new HashSet<>();
    private int maxRunning;

    ConcurrentSemaphore(int maxRunning) {
      super(maxRunning);
      if (maxRunning < 1) {
        throw new IllegalArgumentException("maxRunning must be >= 1, got " + maxRunning);
      }
      this.defaultMaxRunning = maxRunning;
      this.maxRunning = maxRunning;
    }

    int getMaxRunning() {
      synchronized (this) {
        return maxRunning;
      }
    }

    int getActiveMergeCount() {
      synchronized (this) {
        return activeMerges.size();
      }
    }

    @Override
    public synchronized boolean tryAcquire(MergePolicy.OneMerge merge) {
      if (activeMerges.contains(merge)) { // already has permit
        return true;
      }
      boolean acquired = tryAcquire();
      if (acquired) {
        activeMerges.add(merge);
      }
      return acquired;
    }

    @Override
    public synchronized void release(MergePolicy.OneMerge merge) {
      if (activeMerges.remove(merge)) {
        release();
      }
    }

    void clearMaxRunningOverride() {
      setMaxRunningOverride(defaultMaxRunning);
    }

    void setMaxRunningOverride(int maxRunningOverride) {
      if (maxRunningOverride < 1) {
        throw new IllegalArgumentException("maxRunning must be >= 1, got " + maxRunningOverride);
      }
      synchronized (this) {
        int diff = maxRunningOverride - maxRunning;
        if (diff > 0) {
          release(diff);
        } else if (diff < 0) {
          reducePermits(-diff);
        }
        maxRunning = maxRunningOverride;
      }
    }
  }

  /**
   * Watches {@code ext.globalMergeScheduler} and applies {@code maxThreadCount} as the node-wide
   * max concurrent merge threads on the shared semaphore.
   */
  final class Watcher {
    static final String GLOBAL_MERGE_SCHEDULER_KEY =
        ClusterProperties.EXT_PROPRTTY_PREFIX + "globalMergeScheduler";

    private final String nodeName;
    private final ConcurrentSemaphore semaphore;

    private volatile Integer overrideMaxThreadCount;
    /** null means apply to all nodes; empty list means apply to no nodes. */
    private volatile List<String> overrideNodes;

    private Watcher(String nodeName, ConcurrentSemaphore semaphore) {
      this.nodeName = nodeName;
      this.semaphore = semaphore;
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

    /**
     * Clear the overrides stored in this watcher and apply such to the semaphore.
     * @param reason
     */
    private void clearOverrideAndApply(String reason) {
      boolean hadOverride = overrideMaxThreadCount != null || overrideNodes != null;
      overrideMaxThreadCount = null;
      overrideNodes = null;
      if (hadOverride) {
        log.info("{} cleared ({}); removing node-wide running-merge cap", GLOBAL_MERGE_SCHEDULER_KEY, reason);
      }
      applyLimits();
    }

    /**
     * Apply the new limits to the semaphore.
     */
    private void applyLimits() {
      if (isOverrideActiveForThisNode()) {
        int maxRunning = overrideMaxThreadCount;
        log.info(
            "Applying {} override: maxRunning={}", GLOBAL_MERGE_SCHEDULER_KEY, maxRunning);
        semaphore.setMaxRunningOverride(maxRunning);
      } else {
        semaphore.clearMaxRunningOverride();
      }
    }

    /**
     * @return whether an override should be applied to this node
     */
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
