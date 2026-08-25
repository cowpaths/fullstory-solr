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
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.GlobalConcurrentMergeScheduler;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.ZkStateReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JVM singleton that owns the shared {@link GlobalConcurrentMergeScheduler} and a ZK clusterprops
 * watcher for live {@code ext.globalMergeScheduler} overrides.
 *
 * <p>Example:
 *
 * <pre>
 * {
 *   "ext.globalMergeScheduler": {
 *     "maxThreadCount": 2,
 *     "maxMergeCount": 8,
 *     "nodes": ["solr-c92-8:8986_solr"]
 *   }
 * }
 * </pre>
 *
 * <p>solrconfig {@code maxThreadCount}/{@code maxMergeCount} are applied via the normal {@link
 * org.apache.lucene.index.ConcurrentMergeScheduler#setMaxMergesAndThreads} path. When clusterprops
 * change, the watcher calls {@code setMaxMergesAndThreads} again. Clearing the override restores
 * the limits that were in effect just before the override was applied.
 */
final class GlobalMergeSchedulerManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Object INSTANCE_LOCK = new Object();
  private static GlobalMergeSchedulerManager INSTANCE;

  private final GlobalConcurrentMergeScheduler scheduler;
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
    this.scheduler = GlobalConcurrentMergeScheduler.getInstance();
    this.watcher = new Watcher(zkController.getNodeName());
    ZkStateReader zkStateReader = zkController.getZkStateReader();
    if (zkStateReader != null) {
      watcher.startListening(zkStateReader);
    }
  }

  GlobalConcurrentMergeScheduler getScheduler() {
    return scheduler;
  }

  /** Clears static state for unit tests. */
  static void resetForTesting() {
    synchronized (INSTANCE_LOCK) {
      INSTANCE = null;
    }
  }

  // package-visible for tests
  Watcher getWatcher() {
    return watcher;
  }

  /**
   * Watches {@code ext.globalMergeScheduler} and applies/reverts limits by calling {@link
   * GlobalConcurrentMergeScheduler#setMaxMergesAndThreads(int, int)}.
   */
  final class Watcher {
    static final String GLOBAL_MERGE_SCHEDULER_KEY =
        ClusterProperties.EXT_PROPRTTY_PREFIX + "globalMergeScheduler";

    private final String nodeName;

    /** Limits in effect before the current override was applied; used when override is cleared. */
    private volatile Integer restoredMaxMergeCount;

    private volatile Integer restoredMaxThreadCount;

    private volatile Integer overrideMaxMergeCount;
    private volatile Integer overrideMaxThreadCount;
    /** null means apply to all nodes; empty list means apply to no nodes. */
    private volatile List<String> overrideNodes;

    private Watcher(String nodeName) {
      this.nodeName = nodeName;
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
      Integer maxMergeCount = asInteger(map.get("maxMergeCount"));
      if (maxThreadCount == null || maxMergeCount == null) {
        log.warn(
            "{} requires maxThreadCount and maxMergeCount; got {}; ignoring",
            GLOBAL_MERGE_SCHEDULER_KEY,
            map);
        return;
      }
      if (maxThreadCount < 1 || maxMergeCount < 1 || maxThreadCount > maxMergeCount) {
        log.warn(
            "{} invalid limits maxThreadCount={} maxMergeCount={}; ignoring",
            GLOBAL_MERGE_SCHEDULER_KEY,
            maxThreadCount,
            maxMergeCount);
        return;
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
      overrideMaxMergeCount = maxMergeCount;
      overrideNodes = nodes == null ? null : List.copyOf(nodes);

      log.info(
          "{} change detected node={} nodesFilter={} maxMergeCount={} maxThreadCount={}",
          GLOBAL_MERGE_SCHEDULER_KEY,
          nodeName,
          overrideNodes == null ? "(all)" : overrideNodes,
          maxMergeCount,
          maxThreadCount);
      applyLimits();
    }

    private void clearOverrideAndApply(String reason) {
      boolean hadOverride =
          overrideMaxMergeCount != null || overrideMaxThreadCount != null || overrideNodes != null;
      overrideMaxMergeCount = null;
      overrideMaxThreadCount = null;
      overrideNodes = null;
      if (hadOverride) {
        log.info(
            "{} cleared ({}); reverting to prior limits maxMergeCount={} maxThreadCount={}",
            GLOBAL_MERGE_SCHEDULER_KEY,
            reason,
            restoredMaxMergeCount,
            restoredMaxThreadCount);
      }
      applyLimits();
    }

    private void applyLimits() {
      if (isOverrideActiveForThisNode()) {
        // Snapshot current limits once so we can restore them if the override is cleared.
        if (restoredMaxMergeCount == null) {
          restoredMaxMergeCount = scheduler.getMaxMergeCount();
          restoredMaxThreadCount = scheduler.getMaxThreadCount();
        }
        int maxMerge = overrideMaxMergeCount;
        int maxThread = overrideMaxThreadCount;
        log.info(
            "Applying {} override: maxMergeCount={} maxThreadCount={}",
            GLOBAL_MERGE_SCHEDULER_KEY,
            maxMerge,
            maxThread);
        scheduler.setMaxMergesAndThreads(maxMerge, maxThread);
        return;
      }
      if (restoredMaxMergeCount != null && restoredMaxThreadCount != null) {
        int maxMerge = restoredMaxMergeCount;
        int maxThread = restoredMaxThreadCount;
        restoredMaxMergeCount = null;
        restoredMaxThreadCount = null;
        log.info(
            "Restoring GlobalConcurrentMergeScheduler limits: maxMergeCount={} maxThreadCount={}",
            maxMerge,
            maxThread);
        scheduler.setMaxMergesAndThreads(maxMerge, maxThread);
      }
    }

    private boolean isOverrideActiveForThisNode() {
      if (overrideMaxMergeCount == null || overrideMaxThreadCount == null) {
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

    Integer getRestoredMaxMergeCount() {
      return restoredMaxMergeCount;
    }

    Integer getOverrideMaxMergeCount() {
      return overrideMaxMergeCount;
    }

    List<String> getOverrideNodes() {
      List<String> nodes = overrideNodes;
      return nodes == null ? null : Collections.unmodifiableList(nodes);
    }
  }
}
