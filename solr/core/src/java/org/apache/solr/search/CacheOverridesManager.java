package org.apache.solr.search;

import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
public class CacheOverridesManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final ConcurrentMap<String, List<CacheOverrides>> overridesByCacheName = new ConcurrentHashMap<>();


  public CacheOverridesManager(SolrZkClient zkClient) {
    byte[] clusterPropsBytes = null;

    final Watcher watcher = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.NodeDataChanged) {
          try {
            // Fetch the updated cluster properties
            byte[] data = zkClient.getData(event.getPath(), this, null, true);
            if (data != null) {
              Map<String, Object> clusterPropsJson =
                      (Map<String, Object>) Utils.fromJSON(data);
              // Process the cache overrides from cluster properties
              processCacheOverrides(clusterPropsJson.get("cacheOverrides"));
            } else {
              processCacheOverrides(null);
            }
          } catch (Exception e) {
            log.warn("Error processing cache overrides from ZooKeeper", e);
          }
        }
      }
    };

    try {
      clusterPropsBytes = zkClient.getData(ZkStateReader.CLUSTER_PROPS, watcher, null, true);
    } catch (Exception e) {
      log.warn("Error fetching cluster properties from ZooKeeper", e);
    }

    if (clusterPropsBytes != null) {
      Map<String, String> clusterPropsJson =
          (Map<String, String>) Utils.fromJSON(clusterPropsBytes);
      processCacheOverrides(clusterPropsJson.get("cacheOverrides"));
    }
  }

  private void processCacheOverrides(Object cacheOverridesContents) {
    if (cacheOverridesContents instanceof List) {
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> entries = (List<Map<String, Object>>) cacheOverridesContents;
      Map<String, List<CacheOverrides>> newOverridesByCacheName = new HashMap<>();
      for (Map<String, Object> overridesMap : entries) {
        List<String> collectionsFilter = (List<String>) overridesMap.get("collections");

        for (Map.Entry<String, Object> entry : overridesMap.entrySet()) {
          if ("collections".equals(entry.getKey())) { //a special case for collection filter
            continue;
          }
          String cacheName = entry.getKey();
          Map<String, String> overrides = new HashMap<>();
          for (Map.Entry<String, Object> propertyKv : ((Map<String, Object>) entry.getValue()).entrySet()) {
            overrides.put(propertyKv.getKey(), String.valueOf(propertyKv.getValue()));
          }
          CacheOverrides cacheOverrides = new CacheOverrides(overrides, collectionsFilter == null ? null : Set.copyOf(collectionsFilter));
          newOverridesByCacheName.computeIfAbsent(cacheName, k -> new java.util.ArrayList<>()).add(cacheOverrides);
        }
      }
      synchronized(overridesByCacheName) {
        overridesByCacheName.clear();
        overridesByCacheName.putAll(newOverridesByCacheName);
        log.info("Cache overrides updated to {}", overridesByCacheName);
      }
    } else if (cacheOverridesContents == null) { //overrides removal
      overridesByCacheName.clear();
      log.info("Cleared cache overrides");
    } else {
      log.warn("Unexpected format for cacheOverrides in cluster properties: {}", cacheOverridesContents);
    }
  }

  public List<Map<String, String>> getOverrides(String cacheName, String collection) {
    List<CacheOverrides> overrides;
    synchronized (overridesByCacheName) {
       overrides = overridesByCacheName.get(cacheName);
    }
    if (overrides == null) {
      return null;
    }
    return overrides.stream()
        .map(override -> override.getOverrides(collection))
        .filter(overridesMap -> overridesMap != null && !overridesMap.isEmpty())
        .collect(Collectors.toList());
  }

  static class CacheOverrides {
    private final Map<String, String> overrides;
    private final Set<String> matchCollections;

    public CacheOverrides(Map<String, String> overrides, Set<String> matchCollections) {
      this.overrides = new HashMap<>(overrides);
      this.matchCollections = matchCollections;
    }

    public Map<String, String> getOverrides(String collection) {
      if (matchCollections == null || matchCollections.contains(collection)) {
        return overrides;
      } else {
        return null;
      }
    }

    @Override
    public String toString() {
      return "CacheOverrides{" +
              "overrides=" + overrides +
              ", matchCollections=" + matchCollections +
              '}';
    }
  }
}
