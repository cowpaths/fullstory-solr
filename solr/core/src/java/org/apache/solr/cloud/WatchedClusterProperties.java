package org.apache.solr.cloud;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import org.apache.solr.common.cloud.ClusterPropertiesListener;

public class WatchedClusterProperties implements ClusterPropertiesListener {

  final String nodeName;

  public static final String WATCHED_PROPERTIES = "watched-properties";
  public static final String WATCHED_NODE_PROPERTIES = "watched-node-properties";

  private Map<String, List<BiConsumer<String, String>>> propertiesListeners =
      Collections.synchronizedMap(new HashMap<>());

  /* private Map<String, List<BiConsumer<String, String>>> nodePropertiesListeners =
  Collections.synchronizedMap(new HashMap<>());*/

  private volatile Map<String, String> knownData = new HashMap<>();
  private volatile Map<String, String> knownNodeData = new HashMap<>();

  public WatchedClusterProperties(String nodeName) {
    this.nodeName = nodeName;
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean onChange(Map<String, Object> properties) {

    Map<String, String> newData =
        ((Map<String, Map<String, String>>)
                properties.getOrDefault(WATCHED_NODE_PROPERTIES, Collections.EMPTY_MAP))
            .getOrDefault(nodeName, Collections.EMPTY_MAP);
    Map<String, String> modified =
        compareAndInvokeListeners(newData, knownNodeData, null, propertiesListeners);
    if (modified != null) knownNodeData = modified;

    newData =
        (Map<String, String>) properties.getOrDefault(WATCHED_PROPERTIES, Collections.EMPTY_MAP);
    modified = compareAndInvokeListeners(newData, knownData, knownNodeData, propertiesListeners);
    if (modified != null) knownData = modified;

    return false;
  }

  private static Map<String, String> compareAndInvokeListeners(
      Map<String, String> newData,
      Map<String, String> knownData,
      Map<String, String> overrideData,
      Map<String, List<BiConsumer<String, String>>> propertiesListeners) {
    boolean isModified = false;
    // look for changed data or missing keys
    for (String k : knownData.keySet()) {
      String oldVal = knownData.get(k);
      String newVal = newData.get(k);
      if (Objects.equals(oldVal, newVal)) continue;
      isModified = true;
      if (overrideData != null && overrideData.containsKey(k)) {
        // per node properties contain this key. do not invoke listener
        continue;
      }
      invokeListener(k, newVal, propertiesListeners);
    }
    for (String k : newData.keySet()) {
      if (knownData.containsKey(k)) continue;
      isModified = true;
      invokeListener(k, newData.get(k), propertiesListeners);
    }

    if (isModified) {
      return Map.copyOf(newData);
    } else {
      return null;
    }
  }

  private static void invokeListener(
      String key,
      String newVal,
      Map<String, List<BiConsumer<String, String>>> propertiesListeners) {
    List<BiConsumer<String, String>> listeners = propertiesListeners.get(key);
    if (listeners != null) {
      for (BiConsumer<String, String> l : listeners) {
        l.accept(key, newVal);
      }
    }
    listeners = propertiesListeners.get(null);
    if (listeners != null) {
      for (BiConsumer<String, String> listener : listeners) {
        listener.accept(key, newVal);
      }
    }
  }

  public void watchProperty(String name, BiConsumer<String, String> listener) {
    propertiesListeners.computeIfAbsent(name, s -> new CopyOnWriteArrayList<>()).add(listener);
  }

  public void unwatchProperty(String name, BiConsumer<String, String> listener) {
    List<BiConsumer<String, String>> listeners = propertiesListeners.get(name);
    if (listeners == null) return;
    listeners.remove(listener);
  }
}
