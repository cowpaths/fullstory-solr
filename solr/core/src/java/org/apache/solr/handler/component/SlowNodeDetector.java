package org.apache.solr.handler.component;

import com.google.common.cache.CacheBuilder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

class SlowNodeDetector {
  //  static final SlowNodeDetector SINGLETON = new SlowNodeDetector(SlowNodeDetectorManager.slowNodeTtl);
  final Set<String> slowNodes;
  final double latencyRatioThreshold = 1.5;
  final int iterationPercentageThreshold = 10; //only iterate up to this percentage of sorted response (slowest first) to find a drop
  final int minCorePerRequest = 512; //minimum number of cores per Shard Request to be considered for slow node detection
  final int minLatency = 10000; //minimum latency to be considered as slow node

  static SlowNodeDetector build(long ttl) {
    return new SlowNodeDetector(ttl);
  }

  private SlowNodeDetector(long ttl) {
    CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder();

    if (ttl >= 0) {
      builder.expireAfterWrite(ttl, java.util.concurrent.TimeUnit.MILLISECONDS);
    }
    slowNodes = builder.<String, Object>build().asMap().keySet();
  }

  Set<String> getSlowNodes() {
    return new HashSet<>(slowNodes);
  }

  void reset() { //for testing
    slowNodes.clear();
  }


  void notifyRequestStats(RequestStats stats) {
    ComputeResult result = computeSlowNodes(stats, new HashSet<>(slowNodes));

    if (result != null) {
      slowNodes.addAll(result.slowNodes);
    }
  }

  private ComputeResult computeSlowNodes(RequestStats stats, HashSet<String> existingSlowNodes) {
    if (stats.responseLatencies.size() <= minCorePerRequest) {
      return null; //not enough responses to make a decision
    }
    int iterationThreshold = stats.responseLatencies.size() * iterationPercentageThreshold / 100;
    if (iterationThreshold < 1) {
      return null; //not enough responses to make a decision
    }

    if (stats.responseLatencies.first().latency < minLatency) {
      return null; //fastest response is not slow enough to consider any node as slow
    }

    Long previousLatency = null;
    boolean foundLatencyDrop = false;
    Map<String, Integer> iteratedResponseCountByNode = new HashMap<>();

    int index = 0;
    for (RequestStats.NodeLatency current : stats.responseLatencies) {
      if (index++ > iterationThreshold) {
        break;
      }
      if (previousLatency != null && previousLatency / current.latency > latencyRatioThreshold) {
        //found the drop in latencies, all the previous nodes are slow
        foundLatencyDrop = true;
        break;
      }

      //no drop point found so far and the rest latencies would not be significant enough to form a drop
      if (current.latency < minLatency) {
        break;
      }

      previousLatency = current.latency;
      iteratedResponseCountByNode.compute(current.node, (k, v) -> v == null ? 1 : v + 1);
    }


    Set<String> slowNodes = new HashSet<>();
    if (foundLatencyDrop) { //then that means there are some nodes that are significantly slower than others
      for (Map.Entry<String, Integer> nodeWithSlowResponseCount : iteratedResponseCountByNode.entrySet()) {
        String potentialSlowNode = nodeWithSlowResponseCount.getKey();

        //all responses of this node is slow, it is a slow node
        if (nodeWithSlowResponseCount.getValue().equals(stats.responseCountByNode.get(potentialSlowNode))) {
          slowNodes.add(potentialSlowNode);
        }
      }
    }

    //only nodes that are known to be slow and involved in this ShardRequest shall be considered as recovered nodes
    Set<String> recoveredNodes = new HashSet<>(existingSlowNodes);
    recoveredNodes.retainAll(stats.responseCountByNode.keySet()); //assume all that are processed are fast again now

    //then remove the ones that are detected slow
    recoveredNodes.removeAll(slowNodes);

    return new ComputeResult(slowNodes, recoveredNodes);
  }

  private static class ComputeResult {
    final Set<String> slowNodes;
    final Set<String> recoveredNodes;

    public ComputeResult(Set<String> slowNodes, Set<String> recoveredNodes) {
      this.slowNodes = slowNodes;
      this.recoveredNodes = recoveredNodes;
    }
  }
}


class RequestStats {
  final SortedSet<NodeLatency> responseLatencies = new TreeSet<>();
  final Map<String, Integer> responseCountByNode = new ConcurrentHashMap<>();

  RequestStats() {
  }

  static class NodeLatency implements Comparable<NodeLatency>{
    final String node;
    final long latency;

    NodeLatency(String node, long latency) {
      this.node = node;
      this.latency = latency;
    }


    @Override
    public int compareTo(NodeLatency other) {
      int timeComparison = Long.compare(other.latency, this.latency); // reverse order
      if (timeComparison != 0) {
        return timeComparison;
      }
      return this.node.compareTo(other.node);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      NodeLatency that = (NodeLatency) o;
      return latency == that.latency && Objects.equals(node, that.node);
    }

    @Override
    public int hashCode() {
      return Objects.hash(node, latency);
    }
  }

  public synchronized void recordLatency(String node, long latency) {
    responseLatencies.add(new NodeLatency(node, latency));
    responseCountByNode.compute(node, (k, c) -> c != null ? c++ : 1);
  }
}
