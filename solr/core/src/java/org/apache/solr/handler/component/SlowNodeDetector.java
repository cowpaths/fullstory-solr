package org.apache.solr.handler.component;

import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class SlowNodeDetector {
  //  static final SlowNodeDetector SINGLETON = new SlowNodeDetector(SlowNodeDetectorManager.slowNodeTtl);
  private final ConcurrentMap<String, Object> slowNodes;
  private static final double DEFAULT_LATENCY_DROP_RATIO_THRESHOLD = 0.5;
  private static final int DEFAULT_ITERATION_PERCENTAGE_THRESHOLD = 10;
  private static final int DEFAULT_MIN_CORE_PER_REQUEST = 512;
  private static final int DEFAULT_SLOW_LATENCY_THRESHOLD = 10000;
  private static final int DEFAULT_SLOW_NODE_TTL = 60000;

  private final double latencyDropRatioThreshold; //identify as a latency drop point when current latency is < 0.5 of previous
  private final int iterationPercentageThreshold; //only iterate up to this percentage of sorted response (slowest first) to find a drop
  private final int minCorePerRequest; //minimum number of cores per Shard Request to be considered for slow node detection
  private final int slowLatencyThreshold; //minimum latency to be considered as slow node


  private SlowNodeDetector(double latencyDropRatioThreshold, int iterationPercentageThreshold, int minCorePerRequest, int slowLatencyThreshold, long slowNodeTtl) {
    this.latencyDropRatioThreshold = latencyDropRatioThreshold;
    this.iterationPercentageThreshold = iterationPercentageThreshold;
    this.minCorePerRequest = minCorePerRequest;
    this.slowLatencyThreshold = slowLatencyThreshold;

    Caffeine<Object, Object> builder = Caffeine.newBuilder();

    if (slowNodeTtl >= 0) {
      builder.expireAfterWrite(slowNodeTtl, java.util.concurrent.TimeUnit.MILLISECONDS);
    }
    slowNodes = builder.<String, Object>build().asMap();
  }

  Set<String> getSlowNodes() {
    return new HashSet<>(slowNodes.keySet());
  }

  /**
   * For test only
   */
  void setSlowNodes(Set<String> slowNodes) {
    this.slowNodes.clear();
    for (String slowNode : slowNodes) {
      this.slowNodes.put(slowNode, Boolean.TRUE);
    }
  }



  void notifyRequestStats(RequestStats stats) {
    Set<String> newSlowNodes = computeSlowNodes(stats);

    if (newSlowNodes != null) {
      for (String slowNode : newSlowNodes) {
        slowNodes.put(slowNode, Boolean.TRUE);
      }
    }
  }

  private Set<String> computeSlowNodes(RequestStats stats) {
    if (stats.responseLatencies.size() < minCorePerRequest) {
      return null; //not enough responses to make a decision
    }
    int iterationThreshold = stats.responseLatencies.size() * iterationPercentageThreshold / 100;
    if (iterationThreshold < 1) {
      return null; //not enough responses to make a decision
    }

    Collections.sort(stats.responseLatencies);

    if (stats.responseLatencies.get(0).latency < slowLatencyThreshold) {
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
      if (previousLatency != null && (double) current.latency / previousLatency < latencyDropRatioThreshold) {
        //found the drop in latencies, all the iterated nodes are potentially slow
        foundLatencyDrop = true;
        break;
      }

      //no latency drop point found so far and the rest latencies would not be significant enough to form a drop
      if (current.latency < slowLatencyThreshold) {
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

    return slowNodes;
  }

  static class Builder {
    private double latencyDropRatioThreshold = DEFAULT_LATENCY_DROP_RATIO_THRESHOLD; //identify as a latency drop point when current latency is < 0.5 of previous
    private int iterationPercentageThreshold = DEFAULT_ITERATION_PERCENTAGE_THRESHOLD; //only iterate up to this percentage of sorted response (slowest first) to find a drop
    private int minCorePerRequest = DEFAULT_MIN_CORE_PER_REQUEST; //minimum number of cores per Shard Request to be considered for slow node detection
    private int slowLatencyThreshold = DEFAULT_SLOW_LATENCY_THRESHOLD; //minimum latency to be considered as slow node
    private long slowNodeTtl = DEFAULT_SLOW_NODE_TTL;

    public Builder withLatencyDropRatioThreshold(double latencyDropRatioThreshold) {
      this.latencyDropRatioThreshold = latencyDropRatioThreshold;
      return this;
    }

    public Builder withIterationPercentageThreshold(int iterationPercentageThreshold) {
      this.iterationPercentageThreshold = iterationPercentageThreshold;
      return this;
    }

    public Builder withMinCorePerRequest(int minCorePerRequest) {
      this.minCorePerRequest = minCorePerRequest;
      return this;
    }

    public Builder withSlowLatencyThreshold(int slowLatencyThreshold) {
      this.slowLatencyThreshold = slowLatencyThreshold;
      return this;
    }

    public Builder withSlowNodeTtl(long slowNodeTtl) {
      this.slowNodeTtl = slowNodeTtl;
      return this;
    }

    public SlowNodeDetector build() {
      return new SlowNodeDetector(latencyDropRatioThreshold, iterationPercentageThreshold, minCorePerRequest, slowLatencyThreshold, slowNodeTtl);
    }

  }
}


class RequestStats {
  final List<NodeLatency> responseLatencies = new ArrayList<>();
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
  }

  public synchronized void recordLatency(String node, long latency) {
    responseLatencies.add(new NodeLatency(node, latency));
    responseCountByNode.compute(node, (k, c) -> c != null ? c + 1 : 1);
  }
}
