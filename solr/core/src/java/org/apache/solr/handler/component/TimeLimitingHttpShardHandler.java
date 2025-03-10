package org.apache.solr.handler.component;

import com.google.common.collect.MapMaker;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.solr.client.solrj.impl.LBSolrClient;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TimeLimitingHttpShardHandler extends HttpShardHandler {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final long slowNodeTimeout;
  private final boolean dryRun;

  private final ConcurrentMap<ShardRequest, List<ShardRequestActor>> actorsByShardRequest =
      new MapMaker().weakKeys().makeMap();
  private final SlowNodeDetector slowNodeDetector;
  private final TimeoutCallback timeoutCallback;

  TimeLimitingHttpShardHandler(
      HttpShardHandlerFactory shardHandlerFactory,
      long slowNodeTimeout,
      boolean dryRun,
      SlowNodeDetector slowNodeDetector,
      TimeoutCallback timeoutCallback) {
    super(shardHandlerFactory);
    this.slowNodeTimeout = slowNodeTimeout;
    this.dryRun = dryRun;
    this.slowNodeDetector = slowNodeDetector;
    this.timeoutCallback = timeoutCallback;
  }

  @Override
  protected ShardRequestCallback onRequestSubmit(
      Future<LBSolrClient.Rsp> future,
      ShardRequest shardRequest,
      List<String> shardUrls,
      ModifiableSolrParams params) {
    ShardRequestTrackingCallback callback =
        new ShardRequestTrackingCallback(future, shardRequest, shardUrls);
    List<ShardRequestActor> shardRequestActors =
        actorsByShardRequest.computeIfAbsent(
            shardRequest,
            k -> {
              List<ShardRequestActor> actors = new ArrayList<>();
              actors.add(new NodeStatsCollector(slowNodeDetector));
              if ("true".equalsIgnoreCase(shardRequest.params.get(ShardParams.SHARDS_TOLERANT))) {
                actors.add(
                    new SlowNodeTimeoutActor(
                        slowNodeTimeout, dryRun, slowNodeDetector.getSlowNodes(), timeoutCallback));
              }
              return actors;
            });
    shardRequestActors.forEach(actor -> actor.onRequestSubmitted(shardUrls, future));
    return callback;
  }

  /**
   * Callback to track ShardRequest and a Future created by sending such request to a single shard.
   *
   * <p>Take note that the ShardRequest can be reused/shared by sending to many different shards.
   * Therefore, we need this class as a reference back to the corresponding ShardRequest when such
   * future is completed.
   */
  class ShardRequestTrackingCallback
      implements HttpShardHandler
          .ShardRequestCallback { // TODO maybe use a name that indicates we assume using Slow node
    // canceller for this
    private final ShardRequest shardRequest;
    private final Future<LBSolrClient.Rsp> future;
    private final List<String> shardUrls;

    ShardRequestTrackingCallback(
        Future<LBSolrClient.Rsp> future, ShardRequest shardRequest, List<String> shardUrls) {
      this.future = future;
      this.shardRequest = shardRequest;
      this.shardUrls = shardUrls;
    }

    @Override
    public void onResponse(LBSolrClient.Rsp response, long elapsedTime) {
      onComplete(elapsedTime, response.getServer());
    }

    @Override
    public void onException(Throwable exception, long elapsedTime) {
      // TODO is it possible to infer the selected node? If it has timed out, perhaps we can assume
      // all shardNodes are slow?
      onComplete(elapsedTime, null);
    }

    private void onComplete(long elapsedTime, String selectedShardUrl) {
      try {
        actorsByShardRequest.compute(
            shardRequest,
            (k, actors) -> {
              if (actors != null) {
                actors.removeIf(
                    actor ->
                        actor.onRequestCompleted(selectedShardUrl, shardUrls, future, elapsedTime));
                if (actors.isEmpty()) {
                  return null;
                }
              }
              return actors;
            });
      } catch (Exception e) {
        log.warn("Failed to notify stats to slowNodeDetector", e);
      }
    }
  }
}

/** This gets notified when such ShardRequest instance is submitted/completed to/on any shard url */
interface ShardRequestActor {
  void onRequestSubmitted(List<String> shardUrls, Future<?> future);

  boolean onRequestCompleted(
      String selectedShardUrl, List<String> shardUrls, Future<?> future, long timeElapsed);
}

class Util {
  static String getNode(String urlString) {
    try {
      URL url = new URL(urlString);
      return url.getAuthority();
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException("Invalid URL: " + urlString, e);
    }
  }
}

/**
 * Tied to a single ShardRequest instance, which can be retried/submitted to many shard urls.
 *
 * <p>This actor keeps the stats for each shard response latency and notify the SlowNodeDetector
 * when all of them complete
 */
class NodeStatsCollector implements ShardRequestActor {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final SlowNodeDetector detector;
  private final AtomicInteger pendingCount = new AtomicInteger(0);
  final RequestStats stats = new RequestStats();

  NodeStatsCollector(SlowNodeDetector detector) {
    this.detector = detector;
  }

  @Override
  public void onRequestSubmitted(List<String> shardUrls, Future<?> future) {
    pendingCount.incrementAndGet();
  }

  @Override
  public synchronized boolean onRequestCompleted(
      String selectedShardUrl, List<String> shardUrls, Future<?> future, long timeElapsed) {
    if (selectedShardUrl != null) { // might be null if exception occurred
      stats.recordLatency(Util.getNode(selectedShardUrl), timeElapsed);
    }
    if (pendingCount.decrementAndGet() == 0) {
      detector.notifyRequestStats(
          stats); // notify the slowNodeDetector of the execution stats of all the submissions by
      // this shard request
      return true;
    }
    return false;
  }
}

/**
 * Tied to a single ShardRequest instance, which can be retried/submitted to many shard urls.
 *
 * <p>This actor keeps a list of pending future for all in-flight request submissions. When all the
 * remaining pending futures are from the slowNodes list, start a timer task to timeout/cancel all
 * of them according to the timeout value (only prints a message if dryRun is true)
 */
class SlowNodeTimeoutActor implements ShardRequestActor {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final boolean dryRun;
  private final long timeout;
  private final Set<Future<?>> pendingFutures = ConcurrentHashMap.newKeySet();
  private final Set<String> slowNodes;
  private final AtomicInteger pendingFutureCountFromFastNode = new AtomicInteger(0);
  private final TimeoutCallback timeoutCallback;
  private volatile Future<?> timeoutTask = null;

  SlowNodeTimeoutActor(
      long timeout, boolean dryRun, Set<String> slowNodes, TimeoutCallback timeoutCallback) {
    this.timeout = timeout;
    this.dryRun = dryRun;
    this.slowNodes = slowNodes;
    this.timeoutCallback = timeoutCallback;
  }

  @Override
  public void onRequestSubmitted(List<String> shardUrls, Future<?> future) {
    pendingFutures.add(future);

    // if any of the shard URLs is from a slow node, then we don't count it as a fast node future as
    // it could potentially be slow
    // This is done such that we can more effectively terminate pending futures with a mix of slow
    // and fast nodes for
    // the same shard.
    if (!hasSlowNodeShardUrl(shardUrls)) {
      pendingFutureCountFromFastNode.incrementAndGet();
    }
  }

  private boolean hasSlowNodeShardUrl(List<String> shardUrls) {
    for (String shardUrl : shardUrls) {
      if (slowNodes.contains(Util.getNode(shardUrl))) {
        return true;
      }
    }
    return false;
  }

  @Override
  public synchronized boolean onRequestCompleted(
      String selectedShardUrl, List<String> shardUrls, Future<?> future, long timeElapsed) {
    pendingFutures.remove(future);
    if (pendingFutures.isEmpty()) { // every submitted futures are processed
      if (timeoutTask != null) {
        timeoutTask.cancel(true);
      }
      return true;
    }

    if (!hasSlowNodeShardUrl(shardUrls)) {
      pendingFutureCountFromFastNode.decrementAndGet();
    }

    if (timeoutTask == null
        && pendingFutureCountFromFastNode.get()
            <= 0) { // all pending reqs are from slow nodes, start a timer to possibly timeout
      ExecutorService executorService = null;
      try {
        executorService =
            ExecutorUtil.newMDCAwareSingleThreadExecutor(
                new SolrNamedThreadFactory("SlowNodeTimeout"));
        timeoutTask =
            executorService.submit(
                () -> {
                  try {
                    Thread.sleep(timeout);
                  } catch (InterruptedException e) {
                    // ok. The responses came back before timeout
                    return;
                  }
                  Set<Future<?>> removingFutures = new HashSet<>(pendingFutures);
                  pendingFutures.clear();
                  if (!removingFutures.isEmpty()) {
                    if (timeoutCallback != null) {
                      timeoutCallback.accept(removingFutures);
                    }
                    if (!dryRun) {
                      if (log.isInfoEnabled()) {
                        log.info(
                            "Cancelling {} pending requests due to timeout duration {}ms exceeded. ",
                            removingFutures.size(),
                            timeout);
                      }
                      removingFutures.forEach(f -> f.cancel(true));
                      if (log.isInfoEnabled()) {
                        log.info("{} Pending requests cancelled", removingFutures.size());
                      }
                    } else {
                      if (log.isInfoEnabled()) {
                        log.info(
                            "Dry-run mode: would have cancelled {} pending requests due to timeout duration {}ms exceeded",
                            removingFutures.size(),
                            timeout);
                      }
                    }
                  }
                });
      } finally {
        if (executorService != null) {
          executorService.shutdown();
        }
      }
    }

    return false;
  }
}

interface TimeoutCallback extends Consumer<Set<Future<?>>> {}
