package org.apache.solr.handler.component;

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

/**
 * A {@link HttpShardHandler} that detects slow nodes and times out requests to them if applicable.
 */
class TimeLimitingHttpShardHandler extends HttpShardHandler {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final long slowNodeTimeout;
  private final boolean dryRun;

  private final ConcurrentMap<ShardRequest, ShardRequestTracker> activeShardRequests =
      new ConcurrentHashMap<>();

  private final SlowNodeDetector slowNodeDetector;
  private final TimeoutCallback timeoutCallback;

  /**
   * @param slowNodeTimeout how long in milliseconds to wait before timing out (cancelling) requests
   *     to slow nodes
   * @param dryRun whether to actually cancel the pending requests or just log the cancellation
   * @param slowNodeDetector detector implementation to detect slow nodes
   * @param timeoutCallback callback to be invoked when timeout is reached, whether the pending
   *     requests are cancelled due to dryRun mode
   */
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
    boolean shardsTolerant =
        shardRequest.params != null && ShardParams.getShardsTolerantAsBool(shardRequest.params);
    ShardRequestTrackingCallback callback =
        new ShardRequestTrackingCallback(future, shardRequest, shardsTolerant, shardUrls);
    activeShardRequests.compute(
        shardRequest,
        (k, tracker) -> {
          if (tracker == null) {
            List<ShardRequestActor> actors = new ArrayList<>();
            actors.add(new NodeStatsCollector(slowNodeDetector));
            if (shardsTolerant) {
              actors.add(
                  new SlowNodeTimeoutActor(
                      slowNodeTimeout, dryRun, slowNodeDetector.getSlowNodes(), timeoutCallback));
            }
            tracker = new ShardRequestTracker(actors);
          }
          tracker.actors.forEach(actor -> actor.onRequestSubmitted(shardUrls, future));
          tracker.outstandingRequestCount.incrementAndGet();
          return tracker;
        });

    return callback;
  }

  /**
   * Callback to track ShardRequest and a Future created by sending such request to a single shard
   * URL.
   *
   * <p>Take note that the ShardRequest can be reused/shared by sending to many different shards.
   * Therefore, we need this class as a reference back to the corresponding ShardRequest when such
   * future is completed.
   */
  class ShardRequestTrackingCallback implements HttpShardHandler.ShardRequestCallback {
    private final ShardRequest shardRequest;
    private final Future<LBSolrClient.Rsp> future;
    private final List<String> shardUrls;
    private final boolean shardsTolerant;

    ShardRequestTrackingCallback(
        Future<LBSolrClient.Rsp> future,
        ShardRequest shardRequest,
        boolean shardsTolerant,
        List<String> shardUrls) {
      this.future = future;
      this.shardRequest = shardRequest;
      this.shardUrls = shardUrls;
      this.shardsTolerant = shardsTolerant;
    }

    @Override
    public void onResponse(LBSolrClient.Rsp response, long elapsedTime) {
      onComplete(elapsedTime, response.getServer(), false);
    }

    @Override
    public void onException(Throwable exception, long elapsedTime) {
      // TODO is it possible to infer the selected node? If it has timed out, perhaps we can assume
      // all shardNodes are slow?
      onComplete(elapsedTime, null, true);
    }

    private void onComplete(long elapsedTime, String selectedShardUrl, boolean isException) {
      try {
        activeShardRequests.compute(
            shardRequest,
            (k, tracker) -> {
              if (tracker != null) {
                tracker.actors.forEach(
                    actor ->
                        actor.onRequestCompleted(selectedShardUrl, shardUrls, future, elapsedTime));
                int outstandingRequestCount = tracker.outstandingRequestCount.decrementAndGet();
                if (isLastResponse(isException, outstandingRequestCount)) {
                  tracker.actors.forEach(ShardRequestActor::flush);
                  return null; // remove this shardRequest from the map on last response
                }
              }
              return tracker;
            });
      } catch (Exception e) {
        log.warn("Failed to notify stats to slowNodeDetector", e);
      }
    }

    private boolean isLastResponse(boolean isException, int outstandingRequestCount) {
      if (!shardsTolerant && isException) {
        return true;
      }
      return outstandingRequestCount <= 0;
    }
  }

  static class ShardRequestTracker {
    final List<ShardRequestActor> actors;
    final AtomicInteger outstandingRequestCount = new AtomicInteger(0);

    ShardRequestTracker(List<ShardRequestActor> actors) {
      this.actors = actors;
    }
  }
}

/** This gets notified when such ShardRequest instance is submitted/completed to/on any shard url */
interface ShardRequestActor {
  void onRequestSubmitted(List<String> shardUrls, Future<?> future);

  void onRequestCompleted(
      String selectedShardUrl, List<String> shardUrls, Future<?> future, long timeElapsed);

  /**
   * Flushes when the last response is received.
   *
   * <p>This happens either when all submitted requests are completed or an exception occurred while
   * the handler is not shard fault-tolerant
   *
   * <p>In an edge case, there could be pauses with request submissions such that flush is triggered
   * before all the requests are submitted for a particular ShardRequest. In such case, flush could
   * be invoked more than once.
   */
  void flush();
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
  private final SlowNodeDetector detector;
  final RequestStats stats = new RequestStats();

  NodeStatsCollector(SlowNodeDetector detector) {
    this.detector = detector;
  }

  @Override
  public void onRequestSubmitted(List<String> shardUrls, Future<?> future) {}

  @Override
  public synchronized void onRequestCompleted(
      String selectedShardUrl, List<String> shardUrls, Future<?> future, long timeElapsed) {
    if (selectedShardUrl != null) { // might be null if exception occurred
      stats.recordLatency(Util.getNode(selectedShardUrl), timeElapsed);
    }
  }

  @Override
  public void flush() {
    detector.notifyRequestStats(stats);
    stats.clear();
  }
}

/**
 * Tied to a single ShardRequest instance, which can be retried/submitted to many shard urls.
 *
 * <p>This actor keeps a list of pending future for all in-flight request submissions. When all the
 * remaining pending futures are from the slowNodes list, start a timer task to timeout/cancel all
 * of them according to the timeout value (do not time out and only print a message if dryRun is
 * true)
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

  /**
   * A ShardRequestActor that times out all pending requests if all of them are from slow nodes.
   * Each instance of this class is tied to a single ShardRequest instance, which can be
   * retried/submitted to many shard urls.
   *
   * @param timeout timeout in milliseconds, if all pending requests remain are from slow nodes, a
   *     task to cancelled all pending requests will be scheduled with this timeout as delay
   * @param dryRun whether to actually cancel the pending requests or just log the cancellation
   * @param slowNodes set of slow nodes, this should not change during the lifetime of this actor
   * @param timeoutCallback callback to be invoked when timeout is reached, whether the pending
   *     requests are cancelled due to dryRun mode
   */
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

    // This is a shardRequest being submitted to a single shard. However with multiple replicas,
    // there will be multiple shard URLs.
    // if any of those replicas are from a known slow node, then we will NOT count this pending
    // future as a "fast node future". Therefore, we assume this future can be potentially slow
    // and subjected to timeout.
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
  public synchronized void onRequestCompleted(
      String selectedShardUrl, List<String> shardUrls, Future<?> future, long timeElapsed) {
    pendingFutures.remove(future);
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
  }

  @Override
  public void flush() {
    if (timeoutTask != null) { // should no longer attempt to timeout as all responses are completed
      timeoutTask.cancel(true);
    }
    pendingFutures.clear();
  }
}

interface TimeoutCallback extends Consumer<Set<Future<?>>> {}
