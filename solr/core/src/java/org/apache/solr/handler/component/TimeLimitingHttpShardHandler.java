package org.apache.solr.handler.component;

import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
 *
 * <p>Should only be used for SearchHandler
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
                  tracker.actors.forEach(ShardRequestActor::close);
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
   * Close and flush when the last response is received.
   *
   * <p>This happens either when all previously submitted requests are completed (no in-flight
   * requests) or an exception occurred while the handler is not shard fault-tolerant
   *
   * <p>After close is invoked, there shall be no further method calls to this Actor.
   *
   * <p>In an edge case, there could be pauses with request submissions such that close is triggered
   * before all the requests for a particular ShardRequest are submitted. In such case, a new actor
   * should be instantiated to handle the rest of the requests submission and completion.
   */
  void close();
}

class Util {
  static String getNode(String urlString) {
    try {
      URI uri = new URI(urlString);
      return uri.getAuthority();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid URI: " + urlString, e);
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
  public void close() {
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
  private final ExecutorService executorService;

  // if non-null, then there is an active count down to cancel pending requests once timeout elapsed
  private volatile CountDownLatch cancelCountDownLatch;

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
    this.executorService =
            ExecutorUtil.newMDCAwareSingleThreadExecutor(
                    new SolrNamedThreadFactory("SlowNodeTimeout"));
  }

  @Override
  public synchronized void onRequestSubmitted(List<String> shardUrls, Future<?> future) {
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

    // all pending reqs are from slow nodes, start a countdown to possibly cancel those requests
    if (cancelCountDownLatch == null && pendingFutureCountFromFastNode.get() <= 0) {
      cancelCountDownLatch = new CountDownLatch(1);
      executorService.submit(
        () -> {
          try {
            if (cancelCountDownLatch.await(timeout, TimeUnit.MILLISECONDS)) {
              return; // phew! All pending slow node requests completed before timeout
            }

            synchronized (SlowNodeTimeoutActor.this) {
              if (!pendingFutures.isEmpty()) {
                if (timeoutCallback != null) {
                  timeoutCallback.accept(pendingFutures);
                }
                if (!dryRun) {
                  pendingFutures.forEach(f -> f.cancel(true));
                  if (log.isInfoEnabled()) {
                    log.info("{} Pending requests cancelled due to timeout duration {}ms exceeded", pendingFutures.size(), timeout);
                  }
                } else {
                  if (log.isInfoEnabled()) {
                    log.info(
                        "Dry-run mode: would have cancelled {} pending requests due to timeout duration {}ms exceeded",
                        pendingFutures.size(),
                        timeout);
                  }
                }
                pendingFutures.clear();
              }
            }
          } catch (InterruptedException e) {
            // ok
          }
        });
    }
  }

  @Override
  public synchronized void close() {
    if (cancelCountDownLatch != null) {
      cancelCountDownLatch.countDown();
    }
    pendingFutures.clear();
    executorService.shutdown();
  }
}

interface TimeoutCallback extends Consumer<Set<Future<?>>> {}
