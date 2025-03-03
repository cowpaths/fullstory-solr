package org.apache.solr.handler.component;

import org.apache.solr.client.solrj.impl.LBSolrClient;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class TimeLimitedHttpShardHandler extends HttpShardHandler {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final long minWait;
  private final boolean dryRun;

  private final ConcurrentMap<ShardRequest, ShardRequestListener> listeners = new ConcurrentHashMap<>();
  private final SlowNodeDetector slowNodeDetector;

  public TimeLimitedHttpShardHandler(HttpShardHandlerFactory shardHandlerFactory, long minWait, boolean dryRun, SlowNodeDetector slowNodeDetector) {
    super(shardHandlerFactory);
    this.minWait = minWait;
    this.dryRun = dryRun;
    this.slowNodeDetector = slowNodeDetector;
  }


  @Override
  protected ShardRequestCallback onRequestSubmit(Future<LBSolrClient.Rsp> future, ShardRequest shardRequest, List<String> shardUrls, ModifiableSolrParams params) {
    String node = getNode(shardUrls.get(0)); //only consider the first shard url for now
    ShardRequestTrackingCallback callback = new ShardRequestTrackingCallback(future, shardRequest, node);
    listeners.computeIfAbsent(shardRequest, k -> new ShardRequestListener(minWait, dryRun, slowNodeDetector.getSlowNodes())).onRequestSubmitted(node, future);
    return callback;
  }


  static String getNode(String urlString) {
    try {
      URL url = new URL(urlString);
      return url.getAuthority();
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException("Invalid URL: " + urlString, e);
    }
  }
  /**
   * Callback to track ShardRequest and a Future created by sending such request to a single shard.
   * <p>
   * Take note that the ShardRequest can be reused/shared by sending to many different shards.
   * Therefore, we need this class as a reference back to the corresponding ShardRequest when such future is completed.
   */
  class ShardRequestTrackingCallback implements HttpShardHandler.ShardRequestCallback { //TODO maybe use a name that indicates we assume using Slow node canceller for this
    private final ShardRequest shardRequest;
    private final Future<LBSolrClient.Rsp> future;
    private final String node;

    ShardRequestTrackingCallback(Future<LBSolrClient.Rsp> future, ShardRequest shardRequest, String node) {
      this.future = future;
      this.shardRequest = shardRequest;
      this.node = node;
    }
    @Override
    public void onResponse(LBSolrClient.Rsp response, long elapsedTime) {
      onComplete(elapsedTime);
    }

    @Override
    public void onException(Throwable exception, long elapsedTime) {
      onComplete(elapsedTime);
    }

    private void onComplete(long elapsedTime) {
      try {
        listeners.compute(shardRequest, (k, v) -> {
          if (v != null) {
            if (v.onRequestCompleted(node, future, elapsedTime)) { //if canceller/tracker is done with all pending future/shards with this shard request
              slowNodeDetector.notifyRequestStats(v.stats); //notify the slowNodeDetector of the execution stats of all the submissions by this shard request
              return null;
            }
          }
          return v;
        });
      } catch (Exception e) {
        log.warn("Failed to notify stats to slowNodeDetector", e);
      }
    }
  }
}

/**
 * Tied to a single ShardRequest instance, which can be retried/submitted to many shard urls.
 * <p>
 * This gets notified when such ShardRequest instance is submitted/completed to/on any shard url
 * <p>
 * This listener keeps a list of pending future for all in-flight request submissions, and perform:
 * <ol>
 *   <li> When all the remaining pending futures are from the slowNodes list, start a timer task to timeout/cancel all of
 * them according to the timeout value (only prints a message if dryRun is true)
 *   <li> Keep track of the latency stats of all the response
 * </ol>
 */
class ShardRequestListener {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final boolean dryRun;
  private final long timeout;
  private final Set<Future<?>> pendingFutures = ConcurrentHashMap.newKeySet();
  private final Set<String> slowNodes;
  private final AtomicInteger pendingFuturesCountFromFastNode = new AtomicInteger(0);

  private volatile Future<?> timeoutTask = null;
  final RequestStats stats = new RequestStats();

  ShardRequestListener(long timeout, boolean dryRun, Set<String> slowNodes) {
    this.timeout = timeout;
    this.dryRun = dryRun;
    this.slowNodes = slowNodes;
  }

  void onRequestSubmitted(String node, Future<?> future) {
    pendingFutures.add(future);
    if (!slowNodes.contains(node)) {
      pendingFuturesCountFromFastNode.incrementAndGet();
    }
  }

  synchronized boolean onRequestCompleted(String node, Future<?> future, long timeElapsed) {
    stats.recordLatency(node, timeElapsed);
    pendingFutures.remove(future);
    if (pendingFutures.isEmpty()) { //every submitted futures are processed
      if (timeoutTask != null) {
        timeoutTask.cancel(true);
      }
      return true;
    }

    if (!slowNodes.contains(node)) {
      pendingFuturesCountFromFastNode.decrementAndGet();
    }

    if (timeoutTask == null && pendingFuturesCountFromFastNode.get() <= 0) { //all pending reqs are from slow nodes, start a timer to possibly timeout
      ExecutorService executorService = null;
      try {
        executorService = ExecutorUtil.newMDCAwareSingleThreadExecutor(new SolrNamedThreadFactory("SlowNodeTimeout"));
        timeoutTask = executorService.submit(() -> {
          try {
            Thread.sleep(timeout);
          } catch (InterruptedException e) {
            //ok. The responses came back before timeout
            return;
          }
          Set<Future<?>> removingFutures = new HashSet<>(pendingFutures);
          pendingFutures.clear();
          if (!removingFutures.isEmpty()) {
            if (!dryRun) {
              log.info("Cancelling {} pending requests due to timeout duration {}ms exceeded. ", removingFutures.size(), timeout);
              removingFutures.forEach(f -> f.cancel(true));
              log.info("{} Pending requests cancelled", removingFutures.size());
            } else {
              log.info("Dry-run mode: would have cancelled {} pending requests due to timeout duration {}ms exceeded", removingFutures.size(), timeout);
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

