package org.apache.solr.handler.component;

import org.apache.solr.client.solrj.impl.LBSolrClient;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

public class TimeLimitedHttpShardHandler extends HttpShardHandler {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final long minWait;
  private final double waitMultiplier;
  private final boolean dryRun;

  private final Map<ShardRequest, Tracker> trackers = new ConcurrentHashMap<>();

  public TimeLimitedHttpShardHandler(HttpShardHandlerFactory shardHandlerFactory, long minWait, double waitMultiplier, boolean dryRun) {
    super(shardHandlerFactory);
    this.minWait = minWait;
    this.waitMultiplier = waitMultiplier;
    this.dryRun = dryRun;
  }


  @Override
  protected ShardRequestCallback onRequestSubmit(Future<LBSolrClient.Rsp> future, ShardRequest shardRequest, String shard, ModifiableSolrParams params) {
    return new ShardRequestTrackingCallback(future, shardRequest);
  }


  /**
   * Callback to track ShardRequest and a Future created by sending such request to a shard.
   * <p>
   * Take note that a single ShardRequest can be reused many times for different shards.
   * Therefore, we need this class as a reference back to the corresponding ShardRequest when such future is completed.
   */
  class ShardRequestTrackingCallback implements HttpShardHandler.ShardRequestCallback {
    private final ShardRequest shardRequest;
    private final Future<LBSolrClient.Rsp> future;

    ShardRequestTrackingCallback(Future<LBSolrClient.Rsp> future, ShardRequest shardRequest) {
      this.future = future;
      this.shardRequest = shardRequest;
      trackers.computeIfAbsent(shardRequest, k -> new SlowShardTracker(minWait, waitMultiplier, dryRun)).onRequestSubmitted(future);
    }
    @Override
    public void onComplete(ShardResponse response, long elapsedTime) {
      trackers.compute(shardRequest, (k, v) -> {
        if (v != null) {
          if (v.onRequestCompleted(future, elapsedTime)) { //if tracker is done with this shard request
            return null;
          }
        }
        return v;
      });
    }
  }
}

/**
 * Tracks futures submitted to different shards under the same ShardRequest.
 * <p>
 * If any of the shard requests takes longer than the longest elapsed time observed so far multiplied by the multiplier,
 * then all the pending requests will be cancelled.
 */
class SlowShardTracker implements Tracker {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final boolean dryRun;
  private long longestTimeElapsed;
  private final long minWait;
  private final double multiplier;
  private final Set<Future<?>> pendingFutures = ConcurrentHashMap.newKeySet();
  private final Timer timer = new Timer();
  private Instant lastCompletedTime;

  SlowShardTracker(long minWait, double multiplier, boolean dryRun) {
    this.minWait = minWait;
    this.multiplier = multiplier;
    this.dryRun = dryRun;
  }

  @Override
  public void onRequestSubmitted(Future<?> future) {
    pendingFutures.add(future);
  }

  @Override
  public synchronized boolean onRequestCompleted(Future<?> future, long timeElapsed) {
    timer.cancel(); //cancel previous timer
    lastCompletedTime = Instant.now();
    if (timeElapsed > longestTimeElapsed) {
      longestTimeElapsed = timeElapsed;
    }

    pendingFutures.remove(future);
    if (pendingFutures.isEmpty()) {
      return true;
    }

    if (longestTimeElapsed * multiplier > minWait) {
      timer.schedule(new java.util.TimerTask() {
        @Override
        public void run() {
          if (!dryRun) {
            Set<Future<?>> removingFutures = new HashSet<>(pendingFutures);
            pendingFutures.clear();
            removingFutures.forEach(f -> f.cancel(true));
          } else {
            log.info("Dry-run mode: would have cancelled {} pending requests. Last completed time {} vs Current time {}", pendingFutures.size(), lastCompletedTime, Instant.now());
          }
        }
      }, (long) (longestTimeElapsed * multiplier));
      longestTimeElapsed = timeElapsed;
    }
    return false;
  }
}

interface Tracker {
  void onRequestSubmitted(Future<?> future);
  boolean onRequestCompleted(Future<?> future, long timeElapsed);
}
