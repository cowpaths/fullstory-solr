package org.apache.solr.handler.component;

import org.apache.solr.client.solrj.impl.LBSolrClient;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class TimeLimitedHttpShardHandler extends HttpShardHandler {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final Limiter limiter;
  private final boolean dryRun;

  public TimeLimitedHttpShardHandler(HttpShardHandlerFactory shardHandlerFactory, Limiter limiter, boolean dryRun) {
    super(shardHandlerFactory);
    this.limiter = limiter;
    this.dryRun = dryRun;
  }

  @Override
  protected ShardResponse nextShardResponse() throws InterruptedException {
    Duration limit = limiter.getNextTimeLimit();
    if (limit == null) {
      return super.nextShardResponse();
    } else {
      ShardResponse response = responses.poll(limit.getNano(), TimeUnit.NANOSECONDS);
      if (response == null) {
        log.warn("Time limit {} exceeded in {}. Dry run mode: {}", limit, this, dryRun);
        if (!dryRun) {
          throw new InterruptedException("Time limit " + limit + " exceeded");
        } else {
          return super.nextShardResponse();
        }
      } else {
        return response;
      }
    }
  }


  @Override
  protected void onRequestComplete(Future<LBSolrClient.Rsp> future, ShardResponse response, long elapsedTime) {
    limiter.onRequestCompleted(future, response, elapsedTime);
  }

  @Override
  protected void onRequestSubmit(Future<LBSolrClient.Rsp> future, ShardRequest request, String shard, ModifiableSolrParams params) {
    limiter.onRequestSubmitted(future, request, shard, params);
  }
}

class SlowShardLimiter implements Limiter {
  private long longestTimeElapsed;
  private final long limitThreshold;
  private final double multiplier;

  SlowShardLimiter(long limitThreshold, double multiplier) {
    this.limitThreshold = limitThreshold;
    this.multiplier = multiplier;
  }
  @Override
  public Duration getNextTimeLimit() {
    long nextTimeLimit = (long) (longestTimeElapsed * multiplier);
    return nextTimeLimit < limitThreshold ? null : Duration.ofMillis(nextTimeLimit);
  }

  @Override
  public void onRequestSubmitted(Future<LBSolrClient.Rsp> future, ShardRequest request, String shard, ModifiableSolrParams params) {
    // Do nothing
  }

  @Override
  public void onRequestCompleted(Future<LBSolrClient.Rsp> future, ShardResponse response, long timeElapsed) {
    if (timeElapsed > longestTimeElapsed) {
      longestTimeElapsed = timeElapsed;
    }
  }
}

/**
 * The implementation is likely stateful since getNextTimeLimit() does not take any params
 */
interface Limiter {
  Duration getNextTimeLimit();
  void onRequestSubmitted(Future<LBSolrClient.Rsp> future, ShardRequest request, String shard, ModifiableSolrParams params);
  void onRequestCompleted(Future<LBSolrClient.Rsp> future, ShardResponse response, long timeElapsed);
}
