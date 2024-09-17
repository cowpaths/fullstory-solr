/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.handler.component;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.util.Cancellable;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.SuppressForbidden;

public class TolerantShardHandlerWrapper extends ShardHandler {
  private final HttpShardHandler delegate;
  long start = System.currentTimeMillis();
  private final int maxWaitMillis;

  private static class CancellableWithStart implements Cancellable {
    private final Cancellable cancellable;
    public final long startTime;

    private CancellableWithStart(Cancellable cancellable, long startTime) {
      this.cancellable = cancellable;
      this.startTime = startTime;
    }

    @Override
    public void cancel() {
      cancellable.cancel();
    }
  }

  @SuppressForbidden(reason = "Need currentTimeMillis")
  public TolerantShardHandlerWrapper(HttpShardHandler delegate, int maxWait) {
    this.delegate = delegate;
    this.maxWaitMillis = maxWait;
    delegate.responseCancellableMap =
        new HashMap<>() {
          @Override
          public Cancellable put(ShardResponse key, Cancellable value) {
            return super.put(key, new CancellableWithStart(value, System.currentTimeMillis()));
          }
        };
  }

  @Override
  public void prepDistributed(ResponseBuilder rb) {
    delegate.prepDistributed(rb);
  }

  @Override
  public void submit(ShardRequest sreq, String shard, ModifiableSolrParams params) {
    delegate.submit(sreq, shard, params);
  }

  @Override
  public ShardResponse takeCompletedIncludingErrors() {
    return take();
  }

  @Override
  public ShardResponse takeCompletedOrError() {
    return delegate.takeCompletedOrError();
  }

  @Override
  public void cancelAll() {
    delegate.cancelAll();
  }

  @Override
  public ShardHandlerFactory getShardHandlerFactory() {
    return delegate.getShardHandlerFactory();
  }

  @SuppressForbidden(reason = "Need currentTimeMillis")
  private ShardResponse take() {
    try {
      while (delegate.pending.get() > 0) {
        long currTime = System.currentTimeMillis();
        long timeOut = Math.max(maxWaitMillis - (currTime - start), 5);
        //this uses poll() instead of take() so that we can exit earlier with timeout
        ShardResponse rsp = delegate.responses.poll(timeOut, TimeUnit.MILLISECONDS);
        if (rsp == null) {
          // atleast one node is getting delayed
          for (Map.Entry<ShardResponse, Cancellable> e :
              delegate.responseCancellableMap.entrySet()) {
            if (e.getValue() instanceof CancellableWithStart) {
              CancellableWithStart c = (CancellableWithStart) e.getValue();
              if (currTime > (c.startTime + maxWaitMillis)) {
                rsp = e.getKey();
                rsp.setException(
                    new SolrException(
                        SolrException.ErrorCode.SERVICE_UNAVAILABLE,
                        "Swerver took too long to respond"));
                e.getValue().cancel();
                break;
              }
            }
          }
        }
        if (rsp == null) {
          continue;
        }

        delegate.responseCancellableMap.remove(rsp);

        delegate.pending.decrementAndGet();
        if (rsp.getException() != null) return rsp; // if exception, return immediately
        // add response to the response list... we do this after the take() and
        // not after the completion of "call" so we know when the last response
        // for a request was received.  Otherwise we might return the same
        // request more than once.
        rsp.getShardRequest().responses.add(rsp);
        if (rsp.getShardRequest().responses.size() == rsp.getShardRequest().actualShards.length) {
          return rsp;
        }
      }
    } catch (InterruptedException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
    return null;
  }

  public static ShardHandler wrap(ShardHandler shardHandler, SolrParams params) {
    int maxWait = params.getInt("maxShardWait", -1);
    boolean tolerant = ShardParams.getShardsTolerantAsBool(params);
    if (tolerant && maxWait > 0 && shardHandler instanceof HttpShardHandler) {
      return new TolerantShardHandlerWrapper((HttpShardHandler) shardHandler, maxWait);
    }
    return shardHandler;
  }
}
