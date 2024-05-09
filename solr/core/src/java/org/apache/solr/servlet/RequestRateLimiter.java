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

package org.apache.solr.servlet;

import java.io.Closeable;
import java.lang.invoke.VarHandle;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import net.jcip.annotations.ThreadSafe;
import org.apache.solr.core.RateLimiterConfig;

/**
 * Handles rate limiting for a specific request type.
 *
 * <p>The control flow is as follows: Handle request -- Check if slot is available -- If available,
 * acquire slot and proceed -- else reject the same.
 */
@ThreadSafe
public class RequestRateLimiter {

  private static final class State {
    // Slots that are guaranteed for this request rate limiter.
    private final Semaphore guaranteedSlotsPool;

    // Competitive slots pool that are available for this rate limiter as well as borrowing by other
    // request rate limiters. By competitive, the meaning is that there is no prioritization for the
    // acquisition of these slots -- First Come First Serve, irrespective of whether the request is of
    // this request rate limiter or other.
    private final Semaphore borrowableSlotsPool;

    private State(Semaphore guaranteedSlotsPool, Semaphore borrowableSlotsPool) {
      this.guaranteedSlotsPool = guaranteedSlotsPool;
      this.borrowableSlotsPool = borrowableSlotsPool;
    }
  }

  private State state;
  private final RateLimiterConfig rateLimiterConfig;
  public static final SlotReservation UNLIMITED = () -> {
    // no-op
  };

  public RequestRateLimiter(RateLimiterConfig rateLimiterConfig) {
    this.rateLimiterConfig = rateLimiterConfig;
    init();
    // within the ctor, we need to ensure that subsequent reads get a non-null value for
    // `state`, so call `VarHandle.fullFence()`; beyond that, it's arbitrary what state
    // is applied to a given request
    VarHandle.fullFence();
  }

  public final void init() {
    Semaphore guaranteedSlotsPool = new Semaphore(rateLimiterConfig.guaranteedSlotsThreshold);
    Semaphore borrowableSlotsPool =
        new Semaphore(
            rateLimiterConfig.allowedRequests - rateLimiterConfig.guaranteedSlotsThreshold);
    state = new State(guaranteedSlotsPool, borrowableSlotsPool);
  }

  /**
   * Handles an incoming request. returns a metadata object representing the metadata for the
   * acquired slot, if acquired. If a slot is not acquired, returns a null metadata object.
   */
  public SlotReservation handleRequest() throws InterruptedException {

    if (!rateLimiterConfig.isEnabled) {
      return UNLIMITED;
    }

    final State stateSnapshot = this.state;
    final Semaphore guaranteedSlotsPool = stateSnapshot.guaranteedSlotsPool;

    if (guaranteedSlotsPool.tryAcquire(
        rateLimiterConfig.waitForSlotAcquisition, TimeUnit.MILLISECONDS)) {
      return new SingleSemaphoreReservation(guaranteedSlotsPool);
    }

    final Semaphore borrowableSlotsPool = stateSnapshot.borrowableSlotsPool;
    if (borrowableSlotsPool.tryAcquire(
        rateLimiterConfig.waitForSlotAcquisition, TimeUnit.MILLISECONDS)) {
      return new SingleSemaphoreReservation(borrowableSlotsPool);
    }

    return null;
  }

  /**
   * Whether to allow another request type to borrow a slot from this request rate limiter.
   * Typically works fine if there is a relatively lesser load on this request rate limiter's type
   * compared to the others (think of skew).
   *
   * @return returns a metadata object for the acquired slot, if acquired. If the slot was not
   *     acquired, returns a metadata object with a null pool.
   * @lucene.experimental -- Can cause slots to be blocked if a request borrows a slot and is itself
   *     long lived.
   */
  public SlotReservation allowSlotBorrowing() throws InterruptedException {
    final Semaphore borrowableSlotsPool = state.borrowableSlotsPool;
    if (borrowableSlotsPool.tryAcquire(
        rateLimiterConfig.waitForSlotAcquisition, TimeUnit.MILLISECONDS)) {
      return new SingleSemaphoreReservation(borrowableSlotsPool);
    }

    return null;
  }

  public RateLimiterConfig getRateLimiterConfig() {
    return rateLimiterConfig;
  }

  public interface SlotReservation extends Closeable {}

  // Represents the metadata for a slot
  static class SingleSemaphoreReservation implements SlotReservation {
    private final Semaphore usedPool;

    public SingleSemaphoreReservation(Semaphore usedPool) {
      assert usedPool != null;
      this.usedPool = usedPool;
    }

    @Override
    public void close() {
      usedPool.release();
    }
  }
}
