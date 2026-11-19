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

import static org.apache.solr.common.params.CommonParams.SOLR_REQUEST_CONTEXT_PARAM;
import static org.apache.solr.common.params.CommonParams.SOLR_REQUEST_TYPE_PARAM;
import static org.apache.solr.servlet.PriorityBasedRateLimiter.SOLR_REQUEST_PRIORITY_PARAM;

import java.util.concurrent.atomic.AtomicReference;
import javax.servlet.http.HttpServletRequest;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.core.RateLimiterConfig;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricsContext;
import org.eclipse.jetty.server.Request;
import org.junit.Test;

/**
 * Deterministic tests for how {@link PriorityBasedRateLimiter} cleans up after a request that
 * leaves while it is still waiting (interrupted), and that it keeps its active-request count
 * accurate. No test relies on a real timeout elapsing: the slot-acquisition timeout is set far
 * longer than the test, so a waiter that is never woken shows up as a test failure instead of a
 * slow pass.
 */
public class TestPriorityBasedRateLimiterWaiters extends SolrTestCase {

  /** Long enough that the timeout can never be what rescues a waiter inside a test. */
  private static final long LONG_TIMEOUT_MS = 60_000;

  private static final long WAIT_MS = 10_000;

  private static final SolrMetricsContext METRICS =
      new SolrMetricsContext(new SolrMetricManager(), "RateLimitManager", "rl");

  private static final HttpServletRequest FOREGROUND = request("FOREGROUND");
  private static final HttpServletRequest BACKGROUND = request("BACKGROUND");

  private static PriorityBasedRateLimiter newLimiter(int allowedRequests, int backgroundThreshold) {
    return new PriorityBasedRateLimiter(
        new RateLimiterConfig(
            SolrRequest.SolrRequestType.QUERY,
            true,
            1,
            LONG_TIMEOUT_MS,
            allowedRequests,
            false /* isSlotBorrowing */,
            true,
            backgroundThreshold),
        METRICS);
  }

  /**
   * A background request interrupted while parked must not leave its latch in the wait queue.
   * Otherwise the next release() spends its single wake-up on the abandoned latch and a live waiter
   * stays parked even though a slot is free.
   */
  @Test
  public void testInterruptedBackgroundWaiterDoesNotStealWakeUp() throws Exception {
    // Background requests are only admitted while fewer than 1 request is active.
    PriorityBasedRateLimiter limiter = newLimiter(2, 1);

    RequestRateLimiter.SlotReservation holder = limiter.handleRequest(FOREGROUND);
    assertNotNull(holder);

    AtomicReference<RequestRateLimiter.SlotReservation> resultA = new AtomicReference<>();
    AtomicReference<RequestRateLimiter.SlotReservation> resultB = new AtomicReference<>();
    Thread a = new Thread(() -> resultA.set(limiter.handleRequest(BACKGROUND)), "bg-waiter-A");
    Thread b = new Thread(() -> resultB.set(limiter.handleRequest(BACKGROUND)), "bg-waiter-B");
    try {
      a.start();
      awaitParked(a); // A is first in the wait queue
      b.start();
      awaitParked(b);

      a.interrupt(); // A gives up while waiting
      a.join(WAIT_MS);
      assertFalse("interrupted waiter should have returned", a.isAlive());
      assertNull("interrupted waiter must not be admitted", resultA.get());

      holder.close(); // frees a slot: B is the only live waiter and must be woken

      b.join(WAIT_MS);
      assertFalse(
          "live waiter B was never woken: the wake-up went to the interrupted waiter's latch",
          b.isAlive());
      assertNotNull("B should have been admitted once the slot was released", resultB.get());
      resultB.get().close();
      assertEquals(0, limiter.getRequestsAllowed());
    } finally {
      cleanUp(a, b);
    }
  }

  /**
   * A request interrupted while waiting for a permit must give back the active-request count it
   * took. Otherwise getRequestsAllowed() drifts upward and background requests are queued behind
   * phantom load.
   */
  @Test
  public void testInterruptedPermitWaiterReleasesActiveCount() throws Exception {
    PriorityBasedRateLimiter limiter = newLimiter(1, 1);

    RequestRateLimiter.SlotReservation holder = limiter.handleRequest(FOREGROUND);
    assertNotNull(holder);
    assertEquals(1, limiter.getRequestsAllowed());

    AtomicReference<RequestRateLimiter.SlotReservation> result = new AtomicReference<>();
    Thread waiter = new Thread(() -> result.set(limiter.handleRequest(FOREGROUND)), "fg-waiter");
    try {
      waiter.start();
      awaitParked(waiter); // blocked acquiring the only permit

      waiter.interrupt();
      waiter.join(WAIT_MS);
      assertFalse("interrupted waiter should have returned", waiter.isAlive());
      assertNull(result.get());

      assertEquals(
          "interrupted request leaked its active-request count", 1, limiter.getRequestsAllowed());

      holder.close();
      assertEquals("active-request count did not return to zero", 0, limiter.getRequestsAllowed());
    } finally {
      cleanUp(waiter);
    }
  }

  /** Sanity check, not a bug repro: a parked background waiter is admitted when a slot frees. */
  @Test
  public void testParkedBackgroundWaiterAdmittedOnRelease() throws Exception {
    PriorityBasedRateLimiter limiter = newLimiter(2, 1);

    RequestRateLimiter.SlotReservation holder = limiter.handleRequest(FOREGROUND);
    assertNotNull(holder);

    AtomicReference<RequestRateLimiter.SlotReservation> result = new AtomicReference<>();
    Thread waiter = new Thread(() -> result.set(limiter.handleRequest(BACKGROUND)), "bg-waiter");
    try {
      waiter.start();
      awaitParked(waiter);

      holder.close();
      waiter.join(WAIT_MS);
      assertFalse(waiter.isAlive());
      assertNotNull(result.get());
      result.get().close();
      assertEquals(0, limiter.getRequestsAllowed());
    } finally {
      cleanUp(waiter);
    }
  }

  /** Waits until the thread is blocked in a timed wait (a latch await or a permit acquire). */
  private static void awaitParked(Thread t) throws InterruptedException {
    long deadline = System.nanoTime() + WAIT_MS * 1_000_000L;
    while (t.getState() != Thread.State.TIMED_WAITING) {
      if (System.nanoTime() > deadline || !t.isAlive()) {
        fail("thread " + t.getName() + " never parked; state=" + t.getState());
      }
      Thread.sleep(5);
    }
  }

  /** Never leave a thread behind, even when an assertion failed (SolrTestCase checks for leaks). */
  private static void cleanUp(Thread... threads) throws InterruptedException {
    for (Thread t : threads) {
      if (t.isAlive()) {
        t.interrupt();
        t.join(WAIT_MS);
      }
    }
  }

  private static HttpServletRequest request(String priority) {
    return new PriorityRequest(SolrRequest.SolrRequestType.PRIORITY_BASED.name(), priority);
  }

  private static class PriorityRequest extends Request {
    private final String type;
    private final String priority;

    PriorityRequest(String type, String priority) {
      super(null, null);
      this.type = type;
      this.priority = priority;
    }

    @Override
    public String getHeader(String name) {
      switch (name) {
        case SOLR_REQUEST_CONTEXT_PARAM:
          return null;
        case SOLR_REQUEST_TYPE_PARAM:
          return type;
        case SOLR_REQUEST_PRIORITY_PARAM:
          return priority;
        default:
          throw new IllegalArgumentException(name);
      }
    }
  }
}
