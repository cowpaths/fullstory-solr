package org.apache.solr.servlet;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.core.RateLimiterConfig;

/**
 * PriorityBasedRateLimiter allocates the slot based on their request priority Currently, it has two
 * priorities FOREGROUND and BACKGROUND Requests. Client can pass the {@link
 * org.apache.solr.common.params.CommonParams} SOLR_REQUEST_TYPE_PARAM request header to indicate
 * the foreground and background request. Foreground requests has high priority than background
 * requests
 */
public class PriorityBasedRateLimiter extends RequestRateLimiter {
  private final AtomicInteger priorityOneRequests = new AtomicInteger();
  private final String[] priorities;
  private final Semaphore numRequestsAllowed;

  private final int totalAllowedRequests;

  private final LinkedBlockingQueue<CountDownLatch> waitingList = new LinkedBlockingQueue<>();

  private final long waitTimeoutInMillis;

  public PriorityBasedRateLimiter(RateLimiterConfig rateLimiterConfig) {
    super(rateLimiterConfig);
    this.priorities =
        new String[] {
          SolrRequest.RequestPriorities.FOREGROUND.name(),
          SolrRequest.RequestPriorities.BACKGROUND.name()
        };
    this.numRequestsAllowed = new Semaphore(rateLimiterConfig.allowedRequests, true);
    this.totalAllowedRequests = rateLimiterConfig.allowedRequests;
    this.waitTimeoutInMillis = rateLimiterConfig.waitForSlotAcquisition;
  }

  @Override
  public SlotReservation handleRequest(String requestPriority) {
    if (!rateLimiterConfig.isEnabled) {
      return UNLIMITED;
    }
    try {
      if (!acquire(requestPriority)) {
        return null;
      }
    } catch (InterruptedException ie) {
      return null;
    }
    return () -> PriorityBasedRateLimiter.this.release(requestPriority);
  }

  private boolean acquire(String priority) throws InterruptedException {
    if (priority.equals(this.priorities[0])) {
      return nextInQueue();
    } else if (priority.equals(this.priorities[1])) {
      if (this.priorityOneRequests.get() < this.totalAllowedRequests) {
        return nextInQueue();
      } else {
        CountDownLatch wait = new CountDownLatch(1);
        this.waitingList.put(wait);
        return wait.await(this.waitTimeoutInMillis, TimeUnit.MILLISECONDS) && nextInQueue();
      }
    }
    return true;
  }

  private boolean nextInQueue() throws InterruptedException {
    boolean acquired =
        this.numRequestsAllowed.tryAcquire(1, this.waitTimeoutInMillis, TimeUnit.MILLISECONDS);
    if (!acquired) {
      return false;
    }
    this.priorityOneRequests.addAndGet(1);
    return true;
  }

  private void exitFromQueue() {
    this.numRequestsAllowed.release(1);
    this.priorityOneRequests.addAndGet(-1);
  }

  private void release(String priority) {
    if (this.priorities[0].equals(priority) || this.priorities[1].equals(priority)) {
      if (this.priorityOneRequests.get() > this.totalAllowedRequests) {
        // priority one request is waiting, let's inform it
        this.exitFromQueue();
      } else {
        // next priority
        CountDownLatch waiter = this.waitingList.poll();
        if (waiter != null) {
          waiter.countDown();
        }
        this.exitFromQueue();
      }
    }
  }

  @Override
  public SlotReservation allowSlotBorrowing() throws InterruptedException {
    // if we reach here that means slot is not available
    return null;
  }

  public int getRequestsAllowed() {
    return this.priorityOneRequests.get();
  }
}
