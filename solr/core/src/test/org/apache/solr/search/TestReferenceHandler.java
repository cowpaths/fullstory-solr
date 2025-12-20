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
package org.apache.solr.search;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.SolrTestCaseJ4;

public class TestReferenceHandler extends SolrTestCaseJ4 {

  private static final int MAX_KB = 1024;
  private static final int MIN_KB = 1;
  private static final int MAX_KB_BASELINE = MAX_KB - MIN_KB + 1;

  private static final int N_SECONDS = 5;

  private static final class Dummy implements Accountable {

    private final long ramBytesUsed;

    private Dummy(long ramBytesUsed) {
      this.ramBytesUsed = ramBytesUsed;
    }

    @Override
    public long ramBytesUsed() {
      return ramBytesUsed;
    }
  }

  public void testRefQueueHandling() throws InterruptedException, ExecutionException, IOException {
    int nThreads = 20;
    final int batchSize = 1024;

    ExecutorService exec =
        Executors.newFixedThreadPool(nThreads, new NamedThreadFactory("TestReferenceHandler"));
    LongAdder collectedRefs = new LongAdder();
    LongAdder totalBytesIn = new LongAdder();
    LongAdder totalBytesOut = new LongAdder();
    ReferenceHandler<Dummy> rh = new ReferenceHandler<>((a) -> {
      collectedRefs.increment();
      totalBytesOut.add(a.ramBytesUsed());
    }, null);
    AtomicBoolean finished = new AtomicBoolean();
    @SuppressWarnings("rawtypes")
    Future<?>[] futures = new Future[nThreads];
    LongAdder total = new LongAdder();
    long start = System.nanoTime();
    for (int i = nThreads - 1; i >= 0; i--) {
      futures[i] =
          exec.submit(
              () -> {
                Random r = new Random(random().nextLong());
                try {
                  while (!finished.get()) {
                    for (int j = batchSize - 1; j >= 0; j--) {
                      // between 1k and 1m
                      Dummy d = new Dummy(r.nextInt(1024));
                      totalBytesIn.add(d.ramBytesUsed());
                      rh.addDummyReference(1024 * (r.nextInt(MAX_KB_BASELINE) + MIN_KB), d);
                      total.increment();
                    }
                  }
                } catch (Throwable t) {
                  t.printStackTrace(System.err);
                  throw t;
                }
              });
    }
    LongAdder collectedHoldingRefs = new LongAdder();
    long endNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(N_SECONDS);
    long remainingNanos;
    while ((remainingNanos = endNanos - System.nanoTime()) > 0) {
      long sz = rh.getOutstandingSize();
      System.out.println(
          "seconds remaining: "
              + TimeUnit.NANOSECONDS.toSeconds(remainingNanos)
              + ", outstandingSize="
              + sz
              + ", collected="
              + collectedRefs.sum()
              + " ("
              + RamUsageEstimator.humanReadableUnits(sz * ReferenceHandler.RAMBYTES_PER_REF)
              + ")");
      Thread.sleep(Math.min(1000, TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
    }
    finished.set(true);
    long sum = total.sum();
    for (int i = nThreads - 1; i >= 0; i--) {
      futures[i].get();
    }
    System.out.println(
        "tasks completed " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + "ms");
    start = System.nanoTime();
    int gcIterations = 0;
    long sz;
    while ((sz = rh.getOutstandingSize()) > 0 || rh.nonEmptyRefQueueHeadCount() > 0) {
      gcIterations++;
      System.gc();
      Thread.sleep(250);
      System.err.println(
          "gc iteration "
              + gcIterations
              + ", "
              + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)
              + ", outstandingSize="
              + sz
              + ", collected="
              + collectedRefs.sum()
              + ", nonEmptyRefQueueHeadCount="
              + rh.nonEmptyRefQueueHeadCount());
      if (gcIterations > 40) {
        fail("failed to converge");
      }
    }
    rh.close();
    exec.shutdown();
    assertTrue(exec.awaitTermination(60, TimeUnit.SECONDS));
    long createdSum = total.sum();
    long collectedSum = collectedRefs.sum();
    long collectedHoldingSum = collectedHoldingRefs.sum();
    assertEquals(createdSum, collectedSum + collectedHoldingSum);
    System.err.println(
        "success! "
            + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)
            + " millis; throughput="
            + (sum / N_SECONDS)
            + "/s");
    System.err.println(
        "total created="
            + createdSum
            + ", collectedHolding="
            + collectedHoldingSum
            + ", collected="
            + collectedSum);
    System.err.println(totalBytesIn.sum() + " ?= " + totalBytesOut.sum());
  }
}
