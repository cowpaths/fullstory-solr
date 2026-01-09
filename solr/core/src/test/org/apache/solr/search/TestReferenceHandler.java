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

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.ref.Reference;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;

public class TestReferenceHandler extends SolrTestCaseJ4 {

  private static final int MAX_KB = 1024;
  private static final int MIN_KB = 1;
  private static final int MAX_KB_BASELINE = MAX_KB - MIN_KB + 1;

  private static final int N_SECONDS = 30;

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

  public void testInit() throws IOException, ExecutionException, InterruptedException {
    int nThreads = 10;
    ExecutorService exec =
        ExecutorUtil.newMDCAwareFixedThreadPool(
            nThreads, new SolrNamedThreadFactory("testHeapCache"));
    try (Closeable c = () -> ExecutorUtil.shutdownAndAwaitTermination(exec)) {
      CountDownLatch cdl = new CountDownLatch(nThreads);
      Future<?>[] futures = new Future[nThreads];
      for (int i = nThreads - 1; i >= 0; i--) {
        int idx = i;
        futures[i] =
            exec.submit(
                () -> {
                  cdl.countDown();
                  cdl.await();
                  try (FixedBitSet.Modifier m =
                      FixedBitSets.registerModifier(
                          () ->
                              new FixedBitSet.Modifier() {
                                @Override
                                public void close() {
                                  try {
                                    FixedBitSets.unregisterModifier(this, null);
                                  } catch (IOException ex) {
                                    throw new UncheckedIOException(ex);
                                  }
                                }
                              })) {
                    System.err.println("opened! " + idx + " " + System.identityHashCode(m));
                  }
                  System.err.println("closed! " + idx);
                  return null;
                });
      }
      for (Future<?> f : futures) {
        f.get();
      }
    }
  }

  private static final int BLOCK_SHIFT = BitDocSet.BIT_SHIFT - 6;
  private static final int BLOCK_MASK = (1 << BLOCK_SHIFT) - 1;

  public void testHeapCacheFbs() throws InterruptedException, ExecutionException, IOException {
    int nThreads = 20;
    ExecutorService exec =
        ExecutorUtil.newMDCAwareFixedThreadPool(
            nThreads, new SolrNamedThreadFactory("testHeapCache"));
    try (Closeable c = () -> ExecutorUtil.shutdownAndAwaitTermination(exec);
        HeapCacheFbsModifier h = new HeapCacheFbsModifier(false)) {
      AtomicBoolean finished = new AtomicBoolean(false);
      Future<?>[] futures = new Future[nThreads];
      AtomicInteger errCt = new AtomicInteger();
      int maxSize = (SortedIntDocSet.MAX_ARR_SIZE >> 1) * 16;
      for (int i = 0; i < nThreads; i++) {
        Random r = new Random(random().nextLong());
        futures[i] =
            exec.submit(
                () -> {
                  try {
                    while (!finished.get()) {
                      int size = r.nextInt(maxSize);
                      LongBuffer compare = LongBuffer.allocate(size);
                      Closeable[] sentinel = new Closeable[1];
                      LongBuffer[] bb =
                          Arrays.stream(h.allocateBytesArr(size << 3, sentinel))
                              .map(ByteBuffer::asLongBuffer)
                              .toArray(LongBuffer[]::new);
                      try (Closeable c1 = sentinel[0]) {
                        for (int j = 0; j < size; j++) {
                          long v = j; // r.nextLong();
                          compare.put(j, v);
                          bb[j >> BLOCK_SHIFT].put((j & BLOCK_MASK), v);
                        }
                        for (int j = size - 1; j >= 0; j--) {
                          try {
                            assertEquals(compare.get(j), bb[j >> BLOCK_SHIFT].get(j & BLOCK_MASK));
                          } catch (AssertionError er) {
                            System.err.println(
                                "XXX "
                                    + Long.toUnsignedString(compare.get(j), 16)
                                    + " != "
                                    + Long.toUnsignedString(
                                    bb[j >> BLOCK_SHIFT].get(j & BLOCK_MASK), 16)
                                    + "; idx="
                                    + Integer.toUnsignedString(j, 16));
                            if (errCt.incrementAndGet() > 30) {
                              throw er;
                            }
                          }
                        }
                      } catch (IOException ex) {
                        throw new UncheckedIOException(ex);
                      } finally {
                        Reference.reachabilityFence(sentinel);
                      }
                    }
                  } catch (Throwable t) {
                    finished.set(true);
                    t.printStackTrace(System.err);
                    throw t;
                  }
                });
      }
      long endNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(N_SECONDS);
      long remainingNanos;
      while (!finished.get() && (remainingNanos = endNanos - System.nanoTime()) > 0) {
        System.out.println(
            "seconds remaining: "
                + TimeUnit.NANOSECONDS.toSeconds(remainingNanos)
                + ", activeThreads="
                + h.activeThreadCount()
                + ", outstanding="
                + h.outstandingCount()
                + ", allocated="
                + h.allocatedCount()
                + ", collected="
                + h.collectedCount()
                + ", exhausted="
                + h.exhaustedCount());
        Thread.sleep(Math.min(1000, TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
      }
      finished.set(true);
      for (Future<?> f : futures) {
        f.get();
      }
      long outstanding = Long.MAX_VALUE;
      long allocated = -1;
      long collected = -1;
      for (int i = 0; i < 10 && (outstanding > 0 || allocated != collected); i++) {
        System.gc();
        System.out.println(
            "activeThreads="
                + h.activeThreadCount()
                + ", available="
                + h.available()
                + ", outstanding="
                + (outstanding = h.outstandingCount())
                + ", allocated="
                + (allocated = h.allocatedCount())
                + ", collected="
                + (collected = h.collectedCount())
                + ", exhausted="
                + h.exhaustedCount());
        Thread.sleep(500);
      }
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
    ReferenceHandler<Dummy> rh =
        new ReferenceHandler<>(
            (a) -> {
              collectedRefs.increment();
              totalBytesOut.add(a.ramBytesUsed());
            },
            null);
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
