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

import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.ThreadInterruptedException;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.ref.Reference;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.WeakHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public class MyStressTest {

  private static final int N_SECONDS = 30;

  private static final int BLOCK_SHIFT = BitDocSet.BIT_SHIFT - 6;
  private static final int BLOCK_MASK = (1 << BLOCK_SHIFT) - 1;

  public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
    int busyFactor = Integer.parseInt(args[0]); // default 5
    int nThreads = Integer.parseInt(args[1]); // default 5
    boolean doMemWrite = Boolean.parseBoolean(args[2]); // default false
    testHeapCacheFbs(busyFactor, nThreads, doMemWrite);
  }

  @SuppressWarnings("try")
  private static void testHeapCacheFbs(int busyFactor, int nThreads, boolean doMemWrite) throws InterruptedException, ExecutionException, IOException {
    Random random = new Random(0);
    LongAdder totalNanos = new LongAdder();
    LongAdder busyCt = new LongAdder();
    ExecutorService exec =
        ExecutorUtil.newMDCAwareFixedThreadPool(
            nThreads, new SolrNamedThreadFactory("testHeapCache"));
    try (Closeable c = () -> ExecutorUtil.shutdownAndAwaitTermination(exec);
        HeapCacheFbsModifier h = HeapCacheFbsModifier.getInstance()) {
      AtomicBoolean finished = new AtomicBoolean(false);
      @SuppressWarnings("rawtypes")
      Future<?>[] futures = new Future[nThreads];
      AtomicInteger errCt = new AtomicInteger();
      int maxSize = 16 << 20; // Math.max(1 << 16, (SortedIntDocSet.MAX_ARR_SIZE >> 1) * 16);
      for (int i = 0; i < nThreads; i++) {
        Random r = new Random(random.nextLong());
        byte[] bytes = new byte[256 << 10]; // 256k
        r.nextBytes(bytes);
        futures[i] =
            exec.submit(
                () -> {
                  try {
                    WeakHashMap<String, Object> blah = new WeakHashMap<>();
                    while (!finished.get()) {
                      if (r.nextInt(busyFactor) != 0) {
                        int ct = 0;
                        for (byte b : bytes) {
                          if (b == 0) {
                            ct++;
                          }
                        }
                        blah.put(Integer.toString(r.nextInt()), new byte[r.nextInt(8 << 10)]);
                        busyCt.add(ct);
                        continue;
                      }
                      int size = r.nextInt(maxSize);
                      LongBuffer compare = LongBuffer.allocate(size);
                      Closeable[] sentinel = new Closeable[1];
                      long start = System.nanoTime();
                      FixedBitSet.ByteBufferStruct[] bbs =
                          h.allocateBytesArr(size << 3, sentinel, false);
                      totalNanos.add(System.nanoTime() - start);
                      LongBuffer[] bb =
                          Arrays.stream(bbs)
                              .map(FixedBitSet.ByteBufferStruct::asLongBufferStruct)
                              .map((lbs) -> lbs.buf)
                              .toArray(LongBuffer[]::new);
                      try (Closeable c1 = r.nextBoolean() ? sentinel[0] : null) {
                        for (int j = 0; j < size && doMemWrite; j++) {
                          long v = r.nextLong(); // = j; // for debugging
                          compare.put(j, v);
                          bb[j >> BLOCK_SHIFT].put((j & BLOCK_MASK), v);
                        }
                        for (int j = size - 1; j >= 0 && doMemWrite; j--) {
                          try {
                            if (compare.get(j) != bb[j >> BLOCK_SHIFT].get(j & BLOCK_MASK)) {
                              throw new AssertionError();
                            }
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
                + ", busyCt="
                + (busyCt.sum() >> 20) + "M"
                + ", outstanding="
                + h.outstandingCount()
                + ", allocated="
                + blocksToGB(h.allocatedCount())
                + "G, collected="
                + blocksToGB(h.collectedCount())
                + "G, exhausted="
                + blocksToGB(h.exhaustedCount()) + "G");
        Thread.sleep(Math.min(1000, TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
        System.gc();
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
                + blocksToGB(allocated = h.allocatedCount())
                + "G, collected="
                + blocksToGB(collected = h.collectedCount())
                + "G, exhausted="
                + blocksToGB(h.exhaustedCount()) + "G");
        Thread.sleep(500);
      }
      long totalNanosComplete = totalNanos.sum();
      long total = h.allocatedCount() + h.exhaustedCount();
      System.err.println(
          "total millis: "
              + TimeUnit.NANOSECONDS.toMillis(totalNanosComplete)
              + ", nanos/M="
              + (totalNanosComplete / blocksToMB(total))
              + ", total="
              + blocksToGB(total)
              + "G");
      System.err.println("busy count "+(busyCt.sum() >> 20)+"M");
    }
  }
  private static long blocksToGB(long blocks) {
    return (blocks * HeapCacheFbsModifier.BLOCK_SIZE_BYTES) >> 30;
  }
  private static long blocksToMB(long blocks) {
    return (blocks * HeapCacheFbsModifier.BLOCK_SIZE_BYTES) >> 20;
  }
}
