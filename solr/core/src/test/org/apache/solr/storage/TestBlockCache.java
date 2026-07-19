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
package org.apache.solr.storage;

import static org.apache.solr.storage.CompressingDirectory.COMPRESSION_BLOCK_SIZE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;

public class TestBlockCache extends SolrTestCaseJ4 {

  private static final int N_STRESS_SECONDS = 10;

  // ---------------------------------------------------------------------------
  // Deterministic tests
  // ---------------------------------------------------------------------------

  /** All blocks pinned: acquireNode() must return NULL_HANDLE rather than spin forever. */
  public void testAllPinned() throws IOException {
    Path tmpDir = createTempDir();
    try (BlockCache cache =
        new BlockCache(2L * COMPRESSION_BLOCK_SIZE, tmpDir.resolve("cache.tmp"))) {
      long[] h = new long[1];
      assertNotNull(cache.acquireNode(h));
      long n1 = h[0];
      assertNotNull(cache.acquireNode(h));
      long n2 = h[0];
      assertNull("expected null when all blocks pinned", cache.acquireNode(h));

      // Unpin one; now a block is evictable.
      cache.unpin(n1);
      assertNotNull(cache.acquireNode(h));
      long n3 = h[0];
      assertNull("expected null with n2 and n3 both pinned", cache.acquireNode(h));

      cache.unpin(n2);
      cache.unpin(n3);
    }
  }

  /**
   * close() recycles the buffer to the tail. The next acquireNode() must return a node backed by
   * the same ByteBuffer (highest eviction priority).
   */
  public void testCloseRecyclesToTail() throws IOException {
    Path tmpDir = createTempDir();
    byte[] dummy = new byte[0];
    byte[] expect = new byte[COMPRESSION_BLOCK_SIZE];
    random().nextBytes(expect);
    try (BlockCache cache =
        new BlockCache(2L * COMPRESSION_BLOCK_SIZE, tmpDir.resolve("cache.tmp"))) {
      long[] h = new long[1];
      BlockCache.Val v1 = cache.acquireNode(h);
      long n1 = h[0];
      BlockCache.Val v2 = cache.acquireNode(h);
      long n2 = h[0];
      assertNotNull(v1);
      assertNotNull(v2);
      v1.populate(dummy, 0, 0, null, 0, cache);
      v2.populate(expect, 0, COMPRESSION_BLOCK_SIZE, null, 0, cache); // put real data in

      // Unpin both; n2 was acquired last so it sits at the LRU head.
      cache.unpin(n1);
      cache.unpin(n2);

      // Explicitly close n2 — should push its buffer to the tail (highest eviction priority).
      ByteBuffer n2Buf = v2.join(cache);
      cache.close(n2);

      // Next acquisition should reclaim n2's buffer from the tail.
      BlockCache.Val v3 = cache.acquireNode(h);
      long n3 = h[0];
      assertNotNull(v3);
      ByteBuffer n3Buf = v3.populate(dummy, 0, 0, null, 0, cache);
      assertSame(n3Buf, v3.join(cache));
      assertNotSame(n2Buf, n3Buf);
      byte[] rtx = new byte[COMPRESSION_BLOCK_SIZE];
      n3Buf.clear().get(rtx);
      assertArrayEquals(
          "close() should recycle the buffer to tail for immediate reuse", expect, rtx);

      cache.unpin(n3);
    }
  }

  /** pin() on an evicted node returns false; the node's buffer may be live under a new Node. */
  public void testPinEvictedNodeReturnsFalse() throws IOException {
    Path tmpDir = createTempDir();
    Random r = random();
    byte[] dummy = new byte[COMPRESSION_BLOCK_SIZE];
    r.nextBytes(dummy);
    try (BlockCache cache = new BlockCache(COMPRESSION_BLOCK_SIZE, tmpDir.resolve("cache.tmp"))) {
      long[] h = new long[1];
      BlockCache.Val v1 = cache.acquireNode(h);
      long n1 = h[0];
      assertNotNull(v1);
      ByteBuffer n1Buf = v1.populate(dummy, 0, COMPRESSION_BLOCK_SIZE, null, 0, cache);
      assertSame(n1Buf, v1.join(cache));
      cache.unpin(n1); // now evictable

      // Evict n1 by acquiring the only block.
      BlockCache.Val v2 = cache.acquireNode(h);
      long n2 = h[0];
      assertNotNull(v2);
      ByteBuffer n2Buf = v2.populate(new byte[0], 0, 0, null, 0, cache);
      assertSame(n2Buf, v2.join(cache));
      assertNotSame(n1Buf, n2Buf); // different wrappers
      byte[] rtx = new byte[COMPRESSION_BLOCK_SIZE];
      n2Buf.clear().get(rtx);
      assertArrayEquals(dummy, rtx); // should have same backing bytes

      // n1 is dead — pin must fail.
      assertNull("pin() on evicted node should return null", cache.pin(n1));

      cache.unpin(n2);
    }
  }

  /**
   * Warm-start concurrent stress test: pre-populates a persistent cache, reopens it, then hammers
   * the warm-start acquireNode path from N threads. Each thread repeatedly acquires a random block
   * (hitting the extant map) and immediately unpins it, exercising concurrent warm-start pins and
   * last-unpin races on the same slots.
   */
  public void testWarmStartStress() throws Exception {
    final int nBlocks = 4;
    final int N = 8;
    final int ITERS = 1000;

    Path tmpDir = createTempDir();
    Path cachePath = tmpDir.resolve("cache.tmp");
    // Each block gets a UUID whose LSB encodes its index; MSB is zero.
    // extantMapLookup key = (blobUUID, blockIdx=0) per block.

    // Pre-create backing file at the correct size.
    try (FileChannel fc =
        FileChannel.open(cachePath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
      fc.write(ByteBuffer.wrap(new byte[1]), BlockCache.backingFileBytes(nBlocks) - 1);
    }

    // Populate pass: fill every block with a sentinel (its index).
    try (BlockCache cache = new BlockCache(cachePath)) {
      long[] h = new long[1];
      byte[] sentinel = new byte[Integer.BYTES];
      for (int idx = 0; idx < nBlocks; idx++) {
        UUID uuid = new UUID(0, idx);
        BlockCache.Val v = cache.acquireNode(h, uuid, 0);
        assertNotNull(v);
        ByteBuffer.wrap(sentinel).putInt(0, idx);
        v.populate(sentinel, 0, Integer.BYTES, uuid, 0, cache);
        cache.unpin(h[0]);
      }
    }

    // Stress pass: reopen (warm start) and hammer acquireNode from N threads.
    ExecutorService exec =
        ExecutorUtil.newMDCAwareFixedThreadPool(
            N, new SolrNamedThreadFactory("testWarmStartStress"));
    @SuppressWarnings("rawtypes")
    Future<?>[] futures = new Future[N];
    Phaser p = new Phaser(N + 1);
    AtomicReference<BlockCache> c = new AtomicReference<>(new BlockCache(cachePath));
    int valCount = nBlocks + 3;
    AtomicLongArray handles = new AtomicLongArray(valCount);
    for (int i = 0; i < N; i++) {
      Random r = new Random(random().nextLong());
      futures[i] =
          exec.submit(
              () -> {
                try {
                  long[] h = new long[1];
                  BlockCache cache;
                  p.arriveAndAwaitAdvance();
                  while ((cache = c.get()) != null) {
                    for (int iter = 0; iter < 128; iter++) {
                      int idx = r.nextInt(valCount);
                      UUID uuid = new UUID(0, idx);
                      long handle = handles.get(idx);
                      BlockCache.Val v;
                      if (handle != BlockCache.NULL_HANDLE && (v = cache.pin(handle)) != null) {
                        ByteBuffer join = v.join(cache);
                        assertEquals(idx, join.getInt(0));
                        cache.unpin(handle);
                      } else {
                        v = cache.acquireNode(h, uuid, 0);
                        if (v != null) {
                          long extant = handles.compareAndExchange(idx, handle, h[0]);
                          if (extant == handle) {
                            handle = h[0];
                            if (v.isPopulated()) {
                              ByteBuffer join = v.join(cache);
                              assertEquals(idx, join.getInt(0));
                            } else {
                              byte[] arr =
                                  ByteBuffer.allocate(Integer.BYTES).putInt(0, idx).array();
                              v.populate(arr, 0, Integer.BYTES, uuid, 0, cache);
                            }
                          } else {
                            cache.close(h[0], v);
                            v = cache.pin(extant);
                            if (v == null) {
                              continue; // whatever
                            }
                            handle = extant;
                            ByteBuffer join = v.join(cache);
                            assertEquals(idx, join.getInt(0));
                          }
                          cache.unpin(handle);
                        }
                      }
                    }
                    p.arriveAndAwaitAdvance();
                    p.arriveAndAwaitAdvance();
                  }
                  return null;
                } catch (Throwable t) {
                  t.printStackTrace(System.err);
                  throw t;
                }
              });
    }
    p.arriveAndAwaitAdvance();
    for (int i = 1; i <= ITERS; i++) {
      p.arriveAndAwaitAdvance();
      BlockCache cache = c.getAndSet(null);
      cache.close();
      if (i == ITERS) {
        c.set(null);
        p.arriveAndAwaitAdvance();
      } else {
        // Clear stale handles before making the new cache visible: after reopen all slots reset
        // to generation=0, so any handle from the old cache whose generation now matches would
        // pass pin()'s generation check and double-pin a freshly-acquired slot.
        for (int k = valCount - 1; k >= 0; k--) {
          handles.set(k, BlockCache.NULL_HANDLE);
        }
        c.set(new BlockCache(cachePath));
        p.arrive();
      }
    }
    for (Future<?> f : futures) {
      f.get();
    }
    exec.shutdown();
    assertTrue(exec.awaitTermination(5, TimeUnit.SECONDS));
  }

  // ---------------------------------------------------------------------------
  // Concurrent stress test
  // ---------------------------------------------------------------------------

  /**
   * Stress test: multiple threads concurrently exercise acquire, pin/unpin, and explicit close().
   * Threads simulate the accessMapped pattern: a shared slot array holds the most-recently-cached
   * Node per logical block. Each thread picks a random slot, tries to pin the existing node (cache
   * hit), or acquires a new one (cache miss / eviction). A sentinel value written at acquire time
   * is verified on every successful pin, proving that pinned buffers are never concurrently
   * overwritten.
   */
  @SuppressWarnings("AssertionFailureIgnored") // for now ... TODO: re-evaluate
  public void testStress() throws InterruptedException, ExecutionException, IOException {
    final int nBlocks = 32;
    final int nSlots = 64; // more slots than blocks to force eviction
    final int nThreads = 16;

    Path tmpDir = createTempDir();
    try (BlockCache cache =
        new BlockCache((long) nBlocks * COMPRESSION_BLOCK_SIZE, tmpDir.resolve("cache.tmp"))) {

      AtomicLongArray slots = new AtomicLongArray(nSlots); // zero-initializes to NULL_HANDLE

      ExecutorService exec =
          ExecutorUtil.newMDCAwareFixedThreadPool(
              nThreads, new SolrNamedThreadFactory("testBlockCache"));
      AtomicBoolean finished = new AtomicBoolean(false);
      AtomicBoolean failed = new AtomicBoolean(false);

      LongAdder acquired = new LongAdder();
      LongAdder hits = new LongAdder();
      LongAdder misses = new LongAdder();
      LongAdder closed = new LongAdder();
      LongAdder exhausted = new LongAdder();

      @SuppressWarnings("rawtypes")
      Future<?>[] futures = new Future[nThreads];
      for (int i = 0; i < nThreads; i++) {
        Random r = new Random(random().nextLong());
        futures[i] =
            exec.submit(
                () -> {
                  try {
                    byte[] sentinelBuf = new byte[Integer.BYTES];
                    long[] nodeHandle = new long[1];
                    while (!finished.get()) {
                      int slotIdx = r.nextInt(nSlots);

                      // --- Cache hit path: try to pin the existing node ---
                      long existing = slots.get(slotIdx);
                      BlockCache.Val existingVal = null;
                      if (existing != BlockCache.NULL_HANDLE
                          && (existingVal = cache.pin(existing)) != null) {
                        hits.increment();
                        // Verify that the sentinel written at acquire time is intact.
                        assertEquals(slotIdx, existingVal.join(cache).getInt(0));
                        if (r.nextInt(8) == 0) {
                          // Explicit close: unpin, vacate slot, close.
                          cache.unpin(existing);
                          if (slots.compareAndSet(slotIdx, existing, BlockCache.NULL_HANDLE)) {
                            cache.close(existing);
                            closed.increment();
                          }
                          // If CAS lost the race, another thread updated the slot; just move on.
                        } else {
                          cache.unpin(existing);
                        }
                        continue;
                      }

                      // --- Cache miss path: acquire a new node (evicts LRU if needed) ---
                      BlockCache.Val nodeVal = cache.acquireNode(nodeHandle);
                      if (nodeVal == null) {
                        exhausted.increment();
                        continue;
                      }
                      long node = nodeHandle[0];
                      acquired.increment();
                      // Write sentinel via populate() before publishing.
                      ByteBuffer.wrap(sentinelBuf).putInt(0, slotIdx);
                      nodeVal.populate(sentinelBuf, 0, Integer.BYTES, null, 0, cache);
                      cache.unpin(node);
                      // Publish to slot; old occupant (if any) will be evicted via normal LRU.
                      long prev = slots.getAndSet(slotIdx, node);
                      if (prev != BlockCache.NULL_HANDLE) {
                        misses.increment();
                      }
                    }
                  } catch (Throwable t) {
                    failed.set(true);
                    finished.set(true);
                    t.printStackTrace(System.err);
                  }
                });
      }

      long endNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(N_STRESS_SECONDS);
      long remainingNanos;
      while (!finished.get() && (remainingNanos = endNanos - System.nanoTime()) > 0) {
        System.out.printf(
            "seconds remaining: %d, acquired=%d, hits=%d, misses=%d, closed=%d, exhausted=%d%n",
            TimeUnit.NANOSECONDS.toSeconds(remainingNanos),
            acquired.sum(),
            hits.sum(),
            misses.sum(),
            closed.sum(),
            exhausted.sum());
        Thread.sleep(Math.min(1000, TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
      }
      finished.set(true);

      Throwable th = null;
      for (Future<?> f : futures) {
        try {
          f.get();
        } catch (Throwable t) {
          th = IOUtils.useOrSuppress(th, t);
        }
      }
      exec.shutdown();
      if (th != null) {
        throw new RuntimeException(th);
      }
      assertTrue(exec.awaitTermination(10, TimeUnit.SECONDS));
      assertFalse("stress test encountered an error", failed.get());

      System.out.printf(
          "done: acquired=%d, hits=%d, misses=%d, closed=%d, exhausted=%d%n",
          acquired.sum(), hits.sum(), misses.sum(), closed.sum(), exhausted.sum());
    }
  }
}
