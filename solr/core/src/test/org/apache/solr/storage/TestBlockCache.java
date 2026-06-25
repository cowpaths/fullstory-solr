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
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.LongAdder;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;

public class TestBlockCache extends SolrTestCaseJ4 {

  private static final int N_STRESS_SECONDS = 10;

  // ---------------------------------------------------------------------------
  // Deterministic tests
  // ---------------------------------------------------------------------------

  /** All blocks pinned: acquireNode() must return null rather than spin forever. */
  public void testAllPinned() throws IOException {
    Path tmpDir = createTempDir();
    try (BlockCache cache =
        new BlockCache(2L * COMPRESSION_BLOCK_SIZE, tmpDir.resolve("cache.tmp"))) {
      Cache.Node<ByteBuffer, BlockCache.Val> n1 = cache.acquireNode();
      Cache.Node<ByteBuffer, BlockCache.Val> n2 = cache.acquireNode();
      assertNotNull(n1);
      assertNotNull(n2);
      assertNull("expected null when all blocks pinned", cache.acquireNode());

      // Unpin one; now a block is evictable.
      cache.unpin(n1);
      Cache.Node<ByteBuffer, BlockCache.Val> n3 = cache.acquireNode();
      assertNotNull(n3);
      assertNull("expected null with n2 and n3 both pinned", cache.acquireNode());

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
    try (BlockCache cache =
        new BlockCache(2L * COMPRESSION_BLOCK_SIZE, tmpDir.resolve("cache.tmp"))) {
      Cache.Node<ByteBuffer, BlockCache.Val> n1 = cache.acquireNode();
      Cache.Node<ByteBuffer, BlockCache.Val> n2 = cache.acquireNode();
      assertNotNull(n1);
      assertNotNull(n2);
      n1.getPayload().populate(dummy, 0, 0);
      n2.getPayload().populate(dummy, 0, 0);

      // Unpin both; n2 was acquired last so it sits at the LRU head.
      cache.unpin(n1);
      cache.unpin(n2);

      // Explicitly close n2 — should push its buffer to the tail (highest eviction priority).
      ByteBuffer n2Buf = n2.getPayload().join();
      cache.close(n2);

      // Next acquisition should reclaim n2's buffer from the tail.
      Cache.Node<ByteBuffer, BlockCache.Val> n3 = cache.acquireNode();
      assertNotNull(n3);
      n3.getPayload().populate(dummy, 0, 0);
      assertSame(
          "close() should recycle the buffer to tail for immediate reuse", n2Buf, n3.getPayload().join());

      cache.unpin(n3);
    }
  }

  /** pin() on an evicted node returns false; the node's buffer may be live under a new Node. */
  public void testPinEvictedNodeReturnsFalse() throws IOException {
    Path tmpDir = createTempDir();
    byte[] dummy = new byte[0];
    try (BlockCache cache =
        new BlockCache(COMPRESSION_BLOCK_SIZE, tmpDir.resolve("cache.tmp"))) {
      Cache.Node<ByteBuffer, BlockCache.Val> n1 = cache.acquireNode();
      assertNotNull(n1);
      n1.getPayload().populate(dummy, 0, 0);
      ByteBuffer n1Buf = n1.getPayload().join();
      cache.unpin(n1); // now evictable

      // Evict n1 by acquiring the only block.
      Cache.Node<ByteBuffer, BlockCache.Val> n2 = cache.acquireNode();
      assertNotNull(n2);
      n2.getPayload().populate(dummy, 0, 0);
      assertSame(n1Buf, n2.getPayload().join()); // same underlying buffer

      // n1 is dead — pin must fail.
      assertFalse("pin() on evicted node should return false", cache.pin(n1));

      cache.unpin(n2);
    }
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

      AtomicReferenceArray<Cache.Node<ByteBuffer, BlockCache.Val>> slots = new AtomicReferenceArray<>(nSlots);

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
                    while (!finished.get()) {
                      int slotIdx = r.nextInt(nSlots);

                      // --- Cache hit path: try to pin the existing node ---
                      Cache.Node<ByteBuffer, BlockCache.Val> existing = slots.get(slotIdx);
                      if (existing != null && cache.pin(existing)) {
                        hits.increment();
                        // Verify that the sentinel written at acquire time is intact.
                        assertEquals(slotIdx, existing.getPayload().join().getInt(0));
                        if (r.nextInt(8) == 0) {
                          // Explicit close: unpin, vacate slot, close.
                          cache.unpin(existing);
                          if (slots.compareAndSet(slotIdx, existing, null)) {
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
                      Cache.Node<ByteBuffer, BlockCache.Val> node = cache.acquireNode();
                      if (node == null) {
                        exhausted.increment();
                        continue;
                      }
                      acquired.increment();
                      // Write sentinel via populate() before publishing.
                      ByteBuffer.wrap(sentinelBuf)
                          .order(ByteOrder.LITTLE_ENDIAN)
                          .putInt(0, slotIdx);
                      node.getPayload().populate(sentinelBuf, 0, Integer.BYTES);
                      cache.unpin(node);
                      // Publish to slot; old occupant (if any) will be evicted via normal LRU.
                      Cache.Node<ByteBuffer, BlockCache.Val> prev = slots.getAndSet(slotIdx, node);
                      if (prev != null) {
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

      for (Future<?> f : futures) {
        f.get();
      }
      exec.shutdown();
      assertTrue(exec.awaitTermination(10, TimeUnit.SECONDS));
      assertFalse("stress test encountered an error", failed.get());

      System.out.printf(
          "done: acquired=%d, hits=%d, misses=%d, closed=%d, exhausted=%d%n",
          acquired.sum(), hits.sum(), misses.sum(), closed.sum(), exhausted.sum());
    }
  }
}
