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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.SolrTestCaseJ4;

/**
 * Verifies {@link LruLinkageStore}'s per-slot field isolation: no cross-slot or cross-field
 * aliasing from stride/offset arithmetic mistakes, and correct zero-initialization. This is the
 * test that would catch a stride/offset bug that plain bounds-checking cannot (e.g. two fields or
 * two slots silently overlapping within the buffer).
 */
public class TestLruLinkageStore extends SolrTestCaseJ4 {

  /** Fresh store: every field of every slot should read as zero/false. */
  public void testZeroInitialized() {
    int length = 100;
    LruLinkageStore store = new LruLinkageStore(length);
    for (int slot = 0; slot < length; slot++) {
      assertEquals("slot " + slot + " next", 0, store.getNextVolatile(slot));
      assertEquals("slot " + slot + " prev", 0, store.getPrevVolatile(slot));
      assertFalse("slot " + slot + " fromHot", store.getFromHotVolatile(slot));
      assertEquals("slot " + slot + " lastUnpinNanos", 0L, store.getLastUnpinNanos(slot));
    }
  }

  /**
   * Writes a distinct pattern into every field of every slot, then reads back every field of every
   * slot and asserts no cross-slot or cross-field aliasing.
   */
  public void testNoAliasing() {
    int length = 200;
    LruLinkageStore store = new LruLinkageStore(length);

    for (int slot = 0; slot < length; slot++) {
      store.setNextPlain(slot, slot * 4 + 1);
      store.setPrevPlain(slot, slot * 4 + 2);
      store.setFromHotVolatile(slot, (slot & 1) == 0);
      store.setLastUnpinNanos(slot, (long) slot * 1_000_000_007L + 3);
    }

    for (int slot = 0; slot < length; slot++) {
      assertEquals("slot " + slot + " next", slot * 4 + 1, store.getNextVolatile(slot));
      assertEquals("slot " + slot + " prev", slot * 4 + 2, store.getPrevVolatile(slot));
      assertEquals("slot " + slot + " fromHot", (slot & 1) == 0, store.getFromHotVolatile(slot));
      assertEquals(
          "slot " + slot + " lastUnpinNanos",
          (long) slot * 1_000_000_007L + 3,
          store.getLastUnpinNanos(slot));
    }
  }

  /** CAS/compareAndExchange semantics on {@code next}: success, failure, and witness value. */
  public void testNextCasSemantics() {
    LruLinkageStore store = new LruLinkageStore(10);
    int slot = 5;

    assertTrue(store.compareAndSetNext(slot, 0, 42));
    assertEquals(42, store.getNextVolatile(slot));
    assertFalse("CAS should fail on stale expected value", store.compareAndSetNext(slot, 0, 99));
    assertEquals(42, store.getNextVolatile(slot));

    int witness = store.compareAndExchangeNext(slot, 999, 7);
    assertEquals("witness should be the actual current value on failure", 42, witness);
    assertEquals(42, store.getNextVolatile(slot));

    witness = store.compareAndExchangeNext(slot, 42, 7);
    assertEquals("witness should equal expected on success", 42, witness);
    assertEquals(7, store.getNextVolatile(slot));

    // Neighboring slot must be untouched by all of the above.
    assertEquals(0, store.getNextVolatile(slot - 1));
    assertEquals(0, store.getNextVolatile(slot + 1));
  }

  /**
   * 16 threads concurrently CAS-looping increments on the same slot's {@code next} field; final
   * value must match the expected total, and neighboring slots must remain untouched -- validates
   * concurrent correctness of the CAS path specifically (not just single-threaded aliasing).
   */
  public void testConcurrentCasOnSameSlot() throws Exception {
    LruLinkageStore store = new LruLinkageStore(10);
    int slot = 5;
    int nThreads = 16;
    int itersPerThread = 5000;

    ExecutorService pool = Executors.newFixedThreadPool(nThreads);
    try {
      CountDownLatch start = new CountDownLatch(1);
      AtomicInteger errors = new AtomicInteger();
      for (int t = 0; t < nThreads; t++) {
        pool.submit(
            () -> {
              try {
                start.await();
                for (int i = 0; i < itersPerThread; i++) {
                  int cur;
                  int next;
                  do {
                    cur = store.getNextVolatile(slot);
                    next = cur + 1;
                  } while (store.compareAndExchangeNext(slot, cur, next) != cur);
                }
              } catch (Exception e) {
                errors.incrementAndGet();
              }
            });
      }
      start.countDown();
      pool.shutdown();
      assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
      assertEquals(0, errors.get());
      assertEquals(nThreads * itersPerThread, store.getNextVolatile(slot));
      // Neighboring slots (sharing or not sharing a cache line, depending on stride) must be
      // unaffected by all this same-slot contention.
      assertEquals(0, store.getNextVolatile(slot - 1));
      assertEquals(0, store.getNextVolatile(slot + 1));
    } finally {
      pool.shutdownNow();
    }
  }
}
