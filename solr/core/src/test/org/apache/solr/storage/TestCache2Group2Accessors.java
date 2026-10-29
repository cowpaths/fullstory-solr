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
 * Verifies {@link Cache2}'s Group 2 (next/prev/fromHot/lastUnpinNanos) off-heap field accessors: no
 * cross-slot or cross-field aliasing from stride/offset arithmetic mistakes, and correct CAS
 * semantics on {@code next}. This is the test that would catch a stride/offset bug that plain
 * bounds-checking cannot (e.g. two fields or two slots silently overlapping within the buffer).
 */
public class TestCache2Group2Accessors extends SolrTestCaseJ4 {

  private static Cache2 newCache(int capacity) {
    return new Cache2(capacity, 0);
  }

  /**
   * Writes a distinct pattern into every Group 2 field of every addressable slot (real slots and
   * sentinels), then reads back every field of every slot and asserts no cross-slot or cross-field
   * aliasing. {@code next}/{@code prev} are already populated by the constructor's chain-building
   * pass; this test overwrites them with its own distinct pattern before checking.
   */
  public void testNoAliasing() {
    int capacity = 200;
    Cache2 cache = newCache(capacity);
    int length = cache.totalSlots;

    for (int slot = 0; slot < length; slot++) {
      cache.setNextPlain(slot, slot * 4 + 1);
      cache.setPrevPlain(slot, slot * 4 + 2);
      cache.setFromHotVolatile(slot, (slot & 1) == 0);
      cache.setLastUnpinNanos(slot, (long) slot * 1_000_000_007L + 3);
    }

    for (int slot = 0; slot < length; slot++) {
      assertEquals("slot " + slot + " next", slot * 4 + 1, cache.getNextVolatile(slot));
      assertEquals("slot " + slot + " prev", slot * 4 + 2, cache.getPrevVolatile(slot));
      assertEquals("slot " + slot + " fromHot", (slot & 1) == 0, cache.getFromHotVolatile(slot));
      assertEquals(
          "slot " + slot + " lastUnpinNanos",
          (long) slot * 1_000_000_007L + 3,
          cache.getLastUnpinNanos(slot));
    }
  }

  /** CAS/compareAndExchange semantics on {@code next}: success, failure, and witness value. */
  public void testNextCasSemantics() {
    Cache2 cache = newCache(10);
    int slot = 5;

    assertTrue(cache.compareAndSetNext(slot, cache.getNextVolatile(slot), 42));
    assertEquals(42, cache.getNextVolatile(slot));
    assertFalse("CAS should fail on stale expected value", cache.compareAndSetNext(slot, 0, 99));
    assertEquals(42, cache.getNextVolatile(slot));

    int witness = cache.compareAndExchangeNext(slot, 999, 7);
    assertEquals("witness should be the actual current value on failure", 42, witness);
    assertEquals(42, cache.getNextVolatile(slot));

    witness = cache.compareAndExchangeNext(slot, 42, 7);
    assertEquals("witness should equal expected on success", 42, witness);
    assertEquals(7, cache.getNextVolatile(slot));
  }

  /**
   * 16 threads concurrently CAS-looping increments on the same slot's {@code next} field; final
   * value must match the starting value plus the expected total, and neighboring slots must remain
   * untouched -- validates concurrent correctness of the CAS path specifically (not just
   * single-threaded aliasing).
   */
  public void testConcurrentCasOnSameSlot() throws Exception {
    Cache2 cache = newCache(10);
    int slot = 5;
    int nThreads = 16;
    int itersPerThread = 5000;

    int before = cache.getNextVolatile(slot);
    int neighborBefore1 = cache.getNextVolatile(slot - 1);
    int neighborBefore2 = cache.getNextVolatile(slot + 1);

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
                    cur = cache.getNextVolatile(slot);
                    next = cur + 1;
                  } while (cache.compareAndExchangeNext(slot, cur, next) != cur);
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
      assertEquals(before + nThreads * itersPerThread, cache.getNextVolatile(slot));
      // Neighboring slots (sharing or not sharing a cache line, depending on stride) must be
      // unaffected by all this same-slot contention.
      assertEquals(neighborBefore1, cache.getNextVolatile(slot - 1));
      assertEquals(neighborBefore2, cache.getNextVolatile(slot + 1));
    } finally {
      pool.shutdownNow();
    }
  }
}
