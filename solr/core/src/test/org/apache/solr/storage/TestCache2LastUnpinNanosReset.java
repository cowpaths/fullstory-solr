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

import java.util.ArrayList;
import java.util.List;
import org.apache.solr.SolrTestCaseJ4;

/**
 * Regression test: {@code lastUnpinNanos} lives in {@code Cache2}'s off-heap Group 2 storage, not
 * as a {@code Val} field, so {@code Val.reset()} alone cannot clear it on slot recycle. Both {@link
 * Cache2#acquireNode(long[])} and {@link Cache2#close(long)} must explicitly zero it via {@code
 * setLastUnpinNanos(slot, 0)}; without that, a recycled slot would carry its previous occupant's
 * timestamp into the new occupant's first {@code toHot()} decision (incorrectly promoting a
 * first-time entry to the hot queue) and into {@code acquireTail()}'s staleness comparison (the
 * slot would read as "fresher" than it should, i.e. not maximally stale, which {@code
 * lastUnpinNanos == 0} is specifically meant to represent for never-yet-unpinned slots).
 */
public class TestCache2LastUnpinNanosReset extends SolrTestCaseJ4 {

  private static Cache2.DualQueueCache<Cache2.Val> newCache(int capacity) {
    List<Cache2.Val> initialValues = new ArrayList<>(capacity);
    for (int i = 0; i < capacity; i++) {
      initialValues.add(new Cache2.Val());
    }
    return new Cache2.DualQueueCache<>(capacity, initialValues);
  }

  /**
   * On a freshly-constructed cache (slots are evictable immediately, never pinned), poison every
   * slot's lastUnpinNanos, then evict (acquireNode) each one and verify the store reads back 0
   * immediately after -- the exact value acquireNode() must write, not whatever a slot happened to
   * carry over from being poisoned (standing in for a previous occupant's leftover timestamp).
   */
  public void testAcquireNodeClearsPoisonedLastUnpinNanos() {
    int capacity = 8;
    Cache2.DualQueueCache<Cache2.Val> cache = newCache(capacity);

    // Poison every real slot's lastUnpinNanos to a distinctive nonzero value before any
    // acquireNode() call -- simulates a slot that was unpinned long ago in a previous generation.
    long poison = 987654321L;
    for (int slot = 1; slot <= capacity; slot++) {
      cache.setLastUnpinNanos(slot, poison);
    }

    long[] handle = new long[1];
    for (int i = 0; i < capacity; i++) {
      Cache2.Val v = cache.acquireNode(handle);
      assertNotNull("expected " + capacity + " evictable slots, failed at i=" + i, v);
      int slot = (int) handle[0] & ((1 << 20) - 1); // SLOT_MASK, mirrored from Cache2
      assertEquals(
          "acquireNode() must zero lastUnpinNanos on recycle, slot=" + slot,
          0L,
          cache.getLastUnpinNanos(slot));
    }
  }

  /**
   * Same check for {@link Cache2#close(long)}'s recycle path: a slot closed back to the pool must
   * also read back lastUnpinNanos==0, since it re-enters the cold list via {@code insertAtTail()}
   * directly (bypassing {@code DualQueueCache.insertAtHead()}'s own fresh-timestamp write).
   *
   * <p>{@code close()} only actually recycles a slot that is evictable ({@code refCount==0}), so
   * the slot must be unpinned first; the poisoned value is written only after that (and after
   * {@code unpin()}'s own {@code insertAtHead()} write), simulating staleness accumulated while the
   * slot sat idle in the cold list before being explicitly closed.
   */
  public void testCloseClearsPoisonedLastUnpinNanos() {
    int capacity = 4;
    Cache2.DualQueueCache<Cache2.Val> cache = newCache(capacity);

    long[] handle = new long[1];
    assertNotNull(cache.acquireNode(handle));
    long h = handle[0];
    int slot = (int) h & ((1 << 20) - 1);

    cache.unpin(h, false); // make the slot evictable (refCount -> 0); required for close() to act
    cache.setLastUnpinNanos(slot, 555555555L); // poison after unpin's own write

    assertEquals("close() should have recycled the now-evictable slot", 0, cache.close(h));
    assertEquals("close() must zero lastUnpinNanos on recycle", 0L, cache.getLastUnpinNanos(slot));
  }
}
