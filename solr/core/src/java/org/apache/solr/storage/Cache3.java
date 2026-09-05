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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * Fixed-capacity concurrent slot pool backed by a doubly-linked free list. Designed for tracking
 * live references where each slot is held by exactly one owner and returned to the pool when no
 * longer needed (e.g. {@link BlockCache} hold-refs).
 *
 * <p>Unlike {@link Cache2}, there is no LRU eviction, no generation counter, and no pin/unpin
 * reference-count layering. A slot is either free (in the free list, refCount==0) or held (not in
 * the list, refCount==1). {@link #acquire} returns {@link #NULL_SLOT} when the pool is exhausted;
 * callers should fall back to heap allocation. {@link #tryRelease} is the atomic "claim and return"
 * operation that prevents double-cleanup when two threads race to release the same slot.
 *
 * <p>Released slots are returned to the free-list head for prompt cache-warm reuse.
 */
class Cache3<V extends Cache3.Val> {

  // ---------------------------------------------------------------------------
  // Val
  // ---------------------------------------------------------------------------

  static class Val {
    /**
     * 0 = free (in free list), 1 = held (not in list), -1 = transitional (being claimed or
     * recycled).
     */
    volatile int refCount;

    /**
     * Doubly-linked free-list successor (toward the head) and predecessor (toward the tail).
     * Accessed atomically via {@link Cache3#NEXT_VH} and {@link Cache3#PREV_VH}.
     */
    int next;

    int prev;

    Val(int initialRefCount) {
      this.refCount = initialRefCount;
    }

    /**
     * Called by {@link Cache3#tryRelease} before re-inserting the slot into the free list.
     * Subclasses clear their application-specific fields here.
     */
    void reset() {}
  }

  private static final VarHandle REF_COUNT;
  static final VarHandle NEXT_VH;
  static final VarHandle PREV_VH;

  static {
    try {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      REF_COUNT = lookup.findVarHandle(Val.class, "refCount", int.class);
      NEXT_VH = lookup.findVarHandle(Val.class, "next", int.class);
      PREV_VH = lookup.findVarHandle(Val.class, "prev", int.class);
    } catch (ReflectiveOperationException e) {
      throw new Error(e);
    }
  }

  // ---------------------------------------------------------------------------
  // Sentinels
  // ---------------------------------------------------------------------------

  // Sentinel link values stored in Val.next to signal concurrent list-operation state.
  private static final int RESERVED_LINK = Integer.MIN_VALUE;
  private static final int REMOVED_LINK = Integer.MIN_VALUE + 1;

  /** Slot 0 is reserved and never allocated; returned by {@link #acquire} when the pool is full. */
  static final int NULL_SLOT = 0;

  /** Free-list head sentinel (most-recently-used / preferred-reuse end). */
  private final int HEAD;

  /** Free-list tail sentinel (least-recently-used end; acquire scans from here). */
  private final int TAIL;

  // ---------------------------------------------------------------------------
  // Payload array
  // ---------------------------------------------------------------------------

  /**
   * Payload for all slot indices. Slot 0 is reserved (never allocated). Real slots are 1..capacity.
   * HEAD and TAIL sentinels occupy the remaining two indices.
   */
  final Val[] payload;

  // ---------------------------------------------------------------------------
  // Constructor
  // ---------------------------------------------------------------------------

  /**
   * Constructs a pool of {@code capacity} slots, all initially free, backed by the supplied Val
   * objects (one per slot, in free-list order from head to tail).
   */
  Cache3(int capacity, Iterable<? extends V> initialValues) {
    this.HEAD = capacity + 1;
    this.TAIL = capacity + 2;
    this.payload = new Val[capacity + 3];

    // Sentinel Vals: rc=-1 so acquireTail's CAS(0→-1) never claims them.
    payload[HEAD] = new Val(-1);
    payload[TAIL] = new Val(-1);

    // Build free-list chain: HEAD ↔ slot_1 ↔ … ↔ slot_N ↔ TAIL.
    // Slot 0 is reserved and never linked. Single-threaded init; plain field writes fine.
    int prevIdx = HEAD;
    int slot = 1;
    for (V v : initialValues) {
      if (slot > capacity) throw new IllegalArgumentException("too many initial values");
      payload[slot] = v;
      payload[prevIdx].next = slot;
      v.prev = prevIdx;
      prevIdx = slot++;
    }
    payload[prevIdx].next = TAIL;
    payload[TAIL].prev = prevIdx;
  }

  // ---------------------------------------------------------------------------
  // Free-list operations
  // ---------------------------------------------------------------------------

  /**
   * Atomically acquires the exclusive right to modify {@code slot}'s next link. Spins while the
   * link is held by another thread (RESERVED_LINK), then CAS-swaps it to {@code reservation} and
   * returns the prior value. Returns REMOVED_LINK immediately if the slot has already been spliced
   * out.
   */
  private int reserve(int slot, int reservation) {
    Val v = payload[slot];
    int cur = (int) NEXT_VH.getVolatile(v);
    for (; ; ) {
      while (cur == RESERVED_LINK) {
        if (reservation == REMOVED_LINK) {
          Thread.yield();
        }
        cur = (int) NEXT_VH.getVolatile(v);
      }
      if (cur == REMOVED_LINK) {
        return cur;
      }
      int witness = (int) NEXT_VH.compareAndExchange(v, cur, reservation);
      if (witness == cur) {
        return cur;
      }
      cur = witness;
    }
  }

  private void insertAtHead(int slot) {
    Val vSlot = payload[slot];
    Val vHead = payload[HEAD];
    PREV_VH.setVolatile(vSlot, HEAD);
    int oldNext = reserve(HEAD, RESERVED_LINK);
    assert oldNext != REMOVED_LINK : "HEAD sentinel should never be removed";
    vSlot.next = oldNext;
    PREV_VH.setVolatile(payload[oldNext], slot);
    if (!NEXT_VH.compareAndSet(vHead, RESERVED_LINK, slot)) {
      throw new IllegalStateException("unexpected concurrent modification of HEAD.next");
    }
  }

  private boolean removeFromList(int slot) {
    int oldNext = reserve(slot, REMOVED_LINK);
    if (oldNext == REMOVED_LINK) return false;
    Val vSlot = payload[slot];
    int prevSlot;
    for (; ; ) {
      prevSlot = (int) PREV_VH.getVolatile(vSlot);
      if (NEXT_VH.compareAndSet(payload[prevSlot], slot, RESERVED_LINK)) {
        break;
      }
      Thread.yield();
    }
    PREV_VH.setVolatile(payload[oldNext], prevSlot);
    if (!NEXT_VH.compareAndSet(payload[prevSlot], RESERVED_LINK, oldNext)) {
      throw new IllegalStateException("unexpected concurrent modification during list removal");
    }
    return true;
  }

  // ---------------------------------------------------------------------------
  // Acquire / release
  // ---------------------------------------------------------------------------

  /**
   * Acquires a free slot from the pool. Returns the slot index (&ge;1) on success, or {@link
   * #NULL_SLOT} if the pool is exhausted. The acquired slot has refCount==1 and is not in the free
   * list; the caller owns it exclusively until {@link #tryRelease} is called.
   */
  int acquire() {
    for (int candidate; (candidate = (int) PREV_VH.getVolatile(payload[TAIL])) != HEAD; ) {
      Val p = payload[candidate];
      if (REF_COUNT.compareAndSet(p, 0, -1)) {
        if (!removeFromList(candidate)) {
          throw new IllegalStateException();
        }
        p.reset();
        // Volatile write: publishing step; marks slot as held.
        p.refCount = 1;
        return candidate;
      }
    }
    return NULL_SLOT;
  }

  /**
   * Atomically claims and releases slot {@code slot} back to the free list. Returns {@code true} if
   * this thread won the claim (CAS refCount 1&rarr;-1); {@code false} if another thread already
   * claimed it (concurrent explicit-close vs. GC-drain race). On success, {@link Val#reset} is
   * called before the slot becomes acquirable again.
   */
  boolean tryRelease(int slot) {
    Val p = payload[slot];
    if (!REF_COUNT.compareAndSet(p, 1, -1)) {
      return false;
    }
    p.reset(); // clear subclass fields; refCount is -1, slot not yet in free list
    insertAtHead(slot); // wire into list; can't be acquired yet (refCount=-1, not 0)
    p.refCount = 0; // volatile publish: slot is now free and acquirable
    return true;
  }

  // ---------------------------------------------------------------------------
  // Payload access
  // ---------------------------------------------------------------------------

  @SuppressWarnings("unchecked")
  final V getPayload(int slot) {
    return (V) payload[slot];
  }
}
