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

import java.util.concurrent.TimeUnit;

/**
 * Fixed-capacity concurrent LRU cache.
 *
 * <p>Two field clusters live in external, off-heap stores rather than as {@link Val} instance
 * fields:
 *
 * <ul>
 *   <li>{@code refCount}/{@code generation} ("Group 1", in {@link RefCountGenerationStore}) --
 *       carries roughly 9x more write traffic than Group 2 (most pins are "re-pins" of an
 *       already-pinned slot -- a pure {@code refCount} CAS with no list manipulation). Stored one
 *       pair per 64-byte cache line, so no two slots' hottest fields ever share a line.
 *   <li>{@code next}/{@code prev}/{@code fromHot}/{@code lastUnpinNanos} ("Group 2", in {@link
 *       LruLinkageStore}) -- touched only on genuine first-pin/last-unpin list transitions. Stored
 *       2 slots per 64-byte line (lower-traffic tier than Group 1, so a looser packing is an
 *       acceptable tradeoff for roughly a third of the memory cost of 1-per-line).
 * </ul>
 *
 * <p>Colocating either group with the other, or with {@link Val}'s own residual fields (as earlier
 * versions of this class did), meant unrelated fields with very different traffic profiles shared a
 * cache line -- Group 1's hot CAS traffic needlessly invalidating Group 2's much colder
 * reads/writes and vice versa, and (for {@code BlockCache.Val} specifically) Group 2's write
 * traffic needlessly invalidating reads of {@code cacheBlockOrd}, which is read on essentially
 * every cache hit by an unrelated thread population (query-serving threads, as opposed to the
 * I/O-completion threads that drive list-splice writes).
 *
 * <p>Unlike {@link RefCountGenerationStore} (sized for real slots {@code 1..capacity} only, since
 * {@code refCount}/{@code generation} are never touched on sentinels), {@link LruLinkageStore} must
 * cover every addressable slot, sentinels included: sentinel {@link Val}s
 * (COLD_HEAD/COLD_TAIL/HOT_HEAD/HOT_TAIL) are the literal list-boundary pointers, with their {@code
 * next}/{@code prev} read and written on essentially every list operation.
 *
 * <p>The LRU semantics, pin/unpin reference-count protocol, and hot/cold dual-queue logic are
 * identical to {@link Cache} and {@link Cache.DualQueueCache}.
 *
 * <p><b>LRU invariant:</b> a slot is in the doubly-linked list if and only if its {@code refCount}
 * is 0 (evictable). Pinned slots (refCount &gt; 0) are spliced out of the list.
 *
 * <ul>
 *   <li>{@link #pin}: first pin (CAS 0&rarr;1) removes the slot from the list; re-pins (CAS
 *       N&rarr;N+1, N&gt;0) are pure refcount increments.
 *   <li>{@link #unpin}: last unpin (result=0) inserts the slot at the list head (most-recently
 *       used); higher unpins are pure refcount decrements.
 *   <li>{@link #acquireNode}: evicts the tail slot, resets its payload via {@link Val#reset(int)},
 *       and returns it pinned (refCount=1).
 * </ul>
 *
 * <p>The capacity is fixed at construction. {@link #acquireNode} writes an opaque {@code long}
 * handle encoding the slot index and a generation counter into a caller-supplied {@code long[1]}
 * array. {@link BlockCache#encodeHandle} ORs in the partition index to form the full
 * cross-partition handle stored in {@code accessMapped}.
 */
class Cache2<V extends Cache2.Val> {

  // ---------------------------------------------------------------------------
  // Val hierarchy
  // ---------------------------------------------------------------------------

  static class Val {
    void reset() {
      // no-op default impl;
    }
  }

  // ---------------------------------------------------------------------------
  // Sentinels
  // ---------------------------------------------------------------------------

  // Sentinel link values stored in LruLinkageStore's next field to signal list-operation state.
  // Analogous to the RESERVED / REMOVED Node sentinels in Cache, but encoded as ints rather than
  // as object references.
  private static final int RESERVED_LINK = Integer.MIN_VALUE;
  private static final int REMOVED_LINK = Integer.MIN_VALUE + 1;

  // Slot 0 is reserved and never allocated; NULL_SLOT doubles as the null handle sentinel.
  static final int NULL_SLOT = 0;

  private static final int SLOT_MASK = (1 << 20) - 1;

  /** Cold queue: head sentinel (most-recently-used end). Index = {@code capacity + 1}. */
  protected final int COLD_HEAD;

  /** Cold queue: tail sentinel (eviction end). Index = {@code capacity + 2}. */
  protected final int COLD_TAIL;

  /**
   * Multiplier applied to the hot tail's age during eviction scoring. A hot slot is evicted only
   * when the cold tail is at least {@code HOT_EVICTION_MULTIPLIER} times as stale, giving hot slots
   * proportional protection regardless of absolute time scale.
   */
  private static final int HOT_EVICTION_SHIFT = 2;

  /** A year should be stale enough for our purposes, without risking overflow. */
  private static final long VERY_STALE = TimeUnit.DAYS.toNanos(365);

  /**
   * Returns the age (nanos since last unpin) of a slot given its {@code lastUnpinNanos} stamp.
   * Returns a large sentinel age for never-used slots ({@code lastUnpinNanos == 0}) so they are
   * always preferred for eviction over any slot that has been genuinely used.
   */
  private static long age(long now, long lastUnpinNanos) {
    return lastUnpinNanos == 0 ? VERY_STALE : now - lastUnpinNanos;
  }

  // ---------------------------------------------------------------------------
  // Payload array (covers real slots 0..capacity and sentinel slots)
  // ---------------------------------------------------------------------------

  final int capacity;

  /**
   * Payload for all slot indices 0..arrayLen-1. Slot 0 is reserved (null). Real slots are
   * 1..capacity. Sentinel slots (COLD_HEAD, COLD_TAIL, and optionally HOT_HEAD, HOT_TAIL) occupy
   * the remaining indices, holding placeholder {@link Val} objects (their {@code next}/{@code prev}
   * live in {@link #lruLinkage}, indexed the same way as any other slot). Elements are set at
   * construction and never swapped.
   */
  final Val[] payload;

  /**
   * Group 1 (refCount/generation) storage -- see the class javadoc for why this isn't a {@link Val}
   * field. Sized for real slots {@code 1..capacity} only: sentinel slots (COLD_HEAD/COLD_TAIL
   * /HOT_HEAD/HOT_TAIL) never have {@code refCount}/{@code generation} read by any live code path
   * (confirmed: {@code acquireTail}'s loop/guard conditions structurally exclude sentinels from
   * ever reaching a {@code refCount} CAS; {@code generation} is never touched on a sentinel at
   * all), so no sentinel coverage is needed here, unlike {@link #lruLinkage}.
   */
  final RefCountGenerationStore refCountGeneration;

  /**
   * Group 2 (next/prev/fromHot/lastUnpinNanos) storage -- see the class javadoc. Sized to cover
   * every addressable slot, sentinels included (unlike {@link #refCountGeneration}): sentinel
   * {@link Val}s are the literal list-boundary pointers, with {@code next}/{@code prev} read and
   * written on essentially every list operation.
   */
  final LruLinkageStore lruLinkage;

  // ---------------------------------------------------------------------------
  // Constructor
  // ---------------------------------------------------------------------------

  /**
   * Initializing constructor: wires {@code initialValues} into the LRU list in order (first value
   * at the head, last at the tail). Equivalent to calling a no-arg constructor followed by
   * insertAtTail for each value, but faster because no CAS is needed during single-threaded
   * initialization.
   *
   * @param capacity number of real slots
   * @param extraSentinelSlots additional sentinel slots beyond COLD_HEAD/COLD_TAIL (e.g. 2 for
   *     {@link DualQueueCache}'s HOT_HEAD/HOT_TAIL)
   * @param initialValues Val objects in desired initial LRU order (head→tail)
   */
  Cache2(int capacity, int extraSentinelSlots, Iterable<? extends V> initialValues) {
    this.capacity = capacity;
    // Real slots start at refCount=0/generation=0 (the zero-initialized default), matching the
    // implicit-zero `new Val(i--, 0)` call this construction replaces.
    this.refCountGeneration = new RefCountGenerationStore(capacity);
    this.COLD_HEAD = capacity + 1;
    this.COLD_TAIL = capacity + 2;
    int arrayLen = capacity + 3 + extraSentinelSlots;
    this.payload = new Val[arrayLen];
    this.lruLinkage = new LruLinkageStore(arrayLen);
    assert COLD_TAIL < arrayLen;

    // Placeholder sentinel Val objects for the cold queue sentinels -- next/prev live in
    // lruLinkage, indexed by COLD_HEAD/COLD_TAIL directly; these objects are never read.
    // DualQueueCache initializes HOT_HEAD / HOT_TAIL sentinel Vals in its own constructor.
    this.payload[COLD_HEAD] = new Val();
    this.payload[COLD_TAIL] = new Val();

    // Build cold-queue chain: COLD_HEAD ↔ slot_1 ↔ slot_2 ↔ … ↔ COLD_TAIL.
    // Slot 0 is reserved (null) and never linked. Single-threaded init; plain writes fine.
    int prevIdx = COLD_HEAD;
    int slot = 1;
    for (V v : initialValues) {
      if (slot > capacity) throw new IllegalArgumentException("too many initial values");
      payload[slot] = v;
      lruLinkage.setNextPlain(prevIdx, slot);
      lruLinkage.setPrevPlain(slot, prevIdx);
      prevIdx = slot++;
    }
    lruLinkage.setNextPlain(prevIdx, COLD_TAIL);
    lruLinkage.setPrevPlain(COLD_TAIL, prevIdx);
    // COLD_HEAD.prev is never read (head sentinel has no meaningful predecessor).
  }

  // ---------------------------------------------------------------------------
  // Payload access / reset hook
  // ---------------------------------------------------------------------------

  @SuppressWarnings("unchecked")
  final V getPayload(long handle) {
    return (V) payload[(int) handle & SLOT_MASK];
  }

  /** True if the slot identified by {@code handle} was most recently routed to the hot queue. */
  final boolean fromHot(long handle) {
    return lruLinkage.getFromHotVolatile((int) handle & SLOT_MASK);
  }

  // ---------------------------------------------------------------------------
  // LRU list operations
  // ---------------------------------------------------------------------------

  /**
   * Atomically acquires the exclusive right to modify {@code slot}'s next link. Spins while the
   * link is held by another thread (RESERVED_LINK), then CAS-swaps it to {@code reservation} and
   * returns the prior value. Returns REMOVED_LINK immediately if the slot has already been spliced
   * out.
   */
  private int reserve(int slot, int reservation) {
    int cur = lruLinkage.getNextVolatile(slot);
    for (; ; ) {
      while (cur == RESERVED_LINK) {
        if (reservation == REMOVED_LINK) {
          Thread.yield();
        }
        cur = lruLinkage.getNextVolatile(slot);
      }
      if (cur == REMOVED_LINK) {
        return cur;
      }
      int witness = lruLinkage.compareAndExchangeNext(slot, cur, reservation);
      if (witness == cur) {
        return cur;
      }
      cur = witness;
    }
  }

  protected boolean insertAtHead(int listHead, int slot, boolean recordAccess) {
    lruLinkage.setPrevVolatile(slot, listHead);
    int oldNext = reserve(listHead, RESERVED_LINK);
    assert oldNext != REMOVED_LINK : "queue head sentinel should never be removed";
    lruLinkage.setNextPlain(slot, oldNext);
    lruLinkage.setPrevVolatile(oldNext, slot);
    if (!lruLinkage.compareAndSetNext(listHead, RESERVED_LINK, slot)) {
      throw new IllegalStateException("unexpected concurrent modification of listHead.next");
    }
    return false;
  }

  private void removeFromList(int slot) {
    int oldNext = reserve(slot, REMOVED_LINK);
    if (oldNext == REMOVED_LINK) {
      throw new IllegalStateException("already removed");
    }
    int prevSlot;
    for (; ; ) {
      prevSlot = lruLinkage.getPrevVolatile(slot);
      if (lruLinkage.compareAndSetNext(prevSlot, slot, RESERVED_LINK)) {
        break;
      }
      Thread.yield();
    }
    lruLinkage.setPrevVolatile(oldNext, prevSlot);
    if (!lruLinkage.compareAndSetNext(prevSlot, RESERVED_LINK, oldNext)) {
      throw new IllegalStateException("unexpected concurrent modification during list removal");
    }
  }

  private void insertAtTail(int slot) {
    for (; ; ) {
      int predSlot = lruLinkage.getPrevVolatile(COLD_TAIL);
      int oldNext = reserve(predSlot, RESERVED_LINK);
      if (oldNext != COLD_TAIL) {
        if (oldNext == REMOVED_LINK) {
          continue; // pred was concurrently removed; retry
        }
        // release reservation; pred is no longer tail's predecessor
        if (!lruLinkage.compareAndSetNext(predSlot, RESERVED_LINK, oldNext)) {
          throw new IllegalStateException();
        }
        continue;
      }
      lruLinkage.setNextPlain(slot, COLD_TAIL);
      lruLinkage.setPrevVolatile(slot, predSlot);
      lruLinkage.setPrevVolatile(COLD_TAIL, slot);
      if (!lruLinkage.compareAndSetNext(predSlot, RESERVED_LINK, slot)) {
        throw new IllegalStateException("unexpected concurrent modification during tail insertion");
      }
      return;
    }
  }

  // ---------------------------------------------------------------------------
  // Pin / unpin
  // ---------------------------------------------------------------------------

  private static final int UNPIN_SENTINEL = Integer.MIN_VALUE >> 1;

  /**
   * Pins {@code slot} for the duration of a read, preventing eviction.
   *
   * <p>If this is the <em>first</em> pin (refCount transitions 0&rarr;1), the slot is removed from
   * the LRU list (it is no longer evictable). If the slot is already pinned (refCount&gt;0), the
   * refcount is simply incremented with no list operation.
   *
   * <p>Returns 1 if this was a first pin (refCount 0&rarr;1, slot removed from evictable list), 0
   * if the slot was already pinned (refCount incremented only), -1 if the slot is permanently dead
   * or if the handle's encoded generation does not match the slot's current generation (stale).
   */
  int pin(long handle) {
    int slot = (int) handle & SLOT_MASK;
    assert slot >= 1 && slot <= capacity;
    int expectedGen = (int) (handle >>> 32);
    if (refCountGeneration.getGenerationVolatile(slot) != expectedGen) return -1;
    int rc = refCountGeneration.getRefCountVolatile(slot);
    for (; ; ) {
      switch (rc) {
        case -1:
          return -1;
        case UNPIN_SENTINEL:
          rc = refCountGeneration.getRefCountVolatile(slot);
          continue;
      }
      assert rc >= 0;
      int witness = refCountGeneration.compareAndExchangeRefCount(slot, rc, rc + 1);
      if (witness == rc) {
        break;
      } else {
        rc = witness;
      }
    }
    if (rc == 0) {
      // First pin: remove from the evictable list.
      removeFromList(slot);
    }
    // Check generation after acquiring the pin. The CAS above is a volatile read-modify-write
    // that establishes happens-before with the generation write preceding the volatile refCount
    // write in acquireNode, so this generation read is guaranteed to reflect the current
    // generation. If mismatched, the slot was recycled; unpin() correctly reverses the full pin
    // (including re-insertion into the LRU list for the rc==0 case via UNPIN_SENTINEL →
    // insertAtHead).
    if (refCountGeneration.getGenerationVolatile(slot) != expectedGen) {
      unpin(handle, false);
      return -1;
    }
    return rc == 0 ? 1 : 0;
  }

  /**
   * Non-mutating optimistic check: returns {@code true} if the slot identified by {@code handle}
   * appears live and pinnable (refCount &ge; 0, not evicted, generation matches). No CAS is
   * performed; may race with concurrent eviction. Suitable for best-effort preload skip checks.
   */
  Val pinnable(long handle) {
    int slot = (int) handle & SLOT_MASK;
    assert slot >= 1 && slot <= capacity;
    int expectedGen = (int) (handle >>> 32);
    switch (refCountGeneration.getRefCountVolatile(slot)) {
      case UNPIN_SENTINEL:
      case -1:
        return null;
      default:
        return refCountGeneration.getGenerationVolatile(slot) == expectedGen ? payload[slot] : null;
    }
  }

  /**
   * Releases a read pin. If this is the last pin (refCount transitions 1&rarr;0), the slot is
   * inserted at the LRU head (most-recently-used position) and becomes evictable.
   *
   * <p>Returns 1 if last unpin and the slot was routed to the hot queue, 0 if last unpin and routed
   * to the cold queue (or base class with no hot/cold distinction), -1 if not the last unpin
   * (refCount decremented only).
   */
  int unpin(long handle, boolean recordAccess) {
    int slot = (int) handle & SLOT_MASK;
    assert slot >= 1 && slot <= capacity;
    int rc = refCountGeneration.getRefCountVolatile(slot);
    for (; ; ) {
      if (rc == 1) {
        int witness = refCountGeneration.compareAndExchangeRefCount(slot, 1, UNPIN_SENTINEL);
        if (witness == 1) {
          boolean toHot = insertAtHead(COLD_HEAD, slot, recordAccess);
          if (!refCountGeneration.compareAndSetRefCount(slot, UNPIN_SENTINEL, 0)) {
            throw new IllegalStateException();
          }
          return toHot ? 1 : 0;
        } else {
          rc = witness;
        }
      } else {
        assert rc > 1 : "unexpected refCount: " + rc;
        int witness = refCountGeneration.compareAndExchangeRefCount(slot, rc, rc - 1);
        if (witness == rc) {
          return -1;
        } else {
          rc = witness;
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Acquire / release
  // ---------------------------------------------------------------------------

  /**
   * Acquires a pinned slot whose payload is ready to be used. Evicts the least-recently-used
   * evictable slot from the tail of the list, resets its payload via {@link Val#reset(int)}, and
   * returns it pinned (refCount=1) and not yet in the list.
   *
   * <p>Returns {@link #NULL_SLOT} if the list is empty (all slots are pinned).
   */
  protected int acquireTail() {
    for (int candidate; (candidate = lruLinkage.getPrevVolatile(COLD_TAIL)) != COLD_HEAD; ) {
      if (refCountGeneration.compareAndSetRefCount(candidate, 0, -1)) {
        return candidate;
      }
    }
    return NULL_SLOT;
  }

  /**
   * Evicts the LRU slot, resets it, and returns it pinned (refCount=1). Writes the opaque handle
   * (generation&lt;&lt;32|slot) into {@code outHandle[0]}. Returns {@code null} (and leaves {@code
   * outHandle} unmodified) if all slots are pinned.
   */
  @SuppressWarnings("unchecked")
  V acquireNode(long[] outHandle) {
    int slot = acquireTail();
    if (slot == NULL_SLOT) {
      return null;
    }
    removeFromList(slot);
    // NOTE: generation increment here is safe because we hold an effective lock
    // on this slot here and this is the only place generation is modified.
    int newGen = refCountGeneration.incrementAndGetGeneration(slot);
    Val p = payload[slot];
    p.reset();
    if (!refCountGeneration.compareAndSetRefCount(slot, -1, 1)) {
      throw new IllegalStateException();
    }
    outHandle[0] = (long) newGen << 32 | slot;
    return (V) p;
  }

  /**
   * Explicitly releases a slot when its owning resource is closed. If the slot is still evictable
   * (refCount==0 and in the list), marks it dead and recycles it back to the tail, making it the
   * highest-priority reuse candidate. Returns {@code 0} if the slot was successfully released,
   * existing refCount if the slot was already evicted or pinned (best-effort; caller need not
   * retry).
   */
  int close(long handle) {
    int slot = (int) handle & SLOT_MASK;
    assert slot >= 1 && slot <= capacity;
    int ret = refCountGeneration.compareAndExchangeRefCount(slot, 0, -1);
    if (ret == 0) {
      // successfully closed
      removeFromList(slot);
      // Keep rc=-1 during insertion so acquireTail cannot grab this slot mid-insertion
      // (its CAS(0→-1) won't match rc=-1). reset() then bump generation -- order between the two
      // doesn't matter for correctness (refCount==-1 grants exclusivity across this whole window
      // regardless), but the generation bump must land before any handle read before this close()
      // can pin() again: pin()'s pre-CAS generation check will then fail, so a stale pinner cannot
      // successfully pin -- or hang in join() -- after this slot has been recycled.
      payload[slot].reset();
      refCountGeneration.incrementAndGetGeneration(slot);
      insertAtTail(slot);
      // refCount -> 0 makes reclaimable
      if (!refCountGeneration.compareAndSetRefCount(slot, -1, 0)) {
        throw new IllegalStateException();
      }
    }
    return ret;
  }

  /**
   * Recycles a slot back to the tail of the eviction list. Used when a slot was acquired (pinned,
   * refCount=1 from acquireNode) but ultimately not needed (e.g. lost CAS race).
   *
   * <p>Normally the caller is the only pinner, but a stale {@link #pin} that passed its pre-CAS
   * generation check before the slot was recycled may have raced and bumped refCount above 1. In
   * that case we decrement our own pin and return; the stale pinner's post-CAS generation mismatch
   * will call {@link #unpin} and drive refCount to 0, reinserting the slot into the LRU.
   */
  void closeUnconditional(long handle) {
    int slot = (int) handle & SLOT_MASK;
    assert slot >= 1 && slot <= capacity;
    int rc = refCountGeneration.getRefCountVolatile(slot);
    for (; ; ) {
      if (rc == 1) {
        // Sole holder: use -1 as intermediate to block acquireTail during tail insertion.
        int witness = refCountGeneration.compareAndExchangeRefCount(slot, 1, -1);
        if (witness == 1) {
          insertAtTail(slot);
          if (!refCountGeneration.compareAndSetRefCount(slot, -1, 0)) {
            throw new IllegalStateException("rc changed during insertAtTail in closeUnconditional");
          }
          return;
        }
        rc = witness;
      } else if (rc > 1) {
        // A stale pin() raced: its CAS retry loop incremented rc past 1 before its post-CAS
        // generation check could call unpin(). Decrement our own pin and return; the stale
        // pinner's subsequent unpin() will see rc==1 and reinstate this slot in the LRU.
        int witness = refCountGeneration.compareAndExchangeRefCount(slot, rc, rc - 1);
        if (witness == rc) {
          return;
        }
        rc = witness;
      } else {
        throw new IllegalStateException("unexpected rc=" + rc + " in closeUnconditional");
      }
    }
  }

  static class DualQueueCache<V extends Val> extends Cache2<V> {

    /** Running memoization of last seen non-zero cold queue timestamp */
    private volatile long coldTs;

    /** Hot queue: head sentinel. Slots promoted from cold are inserted here. */
    private final int HOT_HEAD;

    /** Hot queue: tail sentinel. */
    private final int HOT_TAIL;

    DualQueueCache(int capacity, Iterable<? extends V> initialValues) {
      super(capacity, 2, initialValues);
      this.HOT_HEAD = capacity + 3;
      this.HOT_TAIL = capacity + 4;
      assert HOT_TAIL < lruLinkage.length();
      // Placeholder sentinel Val objects for the hot queue -- next/prev live in lruLinkage.
      payload[HOT_HEAD] = new Val();
      payload[HOT_TAIL] = new Val();
      lruLinkage.setNextPlain(HOT_HEAD, HOT_TAIL);
      lruLinkage.setPrevPlain(HOT_TAIL, HOT_HEAD);
    }

    @Override
    protected final boolean insertAtHead(int head, int slot, boolean recordAccess) {
      long lastUnpin = lruLinkage.getLastUnpinNanos(slot);
      lruLinkage.setLastUnpinNanos(slot, recordAccess ? System.nanoTime() : 0);
      boolean toHot = toHot(lastUnpin);
      lruLinkage.setFromHotVolatile(slot, toHot);
      super.insertAtHead(toHot ? HOT_HEAD : head, slot, recordAccess);
      return toHot;
    }

    @Override
    protected int acquireTail() {
      long coldCandidateTs;
      int candidate;
      boolean fromHot;
      long now = System.nanoTime();
      do {
        int coldCandidate = lruLinkage.getPrevVolatile(COLD_TAIL);
        int hotCandidate = lruLinkage.getPrevVolatile(HOT_TAIL);
        if (coldCandidate == COLD_HEAD) {
          // cold queue is empty
          if (hotCandidate == HOT_HEAD) {
            // both queues empty
            return NULL_SLOT;
          }
          candidate = hotCandidate;
          fromHot = true;
          coldCandidateTs = 0;
        } else if (hotCandidate == HOT_HEAD) {
          candidate = coldCandidate;
          fromHot = false;
          coldCandidateTs = lruLinkage.getLastUnpinNanos(coldCandidate);
        } else {
          // Evict from hot only when cold is at least HOT_EVICTION_MULTIPLIER times as stale,
          // giving hot slots proportional protection. age() maps lastUnpinNanos=0 to a large
          // sentinel so never-used cold slots are always preferred over any real hot slot.
          coldCandidateTs = lruLinkage.getLastUnpinNanos(coldCandidate);
          fromHot =
              age(now, coldCandidateTs)
                  < age(now, lruLinkage.getLastUnpinNanos(hotCandidate)) >> HOT_EVICTION_SHIFT;
          candidate = fromHot ? hotCandidate : coldCandidate;
        }
      } while (!refCountGeneration.compareAndSetRefCount(candidate, 0, -1));
      if (coldCandidateTs != 0) {
        this.coldTs = coldCandidateTs;
      }
      lruLinkage.setFromHotVolatile(candidate, fromHot);
      return candidate;
    }

    private boolean toHot(long prev) {
      // Promote to hot if previously used and fresher than the last known cold-tail threshold.
      // coldTs tracks the most recently observed non-zero cold tail timestamp (updated by
      // acquireNode on each eviction). When zero (initial warm-up), fall back to hot.
      long threshold;
      return prev != 0 && ((threshold = coldTs) == 0 || threshold - prev < 0);
    }
  }
}
