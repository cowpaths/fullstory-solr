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
import java.util.concurrent.TimeUnit;

/**
 * Fixed-capacity concurrent LRU cache backed by parallel {@code int[]} arrays for the doubly-linked
 * list structure. Compared to {@link Cache} (which stores {@code next}/{@code prev} as object
 * references inside each node), the primitive arrays are not traversed by the GC, reducing the
 * number of GC-scanned references from 3N to N (just the payload array).
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
 *   <li>{@link #acquireNode}: evicts the tail slot, resets its payload via {@link #resetPayload},
 *       and returns the slot index pinned (refCount=1).
 * </ul>
 *
 * <p>The capacity is fixed at construction. {@link #acquireNode()} returns an opaque {@code long}
 * handle encoding the slot index and a generation counter (or -1L when exhausted). {@link
 * BlockCache#encodeHandle} ORs in the partition index to form the full cross-partition handle
 * stored in {@code accessMapped}.
 */
class Cache2<V extends Cache2.Val> {

  // ---------------------------------------------------------------------------
  // Val hierarchy
  // ---------------------------------------------------------------------------

  static class Val {
    /**
     * Reference count.
     *
     * <ul>
     *   <li>&ge;1 — pinned (not in LRU list)
     *   <li>0 — evictable (in LRU list)
     *   <li>-1 — permanently dead (evicted); slot must not be used
     * </ul>
     */
    private volatile int refCount;

    // Incremented by acquireNode() on each recycle, before the volatile write to refCount.
    // Readers who volatile-read refCount before reading this field are guaranteed to see the
    // current value (happens-before via the volatile write in resetPayload).
    private volatile int generation;

    Val(int initialRefCount) {
      this.refCount = initialRefCount;
    }

    int refCount() {
      return refCount;
    }

    void reset(int newRefCount) {
      refCount = newRefCount;
    }
  }

  private static final VarHandle REF_COUNT;

  static {
    try {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      REF_COUNT = lookup.findVarHandle(Val.class, "refCount", int.class);
    } catch (ReflectiveOperationException e) {
      throw new Error(e);
    }
  }

  // ---------------------------------------------------------------------------
  // Sentinels
  // ---------------------------------------------------------------------------

  // Sentinel link values stored in next[] to signal list-operation state.
  // Analogous to the RESERVED / REMOVED Node sentinels in Cache, but encoded as ints
  // so the int[] arrays remain GC-opaque.
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
  // Fixed arrays
  // ---------------------------------------------------------------------------

  private final int capacity;

  /**
   * {@code next[i]} = successor slot index toward the head, or {@link #RESERVED_LINK}/{@link
   * #REMOVED_LINK}. Indexed over real slots + sentinel slots. CAS'd via {@link #INT_ARRAY_VH}.
   */
  protected final int[] next;

  /**
   * {@code prev[i]} = predecessor slot index toward the tail. Reads/writes use
   * getVolatile/setVolatile via {@link #INT_ARRAY_VH} to match the {@code volatile} semantics of
   * {@link Cache}'s {@code Node.prev} field.
   */
  protected final int[] prev;

  /**
   * VarHandle for CAS on {@code int[]} elements (shared for both {@link #next} and {@link #prev}).
   */
  private static final VarHandle INT_ARRAY_VH;

  static {
    try {
      INT_ARRAY_VH = MethodHandles.arrayElementVarHandle(int[].class);
    } catch (Exception e) {
      throw new Error(e);
    }
  }

  /**
   * Payload for slots 0..capacity. Slot 0 is reserved (always null); real slots are 1..capacity.
   * Elements are set at construction and never swapped; only the Val's internal fields change over
   * time. Since elements are never replaced, GC sees exactly N live references here (vs. 3N for a
   * traditional doubly-linked Node<V> array).
   */
  final Val[] payload;

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
  private Cache2(int capacity, int extraSentinelSlots, Iterable<? extends V> initialValues) {
    this.capacity = capacity;
    this.COLD_HEAD = capacity + 1;
    this.COLD_TAIL = capacity + 2;
    int arrayLen = capacity + 3 + extraSentinelSlots;
    this.next = new int[arrayLen];
    this.prev = new int[arrayLen];
    this.payload = new Val[capacity + 1];

    // Build cold-queue chain: COLD_HEAD ↔ slot_1 ↔ slot_2 ↔ … ↔ COLD_TAIL.
    // Slot 0 is reserved (null) and never linked. Single-threaded init; plain array writes fine.
    int prevIdx = COLD_HEAD;
    int slot = 1;
    for (V v : initialValues) {
      if (slot > capacity) throw new IllegalArgumentException("too many initial values");
      payload[slot] = v;
      next[prevIdx] = slot;
      prev[slot] = prevIdx;
      prevIdx = slot++;
    }
    next[prevIdx] = COLD_TAIL;
    prev[COLD_TAIL] = prevIdx;
    // COLD_HEAD.prev is never read (head sentinel has no meaningful predecessor).
  }

  // ---------------------------------------------------------------------------
  // Payload access / reset hook
  // ---------------------------------------------------------------------------

  @SuppressWarnings("unchecked")
  final V getPayload(long handle) {
    return (V) payload[(int) handle & SLOT_MASK];
  }

  /**
   * Resets a slot's payload for reuse after it is claimed by {@link #acquireNode()}. Subclasses
   * override to reset application-specific fields (e.g. populated, cached) in addition to refCount.
   */
  protected void resetPayload(int slot, int newRefCount) {
    payload[slot].reset(newRefCount);
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
    int cur = (int) INT_ARRAY_VH.getVolatile(next, slot);
    for (; ; ) {
      while (cur == RESERVED_LINK) {
        if (reservation == REMOVED_LINK) {
          Thread.yield();
        }
        cur = (int) INT_ARRAY_VH.getVolatile(next, slot);
      }
      if (cur == REMOVED_LINK) {
        return cur;
      }
      int witness = (int) INT_ARRAY_VH.compareAndExchange(next, slot, cur, reservation);
      if (witness == cur) {
        return cur;
      }
      cur = witness;
    }
  }

  protected boolean insertAtHead(int listHead, int slot, boolean recordAccess) {
    INT_ARRAY_VH.setVolatile(prev, slot, listHead);
    int oldNext = reserve(listHead, RESERVED_LINK);
    assert oldNext != REMOVED_LINK : "queue head sentinel should never be removed";
    next[slot] = oldNext;
    INT_ARRAY_VH.setVolatile(prev, oldNext, slot);
    if (!INT_ARRAY_VH.compareAndSet(next, listHead, RESERVED_LINK, slot)) {
      throw new IllegalStateException("unexpected concurrent modification of listHead.next");
    }
    return false;
  }

  private boolean removeFromList(int slot) {
    int oldNext = reserve(slot, REMOVED_LINK);
    if (oldNext == REMOVED_LINK) {
      return false;
    }
    int prevSlot;
    for (; ; ) {
      prevSlot = (int) INT_ARRAY_VH.getVolatile(prev, slot);
      if (INT_ARRAY_VH.compareAndSet(next, prevSlot, slot, RESERVED_LINK)) {
        break;
      }
      Thread.yield();
    }
    INT_ARRAY_VH.setVolatile(prev, oldNext, prevSlot);
    if (!INT_ARRAY_VH.compareAndSet(next, prevSlot, RESERVED_LINK, oldNext)) {
      throw new IllegalStateException("unexpected concurrent modification during list removal");
    }
    return true;
  }

  private void insertAtTail(int slot) {
    for (; ; ) {
      int predSlot = (int) INT_ARRAY_VH.getVolatile(prev, COLD_TAIL);
      int oldNext = reserve(predSlot, RESERVED_LINK);
      if (oldNext != COLD_TAIL) {
        if (oldNext == REMOVED_LINK) {
          continue; // pred was concurrently removed; retry
        }
        // release reservation; pred is no longer tail's predecessor
        if (!INT_ARRAY_VH.compareAndSet(next, predSlot, RESERVED_LINK, oldNext)) {
          throw new IllegalStateException();
        }
        continue;
      }
      next[slot] = COLD_TAIL;
      INT_ARRAY_VH.setVolatile(prev, slot, predSlot);
      INT_ARRAY_VH.setVolatile(prev, COLD_TAIL, slot);
      if (!INT_ARRAY_VH.compareAndSet(next, predSlot, RESERVED_LINK, slot)) {
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
    int expectedGen = (int) (handle >>> 32);
    Val p = payload[slot];
    if (p.generation != expectedGen) return -1;
    int rc = p.refCount;
    for (; ; ) {
      switch (rc) {
        case -1:
          return -1;
        case UNPIN_SENTINEL:
          rc = p.refCount;
          continue;
      }
      assert rc >= 0;
      int witness = (int) REF_COUNT.compareAndExchange(p, rc, rc + 1);
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
    // write in acquireNode, so p.generation is guaranteed to reflect the current generation.
    // If mismatched, the slot was recycled; unpin() correctly reverses the full pin (including
    // re-insertion into the LRU list for the rc==0 case via UNPIN_SENTINEL → insertAtHead).
    if (p.generation != expectedGen) {
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
  boolean pinnable(long handle) {
    int slot = (int) handle & SLOT_MASK;
    int expectedGen = (int) (handle >>> 32);
    Val p = payload[slot];
    int rc = p.refCount;
    return rc != -1 && rc != UNPIN_SENTINEL && p.generation == expectedGen;
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
    Val p = payload[slot];
    assert p != null;
    int rc = p.refCount;
    for (; ; ) {
      if (rc == 1) {
        int witness = (int) REF_COUNT.compareAndExchange(p, 1, UNPIN_SENTINEL);
        if (witness == 1) {
          boolean toHot = insertAtHead(COLD_HEAD, slot, recordAccess);
          if (!REF_COUNT.compareAndSet(p, UNPIN_SENTINEL, 0)) {
            throw new IllegalStateException();
          }
          return toHot ? 1 : 0;
        } else {
          rc = witness;
        }
      } else {
        assert rc > 1;
        int witness = (int) REF_COUNT.compareAndExchange(p, rc, rc - 1);
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
   * evictable slot from the tail of the list, resets its payload via {@link #resetPayload}, and
   * returns it pinned (refCount=1) and not yet in the list.
   *
   * <p>Returns {@link #NULL_SLOT} if the list is empty (all slots are pinned).
   */
  protected int acquireTail() {
    for (int candidate;
        (candidate = (int) INT_ARRAY_VH.getVolatile(prev, COLD_TAIL)) != COLD_HEAD; ) {
      Val p = payload[candidate];
      if (p != null && REF_COUNT.compareAndSet(p, 0, -1)) {
        return candidate;
      }
    }
    return NULL_SLOT;
  }

  long acquireNode() {
    int slot = acquireTail();
    if (slot == NULL_SLOT) {
      return 0L;
    }
    if (!removeFromList(slot)) {
      throw new IllegalStateException();
    }
    // NOTE: generation increment here is safe because we hold an effective lock
    // on this slot here and this is the only place generation is modified.
    int newGen = ++payload[slot].generation;
    resetPayload(slot, 1);
    return (long) newGen << 32 | slot;
  }

  /**
   * Explicitly releases a slot when its owning resource is closed. If the slot is still evictable
   * (refCount==0 and in the list), marks it dead and recycles it back to the tail, making it the
   * highest-priority reuse candidate. Returns {@code true} if the slot was successfully released,
   * {@code false} if the slot was already evicted or pinned (best-effort; caller need not retry).
   */
  boolean close(long handle) {
    int slot = (int) handle & SLOT_MASK;
    Val p = payload[slot];
    if (p == null || !REF_COUNT.compareAndSet(p, 0, -1)) {
      // pinned or already dead — best effort, bail
      return false;
    }
    if (removeFromList(slot)) {
      // Keep rc=-1 ("dead") through insertion so that concurrent pin() attempts see -1 and bail
      // immediately rather than racing into a window where rc==0 but the slot is not yet in the
      // list. After insertAtTail the slot is safely linked; only then expose it as evictable.
      resetPayload(slot, -1);
      insertAtTail(slot);
      p.reset(0);
    } else {
      throw new IllegalStateException();
    }
    return true;
  }

  /**
   * Unconditionally recycles a slot back to the tail. Used when a slot was acquired (pinned,
   * refCount=1) but ultimately not needed (e.g. lost CAS race).
   *
   * <p>Only call if the caller is the only possible owner of a pin on this slot.
   */
  void closeUnconditional(long handle) {
    int slot = (int) handle & SLOT_MASK;
    resetPayload(slot, 0);
    insertAtTail(slot);
  }

  static class TsVal extends Val {

    /**
     * Nanosecond timestamp of the most recent {@link Cache2#unpin}, or {@code 0} if never unpinned.
     * {@code 0} is treated as maximally stale, making never-used slots the highest-priority
     * eviction candidates.
     */
    private long lastUnpinNanos;

    /**
     * True if this slot was most recently routed to the hot queue by {@link
     * DualQueueCache#insertAtHead}. Set through {@link #resetPayload} on eviction so that {@link
     * BlockCache} can maintain per-queue counters without any list traversal. Volatile for
     * cross-thread visibility between the unpinning thread (writer) and the evicting/pinning thread
     * (reader).
     */
    private volatile boolean fromHot;

    boolean fromHot() {
      return fromHot;
    }

    void reset(int newRefCount) {
      super.reset(newRefCount);
      lastUnpinNanos = 0;
    }

    TsVal(int initialRefCount) {
      super(initialRefCount);
    }
  }

  private static long lastUnpinNanos(TsVal v) {
    return v == null ? 0 : v.lastUnpinNanos;
  }

  static class DualQueueCache<V extends TsVal> extends Cache2<V> {

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
      next[HOT_HEAD] = HOT_TAIL;
      prev[HOT_TAIL] = HOT_HEAD;
    }

    @Override
    protected final boolean insertAtHead(int head, int slot, boolean recordAccess) {
      TsVal tvp = (TsVal) payload[slot];
      long prev = tvp.lastUnpinNanos;
      tvp.lastUnpinNanos = recordAccess ? System.nanoTime() : 0;
      boolean toHot = toHot(prev);
      tvp.fromHot = toHot;
      super.insertAtHead(toHot ? HOT_HEAD : head, slot, recordAccess);
      return toHot;
    }

    @Override
    protected int acquireTail() {
      long coldCandidateTs;
      int candidate;
      boolean fromHot;
      long now = System.nanoTime();
      Val p;
      do {
        int coldCandidate = (int) INT_ARRAY_VH.getVolatile(prev, COLD_TAIL);
        int hotCandidate = (int) INT_ARRAY_VH.getVolatile(prev, HOT_TAIL);
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
          coldCandidateTs = lastUnpinNanos((TsVal) payload[coldCandidate]);
        } else {
          // Evict from hot only when cold is at least HOT_EVICTION_MULTIPLIER times as stale,
          // giving hot slots proportional protection. age() maps lastUnpinNanos=0 to a large
          // sentinel so never-used cold slots are always preferred over any real hot slot.
          coldCandidateTs = lastUnpinNanos((TsVal) payload[coldCandidate]);
          fromHot =
              age(now, coldCandidateTs)
                  < age(now, lastUnpinNanos((TsVal) payload[hotCandidate])) >> HOT_EVICTION_SHIFT;
          candidate = fromHot ? hotCandidate : coldCandidate;
        }
      } while ((p = payload[candidate]) == null || !REF_COUNT.compareAndSet(p, 0, -1));
      if (coldCandidateTs != 0) {
        this.coldTs = coldCandidateTs;
      }
      ((TsVal) payload[candidate]).fromHot = fromHot;
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
