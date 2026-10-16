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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Fixed-capacity concurrent LRU cache.
 *
 * <p>Two field clusters live in off-heap {@link ByteBuffer}s rather than as {@link Val} instance
 * fields:
 *
 * <ul>
 *   <li>{@code refCount}/{@code generation} ("Group 1", in {@link #group1}) -- carries roughly 9x
 *       more write traffic than Group 2 (most pins are "re-pins" of an already-pinned slot -- a
 *       pure {@code refCount} CAS with no list manipulation). Stored one pair per 64-byte cache
 *       line, so no two slots' hottest fields ever share a line. Sized for real slots {@code
 *       1..capacity} only: sentinel slots (COLD_HEAD/COLD_TAIL/HOT_HEAD/HOT_TAIL) never have {@code
 *       refCount}/{@code generation} read by any live code path.
 *   <li>{@code next}/{@code prev}/{@code fromHot}/{@code lastUnpinNanos} ("Group 2", in {@link
 *       #group2}) -- touched only on genuine first-pin/last-unpin list transitions. Stored 2 slots
 *       per 64-byte line (lower-traffic tier than Group 1, so a looser packing is an acceptable
 *       tradeoff for roughly a third of the memory cost of 1-per-line). Unlike Group 1, covers
 *       every addressable slot, sentinels included: sentinel {@link Val}s are the literal
 *       list-boundary pointers, with their {@code next}/{@code prev} read and written on
 *       essentially every list operation.
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
  // Off-heap storage: Group 1 (refCount/generation) and Group 2 (next/prev/fromHot/lastUnpinNanos)
  // ---------------------------------------------------------------------------
  //
  // Both groups are backed by a single 64-byte-aligned direct ByteBuffer, accessed via
  // byteBufferViewVarHandle -- plain java.nio/java.lang.invoke, available since Java 9, no
  // preview APIs or platform-specific code required. allocateAlignedDirect() (below) handles the
  // allocation/alignment/margin arithmetic shared by both groups.
  //
  // Each store's stride is a power of 2, so slot -> byte-offset is a plain left shift
  // (slot << *_STRIDE_SHIFT) rather than a multiply -- safe from overflow without a per-access
  // check, since each constructor's one-time Math.multiplyExact(slots, STRIDE) already proves the
  // largest valid offset fits in an int.

  private static final VarHandle INT_VH =
      MethodHandles.byteBufferViewVarHandle(int[].class, ByteOrder.nativeOrder());
  private static final VarHandle LONG_VH =
      MethodHandles.byteBufferViewVarHandle(long[].class, ByteOrder.nativeOrder());

  /**
   * Group 1 (refCount/generation) storage -- see the class javadoc. One {@code (refCount,
   * generation)} pair per 64-byte stride (at byte offsets 0 and {@link #GENERATION_OFFSET} within
   * the stride; the remaining 56 bytes are unused padding), indexed by real slot {@code
   * 1..capacity} (slot 0 reserved/unused but still addressable).
   */
  private final ByteBuffer group1;

  private static final int GROUP1_STRIDE = 64; // one cache line per slot; must be a power of 2
  private static final int GROUP1_STRIDE_SHIFT = Integer.numberOfTrailingZeros(GROUP1_STRIDE);
  // refCount's offset within the stride is 0, so it needs no named offset constant -- just the
  // raw slot base.
  private static final int GENERATION_OFFSET = 4;

  /**
   * Group 2 (next/prev/fromHot/lastUnpinNanos) storage -- see the class javadoc. 2 slots per
   * 32-byte stride (comfortably more than the 17 bytes of real data per slot: {@code next} 4B +
   * {@code prev} 4B + {@code fromHot} 1B + {@code lastUnpinNanos} 8B), indexed by every addressable
   * slot (real slots and all sentinels).
   *
   * <p>Method semantics intentionally mirror today's exact per-field access patterns rather than
   * uniformly using volatile/CAS everywhere: {@code next} is mixed plain/volatile/CAS; {@code prev}
   * is volatile-only (never CAS'd); {@code lastUnpinNanos} is plain (a racy heuristic used only for
   * eviction-priority scoring, not correctness-critical); {@code fromHot} is a volatile boolean,
   * never CAS'd.
   *
   * <p>TODO: slot numbering is sequential/adjacent-correlated with real-world access-burst
   * adjacency ({@code BlockCache.distribute()} assigns consecutive array slots to consecutive/
   * file-adjacent {@code cacheBlockOrd} values). With 2 slots sharing a line, the two slots paired
   * together are presently whichever two land at adjacent even/odd stride offsets -- i.e.
   * likely-adjacent-access slots. Randomizing/decorrelating which two slots pair up on a line
   * (independent of numeric adjacency) is deferred, to be picked up together with the analogous
   * "shuffle initial slot distribution" idea deferred from the Group 1 round.
   */
  private final ByteBuffer group2;

  private static final int GROUP2_CACHE_LINE = 64;
  private static final int GROUP2_STRIDE = 32; // 2 slots per 64-byte cache line; must be a power
  // of 2
  private static final int GROUP2_STRIDE_SHIFT = Integer.numberOfTrailingZeros(GROUP2_STRIDE);
  // next's offset within the stride is 0, so it needs no named offset constant -- just the raw
  // slot base.
  private static final int PREV_OFFSET = 4;
  private static final int FROM_HOT_OFFSET = 8;
  private static final int LAST_UNPIN_NANOS_OFFSET = 16; // 8-byte-aligned within the 32B stride

  /**
   * Allocates a direct {@link ByteBuffer} of at least {@code slots * stride} bytes, aligned to a
   * full {@code alignment}-byte boundary -- a hard guarantee, not merely {@code allocateDirect}'s
   * own (~8-32 byte, platform-dependent) alignment.
   *
   * <p>{@code alignment} must equal or be a multiple of {@code stride} wherever the caller's own
   * slot-adjacency assumptions (e.g. "every even slot index starts a fresh cache line") depend on
   * the buffer's base being aligned to more than just {@code stride} itself -- requesting only
   * {@code stride}-alignment when the real requirement is a wider {@code alignment} would silently
   * shift such assumptions off by one slot on some fraction of allocations.
   *
   * <p>{@link ByteBuffer#alignedSlice(int)} rounds its limit down as well as rounding its start up
   * to the nearest {@code alignment} boundary, so up to {@code (alignment - 1)} bytes can be lost
   * from each end independently; requesting only a {@code (alignment - 1)} margin is sufficient
   * only when {@code slots * stride} happens to already be an exact multiple of {@code alignment}
   * -- in general (e.g. {@code stride < alignment}) it is not, and can silently yield a slice
   * narrower than requested. A margin of {@code 2 * (alignment - 1)} is sufficient regardless.
   */
  // Package-private (not private) so it can be unit-tested directly against ByteBuffer's own
  // public alignmentOffset()/capacity() APIs, without needing dedicated test-only accessors.
  static ByteBuffer allocateAlignedDirect(int slots, int stride, int alignment) {
    int needed = Math.multiplyExact(slots, stride);
    return ByteBuffer.allocateDirect(needed + 2 * (alignment - 1)).alignedSlice(alignment);
  }

  // ---------------------------------------------------------------------------
  // Sentinels
  // ---------------------------------------------------------------------------

  // Sentinel link values stored in group2's next field to signal list-operation state. Analogous
  // to the RESERVED / REMOVED Node sentinels in Cache, but encoded as ints rather than as object
  // references.
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
   * live in {@link #group2}, indexed the same way as any other slot). Elements are set at
   * construction and never swapped.
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
  Cache2(int capacity, int extraSentinelSlots, Iterable<? extends V> initialValues) {
    this.capacity = capacity;
    // Real slots start at refCount=0/generation=0 (the zero-initialized default), matching the
    // implicit-zero `new Val(i--, 0)` call this construction replaces. Sized for real slots
    // 1..capacity only (slot 0 reserved/unused but still addressable) -- sentinel slots never have
    // refCount/generation read by any live code path, so no sentinel coverage is needed here,
    // unlike group2 below.
    this.group1 = allocateAlignedDirect(capacity + 1, GROUP1_STRIDE, GROUP1_STRIDE);
    // Sentinels are spaced 2 apart (not 1) in group2's index space, each leaving its odd-offset
    // "pair partner" slot permanently unused -- at group2's 2-slots-per-64-byte-line stride, this
    // guarantees every sentinel gets its own exclusive cache line, never sharing one with another
    // sentinel or with the adjacent real slot (the hottest objects in the whole structure would
    // otherwise be exactly the ones left exposed to the same false sharing this effort targets).
    // Rounding the base up to the next even index (rather than using capacity+1 directly) is what
    // prevents the last real slot from landing in the same pair as the first sentinel: for any
    // capacity, floor((capacity+1 rounded up to even)/2) != floor(capacity/2).
    int sentinelBase = capacity + 1;
    if ((sentinelBase & 1) != 0) {
      sentinelBase++;
    }
    this.COLD_HEAD = sentinelBase;
    this.COLD_TAIL = sentinelBase + 2;
    int arrayLen = COLD_TAIL + 1 + 2 * extraSentinelSlots;
    this.payload = new Val[arrayLen];
    // Must align to GROUP2_CACHE_LINE (64), not GROUP2_STRIDE (32): the cache-line-isolation
    // guarantee group2 exists for (and that the sentinel spacing above depends on) requires every
    // even slot index to start a fresh 64-byte line, which only holds if group2's own base is
    // 64-byte aligned -- 32-byte alignment alone is not sufficient (allocateDirect's underlying
    // native allocation is not guaranteed better than 32-byte aligned, and a
    // 32-byte-but-not-64-byte-aligned base would silently shift every "even slot starts a line"
    // assumption off by one slot).
    this.group2 = allocateAlignedDirect(arrayLen, GROUP2_STRIDE, GROUP2_CACHE_LINE);
    assert COLD_TAIL < arrayLen;

    // Placeholder sentinel Val objects for the cold queue sentinels -- next/prev live in group2,
    // indexed by COLD_HEAD/COLD_TAIL directly; these objects are never read. DualQueueCache
    // initializes HOT_HEAD / HOT_TAIL sentinel Vals in its own constructor.
    this.payload[COLD_HEAD] = new Val();
    this.payload[COLD_TAIL] = new Val();

    // Pass 1: populate payload[] sequentially, slot = 1..capacity. This ordering is load-bearing
    // and must NOT change -- BlockCache.distribute()'s reverse-order Val construction and
    // buildExtantMap()'s warm-start slot-number reconstruction formula both depend on it.
    int slot = 1;
    for (V v : initialValues) {
      if (slot > capacity) throw new IllegalArgumentException("too many initial values");
      payload[slot] = v;
      slot++;
    }

    // Pass 2: link slots 1..capacity into the cold-queue chain (COLD_HEAD <-> ... <-> COLD_TAIL)
    // in a randomized stride order rather than sequential slot order. Chain position determines
    // acquisition order (acquireTail() always takes from the COLD_TAIL end), so sequential linking
    // would mean a burst of initial/warm-up acquisitions drains physically-adjacent slots in
    // immediate succession -- the same kind of temporal/physical adjacency correlation that
    // motivated group1's/group2's cache-line-aware layouts in the first place, except here it's an
    // artifact of link order rather than slot-number assignment. Does NOT affect
    // payload[]/cacheBlockOrd/buildExtantMap, all governed solely by pass 1 above.
    int stride = choosePermutationStride(capacity);
    int prevIdx = COLD_HEAD;
    // random starting slot, from 1 (index 0 is unused)
    int cur = ThreadLocalRandom.current().nextInt(capacity) + 1;
    for (int i = 0; i < capacity; i++) {
      setNextPlain(prevIdx, cur);
      setPrevPlain(cur, prevIdx);
      prevIdx = cur;
      cur = (cur - 1 + stride) % capacity + 1; // 1-indexed modular stride advance
    }
    setNextPlain(prevIdx, COLD_TAIL);
    setPrevPlain(COLD_TAIL, prevIdx);
    // COLD_HEAD.prev is never read (head sentinel has no meaningful predecessor).
  }

  /**
   * Rejects a candidate stride if the largest partial quotient in the continued-fraction expansion
   * of {@code capacity/min(stride, capacity-stride)} exceeds this threshold. By standard
   * continued-fraction / three-distance-theorem results, bounding every partial quotient bounds the
   * worst-case gap achievable at <em>every</em> lag simultaneously, not just a checked prefix:
   * {@code dist(k) >= capacity / ((MAX_PARTIAL_QUOTIENT + 2) * k)} for all {@code k} -- an
   * unusually large partial quotient anywhere in the expansion means some (possibly large) lag
   * produces an unusually good rational approximation to {@code stride/capacity}, i.e. a near-zero
   * wraparound, i.e. two chain entries acquired that many steps apart would land on numerically-
   * (and therefore physically-) adjacent slots. This check is computed via the same
   * Euclidean-algorithm loop structure as {@link #gcd} ({@code O(log capacity)} steps), so its cost
   * is independent of how large a lag is actually being protected against -- unlike brute-force
   * checking of individual lags, which cannot scale to the thousands-to-tens-of-thousands lag range
   * realistic concurrent bursts require (even after this cache's own per-CPU partitioning).
   *
   * <p><b>This bound inherently degrades as burst size grows</b> (by the pigeonhole principle:
   * {@code B} points distributed around a circle of circumference {@code capacity} cannot all be
   * more than {@code ~capacity/B} apart, regardless of how the stride is chosen) -- this check
   * gives the best achievable dispersion for a given stride, not an escape from that fundamental
   * limit. At this threshold and the 2,621,440-slot realistic partition-capacity ceiling, the
   * guaranteed gap is in the tens at a burst size of 10,000 and comfortably in the hundreds at a
   * few thousand; single-digit gaps become possible once burst size reaches the tens-of-thousands-
   * to-200,000 range (roughly 1-8% of partition capacity) -- not only once burst size approaches
   * the full partition capacity itself.
   *
   * <p>Unlike an initial version of this check, the stride's own distance from {@code 0}/{@code
   * capacity} (the {@code k=1} case) must NOT be evaluated by excluding the first
   * continued-fraction quotient: that first quotient, {@code floor(capacity / min(stride,
   * capacity-stride))}, directly encodes how small the stride is and is exactly what must be
   * bounded to rule out a near-sequential chain -- omitting it let pathologically small strides
   * slip through undetected. Likewise, the expansion must be taken of {@code min(stride,
   * capacity-stride)}, not {@code stride} itself: strides {@code s} and {@code capacity-s} produce
   * the identical cycle (the same chain traversed in the opposite direction, hence identical
   * slot-adjacency/cache-line-sharing properties), so accepting one while rejecting the other -- as
   * an unreduced expansion would, since {@code capacity/(capacity-s)} and {@code capacity/s} have
   * unrelated continued fractions in general -- would be an arbitrary, unjustified asymmetry.
   */
  private static final int MAX_PARTIAL_QUOTIENT = 20;

  /**
   * Hard cap on rejection-sampling attempts in {@link #choosePermutationStride}, after which it
   * falls back to {@code stride=1} (always coprime to any {@code capacity}, hence always
   * structurally valid, though locality-poor -- equivalent to the original unpermuted sequential
   * chain). Empirical sweeps (exhaustive for small capacities, spot-checked including adversarial
   * cases -- Fibonacci numbers, highly composite values -- up to several hundred thousand) never
   * found a capacity requiring more than ~60 attempts, and never found one with zero valid strides
   * at all; this cap is set with generous headroom above that.
   *
   * <p>That said, "a valid stride exists for every possible capacity" is not something this
   * approach can mathematically guarantee in general: it is directly adjacent to Zaremba's
   * conjecture (open since 1971 -- whether every integer has a coprime numerator whose
   * continued-fraction partial quotients are all &le;5; proven only for a density-one, not
   * density-<em>all</em>, subset of integers). {@code MAX_PARTIAL_QUOTIENT=20} here is far more
   * permissive than Zaremba's conjectured-sufficient bound of 5, making a real failure far less
   * likely still, but this fallback exists specifically so an unproven number-theoretic edge case
   * can never manifest as a hung or crashing partition construction -- correctness degrades
   * gracefully to "no permutation benefit" rather than failing.
   */
  private static final int MAX_STRIDE_SELECTION_ATTEMPTS = 1000;

  /**
   * Returns a stride coprime to {@code capacity}, so that repeatedly advancing by this stride (mod
   * {@code capacity}, 1-indexed) visits every slot in {@code 1..capacity} exactly once before
   * returning to the start -- a single Hamiltonian cycle through the slot space, required for
   * {@link #Cache2}'s chain-building pass to produce a valid, fully-connected LRU list (an
   * incoprime stride would silently produce multiple disjoint cycles, orphaning some slots as
   * permanently unevictable while linking others twice).
   *
   * <p>Beyond mere coprimality, the stride is rejected if its continued-fraction expansion (see
   * {@link #MAX_PARTIAL_QUOTIENT}) contains too large a partial quotient -- this single check
   * covers both immediate-neighbor and arbitrarily-distant locality concerns uniformly. See {@link
   * #MAX_STRIDE_SELECTION_ATTEMPTS} for why this rejection loop is bounded rather than
   * unconditional.
   *
   * <p>The stride is chosen freshly at random on every call (i.e. every {@code Cache2}/partition
   * construction, every JVM run) -- not a single fixed "good" value -- so initial chain layout
   * varies run to run rather than reproducing an identical pattern on every restart.
   */
  private static int choosePermutationStride(int capacity) {
    if (capacity <= 2) {
      // Trivial cases: a single slot has no meaningful permutation space; for two slots, stride=1
      // is the only valid nontrivial choice (gcd(1,2)=1) and rejection sampling would just find it
      // immediately anyway.
      return 1;
    }
    // A bound of MAX_PARTIAL_QUOTIENT on the first continued-fraction quotient alone already
    // implies min(stride, capacity-stride) > capacity/(MAX_PARTIAL_QUOTIENT+1), so sampling
    // directly from that range skips candidates guaranteed to fail regardless of the rest of the
    // expansion. floor <= capacity-floor always holds here (capacity >= 3 at this point): floor <=
    // capacity/(MAX_PARTIAL_QUOTIENT+1) + 1 <= capacity/2 for every capacity >= 3 at
    // MAX_PARTIAL_QUOTIENT=20, so the sampling range below is never empty.
    int floor = Math.max(1, capacity / (MAX_PARTIAL_QUOTIENT + 1) + 1);
    ThreadLocalRandom rnd = ThreadLocalRandom.current();
    for (int attempt = 0; attempt < MAX_STRIDE_SELECTION_ATTEMPTS; attempt++) {
      int stride = floor + rnd.nextInt(capacity - (2 * floor) + 1); // [floor, capacity-floor]
      if (gcd(stride, capacity) == 1
          && maxPartialQuotient(capacity, Math.min(stride, capacity - stride))
              <= MAX_PARTIAL_QUOTIENT) {
        return stride;
      }
    }
    // See MAX_STRIDE_SELECTION_ATTEMPTS javadoc: never observed in practice, but a correctness-
    // preserving fallback rather than an unbounded loop.
    return 1;
  }

  /**
   * Largest partial quotient in the continued-fraction expansion of {@code a/b} (including the
   * first quotient -- see {@link #MAX_PARTIAL_QUOTIENT}'s javadoc for why it must not be excluded),
   * computed via the same Euclidean-algorithm loop structure as {@link #gcd} -- {@code
   * O(log(min(a,b)))} steps.
   */
  private static int maxPartialQuotient(int a, int b) {
    int max = 0;
    while (b != 0) {
      int q = a / b;
      max = Math.max(max, q);
      int t = b;
      b = a - q * b;
      a = t;
    }
    return max;
  }

  private static int gcd(int a, int b) {
    while (b != 0) {
      int t = b;
      b = a % b;
      a = t;
    }
    return a;
  }

  // ---------------------------------------------------------------------------
  // Group 1 (refCount/generation) accessors
  // ---------------------------------------------------------------------------

  // Package-private (not private): DualQueueCache, a nested subclass, calls these directly as
  // plain inherited instance methods. Cache2 itself is package-private, so this exposes nothing
  // beyond org.apache.solr.storage.

  /** Volatile read of {@code refCount} at {@code slot}. */
  int getRefCountVolatile(int slot) {
    return (int) INT_VH.getVolatile(group1, slot << GROUP1_STRIDE_SHIFT);
  }

  /**
   * Atomically sets {@code refCount} at {@code slot} to {@code update} if it currently equals
   * {@code expect}, returning the witness value (the value observed immediately before this call,
   * which equals {@code expect} iff the exchange succeeded) -- same semantics as {@code
   * VarHandle.compareAndExchange}.
   */
  int compareAndExchangeRefCount(int slot, int expect, int update) {
    return (int) INT_VH.compareAndExchange(group1, slot << GROUP1_STRIDE_SHIFT, expect, update);
  }

  /**
   * Atomically sets {@code refCount} at {@code slot} to {@code update} if it currently equals
   * {@code expect}, returning {@code true} iff the exchange succeeded -- same semantics as {@code
   * VarHandle.compareAndSet}.
   */
  boolean compareAndSetRefCount(int slot, int expect, int update) {
    return (boolean) INT_VH.compareAndSet(group1, slot << GROUP1_STRIDE_SHIFT, expect, update);
  }

  /** Volatile read of {@code generation} at {@code slot}. */
  int getGenerationVolatile(int slot) {
    return (int) INT_VH.getVolatile(group1, (slot << GROUP1_STRIDE_SHIFT) + GENERATION_OFFSET);
  }

  /**
   * Atomically increments {@code generation} at {@code slot} by one and returns the <em>new</em>
   * (post-increment) value. Callers must hold exclusive ownership of {@code slot} when calling this
   * (e.g. via a prior successful {@code refCount} CAS to an exclusive sentinel value) -- concurrent
   * calls for the same slot are not supported and may lose updates.
   */
  int incrementAndGetGeneration(int slot) {
    int offset = (slot << GROUP1_STRIDE_SHIFT) + GENERATION_OFFSET;
    int newGen = (int) INT_VH.getVolatile(group1, offset) + 1;
    INT_VH.setVolatile(group1, offset, newGen);
    return newGen;
  }

  // ---------------------------------------------------------------------------
  // Group 2 (next/prev/fromHot/lastUnpinNanos) accessors
  // ---------------------------------------------------------------------------

  int getNextVolatile(int slot) {
    return (int) INT_VH.getVolatile(group2, slot << GROUP2_STRIDE_SHIFT);
  }

  /**
   * Pre-publication-safe plain write -- used only before the slot is reachable by other threads.
   */
  void setNextPlain(int slot, int value) {
    INT_VH.set(group2, slot << GROUP2_STRIDE_SHIFT, value);
  }

  int compareAndExchangeNext(int slot, int expect, int update) {
    return (int) INT_VH.compareAndExchange(group2, slot << GROUP2_STRIDE_SHIFT, expect, update);
  }

  boolean compareAndSetNext(int slot, int expect, int update) {
    return (boolean) INT_VH.compareAndSet(group2, slot << GROUP2_STRIDE_SHIFT, expect, update);
  }

  int getPrevVolatile(int slot) {
    return (int) INT_VH.getVolatile(group2, (slot << GROUP2_STRIDE_SHIFT) + PREV_OFFSET);
  }

  /**
   * Pre-publication-safe plain write -- used only before the slot is reachable by other threads.
   */
  void setPrevPlain(int slot, int value) {
    INT_VH.set(group2, (slot << GROUP2_STRIDE_SHIFT) + PREV_OFFSET, value);
  }

  void setPrevVolatile(int slot, int value) {
    INT_VH.setVolatile(group2, (slot << GROUP2_STRIDE_SHIFT) + PREV_OFFSET, value);
  }

  /** Plain (non-atomic) read, matching today's ordinary field-access semantics. */
  long getLastUnpinNanos(int slot) {
    return (long) LONG_VH.get(group2, (slot << GROUP2_STRIDE_SHIFT) + LAST_UNPIN_NANOS_OFFSET);
  }

  /** Plain (non-atomic) write, matching today's ordinary field-access semantics. */
  void setLastUnpinNanos(int slot, long value) {
    LONG_VH.set(group2, (slot << GROUP2_STRIDE_SHIFT) + LAST_UNPIN_NANOS_OFFSET, value);
  }

  boolean getFromHotVolatile(int slot) {
    return (int) INT_VH.getVolatile(group2, (slot << GROUP2_STRIDE_SHIFT) + FROM_HOT_OFFSET) != 0;
  }

  void setFromHotVolatile(int slot, boolean value) {
    INT_VH.setVolatile(group2, (slot << GROUP2_STRIDE_SHIFT) + FROM_HOT_OFFSET, value ? 1 : 0);
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
    return getFromHotVolatile((int) handle & SLOT_MASK);
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
    int cur = getNextVolatile(slot);
    for (; ; ) {
      while (cur == RESERVED_LINK) {
        if (reservation == REMOVED_LINK) {
          Thread.yield();
        }
        cur = getNextVolatile(slot);
      }
      if (cur == REMOVED_LINK) {
        return cur;
      }
      int witness = compareAndExchangeNext(slot, cur, reservation);
      if (witness == cur) {
        return cur;
      }
      cur = witness;
    }
  }

  protected boolean insertAtHead(int listHead, int slot, boolean recordAccess) {
    setPrevVolatile(slot, listHead);
    int oldNext = reserve(listHead, RESERVED_LINK);
    assert oldNext != REMOVED_LINK : "queue head sentinel should never be removed";
    setNextPlain(slot, oldNext);
    setPrevVolatile(oldNext, slot);
    if (!compareAndSetNext(listHead, RESERVED_LINK, slot)) {
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
      prevSlot = getPrevVolatile(slot);
      if (compareAndSetNext(prevSlot, slot, RESERVED_LINK)) {
        break;
      }
      Thread.yield();
    }
    setPrevVolatile(oldNext, prevSlot);
    if (!compareAndSetNext(prevSlot, RESERVED_LINK, oldNext)) {
      throw new IllegalStateException("unexpected concurrent modification during list removal");
    }
  }

  private void insertAtTail(int slot) {
    for (; ; ) {
      int predSlot = getPrevVolatile(COLD_TAIL);
      int oldNext = reserve(predSlot, RESERVED_LINK);
      if (oldNext != COLD_TAIL) {
        if (oldNext == REMOVED_LINK) {
          continue; // pred was concurrently removed; retry
        }
        // release reservation; pred is no longer tail's predecessor
        if (!compareAndSetNext(predSlot, RESERVED_LINK, oldNext)) {
          throw new IllegalStateException();
        }
        continue;
      }
      setNextPlain(slot, COLD_TAIL);
      setPrevVolatile(slot, predSlot);
      setPrevVolatile(COLD_TAIL, slot);
      if (!compareAndSetNext(predSlot, RESERVED_LINK, slot)) {
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
    if (getGenerationVolatile(slot) != expectedGen) return -1;
    int rc = getRefCountVolatile(slot);
    for (; ; ) {
      switch (rc) {
        case -1:
          return -1;
        case UNPIN_SENTINEL:
          rc = getRefCountVolatile(slot);
          continue;
      }
      assert rc >= 0;
      int witness = compareAndExchangeRefCount(slot, rc, rc + 1);
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
    if (getGenerationVolatile(slot) != expectedGen) {
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
    switch (getRefCountVolatile(slot)) {
      case UNPIN_SENTINEL:
      case -1:
        return null;
      default:
        return getGenerationVolatile(slot) == expectedGen ? payload[slot] : null;
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
    int rc = getRefCountVolatile(slot);
    for (; ; ) {
      if (rc == 1) {
        int witness = compareAndExchangeRefCount(slot, 1, UNPIN_SENTINEL);
        if (witness == 1) {
          boolean toHot = insertAtHead(COLD_HEAD, slot, recordAccess);
          if (!compareAndSetRefCount(slot, UNPIN_SENTINEL, 0)) {
            throw new IllegalStateException();
          }
          return toHot ? 1 : 0;
        } else {
          rc = witness;
        }
      } else {
        assert rc > 1 : "unexpected refCount: " + rc;
        int witness = compareAndExchangeRefCount(slot, rc, rc - 1);
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
    for (int candidate; (candidate = getPrevVolatile(COLD_TAIL)) != COLD_HEAD; ) {
      if (compareAndSetRefCount(candidate, 0, -1)) {
        return candidate;
      }
    }
    return NULL_SLOT;
  }

  /**
   * Hook for subclasses to reset per-slot state they maintain outside {@link Val} (e.g. in {@link
   * #group2}, whose contents base {@code Cache2} has no need to know). Called from {@link
   * #acquireNode(long[])} and {@link #close(long)} on every slot recycle, alongside {@link
   * Val#reset()}. No-op by default.
   */
  protected void reset(int slot) {
    // no-op default impl; see DualQueueCache's override.
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
    int newGen = incrementAndGetGeneration(slot);
    reset(slot);
    Val p = payload[slot];
    p.reset();
    if (!compareAndSetRefCount(slot, -1, 1)) {
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
    int ret = compareAndExchangeRefCount(slot, 0, -1);
    if (ret == 0) {
      // successfully closed
      removeFromList(slot);
      // Keep rc=-1 during insertion so acquireTail cannot grab this slot mid-insertion
      // (its CAS(0→-1) won't match rc=-1). reset() then bump generation -- order between the two
      // doesn't matter for correctness (refCount==-1 grants exclusivity across this whole window
      // regardless), but the generation bump must land before any handle read before this close()
      // can pin() again: pin()'s pre-CAS generation check will then fail, so a stale pinner cannot
      // successfully pin -- or hang in join() -- after this slot has been recycled.
      // insertAtTail() below splices directly into the cold list without going through
      // DualQueueCache.insertAtHead(), so reset(slot) must be called explicitly here too.
      reset(slot);
      payload[slot].reset();
      incrementAndGetGeneration(slot);
      insertAtTail(slot);
      // refCount -> 0 makes reclaimable
      if (!compareAndSetRefCount(slot, -1, 0)) {
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
    int rc = getRefCountVolatile(slot);
    for (; ; ) {
      if (rc == 1) {
        // Sole holder: use -1 as intermediate to block acquireTail during tail insertion.
        int witness = compareAndExchangeRefCount(slot, 1, -1);
        if (witness == 1) {
          insertAtTail(slot);
          if (!compareAndSetRefCount(slot, -1, 0)) {
            throw new IllegalStateException("rc changed during insertAtTail in closeUnconditional");
          }
          return;
        }
        rc = witness;
      } else if (rc > 1) {
        // A stale pin() raced: its CAS retry loop incremented rc past 1 before its post-CAS
        // generation check could call unpin(). Decrement our own pin and return; the stale
        // pinner's subsequent unpin() will see rc==1 and reinstate this slot in the LRU.
        int witness = compareAndExchangeRefCount(slot, rc, rc - 1);
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
      // Continue the spaced-by-2 sentinel scheme from COLD_TAIL (see base constructor) so
      // HOT_HEAD/HOT_TAIL each also get an exclusive cache line.
      this.HOT_HEAD = COLD_TAIL + 2;
      this.HOT_TAIL = COLD_TAIL + 4;
      assert HOT_TAIL < payload.length;
      // Placeholder sentinel Val objects for the hot queue -- next/prev live in group2.
      payload[HOT_HEAD] = new Val();
      payload[HOT_TAIL] = new Val();
      setNextPlain(HOT_HEAD, HOT_TAIL);
      setPrevPlain(HOT_TAIL, HOT_HEAD);
    }

    @Override
    protected void reset(int slot) {
      // 0 is the maximally-stale sentinel age()/toHot() rely on. Without this, a recycled slot
      // would carry its previous occupant's timestamp into the new occupant's first
      // insertAtHead() decision and into acquireTail()'s staleness comparison.
      setLastUnpinNanos(slot, 0);
    }

    @Override
    protected final boolean insertAtHead(int head, int slot, boolean recordAccess) {
      long lastUnpin = getLastUnpinNanos(slot);
      setLastUnpinNanos(slot, recordAccess ? System.nanoTime() : 0);
      boolean toHot = toHot(lastUnpin);
      setFromHotVolatile(slot, toHot);
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
        int coldCandidate = getPrevVolatile(COLD_TAIL);
        int hotCandidate = getPrevVolatile(HOT_TAIL);
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
          coldCandidateTs = getLastUnpinNanos(coldCandidate);
        } else {
          // Evict from hot only when cold is at least HOT_EVICTION_MULTIPLIER times as stale,
          // giving hot slots proportional protection. age() maps lastUnpinNanos=0 to a large
          // sentinel so never-used cold slots are always preferred over any real hot slot.
          coldCandidateTs = getLastUnpinNanos(coldCandidate);
          fromHot =
              age(now, coldCandidateTs)
                  < age(now, getLastUnpinNanos(hotCandidate)) >> HOT_EVICTION_SHIFT;
          candidate = fromHot ? hotCandidate : coldCandidate;
        }
      } while (!compareAndSetRefCount(candidate, 0, -1));
      if (coldCandidateTs != 0) {
        this.coldTs = coldCandidateTs;
      }
      setFromHotVolatile(candidate, fromHot);
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
