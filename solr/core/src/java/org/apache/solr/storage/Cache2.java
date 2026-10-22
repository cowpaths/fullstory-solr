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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.store.OffHeapStore;
import org.apache.lucene.store.OffHeapStores;
import org.apache.solr.common.util.EnvUtils;

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
  // Both groups are backed by an OffHeapStore -- an abstraction over the raw memory op mechanism
  // (see OffHeapStore's javadoc). Which concrete implementation backs a given instance (a
  // MemorySegment-backed store, a plain ByteBuffer-backed store, or sun.misc.Unsafe) is selected
  // by allocateSharedGroup1And2Stores, below, via the solr.cache2.offHeapStoreMode system
  // property.
  //
  // Each store's stride is a power of 2, so slot -> byte-offset is a plain left shift
  // (slot << *_STRIDE_SHIFT) rather than a multiply -- safe from overflow without a per-access
  // check, since each constructor's one-time Math.multiplyExact(slots, STRIDE) already proves the
  // largest valid offset fits in an int.

  static {
    // getRefCountAndGenerationVolatile()/getNextAndPrevVolatile() pack two adjacent ints (at
    // offset 0 and +4) into one long read and unpack via low=(int)combined,
    // high=(int)(combined>>>32) -- correct only on little-endian, where the lower memory address
    // lands in the long's low bits. Fail fast at class init on any platform where that doesn't
    // hold, rather than silently miscomputing generation checks in pin()/pinnable() (which could
    // manifest as spurious -1 returns and, depending on caller retry logic, a hang) on a
    // big-endian JVM.
    if (ByteOrder.nativeOrder() != ByteOrder.LITTLE_ENDIAN) {
      throw new ExceptionInInitializerError(
          "Cache2 assumes little-endian native byte order (required by"
              + " getRefCountAndGenerationVolatile's bit-packing); actual native order is "
              + ByteOrder.nativeOrder());
    }
  }

  /**
   * Group 1 (refCount/generation) storage -- see the class javadoc. One {@code (refCount,
   * generation)} pair per 64-byte stride (at offsets 0 and {@link #GENERATION_OFFSET} within the
   * stride; the remaining 56 bytes are unused padding), indexed by real slot {@code 1..capacity}
   * (slot 0 reserved/unused but still addressable).
   *
   * <p>The store itself may be shared across every partition of a single {@link BlockCache} (see
   * {@code BlockCache.distribute()}) rather than allocated exclusively for this one {@code Cache2}
   * instance, in which case {@link #group1BaseOffset} is this instance's non-zero slice into it.
   * Deliberately kept as two separate {@code private final} fields (a reference and a primitive
   * {@code long}), NOT wrapped in a single pair object: wrapping them would add a second
   * reference-field dereference to every single access (one hop to reach the wrapper, a second to
   * reach its {@code store}/{@code baseOffset} fields), which for a {@code MemorySegment}-backed
   * store also costs the JIT constant-folding a {@code static final} segment reference otherwise
   * gets. Two bare fields avoids that extra hop entirely.
   */
  private final OffHeapStore group1;

  /** This instance's base offset into {@link #group1} -- see that field's javadoc. */
  private final long group1BaseOffset;

  private static final int GROUP1_STRIDE = 64; // one cache line per slot; must be a power of 2
  private static final int GROUP1_STRIDE_SHIFT = Integer.numberOfTrailingZeros(GROUP1_STRIDE);
  // Per-slot shift into group1, in OffHeapStore's int-indexed units (4 bytes/int) rather than
  // bytes.
  private static final int GROUP1_SLOT_SHIFT = GROUP1_STRIDE_SHIFT - 2;
  // refCount's offset within the stride is 0, so it needs no named offset constant -- just the
  // raw slot base.
  private static final int GENERATION_OFFSET = 1;

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
   *
   * <p>See {@link #group1}'s javadoc for why this is a bare store + separate baseOffset field.
   */
  private final OffHeapStore group2;

  /** This instance's base offset into {@link #group2} -- see that field's javadoc. */
  private final long group2BaseOffset;

  private static final int GROUP2_CACHE_LINE = 64;
  private static final int GROUP2_STRIDE = 32; // 2 slots per 64-byte cache line; must be a power
  // of 2
  private static final int GROUP2_STRIDE_SHIFT = Integer.numberOfTrailingZeros(GROUP2_STRIDE);
  // Per-slot shift into group2, in OffHeapStore's int-indexed units (4 bytes/int) rather than
  // bytes.
  private static final int GROUP2_SLOT_SHIFT = GROUP2_STRIDE_SHIFT - 2;
  // next's offset within the stride is 0, so it needs no named offset constant -- just the raw
  // slot base.
  private static final int PREV_OFFSET = 1;
  private static final int FROM_HOT_OFFSET = 2;
  // lastUnpinNanos is accessed via the long-typed getLong/putLong, whose index unit is 8 bytes
  // (double PREV_OFFSET/FROM_HOT_OFFSET's 4-byte unit) -- offset 16 within the 32-byte stride is
  // the 3rd 8-byte unit, i.e. long-index 2.
  private static final int LAST_UNPIN_NANOS_LONG_OFFSET = 2;

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
  // Shared-store support (BlockCache.distribute() pre-allocating one combined Group 1/Group 2
  // store spanning every partition of a single BlockCache, rather than each Cache2/Partition
  // allocating its own -- see BlockCache#distribute)
  // ---------------------------------------------------------------------------

  /**
   * Number of Group 1 slots a {@code Cache2}/{@code Partition} of the given {@code capacity}
   * occupies -- exactly the {@code slots} argument the single-partition constructor below passes to
   * {@code new UnsafeOffHeapStore(...)}. Exposed so {@code BlockCache.distribute()} can sum this
   * across all its partitions to size one shared Group 1 store up front.
   */
  static int group1SlotCount(int capacity) {
    return capacity + 1;
  }

  /**
   * Number of Group 2 slots (including this partition's own sentinels) a {@code Cache2}/{@code
   * Partition} of the given {@code capacity} and {@code extraSentinelSlots} occupies. Mirrors the
   * {@code sentinelBase}/{@code COLD_HEAD}/{@code COLD_TAIL}/{@code arrayLen} computation inlined
   * in the constructor below -- kept as a single shared formula (called from both places) rather
   * than duplicated, since {@code BlockCache.distribute()} must reproduce it exactly to size a
   * shared store correctly.
   */
  static int group2SlotCount(int capacity, int extraSentinelSlots) {
    int sentinelBase = capacity + 1;
    if ((sentinelBase & 1) != 0) {
      sentinelBase++;
    }
    int coldTail = sentinelBase + 2;
    return coldTail + 1 + 2 * extraSentinelSlots;
  }

  /**
   * Selects which backing implementation {@link #allocateSharedGroup1And2Stores} uses, via the
   * {@code solr.cache2.offHeapStoreMode} system property (read once, at class-init time -- see
   * {@link #OFF_HEAP_STORE_MODE}). Values are matched case-insensitively against each constant's
   * name; an unrecognized value falls back to {@link #DEFAULT} (logged, not thrown, since an
   * off-heap backing choice is a performance tuning knob, not a correctness-critical setting).
   */
  private enum OffHeapStoreMode {
    /**
     * Try the one-shot, JVM-wide {@code MemorySegment} claim ({@link
     * OffHeapStores#tryClaimMemorySegment}); the first caller across the whole JVM to reach this
     * point (whichever {@link BlockCache} initializes first) gets real {@code MemorySegment}
     * backing, every subsequent caller falls back to a plain {@link
     * OffHeapStores#createByteBuffer}-backed store. No {@code Unsafe} involved in either case.
     */
    DEFAULT,
    /** Always use {@link UnsafeOffHeapStore}; never attempts the {@code MemorySegment} claim. */
    PREFER_UNSAFE,
    /**
     * Try the {@code MemorySegment} claim like {@link #DEFAULT}, but fall back to {@link
     * UnsafeOffHeapStore} (not {@code ByteBuffer}) for every caller that doesn't win it.
     */
    UNSAFE_FALLBACK,
    /**
     * Try the {@code MemorySegment} claim; if this caller doesn't win it (either the claim was
     * already taken by an earlier caller, or the JDK21+ implementation couldn't be loaded at all),
     * fail fast with an {@link IllegalStateException} rather than silently degrading to any
     * fallback. For deployments that have deliberately sized their JVM/topology around exactly one
     * real {@code MemorySegment}-backed {@link BlockCache} and want a loud, immediate signal if
     * that assumption is ever violated (e.g. an unexpected second {@code BlockCache} in the same
     * JVM, or an unsupported JDK), rather than quietly running in a degraded-performance mode that
     * could go unnoticed.
     */
    STRICT;

    private static OffHeapStoreMode fromSysprop(String value) {
      if (value == null) {
        return DEFAULT;
      }
      for (OffHeapStoreMode mode : values()) {
        if (mode.name().equalsIgnoreCase(value)) {
          return mode;
        }
      }
      return DEFAULT;
    }
  }

  private static final OffHeapStoreMode OFF_HEAP_STORE_MODE =
      OffHeapStoreMode.fromSysprop(EnvUtils.getProperty("solr.cache2.offHeapStoreMode"));

  /**
   * Allocates ONE combined {@link OffHeapStore} sized to hold both Group 1 ({@code
   * totalGroup1Slots} slots) and Group 2 ({@code totalGroup2Slots} slots) storage for every
   * partition of a single {@link BlockCache} combined, rather than each group (or each {@code
   * Cache2}/{@code Partition}) allocating its own -- see {@code BlockCache.distribute()}, which
   * sums {@link #group1SlotCount}/{@link #group2SlotCount} across all its partitions to compute
   * these totals, then slices the single returned store into one {@code (store, baseOffset)} pair
   * per group per partition.
   *
   * <p>Group 1 and Group 2 must be combined into a single allocation for the {@code MemorySegment}
   * path: a {@code MemorySegment}-backed store's one-shot, JVM-wide claim ({@link
   * OffHeapStores#tryClaimMemorySegment}) can only ever be won once, by one caller, for one size --
   * so {@code BlockCache.distribute()} must make exactly one combined-size claim attempt covering
   * both groups, not two independent ones (where only whichever group's call happened to run first
   * would ever get real {@code MemorySegment} backing, and the other would always fall back).
   *
   * <p>Returns a {@link SharedStores} holding the one store plus Group 2's base offset within it
   * (Group 1 always starts at offset 0; Group 2 immediately follows, at an offset that is
   * automatically a multiple of {@link #GROUP2_SLOT_SHIFT}'s granularity since Group 1's own total
   * size is already, by construction, a multiple of {@link #GROUP1_SLOT_SHIFT}'s -- a stronger
   * alignment than Group 2 requires).
   *
   * <p><b>Known ceiling for the {@code Unsafe}/{@code ByteBuffer} fallback paths (not {@code
   * MemorySegment}):</b> {@code totalBytes} below is computed as a {@code long} and is itself safe
   * from overflow, but every mode except {@link OffHeapStoreMode#STRICT}'s success path eventually
   * narrows it back to an {@code int} slot count (via {@code Math.toIntExact}) to call {@link
   * UnsafeOffHeapStore}'s or {@link OffHeapStores#createByteBuffer}'s {@code (slots, stride,
   * alignment)} constructors, which internally re-multiply back up to a byte count ({@code
   * Math.multiplyExact(slots, stride)}) -- so the combined Group 1 + Group 2 byte total for those
   * two paths is capped at {@code Integer.MAX_VALUE} bytes, same as the direct-buffer allocation it
   * bottoms out in. At the default 256 KB compression block size this works out to roughly 5.3 TiB
   * of cache capacity before the combined total would exceed that cap (throwing {@code
   * ArithmeticException}); a much smaller {@code solr.compressionBlockKilobytes} setting lowers
   * that ceiling proportionally (e.g. a 1 KB block size caps out around 32 GiB), though such small
   * block sizes are realistically only used for testing/exercising the cache, not production-scale
   * deployments. {@code MemorySegment} (via {@link OffHeapStores#tryClaimMemorySegment}, which
   * takes {@code totalBytes} as a {@code long} directly, with no int round-trip) is not subject to
   * this ceiling -- only {@link OffHeapStoreMode#PREFER_UNSAFE} (always hits the int-capped path)
   * and whichever {@link BlockCache} in a JVM doesn't win the one-shot {@code MemorySegment} claim
   * (falls back to the int-capped path) are exposed.
   */
  static SharedStores allocateSharedGroup1And2Stores(int totalGroup1Slots, int totalGroup2Slots) {
    // totalGroup2Slots must be even for totalGroup2Bytes to be a multiple of GROUP1_STRIDE (64) --
    // GROUP2_STRIDE is 32, half of GROUP1_STRIDE, so an odd slot count leaves a dangling half-
    // GROUP1_STRIDE remainder that would silently truncate away when a fallback (Unsafe/ByteBuffer)
    // path later recovers an int slot count via totalBytes / GROUP1_STRIDE, undersizing the real
    // allocation by one Group 2 slot (32 bytes) and letting the last Group 2 sentinel write land
    // past the actual buffer. Round up defensively here (the one shared choke point every caller
    // goes through) rather than relying on every caller to pad its own totalGroup2Slots -- {@code
    // BlockCache.distribute()} already pads per-partition for an unrelated reason (keeping each
    // partition's own baseOffset cache-line-aligned) and happens to arrive here with an
    // already-even sum, but the single-partition convenience constructor does not pad at all, so
    // this check must not be caller-optional.
    if ((totalGroup2Slots & 1) != 0) {
      totalGroup2Slots++;
    }
    long totalGroup1Bytes = (long) totalGroup1Slots << GROUP1_STRIDE_SHIFT;
    long totalGroup2Bytes = (long) totalGroup2Slots << GROUP2_STRIDE_SHIFT;
    long totalBytes = totalGroup1Bytes + totalGroup2Bytes;
    // GROUP1_STRIDE (64) is a multiple of GROUP2_CACHE_LINE (64) -- in fact currently equal --
    // so requesting the wider (or equal) of the two alignments for the combined allocation
    // satisfies both groups' own alignment requirements at once.
    int alignment = Math.max(GROUP1_STRIDE, GROUP2_CACHE_LINE);

    OffHeapStore store;
    switch (OFF_HEAP_STORE_MODE) {
      case PREFER_UNSAFE:
        store =
            new UnsafeOffHeapStore(
                Math.toIntExact(totalBytes / GROUP1_STRIDE), GROUP1_STRIDE, alignment);
        break;
      case STRICT:
        store = OffHeapStores.tryClaimMemorySegment(totalBytes, alignment);
        if (store == null) {
          throw new IllegalStateException(
              "solr.cache2.offHeapStoreMode=STRICT but the MemorySegment claim was not won"
                  + " (either already claimed by an earlier BlockCache in this JVM, or this JDK"
                  + " doesn't support it)");
        }
        break;
      case UNSAFE_FALLBACK:
        store = OffHeapStores.tryClaimMemorySegment(totalBytes, alignment);
        if (store == null) {
          store =
              new UnsafeOffHeapStore(
                  Math.toIntExact(totalBytes / GROUP1_STRIDE), GROUP1_STRIDE, alignment);
        }
        break;
      case DEFAULT:
      default:
        store = OffHeapStores.tryClaimMemorySegment(totalBytes, alignment);
        if (store == null) {
          store =
              OffHeapStores.createByteBuffer(
                  Math.toIntExact(totalBytes / GROUP1_STRIDE), GROUP1_STRIDE, alignment);
        }
        break;
    }
    // Group 2's base offset is Group 1's own total size, in OffHeapStore's int-indexed units (4
    // bytes/int) rather than bytes.
    long group2BaseOffset = totalGroup1Bytes >>> 2;
    return new SharedStores(store, group2BaseOffset);
  }

  /**
   * Holds the single {@link OffHeapStore} {@link #allocateSharedGroup1And2Stores} allocates, plus
   * Group 2's base offset within it (Group 1 always starts at offset 0 -- not itself stored here,
   * since it's always zero). A plain value-holder, not an {@code OffHeapStore} wrapper itself --
   * unrelated to the "bare fields vs. wrapper object" JIT-folding concern {@link #group1}'s javadoc
   * discusses, since this is returned once at construction time (never held as a per-access hot
   * field) and immediately unpacked by the caller into its own bare fields.
   */
  static final class SharedStores {
    final OffHeapStore store;
    final long group2BaseOffset;

    private SharedStores(OffHeapStore store, long group2BaseOffset) {
      this.store = store;
      this.group2BaseOffset = group2BaseOffset;
    }
  }

  /**
   * Converts a Group 1 slot offset (e.g. the running total of {@link #group1SlotCount} across
   * partitions already placed into a shared store) into the offset a given partition's own {@code
   * group1BaseOffset} must be, relative to Group 1's own start (offset 0) within the combined store
   * {@link #allocateSharedGroup1And2Stores} returns. Exposed so {@code BlockCache.distribute()}
   * never needs to know {@link #GROUP1_SLOT_SHIFT} itself -- using this instead of reimplementing
   * the shift keeps the two forever in lockstep, since both are driven by the identical constant.
   */
  static long group1SlotToOffset(int slotOffset) {
    return (long) slotOffset << GROUP1_SLOT_SHIFT;
  }

  /**
   * Group 2 analogue of {@link #group1SlotToOffset} -- relative to Group 2's own start within the
   * combined store, i.e. the caller must separately add {@link SharedStores#group2BaseOffset} to
   * get that group's real absolute offset into the combined store (see {@code
   * BlockCache.distribute()}'s usage).
   */
  static long group2SlotToOffset(int slotOffset) {
    return (long) slotOffset << GROUP2_SLOT_SHIFT;
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
   * Convenience constructor for an exclusively-owned {@code Cache2} instance: allocates its own
   * private combined Group 1/Group 2 store (via {@link #allocateSharedGroup1And2Stores}, sized for
   * exactly this one instance). Delegates to the primary constructor below. This is the constructor
   * every existing caller (including all current tests) uses, and its behavior is unchanged from
   * before shared-store support existed, apart from now picking its backing implementation per
   * {@link #OFF_HEAP_STORE_MODE} rather than always using {@code UnsafeOffHeapStore}.
   *
   * @param capacity number of real slots
   * @param extraSentinelSlots additional sentinel slots beyond COLD_HEAD/COLD_TAIL (e.g. 2 for
   *     {@link DualQueueCache}'s HOT_HEAD/HOT_TAIL)
   * @param initialValues Val objects in desired initial LRU order (head→tail)
   */
  Cache2(int capacity, int extraSentinelSlots, Iterable<? extends V> initialValues) {
    this(
        capacity,
        extraSentinelSlots,
        initialValues,
        allocateSharedGroup1And2Stores(
            group1SlotCount(capacity), group2SlotCount(capacity, extraSentinelSlots)));
  }

  private Cache2(
      int capacity,
      int extraSentinelSlots,
      Iterable<? extends V> initialValues,
      SharedStores stores) {
    this(
        capacity,
        extraSentinelSlots,
        initialValues,
        stores.store,
        0,
        stores.store,
        stores.group2BaseOffset);
  }

  /**
   * Initializing constructor: wires {@code initialValues} into the LRU list in order (first value
   * at the head, last at the tail). Equivalent to calling a no-arg constructor followed by
   * insertAtTail for each value, but faster because no CAS is needed during single-threaded
   * initialization.
   *
   * <p>Accepts pre-built {@code (store, baseOffset)} pairs for Group 1/Group 2 rather than
   * allocating its own, so a caller (e.g. {@code BlockCache.distribute()}) can pass in a slice of
   * one store shared across many partitions instead of a store exclusively owned by this one
   * instance -- see {@link #group1}'s javadoc for why these are two separate parameters rather than
   * a single pair object. {@code group1Store} must have been sized via {@link
   * #group1SlotCount}{@code (capacity)} slots; {@code group2Store} must have been sized via {@link
   * #group2SlotCount}{@code (capacity, extraSentinelSlots)} slots. Both base offsets must
   * themselves be aligned the same way {@link #allocateSharedGroup1And2Stores} aligns its own store
   * (a multiple of each group's own per-slot stride) -- the exclusively-owned convenience
   * constructor above satisfies this trivially with base offset 0; a shared-store caller must
   * guarantee it when slicing up the combined store.
   *
   * @param capacity number of real slots
   * @param extraSentinelSlots additional sentinel slots beyond COLD_HEAD/COLD_TAIL (e.g. 2 for
   *     {@link DualQueueCache}'s HOT_HEAD/HOT_TAIL)
   * @param initialValues Val objects in desired initial LRU order (head→tail)
   * @param group1Store this instance's Group 1 store (exclusively owned, or shared)
   * @param group1BaseOffset this instance's base offset into {@code group1Store}
   * @param group2Store this instance's Group 2 store (exclusively owned, or shared)
   * @param group2BaseOffset this instance's base offset into {@code group2Store}
   */
  Cache2(
      int capacity,
      int extraSentinelSlots,
      Iterable<? extends V> initialValues,
      OffHeapStore group1Store,
      long group1BaseOffset,
      OffHeapStore group2Store,
      long group2BaseOffset) {
    this.capacity = capacity;
    // Real slots start at refCount=0/generation=0 (the zero-initialized default), matching the
    // implicit-zero `new Val(i--, 0)` call this construction replaces. Sized for real slots
    // 1..capacity only (slot 0 reserved/unused but still addressable) -- sentinel slots never have
    // refCount/generation read by any live code path, so no sentinel coverage is needed here,
    // unlike group2 below.
    this.group1 = group1Store;
    this.group1BaseOffset = group1BaseOffset;
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
    assert arrayLen == group2SlotCount(capacity, extraSentinelSlots);
    // Must align to GROUP2_CACHE_LINE (64), not GROUP2_STRIDE (32): the cache-line-isolation
    // guarantee group2 exists for (and that the sentinel spacing above depends on) requires every
    // even slot index to start a fresh 64-byte line, which only holds if group2's own base is
    // 64-byte aligned -- 32-byte alignment alone is not sufficient (allocateDirect's underlying
    // native allocation is not guaranteed better than 32-byte aligned, and a
    // 32-byte-but-not-64-byte-aligned base would silently shift every "even slot starts a line"
    // assumption off by one slot). See this constructor's own javadoc for the equivalent
    // requirement on a caller-supplied group2BaseOffset when the store is shared.
    this.group2 = group2Store;
    this.group2BaseOffset = group2BaseOffset;
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

  /**
   * Offset of {@code slot}'s stride base within {@link #group1}, including {@link
   * #group1BaseOffset} -- always even (every slot's stride is 16 ints, {@link #group1BaseOffset} is
   * always a multiple of {@code 1 << GROUP1_SLOT_SHIFT}, i.e. 16), so halving it (via {@code >>>
   * 1}) for a long-typed access is always exact, never truncating.
   */
  private long group1Offset(int slot) {
    return ((long) slot << GROUP1_SLOT_SHIFT) + group1BaseOffset;
  }

  /**
   * Offset of {@code slot}'s stride base within {@link #group2}, including {@link
   * #group2BaseOffset} -- always even, for the same reason {@link #group1Offset}'s result is (every
   * slot's stride is 8 ints; {@link #group2BaseOffset} is always a multiple of {@code 1 <<
   * GROUP2_SLOT_SHIFT}, i.e. 8, per {@code allocateSharedGroup1And2Stores}'s alignment guarantee).
   */
  private long group2Offset(int slot) {
    return ((long) slot << GROUP2_SLOT_SHIFT) + group2BaseOffset;
  }

  /** Volatile read of {@code refCount} at {@code slot}. */
  int getRefCountVolatile(int slot) {
    return group1.getIntVolatile(group1Offset(slot));
  }

  /**
   * Atomically sets {@code refCount} at {@code slot} to {@code update} if it currently equals
   * {@code expect}, returning the witness value (the value observed immediately before this call,
   * which equals {@code expect} iff the exchange succeeded) -- same semantics as {@code
   * VarHandle.compareAndExchange}.
   */
  int compareAndExchangeRefCount(int slot, int expect, int update) {
    return group1.compareAndExchangeInt(group1Offset(slot), expect, update);
  }

  /**
   * Atomically sets {@code refCount} at {@code slot} to {@code update} if it currently equals
   * {@code expect}, returning {@code true} iff the exchange succeeded -- same semantics as {@code
   * VarHandle.compareAndSet}.
   */
  boolean compareAndSetRefCount(int slot, int expect, int update) {
    return group1.compareAndSetInt(group1Offset(slot), expect, update);
  }

  /** Volatile read of {@code generation} at {@code slot}. */
  int getGenerationVolatile(int slot) {
    return group1.getIntVolatile(group1Offset(slot) + GENERATION_OFFSET);
  }

  /**
   * Volatile read of {@code refCount} and {@code generation} at {@code slot} together, as a single
   * 8-byte access -- {@code refCount} in the low 32 bits, {@code generation} in the high 32 bits
   * (native byte order; both fields occupy one naturally-aligned {@code long}-sized region within
   * the 64-byte stride: offsets 0 and {@link #GENERATION_OFFSET}, with native order placing the
   * lower memory address in the low bits on every little-endian platform this runs on). Equivalent
   * to calling {@link #getRefCountVolatile} and {@link #getGenerationVolatile} separately, except
   * atomic (one consistent point-in-time snapshot of both fields, vs. two independent loads that
   * could observe an interleaved write in between) and, on platforms where a volatile load is not
   * free (e.g. {@code ldar} on aarch64), half the load-acquire cost. Callers that need both fields
   * together (e.g. {@link #pin}'s initial fast-bail check, {@link #pinnable}) should prefer this
   * over two separate calls; callers that only need one field should keep using the single-field
   * accessor.
   */
  long getRefCountAndGenerationVolatile(int slot) {
    return group1.getLongVolatile(group1Offset(slot) >>> 1);
  }

  /**
   * Atomically increments {@code generation} at {@code slot} by one and returns the <em>new</em>
   * (post-increment) value. Callers must hold exclusive ownership of {@code slot} when calling this
   * (e.g. via a prior successful {@code refCount} CAS to an exclusive sentinel value) -- concurrent
   * calls for the same slot are not supported and may lose updates.
   */
  int incrementAndGetGeneration(int slot) {
    long offset = group1Offset(slot) + GENERATION_OFFSET;
    int newGen = group1.getIntVolatile(offset) + 1;
    group1.putIntVolatile(offset, newGen);
    return newGen;
  }

  // ---------------------------------------------------------------------------
  // Group 2 (next/prev/fromHot/lastUnpinNanos) accessors
  // ---------------------------------------------------------------------------

  int getNextVolatile(int slot) {
    return group2.getIntVolatile(group2Offset(slot));
  }

  /**
   * Pre-publication-safe plain write -- used only before the slot is reachable by other threads.
   */
  void setNextPlain(int slot, int value) {
    group2.putInt(group2Offset(slot), value);
  }

  int compareAndExchangeNext(int slot, int expect, int update) {
    return group2.compareAndExchangeInt(group2Offset(slot), expect, update);
  }

  boolean compareAndSetNext(int slot, int expect, int update) {
    return group2.compareAndSetInt(group2Offset(slot), expect, update);
  }

  int getPrevVolatile(int slot) {
    return group2.getIntVolatile(group2Offset(slot) + PREV_OFFSET);
  }

  /**
   * Pre-publication-safe plain write -- used only before the slot is reachable by other threads.
   */
  void setPrevPlain(int slot, int value) {
    group2.putInt(group2Offset(slot) + PREV_OFFSET, value);
  }

  void setPrevVolatile(int slot, int value) {
    group2.putIntVolatile(group2Offset(slot) + PREV_OFFSET, value);
  }

  /** Plain (non-atomic) read, matching today's ordinary field-access semantics. */
  long getLastUnpinNanos(int slot) {
    return group2.getLong((group2Offset(slot) >>> 1) + LAST_UNPIN_NANOS_LONG_OFFSET);
  }

  /** Plain (non-atomic) write, matching today's ordinary field-access semantics. */
  void setLastUnpinNanos(int slot, long value) {
    group2.putLong((group2Offset(slot) >>> 1) + LAST_UNPIN_NANOS_LONG_OFFSET, value);
  }

  /**
   * Atomically sets {@code lastUnpinNanos} at {@code slot} to {@code value} and returns the
   * previous value, as a single {@code VarHandle.getAndSet}-semantics operation -- see {@link
   * OffHeapStore#getAndSetLong}. Used in place of a separate {@link #getLastUnpinNanos} + {@link
   * #setLastUnpinNanos} pair purely to fuse two memory accesses into one (the same motivation as
   * {@link #getRefCountAndGenerationVolatile}/{@link #getNextAndPrevVolatile}'s fused reads);
   * {@code lastUnpinNanos} itself is still just a racy heuristic (see this class's Group 2 javadoc)
   * -- nothing here depends on the read-then-write being atomic for correctness, only the fusion's
   * performance benefit is the point.
   */
  long getAndSetLastUnpinNanos(int slot, long value) {
    return group2.getAndSetLong((group2Offset(slot) >>> 1) + LAST_UNPIN_NANOS_LONG_OFFSET, value);
  }

  /**
   * Volatile read of {@code next} and {@code prev} at {@code slot} together, as a single 8-byte
   * access -- {@code next} in the low 32 bits, {@code prev} in the high 32 bits (native byte order;
   * both fields occupy one naturally-aligned {@code long}-sized region within the stride: offsets 0
   * and {@link #PREV_OFFSET}). See {@link #getRefCountAndGenerationVolatile} for the general
   * rationale (atomicity + half the load-acquire cost on platforms like aarch64).
   *
   * <p>Safe to use even where {@code next}/{@code prev} are read at logically different points in
   * the existing (unfused) code, e.g. {@link #removeFromList}: both {@link #reserve}'s {@code cur}
   * and {@code removeFromList}'s {@code prevSlot} are already treated as mere optimistic guesses,
   * re-validated via CAS and refreshed on failure -- fusing their *initial* reads into one earlier
   * joint snapshot can only turn a would-be-stale guess into an extra retry iteration, never an
   * incorrect outcome.
   */
  long getNextAndPrevVolatile(int slot) {
    return group2.getLongVolatile(group2Offset(slot) >>> 1);
  }

  boolean getFromHotVolatile(int slot) {
    return group2.getIntVolatile(group2Offset(slot) + FROM_HOT_OFFSET) != 0;
  }

  void setFromHotVolatile(int slot, boolean value) {
    group2.putIntVolatile(group2Offset(slot) + FROM_HOT_OFFSET, value ? 1 : 0);
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
   *
   * <p>{@code cur} is the caller's initial guess for {@code next} -- typically a fresh {@link
   * #getNextVolatile} read, but need not be: like every value fed into the CAS loop below, a stale
   * guess only costs an extra iteration (refreshed via the witness on CAS failure, or via a direct
   * re-read if it was {@code RESERVED_LINK}), never an incorrect outcome. {@link #removeFromList}
   * takes advantage of this by passing the {@code next} half of a joint next/prev snapshot (see
   * {@link #getNextAndPrevVolatile}) instead of a dedicated read.
   */
  private int reserve(long group2Offset, int reservation, int cur) {
    for (; ; ) {
      while (cur == RESERVED_LINK) {
        Thread.onSpinWait();
        cur = group2.getIntVolatile(group2Offset); // getNextVolatile(slot)
      }
      if (cur == REMOVED_LINK) {
        return cur;
      }

      // compareAndSetNext(slot, cur, reservation)
      if (group2.compareAndSetInt(group2Offset, cur, reservation)) {
        return cur;
      }
      Thread.onSpinWait();
      cur = group2.getIntVolatile(group2Offset); // getNextVolatile(slot)
    }
  }

  protected boolean insertAtHead(int listHead, int slot, boolean recordAccess) {
    // Hoisted for the same reason as pin()/unpin() (see those methods' comments): listHead's
    // offset is used three times below (reserve()'s own argument, the getNextVolatile it used to
    // wrap, and the final compareAndSetNext) and slot's offset twice -- each wrapper call below
    // would otherwise recompute group2Offset(...) from scratch, even though this method has no
    // loop of its own (unlike reserve()'s internal spin/retry loop, already hoisted).
    long listHeadOffset = group2Offset(listHead);
    long slotOffset = group2Offset(slot);
    group2.putIntVolatile(slotOffset + PREV_OFFSET, listHead); // setPrevVolatile(slot, listHead)

    // reserve(listHead, RESERVED_LINK, getNextVolatile(listHead))
    int oldNext = reserve(listHeadOffset, RESERVED_LINK, group2.getIntVolatile(listHeadOffset));

    assert oldNext != REMOVED_LINK : "queue head sentinel should never be removed";
    group2.putInt(slotOffset, oldNext); // setNextPlain(slot, oldNext)
    setPrevVolatile(oldNext, slot);

    // compareAndSetNext(listHead, RESERVED_LINK, slot)
    if (!group2.compareAndSetInt(listHeadOffset, RESERVED_LINK, slot)) {
      throw new IllegalStateException("unexpected concurrent modification of listHead.next");
    }
    return false;
  }

  private void removeFromList(int slot) {
    // slot's offset is loop-invariant across the while loop below (only prevSlot changes there) --
    // hoisted once and reused for the joint next+prev read, reserve()'s argument, and each retry's
    // getPrevVolatile(slot)-equivalent read, same rationale as insertAtHead's hoist above.
    long slotOffset = group2Offset(slot);
    // Joint initial read of next+prev (see getNextAndPrevVolatile) rather than two separate
    // volatile reads -- both are fed only as optimistic initial guesses below (reserve()'s cur,
    // and the prevSlot loop's first iteration), each independently re-validated/refreshed on
    // failure, so reading them together earlier than today's code does cannot change behavior,
    // only occasionally trade a slightly-more-likely-stale guess for an extra retry iteration.
    long nextAndPrev = group2.getLongVolatile(slotOffset >>> 1); // getNextAndPrevVolatile(slot)

    // reserve(slot, REMOVED_LINK, ...)
    int oldNext = reserve(slotOffset, REMOVED_LINK, (int) nextAndPrev);

    if (oldNext == REMOVED_LINK) {
      throw new IllegalStateException("already removed");
    }
    int prevSlot = (int) (nextAndPrev >>> 32);
    // Unlike slotOffset above, prevSlot's offset is NOT loop-invariant (prevSlot itself changes on
    // retry) -- recomputed fresh each iteration below, same as before this hoist.
    long prevSlotOffset = group2Offset(prevSlot);

    // compareAndSetNext(prevSlot, slot, RESERVED_LINK)
    while (!group2.compareAndSetInt(prevSlotOffset, slot, RESERVED_LINK)) {
      Thread.onSpinWait();
      prevSlot = group2.getIntVolatile(slotOffset + PREV_OFFSET); // getPrevVolatile(slot)
      prevSlotOffset = group2Offset(prevSlot);
    }
    setPrevVolatile(oldNext, prevSlot);

    // compareAndSetNext(prevSlot, RESERVED_LINK, oldNext)
    if (!group2.compareAndSetInt(prevSlotOffset, RESERVED_LINK, oldNext)) {
      throw new IllegalStateException("unexpected concurrent modification during list removal");
    }
  }

  private void insertAtTail(int slot) {
    // COLD_TAIL and slot are both loop-invariant across every iteration below (unlike predSlot,
    // which is re-read fresh each retry) -- hoisted once, outside the loop entirely.
    long coldTailOffset = group2Offset(COLD_TAIL);
    long slotOffset = group2Offset(slot);
    int predSlot;
    long predSlotOffset;
    for (; ; ) {
      // getPrevVolatile(COLD_TAIL)
      predSlot = group2.getIntVolatile(coldTailOffset + PREV_OFFSET);

      // predSlot's offset is used 2-3 times within this iteration (reserve()'s own argument, the
      // getNextVolatile it used to wrap, and whichever compareAndSetNext below applies) -- hoisted
      // once per iteration; it can't be hoisted further out than the loop body since predSlot
      // itself changes across iterations.
      predSlotOffset = group2Offset(predSlot);

      // reserve(predSlot, RESERVED_LINK, getNextVolatile(predSlot))
      int oldNext = reserve(predSlotOffset, RESERVED_LINK, group2.getIntVolatile(predSlotOffset));

      if (oldNext == COLD_TAIL) {
        break;
      }

      // compareAndSetNext(predSlot, RESERVED_LINK, oldNext)
      if (oldNext != REMOVED_LINK
          && !group2.compareAndSetInt(predSlotOffset, RESERVED_LINK, oldNext)) {
        // if `oldNext == REMOVED_LINK` then pred was concurrently removed, and we
        // have no reservation to release.
        // Otherwise, if pred is no longer tail's predecessor and we're unable to release our
        // reservation, it's a hard error.
        throw new IllegalStateException();
      }
    }
    group2.putInt(slotOffset, COLD_TAIL); // setNextPlain(slot, COLD_TAIL)
    group2.putIntVolatile(slotOffset + PREV_OFFSET, predSlot); // setPrevVolatile(slot, predSlot)
    group2.putIntVolatile(coldTailOffset + PREV_OFFSET, slot); // setPrevVolatile(COLD_TAIL, slot)

    // compareAndSetNext(predSlot, RESERVED_LINK, slot)
    if (!group2.compareAndSetInt(predSlotOffset, RESERVED_LINK, slot)) {
      throw new IllegalStateException("unexpected concurrent modification during tail insertion");
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
    // Hoisted once, manually, rather than relying on the JIT to CSE/LICM the equivalent repeated
    // group1Offset(slot) calls inside getRefCountAndGenerationVolatile/getRefCountVolatile/
    // compareAndExchangeRefCount/getGenerationVolatile below: pin()/unpin() are this class's two
    // hottest methods by a wide margin (~9x more Group 1 CAS traffic than all other traffic
    // combined, per this class's own javadoc), so their per-call cost in cold/C1/interpreted code
    // (before C2 warmup, or after any deopt) matters disproportionately -- see profiling notes
    // where this exact arithmetic showed up as non-negligible cost in these two methods
    // specifically. group1 (the OffHeapStore reference) is still loaded once per call to each
    // group1.xxx(...) method below, same as always -- only the slot->offset arithmetic itself is
    // hoisted here.
    long group1Offset = group1Offset(slot);

    // getRefCountAndGenerationVolatile(slot)
    long rcAndGen = group1.getLongVolatile(group1Offset >>> 1);

    if ((int) (rcAndGen >>> 32) != expectedGen) return -1;
    int rc = (int) rcAndGen;
    for (; ; ) {
      switch (rc) {
        case -1:
          return -1;
        case UNPIN_SENTINEL:
          rc = group1.getIntVolatile(group1Offset); // getRefCountVolatile(slot)
          continue;
      }
      assert rc >= 0;

      // compareAndSetRefCount(slot, rc, rc + 1)
      if (group1.compareAndSetInt(group1Offset, rc, rc + 1)) {
        break;
      } else {
        Thread.onSpinWait();
        rc = group1.getIntVolatile(group1Offset); // getRefCountVolatile(slot)
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
    // getGenerationVolatile(slot)
    if (group1.getIntVolatile(group1Offset + GENERATION_OFFSET) != expectedGen) {
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
    long rcAndGen = getRefCountAndGenerationVolatile(slot);
    switch ((int) rcAndGen) {
      case UNPIN_SENTINEL:
      case -1:
        return null;
      default:
        return (int) (rcAndGen >>> 32) == expectedGen ? payload[slot] : null;
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
    // See pin()'s identical hoist above for why this is done manually here too.
    long group1Offset = group1Offset(slot);
    int rc = group1.getIntVolatile(group1Offset); // getRefCountVolatile(slot)
    for (; ; ) {
      if (rc == 1) {
        // compareAndSetRefCount(slot, 1, UNPIN_SENTINEL)
        if (group1.compareAndSetInt(group1Offset, 1, UNPIN_SENTINEL)) {
          boolean toHot = insertAtHead(COLD_HEAD, slot, recordAccess);

          // compareAndSetRefCount(slot, UNPIN_SENTINEL, 0)
          if (!group1.compareAndSetInt(group1Offset, UNPIN_SENTINEL, 0)) {
            throw new IllegalStateException();
          }
          return toHot ? 1 : 0;
        }
      } else {
        assert rc > 1 : "unexpected refCount: " + rc;
        // compareAndSetRefCount(slot, rc, rc - 1)
        if (group1.compareAndSetInt(group1Offset, rc, rc - 1)) {
          return -1;
        }
      }
      Thread.onSpinWait();
      rc = group1.getIntVolatile(group1Offset); // getRefCountVolatile(slot)
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
      Thread.onSpinWait();
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
        if (compareAndSetRefCount(slot, 1, -1)) {
          insertAtTail(slot);
          if (!compareAndSetRefCount(slot, -1, 0)) {
            throw new IllegalStateException("rc changed during insertAtTail in closeUnconditional");
          }
          return;
        }
      } else if (rc > 1) {
        // A stale pin() raced: its CAS retry loop incremented rc past 1 before its post-CAS
        // generation check could call unpin(). Decrement our own pin and return; the stale
        // pinner's subsequent unpin() will see rc==1 and reinstate this slot in the LRU.
        if (compareAndSetRefCount(slot, rc, rc - 1)) {
          return;
        }
      } else {
        throw new IllegalStateException("unexpected rc=" + rc + " in closeUnconditional");
      }
      Thread.onSpinWait();
      rc = getRefCountVolatile(slot);
    }
  }

  static class DualQueueCache<V extends Val> extends Cache2<V> {

    /** Running memoization of last seen non-zero cold queue timestamp */
    private volatile long coldTs;

    /** Hot queue: head sentinel. Slots promoted from cold are inserted here. */
    private final int HOT_HEAD;

    /** Hot queue: tail sentinel. */
    private final int HOT_TAIL;

    /**
     * Convenience constructor for an exclusively-owned {@code DualQueueCache}: allocates its own
     * private Group 1/Group 2 stores. This is the constructor every existing caller (including all
     * current tests) uses, and its behavior is unchanged from before shared-store support existed.
     */
    DualQueueCache(int capacity, Iterable<? extends V> initialValues) {
      super(capacity, 2, initialValues);
      // Continue the spaced-by-2 sentinel scheme from COLD_TAIL (see base constructor) so
      // HOT_HEAD/HOT_TAIL each also get an exclusive cache line. Assigned directly here (and
      // identically in the other constructor below) rather than via a shared private helper
      // method, since HOT_HEAD/HOT_TAIL are blank finals -- Java's definite-assignment rules for
      // a final field require the assignment to appear directly in the constructor body (or a
      // constructor this(...) chains to), not in an arbitrary instance method called from it.
      this.HOT_HEAD = COLD_TAIL + 2;
      this.HOT_TAIL = COLD_TAIL + 4;
      initHotSentinelPayload();
    }

    /**
     * Accepts pre-built {@code (store, baseOffset)} pairs for Group 1/Group 2, exactly like {@link
     * Cache2}'s own wider constructor -- see its javadoc for sizing/alignment requirements ({@code
     * extraSentinelSlots=2} is implicit here, matching this class's own HOT_HEAD/HOT_TAIL
     * sentinels, so callers must have sized {@code group2Store} via {@code
     * Cache2.group2SlotCount(capacity, 2)}).
     */
    DualQueueCache(
        int capacity,
        Iterable<? extends V> initialValues,
        OffHeapStore group1Store,
        long group1BaseOffset,
        OffHeapStore group2Store,
        long group2BaseOffset) {
      super(
          capacity, 2, initialValues, group1Store, group1BaseOffset, group2Store, group2BaseOffset);
      // See the other constructor's comment on why this is duplicated rather than shared.
      this.HOT_HEAD = COLD_TAIL + 2;
      this.HOT_TAIL = COLD_TAIL + 4;
      initHotSentinelPayload();
    }

    /**
     * Shared tail of both constructors above -- everything after HOT_HEAD/HOT_TAIL themselves are
     * assigned (which, being blank finals, cannot happen inside this method -- see the
     * constructors' own comments).
     */
    private void initHotSentinelPayload() {
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
      long lastUnpin = getAndSetLastUnpinNanos(slot, recordAccess ? System.nanoTime() : 0);
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
      for (; ; ) {
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
        if (compareAndSetRefCount(candidate, 0, -1)) {
          break;
        }
        Thread.onSpinWait();
      }
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
