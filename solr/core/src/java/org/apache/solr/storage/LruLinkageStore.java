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

/**
 * Backing store for {@link Cache2}'s "Group 2" fields ({@code next}, {@code prev}, {@code fromHot},
 * {@code lastUnpinNanos}), indexed by slot. Covers every addressable slot -- real slots {@code
 * 1..capacity} <em>and</em> all sentinel slots (COLD_HEAD/COLD_TAIL/HOT_HEAD/HOT_TAIL) -- unlike
 * {@link RefCountGenerationStore}, which only needs real-slot coverage: sentinel slots are the
 * literal list-boundary pointers and have their {@code next}/{@code prev} read/written on
 * essentially every list operation.
 *
 * <p>Backed by a single 64-byte-aligned {@link ByteBuffer} (via {@link ByteBuffer#alignedSlice}),
 * with 2 slots per 32-byte stride (32 bytes is comfortably more than the 17 bytes of real data per
 * slot: {@code next} 4B + {@code prev} 4B + {@code fromHot} 1B + {@code lastUnpinNanos} 8B). This
 * accepts that 2 logically-unrelated slots may share a cache line -- a conscious, lower-traffic-
 * tier tradeoff versus {@link RefCountGenerationStore}'s 1-slot-per-line layout, since Group 2
 * carries roughly 1/9th of Group 1's write traffic (only touched on genuine first-pin/last-unpin
 * list transitions, not on every re-pin).
 *
 * <p>TODO: today's slot numbering is sequential/adjacent-correlated with real-world access-burst
 * adjacency (same structural fact that motivated {@link RefCountGenerationStore}'s one-pair-per-
 * line layout -- {@code BlockCache.distribute()} assigns consecutive array slots to consecutive/
 * file-adjacent {@code cacheBlockOrd} values). With 2 slots sharing a line, the two slots paired
 * together are presently whichever two land at adjacent even/odd stride offsets -- i.e.
 * likely-adjacent-access slots. Randomizing/decorrelating which two slots pair up on a line
 * (independent of numeric adjacency) is deferred, to be picked up together with the analogous
 * "shuffle initial slot distribution" idea deferred from the Group 1 round.
 *
 * <p>Method semantics intentionally mirror today's exact per-field access patterns rather than
 * uniformly using volatile/CAS everywhere:
 *
 * <ul>
 *   <li>{@code next}: mixed plain/volatile/CAS, matching the prior {@code NEXT_VH} usage.
 *   <li>{@code prev}: volatile-only; never CAS'd anywhere in {@link Cache2}.
 *   <li>{@code lastUnpinNanos}: plain {@code long}, read/written with ordinary field semantics
 *       today (a racy heuristic used only for eviction-priority scoring, not correctness-critical)
 *       -- intentionally NOT upgraded to volatile/CAS.
 *   <li>{@code fromHot}: volatile {@code boolean} (plain reads/writes on a {@code volatile} field
 *       today), never CAS'd.
 * </ul>
 */
final class LruLinkageStore {
  private static final int CACHE_LINE = 64;
  private static final int STRIDE = 32; // 2 slots per 64-byte cache line
  private static final int NEXT_OFFSET = 0;
  private static final int PREV_OFFSET = 4;
  private static final int FROM_HOT_OFFSET = 8;
  private static final int LAST_UNPIN_NANOS_OFFSET = 16; // 8-byte-aligned within the 32B stride

  private static final VarHandle INT_VH =
      MethodHandles.byteBufferViewVarHandle(int[].class, ByteOrder.nativeOrder());
  private static final VarHandle LONG_VH =
      MethodHandles.byteBufferViewVarHandle(long[].class, ByteOrder.nativeOrder());

  private final ByteBuffer buf;
  private final int length;

  /**
   * @param length total addressable slots, including all sentinel indices.
   */
  LruLinkageStore(int length) {
    this.length = length;
    int needed = Math.multiplyExact(length, STRIDE);
    // Must align to CACHE_LINE (64), not STRIDE (32): the cache-line-isolation guarantee this
    // store exists for (and that Cache2's sentinel spacing depends on) requires every even slot
    // index to start a fresh 64-byte line, which only holds if the buffer's own base is 64-byte
    // aligned -- 32-byte alignment alone is not sufficient and was a bug in an earlier version of
    // this class (confirmed empirically: ByteBuffer.allocateDirect's underlying native allocation
    // is not guaranteed better than 32-byte aligned, and a 32-byte-but-not-64-byte-aligned base
    // would silently shift every "even slot starts a line" assumption off by one slot).
    //
    // ByteBuffer.alignedSlice(unitSize) rounds its *limit* down to the nearest alignment boundary
    // in addition to rounding its start up, so up to (CACHE_LINE-1) bytes can be lost from each
    // end independently -- a margin of only (CACHE_LINE-1) is insufficient whenever `needed` is
    // not itself an exact multiple of CACHE_LINE (as it is not here: STRIDE=32 means `needed` is
    // only ever a multiple of 32, never guaranteed to be a multiple of 64), and can silently yield
    // a slice narrower than `needed` (confirmed empirically to shortfall 100% of the time at this
    // class's actual slot-count formula, using only a (CACHE_LINE-1) margin). 2*(CACHE_LINE-1)
    // margin is sufficient regardless of `needed`'s own alignment.
    this.buf = ByteBuffer.allocateDirect(needed + 2 * (CACHE_LINE - 1)).alignedSlice(CACHE_LINE);
  }

  /** Total addressable length (real slots + all sentinel slots) -- for bounds asserts. */
  int length() {
    return length;
  }

  /**
   * For testing: {@code buf}'s base-address alignment, via the public {@link
   * ByteBuffer#alignmentOffset(int, int)} (no reflection/{@code --add-opens} needed) -- 0 iff the
   * backing buffer's address is a multiple of {@code unitSize} bytes.
   */
  int alignmentOffset(int unitSize) {
    return buf.alignmentOffset(0, unitSize);
  }

  /**
   * For testing: total byte capacity of the backing buffer, which may exceed {@code length *
   * STRIDE} due to {@link ByteBuffer#alignedSlice}'s rounding -- never less, which is exactly what
   * the margin bug under test would violate.
   */
  int byteCapacity() {
    return buf.capacity();
  }

  private int slotBase(int slot) {
    return Math.multiplyExact(slot, STRIDE);
  }

  int getNextVolatile(int slot) {
    return (int) INT_VH.getVolatile(buf, slotBase(slot) + NEXT_OFFSET);
  }

  /**
   * Pre-publication-safe plain write -- used only before the slot is reachable by other threads.
   */
  void setNextPlain(int slot, int value) {
    INT_VH.set(buf, slotBase(slot) + NEXT_OFFSET, value);
  }

  void setNextVolatile(int slot, int value) {
    INT_VH.setVolatile(buf, slotBase(slot) + NEXT_OFFSET, value);
  }

  int compareAndExchangeNext(int slot, int expect, int update) {
    return (int) INT_VH.compareAndExchange(buf, slotBase(slot) + NEXT_OFFSET, expect, update);
  }

  boolean compareAndSetNext(int slot, int expect, int update) {
    return (boolean) INT_VH.compareAndSet(buf, slotBase(slot) + NEXT_OFFSET, expect, update);
  }

  int getPrevVolatile(int slot) {
    return (int) INT_VH.getVolatile(buf, slotBase(slot) + PREV_OFFSET);
  }

  /**
   * Pre-publication-safe plain write -- used only before the slot is reachable by other threads.
   */
  void setPrevPlain(int slot, int value) {
    INT_VH.set(buf, slotBase(slot) + PREV_OFFSET, value);
  }

  void setPrevVolatile(int slot, int value) {
    INT_VH.setVolatile(buf, slotBase(slot) + PREV_OFFSET, value);
  }

  /** Plain (non-atomic) read, matching today's ordinary field-access semantics. */
  long getLastUnpinNanos(int slot) {
    return (long) LONG_VH.get(buf, slotBase(slot) + LAST_UNPIN_NANOS_OFFSET);
  }

  /** Plain (non-atomic) write, matching today's ordinary field-access semantics. */
  void setLastUnpinNanos(int slot, long value) {
    LONG_VH.set(buf, slotBase(slot) + LAST_UNPIN_NANOS_OFFSET, value);
  }

  boolean getFromHotVolatile(int slot) {
    return (int) INT_VH.getVolatile(buf, slotBase(slot) + FROM_HOT_OFFSET) != 0;
  }

  void setFromHotVolatile(int slot, boolean value) {
    INT_VH.setVolatile(buf, slotBase(slot) + FROM_HOT_OFFSET, value ? 1 : 0);
  }
}
