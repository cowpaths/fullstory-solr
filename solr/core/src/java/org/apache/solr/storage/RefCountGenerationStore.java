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
 * Backing store for a fixed-capacity array of {@code (refCount, generation)} int pairs, indexed by
 * slot ({@code 1..capacity}; slot {@code 0} is reserved/unused by convention and need not be
 * addressable). Backed by a single 64-byte-aligned {@link ByteBuffer}, with each slot's {@code
 * (refCount, generation)} pair occupying its own 64-byte stride (at byte offsets 0 and 4 within the
 * stride; the remaining 56 bytes are unused padding) so that no two slots' hottest fields ever
 * share a cache line, regardless of concurrent access pattern -- avoids false sharing between
 * unrelated slots without entangling {@link Cache2} with the storage mechanics directly.
 *
 * <p>All methods have volatile/CAS semantics equivalent to a {@code VarHandle} over a volatile
 * {@code int} field -- same memory-ordering guarantees as the on-heap fields this replaces.
 *
 * <p>{@code allocateDirect}'s backing allocation is only guaranteed 8-byte aligned (it is a thin
 * wrapper over a {@code malloc}-style native allocation), not 64-byte aligned, so a plain {@code
 * allocateDirect} buffer is sliced via {@link ByteBuffer#alignedSlice} to obtain a true 64-byte-
 * aligned base -- a hard guarantee, requiring no platform-specific code or preview APIs (plain
 * {@code java.nio}/{@code java.lang.invoke}, available since Java 9).
 */
final class RefCountGenerationStore {
  private static final int STRIDE = 64; // one cache line per slot
  private static final int REF_COUNT_OFFSET = 0;
  private static final int GENERATION_OFFSET = 4;

  // byteBufferViewVarHandle's coordinates are (ByteBuffer, int offset); access modes
  // (getVolatile/compareAndSet/compareAndExchange/setVolatile) match plain VarHandle semantics
  // over a volatile int field.
  private static final VarHandle INT_VH =
      MethodHandles.byteBufferViewVarHandle(int[].class, ByteOrder.nativeOrder());

  private final ByteBuffer buf;

  /**
   * @param capacity number of real slots; slot 0 is reserved/unused but still addressable.
   */
  RefCountGenerationStore(int capacity) {
    int needed = Math.multiplyExact(capacity + 1, STRIDE);
    this.buf = ByteBuffer.allocateDirect(needed + STRIDE - 1).alignedSlice(STRIDE);
  }

  private int slotBase(int slot) {
    return Math.multiplyExact(slot, STRIDE);
  }

  /** Volatile read of {@code refCount} at {@code slot}. */
  int getRefCountVolatile(int slot) {
    return (int) INT_VH.getVolatile(buf, slotBase(slot) + REF_COUNT_OFFSET);
  }

  /**
   * Atomically sets {@code refCount} at {@code slot} to {@code update} if it currently equals
   * {@code expect}, returning the witness value (the value observed immediately before this call,
   * which equals {@code expect} iff the exchange succeeded) -- same semantics as {@code
   * VarHandle.compareAndExchange}.
   */
  int compareAndExchangeRefCount(int slot, int expect, int update) {
    return (int) INT_VH.compareAndExchange(buf, slotBase(slot) + REF_COUNT_OFFSET, expect, update);
  }

  /**
   * Atomically sets {@code refCount} at {@code slot} to {@code update} if it currently equals
   * {@code expect}, returning {@code true} iff the exchange succeeded -- same semantics as {@code
   * VarHandle.compareAndSet}.
   */
  boolean compareAndSetRefCount(int slot, int expect, int update) {
    return (boolean) INT_VH.compareAndSet(buf, slotBase(slot) + REF_COUNT_OFFSET, expect, update);
  }

  /** Volatile read of {@code generation} at {@code slot}. */
  int getGenerationVolatile(int slot) {
    return (int) INT_VH.getVolatile(buf, slotBase(slot) + GENERATION_OFFSET);
  }

  /**
   * Atomically increments {@code generation} at {@code slot} by one and returns the <em>new</em>
   * (post-increment) value. Callers must hold exclusive ownership of {@code slot} when calling this
   * (e.g. via a prior successful {@code refCount} CAS to an exclusive sentinel value) -- concurrent
   * calls for the same slot are not supported and may lose updates.
   */
  int incrementAndGetGeneration(int slot) {
    int offset = slotBase(slot) + GENERATION_OFFSET;
    int newGen = (int) INT_VH.getVolatile(buf, offset) + 1;
    INT_VH.setVolatile(buf, offset, newGen);
    return newGen;
  }
}
