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

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import org.apache.lucene.store.OffHeapStore;
import org.apache.solr.common.util.SuppressForbidden;
import sun.misc.Unsafe;

/**
 * {@link OffHeapStore} backed by raw {@code sun.misc.Unsafe} memory ops against a direct {@link
 * ByteBuffer}'s native base address, extracted unchanged from {@link Cache2}'s pre-{@link
 * OffHeapStore}-abstraction implementation.
 *
 * <p>Allocates its own dedicated {@link ByteBuffer} per instance (one per {@code
 * UnsafeOffHeapStore} constructor call) -- unlike the {@code MemorySegment} implementation (see
 * {@code org.apache.lucene.store.MemorySegmentOffHeapStore}), this one has no
 * fast-path/generic-path distinction, since {@code Unsafe}'s per-instance {@code final long
 * baseAddr} field carries none of the JIT constant-folding sensitivity a {@code MemorySegment}
 * reference does -- instance-field addressing already performs identically to the would-be "fast
 * path" for this implementation, so there is nothing to gain from a Holder-pattern split here.
 *
 * <p>Deliberately kept in {@code solr/core} (not moved into Lucene alongside {@link OffHeapStore}):
 * {@code sun.misc.Unsafe} requires {@code solr/core/build.gradle}'s {@code -XDignore.symbol.file}
 * compiler override to suppress its mandatory "internal proprietary API" warning, a {@code
 * solr/core}-specific accommodation Lucene's build does not (and should not need to) carry.
 *
 * <p>{@code Unsafe} is natively byte-addressed, unlike {@code MemorySegmentOffHeapStore}'s {@code
 * arrayElementVarHandle} (a scaled element index) -- so every accessor here converts {@link
 * OffHeapStore}'s element-indexed {@code intIndex}/{@code longIndex} parameters (see that
 * interface's javadoc) to a byte offset via a fixed left-shift before the {@code Unsafe} call.
 */
final class UnsafeOffHeapStore implements OffHeapStore {
  private static final Unsafe UNSAFE = getUnsafe();

  @SuppressForbidden(reason = "Needs access to sun.misc.Unsafe to avoid VarHandle access overhead")
  private static Unsafe getUnsafe() {
    try {
      Field f = Unsafe.class.getDeclaredField("theUnsafe");
      f.setAccessible(true);
      return (Unsafe) f.get(null);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  // See Cache2's original javadoc (preserved here) for why this reflective approach, rather than
  // --add-opens, is used to read the buffer's native base address.
  private static final long BUFFER_ADDRESS_FIELD_OFFSET = bufferAddressFieldOffset();

  @SuppressForbidden(reason = "Needs access to sun.misc.Unsafe to avoid VarHandle access overhead")
  private static long bufferAddressFieldOffset() {
    try {
      return UNSAFE.objectFieldOffset(Buffer.class.getDeclaredField("address"));
    } catch (NoSuchFieldException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static long baseAddress(ByteBuffer direct) {
    return UNSAFE.getLong(direct, BUFFER_ADDRESS_FIELD_OFFSET);
  }

  /**
   * Retained (not just its address) purely to keep the underlying native allocation alive -- the
   * direct buffer's {@code Cleaner} runs when the {@code ByteBuffer} itself becomes unreachable, so
   * dropping this reference while {@link #baseAddr} is still in use would risk the backing memory
   * being freed out from under live {@code Unsafe} accesses.
   */
  private final ByteBuffer buf;

  private final long baseAddr;

  /**
   * Allocates a direct {@link ByteBuffer} of at least {@code bytes} bytes, aligned to a full {@code
   * alignment}-byte boundary -- see {@link Cache2#allocateAlignedDirect} (which this delegates to
   * for the actual allocation/alignment/margin arithmetic; retained there as the single source of
   * truth since it is also unit-tested directly, independent of this class).
   */
  UnsafeOffHeapStore(int slots, int stride, int alignment) {
    this.buf = Cache2.allocateAlignedDirect(slots, stride, alignment);
    this.baseAddr = baseAddress(buf);
  }

  @Override
  public int getIntVolatile(long intIndex) {
    return UNSAFE.getIntVolatile(null, baseAddr + (intIndex << 2));
  }

  @Override
  public void putInt(long intIndex, int value) {
    UNSAFE.putInt(null, baseAddr + (intIndex << 2), value);
  }

  @Override
  public void putIntVolatile(long intIndex, int value) {
    UNSAFE.putIntVolatile(null, baseAddr + (intIndex << 2), value);
  }

  @Override
  public boolean compareAndSetInt(long intIndex, int expect, int update) {
    return UNSAFE.compareAndSwapInt(null, baseAddr + (intIndex << 2), expect, update);
  }

  @Override
  public int compareAndExchangeInt(long intIndex, int expect, int update) {
    // sun.misc.Unsafe has no native compare-and-exchange (witness-returning CAS) -- emulate via
    // the standard getVolatile+compareAndSwap retry loop (the same technique j.u.c.a classes used
    // before VarHandle.compareAndExchange existed). See Cache2's original javadoc for detail.
    long addr = baseAddr + (intIndex << 2);
    for (; ; ) {
      int cur = UNSAFE.getIntVolatile(null, addr);
      if (cur != expect) {
        return cur;
      }
      if (UNSAFE.compareAndSwapInt(null, addr, expect, update)) {
        return expect;
      }
      // Lost a race between the read above and the CAS -- retry with a fresh read, exactly as a
      // real hardware CMPXCHG witness-read would naturally reflect.
    }
  }

  @Override
  public long getLongVolatile(long longIndex) {
    return UNSAFE.getLongVolatile(null, baseAddr + (longIndex << 3));
  }

  @Override
  public long getLong(long longIndex) {
    return UNSAFE.getLong(null, baseAddr + (longIndex << 3));
  }

  @Override
  public void putLong(long longIndex, long value) {
    UNSAFE.putLong(null, baseAddr + (longIndex << 3), value);
  }

  @Override
  public long getAndSetLong(long longIndex, long value) {
    return UNSAFE.getAndSetLong(null, baseAddr + (longIndex << 3), value);
  }
}
