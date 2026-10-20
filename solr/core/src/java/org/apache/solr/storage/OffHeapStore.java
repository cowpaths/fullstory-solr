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

/**
 * Raw primitive off-heap memory operations, abstracting {@link Cache2}'s Group 1/Group 2 storage
 * away from the specific backing mechanism. Two implementations exist (see {@link OffHeapStores}
 * for the factory that produces both):
 *
 * <ul>
 *   <li>{@code UnsafeStore} -- wraps {@code sun.misc.Unsafe} against a direct {@link
 *       java.nio.ByteBuffer}'s native address. This is today's shipped, benchmark-validated
 *       approach (see {@code solr/prodbench/FINDINGS.md} for the investigation that selected it).
 *   <li>A {@code MemorySegment}-backed implementation (experimental) -- see {@link
 *       OffHeapStores#createMemorySegment}'s javadoc for the specific shape chosen and why.
 * </ul>
 *
 * <p>All operations take a {@code long offset} relative to whatever region this specific store
 * <em>instance</em> represents. A store obtained directly from {@link OffHeapStores} (today's only
 * usage: each {@link Cache2} instance gets its own dedicated store, exactly as the existing
 * per-instance {@code group1}/{@code group2} {@link java.nio.ByteBuffer}s already do) is addressed
 * from offset 0. A future shared-arena caller (one store spanning multiple {@link Cache2}
 * instances' combined regions, each instance holding its own {@code final long baseOffset} into the
 * shared store) would add that offset at each call site itself, mirroring exactly the already-
 * benchmarked shape in {@code DirectCaseBenchmark}'s {@code MsegPartitioned}/{@code
 * UnsafePartitioned} rungs (a bare {@code final long partitionOffset} field, added inline -- never
 * absorbed into a wrapper object, which earlier rungs showed defeats the backing segment's
 * constant-folding). Not yet wired up anywhere in this codebase; see {@link OffHeapStores}'s class
 * javadoc for the open question this leaves for {@link BlockCache} integration.
 *
 * <p>Method semantics (plain vs. volatile vs. compare-and-swap/exchange) exactly match {@code
 * sun.misc.Unsafe}'s own naming and behavior, since every implementation must support the identical
 * access patterns {@link Cache2} already relies on.
 */
interface OffHeapStore {
  /** Volatile read of a 4-byte int at {@code offset}. */
  int getIntVolatile(long offset);

  /** Plain (non-atomic, non-volatile) write of a 4-byte int at {@code offset}. */
  void putInt(long offset, int value);

  /** Volatile write of a 4-byte int at {@code offset}. */
  void putIntVolatile(long offset, int value);

  /**
   * Atomically sets the int at {@code offset} to {@code update} if it currently equals {@code
   * expect}, returning {@code true} iff the exchange succeeded -- same semantics as {@code
   * VarHandle.compareAndSet}.
   */
  boolean compareAndSetInt(long offset, int expect, int update);

  /**
   * Atomically sets the int at {@code offset} to {@code update} if it currently equals {@code
   * expect}, returning the witness value (the value observed immediately before this call, which
   * equals {@code expect} iff the exchange succeeded) -- same semantics as {@code
   * VarHandle.compareAndExchange}.
   */
  int compareAndExchangeInt(long offset, int expect, int update);

  /** Volatile read of an 8-byte long at {@code offset}. */
  long getLongVolatile(long offset);

  /** Plain (non-atomic, non-volatile) read of an 8-byte long at {@code offset}. */
  long getLong(long offset);

  /** Plain (non-atomic, non-volatile) write of an 8-byte long at {@code offset}. */
  void putLong(long offset, long value);
}
