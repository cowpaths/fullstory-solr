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
import org.apache.solr.SolrTestCaseJ4;

/**
 * Regression tests for {@link Cache2#allocateAlignedDirect(int, int, int)}, the shared allocation
 * helper backing both Group 1 ({@code group1}) and Group 2 ({@code group2}) off-heap storage.
 *
 * <p>Both regressions below were previously found as separate, latent bugs when Group 1/Group 2
 * storage lived in now-removed dedicated classes ({@code LruLinkageStore}/ {@code
 * RefCountGenerationStore}): (1) requesting alignment equal to the per-slot stride is not
 * sufficient when callers (like {@code Cache2}'s sentinel spacing) actually depend on a wider
 * alignment; (2) {@link ByteBuffer#alignedSlice} rounds its limit down as well as rounding its
 * start up, so a margin of only {@code (alignment - 1)} can silently under-allocate whenever the
 * requested size isn't itself an exact multiple of the alignment.
 */
public class TestCache2AllocateAlignedDirect extends SolrTestCaseJ4 {

  /**
   * The backing buffer must be aligned to the full requested {@code alignment}, not merely to
   * {@code stride} -- exercised here with {@code stride < alignment} (mirroring Group 2's
   * 32-byte-stride/64-byte-alignment split) across many lengths/trials, since {@code
   * allocateDirect}'s actual alignment varies from run to run (not guaranteed better than ~8-32
   * bytes by the JDK).
   */
  public void testBufferIsAlignedToFullAlignmentNotJustStride() {
    int stride = 32;
    int alignment = 64;
    int[] slotCounts = {1, 2, 3, 4, 5, 100, 101, 511, 512, 513, 1000, 2_621_442};
    for (int slots : slotCounts) {
      for (int trial = 0; trial < 20; trial++) {
        ByteBuffer buf = Cache2.allocateAlignedDirect(slots, stride, alignment);
        assertEquals(
            "slots="
                + slots
                + " trial="
                + trial
                + ": buffer must be "
                + alignment
                + "-byte"
                + " aligned",
            0,
            buf.alignmentOffset(0, alignment));
      }
    }
  }

  /**
   * The buffer must have sufficient capacity for the full {@code slots * stride} bytes requested,
   * including the critical last-slot boundary case -- a margin of only {@code (alignment - 1)} is
   * insufficient whenever {@code slots * stride} isn't itself a multiple of {@code alignment},
   * which (for {@code stride=32}, {@code alignment=64}) is true for every odd {@code slots}.
   */
  public void testBufferHasSufficientCapacityForFullLength() {
    int stride = 32;
    int alignment = 64;
    int[] slotCounts = {1, 2, 3, 4, 5, 100, 101, 511, 512, 513, 1000, 2_621_442};
    for (int slots : slotCounts) {
      for (int trial = 0; trial < 20; trial++) {
        ByteBuffer buf = Cache2.allocateAlignedDirect(slots, stride, alignment);
        int needed = slots * stride;
        assertTrue(
            "slots="
                + slots
                + " trial="
                + trial
                + ": buffer too short (capacity="
                + buf.capacity()
                + ", needed="
                + needed
                + ")",
            buf.capacity() >= needed);

        // Exercise the last valid slot directly -- under the margin bug this either throws
        // (buffer literally too short) or silently aliases neighboring native memory.
        int lastSlotBase = (slots - 1) * stride;
        buf.putInt(lastSlotBase, 123);
        assertEquals(123, buf.getInt(lastSlotBase));
      }
    }
  }

  /** When {@code stride == alignment} (Group 1's case), a smaller margin is also sufficient. */
  public void testStrideEqualsAlignmentCase() {
    int stride = 64;
    int alignment = 64;
    int[] slotCounts = {1, 2, 100, 511, 512, 2_621_441};
    for (int slots : slotCounts) {
      ByteBuffer buf = Cache2.allocateAlignedDirect(slots, stride, alignment);
      assertEquals(0, buf.alignmentOffset(0, alignment));
      assertTrue(buf.capacity() >= slots * stride);
    }
  }
}
