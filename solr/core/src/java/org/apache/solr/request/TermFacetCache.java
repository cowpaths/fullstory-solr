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
package org.apache.solr.request;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.search.QueryResultKey;

/** */
public class TermFacetCache {

  public static final String NAME = "termFacetCache";
  public static int DEFAULT_THRESHOLD = 5000; // non-final to support setting by tests

  public static final class FacetCacheKey implements Accountable {

    private static final long BASE_RAM_BYTES =
        RamUsageEstimator.shallowSizeOfInstance(FacetCacheKey.class);

    private final QueryResultKey qrk;
    private final String fieldName;

    public FacetCacheKey(QueryResultKey qrk, String fieldName) {
      this.qrk = qrk;
      this.fieldName = fieldName;
    }

    @Override
    public int hashCode() {
      return qrk == null ? fieldName.hashCode() : qrk.hashCode() ^ fieldName.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof FacetCacheKey)) return false;
      FacetCacheKey other = (FacetCacheKey) obj;
      return fieldName.equals(other.fieldName) && Objects.equals(qrk, other.qrk);
    }

    @Override
    public long ramBytesUsed() {
      return BASE_RAM_BYTES + RamUsageEstimator.sizeOf(fieldName) + qrk.ramBytesUsed();
    }

    public boolean isCrossDoc() {
      return qrk.isCrossDoc();
    }
  }

  public static final class SegmentCacheEntry implements Accountable {

    private static final long BASE_RAM_BYTES =
        RamUsageEstimator.shallowSizeOfInstance(SegmentCacheEntry.class);

    public final byte[] counts;
    private final byte[] topLevelCounts;
    public final boolean hasMissingSlot;

    public SegmentCacheEntry(byte[] counts) {
      this.counts = counts;
      this.topLevelCounts = null;
      this.hasMissingSlot = false;
    }

    public SegmentCacheEntry(int[] topLevelCounts, boolean includesMissingCount) {
      this.counts = null;
      this.topLevelCounts =
          TermFacetCache.encodeCounts(
              topLevelCounts,
              new ByteBuffersDataOutput(((long) topLevelCounts.length) << 1),
              topLevelCounts.length);
      this.hasMissingSlot = includesMissingCount;
    }

    public int[] topLevelCounts() {
      return TermFacetCache.decodeCounts(topLevelCounts);
    }

    @Override
    public long ramBytesUsed() {
      return BASE_RAM_BYTES
          + (counts == null ? 0 : RamUsageEstimator.sizeOf(counts))
          + (topLevelCounts == null ? 0 : RamUsageEstimator.sizeOf(topLevelCounts));
    }
  }

  public interface CacheUpdater {
    boolean incrementFromCachedSegment(LongValues toGlobal);

    void updateLeaf(int[] leafCounts, int segMissingIdx);

    void updateTopLevel();
  }

  public static byte[] encodeCounts(
      int[] segCounts, ByteBuffersDataOutput cachedSegCountsBuilder, int limit) {
    try {
      cachedSegCountsBuilder.writeVInt(limit);
      int last = segCounts[limit - 1];
      cachedSegCountsBuilder.writeVInt(last);
      for (int i = limit - 2; i >= 0; i--) {
        int val = segCounts[i];
        cachedSegCountsBuilder.writeZInt(val - last);
        last = val;
      }
    } catch (IOException ex) {
      throw new RuntimeException(
          ByteBuffersDataOutput.class + " should not throw IOException in practice", ex);
    }
    return cachedSegCountsBuilder.toArrayCopy();
  }

  public static int[] decodeCounts(byte[] encoded) {
    ByteArrayDataInput in = new ByteArrayDataInput(encoded);
    try {
      final int len = in.readVInt();
      final int[] ret = new int[len];
      int val = in.readVInt();
      ret[len - 1] = val;
      for (int i = len - 2; i >= 0; i--) {
        ret[i] = (val += in.readZInt());
      }
      assert in.eof();
      return ret;
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  public static void mergeCachedSegmentCounts(
      int[] counts, byte[] cachedSegCounts, LongValues ordMap) {
    ByteArrayDataInput segCounts = new ByteArrayDataInput(cachedSegCounts);
    try {
      final int len = segCounts.readVInt();
      int val = segCounts.readVInt(); // "missing" count
      counts[len - 1] += val;
      for (int i = len - 2; i >= 0; i--) {
        val += segCounts.readZInt();
        if (val != 0) {
          counts[ordMap == null ? i : (int) ordMap.get(i)] += val;
        }
      }
      assert segCounts.eof();
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }
}
