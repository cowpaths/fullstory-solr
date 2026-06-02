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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Codec for the consolidated refs+manifest blob format shared by {@link ZkBlobLifecycleCoordinator}
 * and {@link GcsBlobLifecycleCoordinator}.
 *
 * <p>Format: {@code [4-byte int N][bytes 4..N: sorted ref UUIDs][bytes N..end: sorted manifest blob
 * UUIDs]}, where N is the byte offset of the first manifest UUID (equivalently, {@code 4 + 16 *
 * refCount}). All UUIDs are stored as two consecutive big-endian {@code long}s (MSB then LSB), 16
 * bytes each.
 */
final class BlobMetadataCodec {

  private static final int UUID_BYTES = Long.BYTES << 1; // 16
  private static final int HEADER_BYTES = Integer.BYTES; // 4
  private static final int NEW_MANIFEST_OFFSET = HEADER_BYTES + UUID_BYTES;

  private BlobMetadataCodec() {}

  /** Encodes a new entry with a single ref and a pre-sorted manifest. */
  static byte[] encodeNew(UUID refId, List<UUID> manifest) {
    byte[] result = new byte[NEW_MANIFEST_OFFSET + (manifest.size() * UUID_BYTES)];
    ByteBuffer buf = ByteBuffer.wrap(result);
    buf.putInt(NEW_MANIFEST_OFFSET);
    buf.putLong(refId.getMostSignificantBits());
    buf.putLong(refId.getLeastSignificantBits());
    for (UUID uuid : manifest) {
      buf.putLong(uuid.getMostSignificantBits());
      buf.putLong(uuid.getLeastSignificantBits());
    }
    return result;
  }

  /**
   * Merges {@code refId} into the refs section and {@code manifest} into the manifest section of
   * {@code existing}. Returns {@code existing} unchanged (identity) if both are already present.
   */
  static byte[] mergeInto(byte[] existing, UUID refId, List<UUID> manifest) {
    int n = ByteBuffer.wrap(existing).getInt(0);
    byte[] newRefs = sortedMergeSlice(existing, HEADER_BYTES, n, List.of(refId));
    byte[] newManifest = sortedMergeSlice(existing, n, existing.length, manifest);
    if (newRefs == null && newManifest == null) {
      return existing;
    }
    int refsLen = newRefs != null ? newRefs.length : n - HEADER_BYTES;
    int manifestLen = newManifest != null ? newManifest.length : existing.length - n;
    int newN = HEADER_BYTES + refsLen;
    byte[] result = new byte[newN + manifestLen];
    ByteBuffer buf = ByteBuffer.wrap(result);
    buf.putInt(newN);
    if (newRefs != null) {
      buf.put(newRefs);
    } else {
      buf.put(existing, HEADER_BYTES, refsLen);
    }
    if (newManifest != null) {
      buf.put(newManifest);
    } else {
      buf.put(existing, n, manifestLen);
    }
    return result;
  }

  /**
   * Removes {@code refId} from the refs section, preserving the manifest section unchanged. Returns
   * {@code existing} unchanged (identity) if the ref is not present.
   */
  static byte[] removeRef(byte[] existing, UUID refId) {
    int n = ByteBuffer.wrap(existing).getInt(0);
    byte[] newRefs = sortedRemoveSlice(existing, HEADER_BYTES, n, refId);
    if (newRefs == null) {
      return existing;
    }
    int newN = HEADER_BYTES + newRefs.length;
    int manifestLen = existing.length - n;
    byte[] result = new byte[newN + manifestLen];
    ByteBuffer buf = ByteBuffer.wrap(result);
    buf.putInt(newN);
    buf.put(newRefs);
    buf.put(existing, n, manifestLen);
    return result;
  }

  /** Returns {@code true} if the refs section contains no UUIDs. */
  static boolean refsEmpty(byte[] data) {
    return ByteBuffer.wrap(data).getInt(0) == HEADER_BYTES;
  }

  /** Decodes the manifest (blob UUID) section. */
  static List<UUID> decodeManifest(byte[] data) {
    int n = ByteBuffer.wrap(data).getInt(0);
    return deserializeUUIDs(data, n, data.length);
  }

  /** Returns a sorted copy of {@code uuids}. */
  static List<UUID> sorted(Collection<UUID> uuids) {
    List<UUID> list = new ArrayList<>(uuids);
    Collections.sort(list);
    return list;
  }

  // ---------------------------------------------------------------------------
  // Serialization primitives
  // ---------------------------------------------------------------------------

  /**
   * Two-pointer merge of the sorted UUID section {@code existing[from..to)} and sorted {@code
   * newItems}, deduplicating equal elements. Returns a trimmed new array containing the merged
   * section, or {@code null} if all new items were already present (no change).
   */
  private static byte[] sortedMergeSlice(byte[] existing, int from, int to, List<UUID> newItems) {
    int ei = from;
    int ni = 0;
    int existingBytes = to - from;
    int newSize = newItems.size();
    ByteBuffer src = ByteBuffer.wrap(existing);
    ByteBuffer dst = ByteBuffer.allocate(existingBytes + (newSize * UUID_BYTES));
    while (ei < to && ni < newSize) {
      long eMsb = src.getLong(ei);
      long eLsb = src.getLong(ei + Long.BYTES);
      UUID newUUID = newItems.get(ni);
      long nMsb = newUUID.getMostSignificantBits();
      long nLsb = newUUID.getLeastSignificantBits();
      int cmp = Long.compare(eMsb, nMsb);
      if (cmp == 0) {
        cmp = Long.compare(eLsb, nLsb);
      }
      if (cmp < 0) {
        dst.putLong(eMsb);
        dst.putLong(eLsb);
        ei += UUID_BYTES;
      } else if (cmp > 0) {
        dst.putLong(nMsb);
        dst.putLong(nLsb);
        ni++;
      } else {
        dst.putLong(eMsb);
        dst.putLong(eLsb);
        ei += UUID_BYTES;
        ni++; // deduplicate
      }
    }
    if (ei < to) {
      // Drain remaining existing entries.
      dst.put(existing, ei, to - ei);
    } else {
      // Drain remaining new items.
      while (ni < newSize) {
        UUID newUUID = newItems.get(ni++);
        dst.putLong(newUUID.getMostSignificantBits());
        dst.putLong(newUUID.getLeastSignificantBits());
      }
    }
    int written = dst.position();
    if (written == existingBytes) {
      return null; // nothing new
    }
    byte[] result = new byte[written];
    System.arraycopy(dst.array(), 0, result, 0, written);
    return result;
  }

  /**
   * Scans the sorted UUID section {@code data[from..to)} for {@code item}. Returns a new array with
   * it removed, or {@code null} if not found.
   */
  private static byte[] sortedRemoveSlice(byte[] data, int from, int to, UUID item) {
    int sectionLen = to - from;
    if (sectionLen < UUID_BYTES) {
      return null;
    }
    long msbTarget = item.getMostSignificantBits();
    long lsbTarget = item.getLeastSignificantBits();
    byte[] result = new byte[sectionLen - UUID_BYTES];
    ByteBuffer src = ByteBuffer.wrap(data, from, sectionLen);
    ByteBuffer dst = ByteBuffer.wrap(result);
    while (dst.remaining() >= UUID_BYTES) {
      long msb = src.getLong();
      long lsb = src.getLong();
      if (msb == msbTarget && lsb == lsbTarget) {
        dst.put(src); // drain remaining entries into result
        return result;
      }
      dst.putLong(msb);
      dst.putLong(lsb);
    }
    if (msbTarget == src.getLong() && lsbTarget == src.getLong()) {
      return result;
    }
    return null; // not found
  }

  private static List<UUID> deserializeUUIDs(byte[] data, int from, int to) {
    if (to <= from) {
      return Collections.emptyList();
    }
    ByteBuffer buf = ByteBuffer.wrap(data, from, to - from);
    List<UUID> result = new ArrayList<>((to - from) / UUID_BYTES);
    while (buf.remaining() >= UUID_BYTES) {
      result.add(new UUID(buf.getLong(), buf.getLong()));
    }
    return result;
  }
}
