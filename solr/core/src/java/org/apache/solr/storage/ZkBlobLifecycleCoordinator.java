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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZooKeeper-backed implementation of {@link GCSDirectory.BlobLifecycleCoordinator} for distributed
 * blob lifecycle management across Solr replicas.
 *
 * <p>ZK layout (paths relative to the Solr ZK chroot):
 *
 * <pre>
 *   /gcs-segment-batches/
 *     {segUUID}          PERSISTENT — data: sorted binary UUID list (16 bytes each)
 *       refs             PERSISTENT — data: sorted binary UUID list (16 bytes each)
 * </pre>
 *
 * <p>{@code refId} is a deterministic UUID derived from the replica's local index directory name,
 * scoping each ref to an individual replica. Persistent nodes survive node restarts, so a replica
 * that goes offline for an hour still holds its ref and its data is not deleted.
 *
 * <p>Both nodes are updated via compare-and-swap (versioned {@code setData}) to handle concurrent
 * access. On release, the versioned delete of the {@code refs} node acts as a single-winner claim:
 * only one replica proceeds to delete the batch and return the blob UUIDs for GCS deletion.
 *
 * <p>Orphaned batches (refs left behind by permanently-removed replicas) require a background sweep
 * for cleanup; that is left for future work.
 */
public class ZkBlobLifecycleCoordinator implements GCSDirectory.BlobLifecycleCoordinator {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int UUID_LENGTH = Long.BYTES << 1;

  static final String BASE_PATH = "/gcs-segment-batches";

  private final SolrZkClient zkClient;

  /**
   * Stable identifier for this replica's entry in the {@code refs} node — a deterministic UUID
   * derived from the local index directory name. Unique per replica even when co-located.
   */
  private final UUID refId;

  public ZkBlobLifecycleCoordinator(SolrZkClient zkClient, UUID refId) throws IOException {
    this.zkClient = zkClient;
    this.refId = refId;
    try {
      zkClient.makePath(BASE_PATH, false, true);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Failed to create ZK base path: " + BASE_PATH, e);
    }
  }

  @Override
  public void registerBatch(UUID segUUID, Collection<UUID> blobUUIDs) throws IOException {
    String batchPath = BASE_PATH + "/" + segUUID;
    String refsPath = batchPath + "/refs";
    try {
      upsert(batchPath, sorted(blobUUIDs));
      upsert(refsPath, List.of(refId));
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Failed to register batch " + segUUID, e);
    }
  }

  @Override
  public Collection<UUID> release(UUID segUUID) throws IOException {
    String batchPath = BASE_PATH + "/" + segUUID;
    String refsPath = batchPath + "/refs";
    try {
      // CAS-remove our refId from the refs node.
      byte[] updatedRefs;
      Stat stat = new Stat();
      while (true) {
        byte[] existing = zkClient.getData(refsPath, null, stat, true);
        updatedRefs = sortedRemove(existing, refId);
        try {
          zkClient.setData(refsPath, updatedRefs, stat.getVersion(), true);
          break;
        } catch (KeeperException.BadVersionException e) {
          // Concurrent update; retry.
        }
      }

      if (updatedRefs.length != 0) {
        return Collections.emptyList();
      }

      // refs is empty — claim deletion via versioned delete.
      // ZK guarantees at most one caller succeeds at this version.
      try {
        zkClient.delete(refsPath, stat.getVersion() + 1, true);
      } catch (KeeperException.BadVersionException | KeeperException.NoNodeException e) {
        // Another replica re-registered (BadVersion) or already cleaned up (NoNode).
        return Collections.emptyList();
      }

      // We own the cleanup. Read blobs before deleting the parent.
      byte[] blobData = zkClient.getData(batchPath, null, null, true);
      Collection<UUID> blobs = deserializeUUIDs(blobData);
      try {
        zkClient.delete(batchPath, -1, true);
      } catch (KeeperException.NoNodeException e) {
        // Already gone; fine.
      }
      return blobs;

    } catch (KeeperException.NoNodeException e) {
      // Batch was already cleaned up by a concurrent release.
      return Collections.emptyList();
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Failed to release batch " + segUUID, e);
    }
  }

  /**
   * Ensures {@code path} exists and contains at least {@code newItems} (must be pre-sorted). Reads
   * current state first; if the node is absent, creates it; otherwise CAS-merges. {@link
   * KeeperException.NodeExistsException} is only possible as a rare concurrent-creation race and is
   * handled by retrying the read.
   */
  private void upsert(String path, List<UUID> newItems)
      throws KeeperException, InterruptedException {
    while (true) {
      Stat stat = new Stat();
      byte[] existing;
      try {
        existing = zkClient.getData(path, null, stat, true);
      } catch (KeeperException.NoNodeException e) {
        try {
          zkClient.create(path, serializeUUIDs(newItems), CreateMode.PERSISTENT, true);
          return;
        } catch (KeeperException.NodeExistsException ex) {
          continue; // Created concurrently; loop back to read-then-merge.
        }
      }
      byte[] merged = sortedMergeBytes(existing, newItems);
      if (merged == existing) {
        return; // All items already present; nothing to write.
      }
      try {
        zkClient.setData(path, merged, stat.getVersion(), true);
        return;
      } catch (KeeperException.BadVersionException e) {
        // Concurrent update; retry.
      }
    }
  }

  private static List<UUID> sorted(Collection<UUID> uuids) {
    List<UUID> list = new ArrayList<>(uuids);
    Collections.sort(list);
    return list;
  }

  /**
   * Two-pointer merge of sorted {@code existing} byte array and sorted {@code newItems} list,
   * deduplicating equal elements. Writes into a heap-allocated ByteBuffer sized for the worst case,
   * then trims via {@link System#arraycopy}. Returns {@code existing} unchanged (identity) if all
   * new items were already present.
   */
  private static byte[] sortedMergeBytes(byte[] existing, List<UUID> newItems) {
    int ei = 0;
    int ni = 0;
    int existingBytes = existing.length;
    int newSize = newItems.size();
    ByteBuffer src = ByteBuffer.wrap(existing);
    ByteBuffer dst = ByteBuffer.allocate(existingBytes + (newSize * UUID_LENGTH));
    while (ei < existingBytes && ni < newSize) {
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
        ei += UUID_LENGTH;
      } else if (cmp > 0) {
        dst.putLong(nMsb);
        dst.putLong(nLsb);
        ni++;
      } else {
        dst.putLong(eMsb);
        dst.putLong(eLsb);
        ei += UUID_LENGTH;
        ni++; // deduplicate
      }
    }
    if (ei < existingBytes) {
      // Drain remaining existing entries.
      dst.put(existing, ei, existingBytes - ei);
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
      return existing; // nothing new — identity return
    }
    byte[] result = new byte[written];
    System.arraycopy(dst.array(), 0, result, 0, written);
    return result;
  }

  /**
   * Scans the sorted 16-byte-per-UUID array for {@code item}, copying units speculatively into a
   * new array. On a match, drains the remaining bytes and returns the new array. If the item is not
   * found, returns the original array unchanged (no allocation is retained).
   */
  private static byte[] sortedRemove(byte[] data, UUID item) {
    if (data.length < UUID_LENGTH) {
      return data;
    }
    long msbTarget = item.getMostSignificantBits();
    long lsbTarget = item.getLeastSignificantBits();
    byte[] result = new byte[data.length - UUID_LENGTH];
    ByteBuffer src = ByteBuffer.wrap(data);
    ByteBuffer dst = ByteBuffer.wrap(result);
    while (dst.remaining() >= UUID_LENGTH) {
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
    } else {
      return data; // not found — discard speculative allocation, return original
    }
  }

  private static byte[] serializeUUIDs(List<UUID> uuids) {
    ByteBuffer buf = ByteBuffer.allocate(uuids.size() * UUID_LENGTH);
    for (UUID uuid : uuids) {
      buf.putLong(uuid.getMostSignificantBits());
      buf.putLong(uuid.getLeastSignificantBits());
    }
    return buf.array();
  }

  private static List<UUID> deserializeUUIDs(byte[] data) {
    if (data == null || data.length == 0) {
      return Collections.emptyList();
    }
    ByteBuffer buf = ByteBuffer.wrap(data);
    List<UUID> result = new ArrayList<>(data.length / UUID_LENGTH);
    while (buf.remaining() >= UUID_LENGTH) {
      result.add(new UUID(buf.getLong(), buf.getLong()));
    }
    return result;
  }
}
