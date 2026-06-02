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
 *     {segUUID}   PERSISTENT — data: {@link BlobMetadataCodec} format (refs + manifest)
 * </pre>
 *
 * <p>Each node stores both the ref list (which replicas hold this segment) and the manifest (blob
 * UUIDs belonging to the segment) in a single PERSISTENT node. Persistent nodes survive node
 * restarts, so a replica that goes offline still holds its ref and its data is not prematurely
 * deleted.
 *
 * <p>All updates use compare-and-swap (versioned {@code setData}). On release, if the refs section
 * becomes empty, a versioned delete at the just-written version acts as a single-winner claim: only
 * one replica proceeds to return the blob UUIDs for GCS deletion.
 *
 * <p>Orphaned batches (refs left behind by permanently-removed replicas) require a background sweep
 * for cleanup; that is left for future work.
 */
public class ZkBlobLifecycleCoordinator implements GCSDirectory.BlobLifecycleCoordinator {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final String BASE_PATH = "/gcs-segment-batches";

  private final SolrZkClient zkClient;

  /**
   * Stable identifier for this replica's entry in the refs section — a deterministic UUID derived
   * from the local index directory name. Unique per replica even when co-located.
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
    try {
      upsert(batchPath, refId, BlobMetadataCodec.sorted(blobUUIDs));
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Failed to register batch " + segUUID, e);
    }
  }

  @Override
  public Collection<UUID> release(UUID segUUID) throws IOException {
    String batchPath = BASE_PATH + "/" + segUUID;
    try {
      byte[] existing;
      byte[] updated;
      Stat stat = new Stat();
      while (true) {
        existing = zkClient.getData(batchPath, null, stat, true);
        updated = BlobMetadataCodec.removeRef(existing, refId);
        if (updated == existing) {
          return Collections.emptyList(); // our ref not present
        }
        try {
          zkClient.setData(batchPath, updated, stat.getVersion(), true);
          break;
        } catch (KeeperException.BadVersionException e) {
          // Concurrent update; retry.
        }
      }

      if (!BlobMetadataCodec.refsEmpty(updated)) {
        return Collections.emptyList(); // other refs remain
      }

      // Last ref — claim cleanup via versioned delete.
      // setData at version V creates version V+1; delete at V+1 guarantees single winner.
      List<UUID> manifest = BlobMetadataCodec.decodeManifest(existing);
      try {
        zkClient.delete(batchPath, stat.getVersion() + 1, true);
      } catch (KeeperException.BadVersionException | KeeperException.NoNodeException e) {
        // Another replica re-registered (BadVersion) or already cleaned up (NoNode).
        return Collections.emptyList();
      }
      return manifest;

    } catch (KeeperException.NoNodeException e) {
      // Batch was already cleaned up by a concurrent release.
      return Collections.emptyList();
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Failed to release batch " + segUUID, e);
    }
  }

  /**
   * Ensures {@code path} exists with {@code refId} in the refs section and {@code manifest} in the
   * manifest section. Reads current state first; creates on absence; otherwise CAS-merges. {@link
   * KeeperException.NodeExistsException} is only possible as a rare concurrent-creation race and is
   * handled by retrying the read.
   */
  private void upsert(String path, UUID refId, List<UUID> manifest)
      throws KeeperException, InterruptedException {
    while (true) {
      Stat stat = new Stat();
      byte[] existing;
      try {
        existing = zkClient.getData(path, null, stat, true);
      } catch (KeeperException.NoNodeException e) {
        try {
          zkClient.create(
              path, BlobMetadataCodec.encodeNew(refId, manifest), CreateMode.PERSISTENT, true);
          return;
        } catch (KeeperException.NodeExistsException ex) {
          continue; // Created concurrently; loop back to read-then-merge.
        }
      }
      byte[] merged = BlobMetadataCodec.mergeInto(existing, refId, manifest);
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
}
