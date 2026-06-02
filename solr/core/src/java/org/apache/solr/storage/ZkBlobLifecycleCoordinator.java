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
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
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
 *     {segUUID}          PERSISTENT — data: newline-separated blob UUID strings
 *       refs             PERSISTENT — data: newline-separated refId strings
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

  static final String BASE_PATH = "/gcs-segment-batches";

  private final SolrZkClient zkClient;

  /**
   * Stable identifier for this replica's entry in the {@code refs} node — a deterministic UUID
   * derived from the local index directory name. Unique per replica even when co-located.
   */
  private final String refId;

  public ZkBlobLifecycleCoordinator(SolrZkClient zkClient, UUID refId) throws IOException {
    this.zkClient = zkClient;
    this.refId = refId.toString();
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
      try {
        zkClient.create(batchPath, serializeItems(blobUUIDs), CreateMode.PERSISTENT, true);
      } catch (KeeperException.NodeExistsException e) {
        mergeItems(batchPath, blobUUIDs);
      }
      try {
        zkClient.create(refsPath, serializeItems(List.of(refId)), CreateMode.PERSISTENT, true);
      } catch (KeeperException.NodeExistsException e) {
        mergeItems(refsPath, List.of(refId));
      }
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
      while (true) {
        Stat stat = new Stat();
        byte[] existing = zkClient.getData(refsPath, null, stat, true);
        Set<String> refs = new LinkedHashSet<>(deserializeItems(existing));
        refs.remove(refId);
        try {
          zkClient.setData(refsPath, serializeItems(refs), stat.getVersion(), true);
          break;
        } catch (KeeperException.BadVersionException e) {
          // Concurrent update; retry.
        }
      }

      // Re-read to check emptiness after our write landed.
      Stat stat = new Stat();
      byte[] refsData = zkClient.getData(refsPath, null, stat, true);
      if (!deserializeItems(refsData).isEmpty()) {
        return Collections.emptyList();
      }

      // refs is empty — claim deletion via versioned delete.
      // ZK guarantees at most one caller succeeds at this version.
      try {
        zkClient.delete(refsPath, stat.getVersion(), true);
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
   * CAS loop to merge {@code newItems} (as strings) into the data at {@code path}. Items are stored
   * as a newline-separated list; duplicates are suppressed via {@link LinkedHashSet}.
   */
  private <T> void mergeItems(String path, Collection<T> newItems)
      throws KeeperException, InterruptedException {
    while (true) {
      Stat stat = new Stat();
      byte[] existing = zkClient.getData(path, null, stat, true);
      Set<String> merged = new LinkedHashSet<>(deserializeItems(existing));
      boolean changed = false;
      for (T item : newItems) {
        changed |= merged.add(item.toString());
      }
      if (!changed) return;
      try {
        zkClient.setData(path, serializeItems(merged), stat.getVersion(), true);
        return;
      } catch (KeeperException.BadVersionException e) {
        // Concurrent update; retry.
      }
    }
  }

  private static <T> byte[] serializeItems(Collection<T> items) {
    return items.stream()
        .map(Object::toString)
        .collect(Collectors.joining("\n"))
        .getBytes(StandardCharsets.UTF_8);
  }

  private static List<String> deserializeItems(byte[] data) {
    if (data == null || data.length == 0) return Collections.emptyList();
    List<String> result = new java.util.ArrayList<>();
    for (String part : new String(data, StandardCharsets.UTF_8).split("\n")) {
      if (!part.isEmpty()) result.add(part);
    }
    return result;
  }

  private static Collection<UUID> deserializeUUIDs(byte[] data) {
    List<String> items = deserializeItems(data);
    List<UUID> result = new java.util.ArrayList<>(items.size());
    for (String s : items) result.add(UUID.fromString(s));
    return result;
  }
}
