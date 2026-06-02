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
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
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
 *     {segUUID}                       PERSISTENT — data: newline-separated blob UUID strings
 *       {refId}                       PERSISTENT — data: empty; presence = this replica holds a ref
 * </pre>
 *
 * <p>{@code refId} is a deterministic UUID derived from the replica's local index directory path.
 * This scopes refs to individual replicas, not Solr nodes, so two replicas on the same node each
 * carry independent refs. Using persistent (not ephemeral) nodes means refs survive node restarts:
 * a replica that shuts down for an hour still holds its ref, and its data is not deleted.
 *
 * <p>Lifecycle:
 *
 * <ul>
 *   <li>{@link #registerBatch} creates the batch node (if absent) and adds a persistent ref node
 *       for this replica.
 *   <li>{@link #release} removes this replica's ref. If no ref nodes remain, the batch node is
 *       deleted and all blob UUIDs are returned to the caller for physical GCS deletion.
 *   <li>Orphaned batches (refs left behind by permanently-removed replicas) require a background
 *       sweep for cleanup; that is left for future work.
 * </ul>
 */
public class ZkBlobLifecycleCoordinator implements GCSDirectory.BlobLifecycleCoordinator {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final String BASE_PATH = "/gcs-segment-batches";

  private final SolrZkClient zkClient;

  /**
   * Stable identifier for this replica's ref node — a deterministic UUID derived from the local
   * index directory path. Unique per replica even when multiple replicas share a node.
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
    byte[] data = serializeBlobs(blobUUIDs);
    try {
      try {
        zkClient.create(batchPath, data, CreateMode.PERSISTENT, true);
      } catch (KeeperException.NodeExistsException e) {
        // Another replica already registered this batch; our blob set should be identical.
      }
      // Persistent ref node: survives node restarts so data is not deleted while offline.
      try {
        zkClient.create(batchPath + "/" + refId, null, CreateMode.PERSISTENT, true);
      } catch (KeeperException.NodeExistsException e) {
        // Re-registering after a restart; ref was already present.
      }
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Failed to register batch " + segUUID, e);
    }
  }

  @Override
  public Collection<UUID> release(UUID segUUID) throws IOException {
    String batchPath = BASE_PATH + "/" + segUUID;
    try {
      zkClient.delete(batchPath + "/" + refId, -1, true);

      List<String> remainingRefs = zkClient.getChildren(batchPath, null, true);
      if (!remainingRefs.isEmpty()) {
        return Collections.emptyList();
      }

      // We're the last holder. Read the blob list, then clean up the ZK node.
      byte[] data = zkClient.getData(batchPath, null, null, true);
      Collection<UUID> blobs = deserializeBlobs(data);
      try {
        zkClient.delete(batchPath, -1, true);
      } catch (KeeperException.NoNodeException e) {
        // Another replica raced us to the deletion; that's fine.
      } catch (KeeperException.NotEmptyException e) {
        // A new ref was added between our getChildren and delete — batch is still live.
        log.debug(
            "batch {} re-acquired by another replica during release; skipping delete", segUUID);
        return Collections.emptyList();
      }
      return blobs;

    } catch (KeeperException.NoNodeException e) {
      // Batch was already cleaned up (e.g. by a racing release on another replica).
      return Collections.emptyList();
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Failed to release batch " + segUUID, e);
    }
  }

  private static byte[] serializeBlobs(Collection<UUID> blobUUIDs) {
    return blobUUIDs.stream()
        .map(UUID::toString)
        .collect(Collectors.joining("\n"))
        .getBytes(StandardCharsets.UTF_8);
  }

  private static Collection<UUID> deserializeBlobs(byte[] data) {
    if (data == null || data.length == 0) return Collections.emptyList();
    String[] parts = new String(data, StandardCharsets.UTF_8).split("\n");
    List<UUID> result = new java.util.ArrayList<>(parts.length);
    for (String part : parts) {
      if (!part.isEmpty()) {
        result.add(UUID.fromString(part));
      }
    }
    return result;
  }
}
