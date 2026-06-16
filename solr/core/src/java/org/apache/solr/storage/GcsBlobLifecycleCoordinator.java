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

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage.BlobTargetOption;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.cloud.storage.StorageException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GCS-backed implementation of {@link GCSDirectory.BlobLifecycleCoordinator}.
 *
 * <p>Stores one object per segment batch in a dedicated metadata bucket (separate from the data
 * bucket, and configured for immediate soft-delete so cleaned-up entries are not retained). Each
 * object is named by its {@code segUUID} string and its content uses the {@link BlobMetadataCodec}
 * format: refs and manifest in a single blob.
 *
 * <p>All updates use GCS generation-based compare-and-swap ({@code generationMatch}). On release, a
 * conditional delete at the generation most recently read acts as the single-winner claim: only one
 * replica proceeds to return the blob UUIDs for GCS data deletion.
 *
 * <p>Orphaned batches (refs left behind by permanently-removed replicas) require a background sweep
 * for cleanup; that is left for future work.
 */
public class GcsBlobLifecycleCoordinator implements GCSDirectory.BlobLifecycleCoordinator {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** HTTP status code returned by GCS when an {@code generationMatch} precondition fails. */
  private static final int HTTP_PRECONDITION_FAILED = 412;

  private final Storage storage;
  private final String metadataBucket;

  /**
   * Stable identifier for this replica — a deterministic UUID derived from the local index
   * directory name. Unique per replica even when co-located.
   */
  private final UUID refId;

  private final boolean useMultipartUpload;

  public GcsBlobLifecycleCoordinator(
      Storage storage, String metadataBucket, UUID refId, boolean useMultipartUpload) {
    this.storage = storage;
    this.metadataBucket = metadataBucket;
    this.refId = refId;
    this.useMultipartUpload = useMultipartUpload;
  }

  @Override
  public void registerBatch(UUID segUUID, Collection<UUID> blobUUIDs) throws IOException {
    String name = segUUID.toString();
    List<UUID> sortedBlobs = BlobMetadataCodec.sorted(blobUUIDs);
    BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(metadataBucket, name)).build();
    while (true) {
      Blob blob = storage.get(BlobId.of(metadataBucket, name));
      if (blob == null) {
        // Object doesn't exist; use generationMatch(0) to guard against a concurrent creation
        // between our get() and this write. If the precondition fails, another replica created
        // the object first — retry and merge our ref into the now-existing object.
        byte[] newData = BlobMetadataCodec.encodeNew(refId, sortedBlobs);
        try {
          writeBlob(blobInfo, newData, 0L);
          return;
        } catch (StorageException e) {
          if (e.getCode() != HTTP_PRECONDITION_FAILED) {
            throw new IOException("Failed to register batch " + segUUID, e);
          } else {
            // Precondition failed; retry and merge our ref into the now-existing object.
            blob = storage.get(BlobId.of(metadataBucket, name));
          }
        }
      }
      byte[] existing = blob.getContent();
      byte[] merged = BlobMetadataCodec.mergeInto(existing, refId, sortedBlobs);
      if (merged == existing) {
        return; // Already registered; nothing to do.
      }
      try {
        writeBlob(blobInfo, merged, blob.getGeneration());
        return;
      } catch (StorageException e) {
        if (e.getCode() != HTTP_PRECONDITION_FAILED) {
          throw new IOException("Failed to register batch " + segUUID, e);
        }
        // Concurrent update; retry.
      }
    }
  }

  @Override
  public Collection<BlobId> release(UUID segUUID, String bucket) throws IOException {
    String name = segUUID.toString();
    BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(metadataBucket, name)).build();
    while (true) {
      Blob blob = storage.get(BlobId.of(metadataBucket, name));
      if (blob == null) {
        return Collections.emptyList(); // Already cleaned up.
      }
      byte[] existing = blob.getContent();
      byte[] updated = BlobMetadataCodec.removeRef(existing, refId);
      if (updated == existing) {
        return Collections.emptyList(); // Our ref not present.
      }
      if (!BlobMetadataCodec.refsEmpty(updated)) {
        // Other refs remain; write back with our ref removed.
        try {
          writeBlob(blobInfo, updated, blob.getGeneration());
          return Collections.emptyList();
        } catch (StorageException e) {
          if (e.getCode() != HTTP_PRECONDITION_FAILED) {
            throw new IOException("Failed to release batch " + segUUID, e);
          }
          continue; // Concurrent update; retry.
        }
      }
      // Last ref — claim cleanup via conditional delete at current generation.
      // GCS deletes at the specified generation only if it is still live; returns false otherwise.
      List<BlobId> manifest = BlobMetadataCodec.decodeManifest(existing, bucket);
      if (storage.delete(BlobId.of(metadataBucket, name, blob.getGeneration()))) {
        return manifest;
      }
      // Lost the race (concurrent re-registration or release); retry.
    }
  }

  /**
   * Dispatches to {@link #writeMultipart} or {@link #writeResumable} based on backend capability.
   */
  private void writeBlob(BlobInfo blobInfo, byte[] data, long ifGenerationMatch) {
    if (useMultipartUpload) {
      writeMultipart(blobInfo, data, ifGenerationMatch);
    } else {
      writeResumable(blobInfo, data, ifGenerationMatch);
    }
  }

  /**
   * Writes {@code data} as a single-request multipart upload with an {@code ifGenerationMatch}
   * precondition. One HTTP round-trip.
   */
  private void writeMultipart(BlobInfo blobInfo, byte[] data, long ifGenerationMatch) {
    try {
      BlobInfo withGen =
          BlobInfo.newBuilder(
                  BlobId.of(blobInfo.getBucket(), blobInfo.getName(), ifGenerationMatch))
              .build();
      storage.create(withGen, data, BlobTargetOption.generationMatch());
    } catch (StorageException e) {
      throw e;
    } catch (Exception e) {
      throw new StorageException(0, "Failed to write via multipart", e);
    }
  }

  /**
   * Writes {@code data} via a resumable upload channel with an {@code ifGenerationMatch}
   * precondition. Two HTTP round-trips; used when the backend does not support multipart.
   */
  private void writeResumable(BlobInfo blobInfo, byte[] data, long ifGenerationMatch) {
    try (WriteChannel writer =
        storage.writer(blobInfo, BlobWriteOption.generationMatch(ifGenerationMatch))) {
      writer.write(ByteBuffer.wrap(data));
    } catch (StorageException e) {
      throw e;
    } catch (Exception e) {
      throw new StorageException(0, "Failed to write via resumable channel", e);
    }
  }
}
