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

import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage.BlobTargetOption;
import com.google.cloud.storage.Storage.BlobWriteOption;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.common.util.EnvUtils;

/**
 * Facade over N independent {@link com.google.cloud.storage.Storage} client instances that
 * distributes operations across them to reduce per-client gRPC channel contention. Analogous to how
 * {@link BlockCache} stripes across multiple {@link Cache} partitions.
 *
 * <p>The number of stripes must be a power of two. By default each operation picks a stripe via
 * {@link ThreadLocalRandom}, spreading concurrent reads of the same blob across clients. Blob-name
 * affinity routing (same blob → same client) can be enabled via the {@code
 * solr.gcsDirectory.blobAffinity} system property.
 *
 * <p>Only the methods actually used by {@link GCSDirectory} and {@link GcsBlobLifecycleCoordinator}
 * are exposed.
 */
public final class Storage implements Closeable {

  /**
   * When true, each operation is routed to the stripe determined by the blob name's UUID prefix, so
   * the same blob always lands on the same client. When false (default), each operation picks a
   * stripe via {@link ThreadLocalRandom}, spreading concurrent reads of the same blob across
   * clients.
   */
  private static final boolean BLOB_AFFINITY =
      EnvUtils.getPropertyAsBool("solr.gcsDirectory.blobAffinity", false);

  private final com.google.cloud.storage.Storage[] stripes;
  private final int mask;
  private final int routingCharCount;

  /**
   * Creates a {@link Storage} instance.
   *
   * @param stripeCount must be a power of 2
   * @param factory supplies {@link com.google.cloud.storage.Storage} instances to populate the
   *     client array.
   */
  public Storage(int stripeCount, Supplier<com.google.cloud.storage.Storage> factory) {
    if (stripeCount == 0 || Integer.bitCount(stripeCount) != 1) {
      throw new IllegalArgumentException(
          "stripes.length must be a power of two, got " + stripeCount);
    }
    this.stripes = new com.google.cloud.storage.Storage[stripeCount];
    this.mask = stripeCount - 1;
    for (int i = mask; i >= 0; i--) {
      stripes[i] = factory.get();
    }
    routingCharCount = (mask / 16) + 1; // for hex
  }

  /** Convenience factory for single-stripe use (tests, simple deployments). */
  public static Storage of(com.google.cloud.storage.Storage storage) {
    return new Storage(1, () -> storage);
  }

  private com.google.cloud.storage.Storage stripeFor(BlobId blob) {
    int idx =
        BLOB_AFFINITY
            ? Integer.parseInt(blob.getName(), 0, routingCharCount, 16) & mask
            : ThreadLocalRandom.current().nextInt(stripes.length);
    return stripes[idx];
  }

  // ---------------------------------------------------------------------------
  // GCSDirectory methods
  // ---------------------------------------------------------------------------

  public ReadChannel reader(BlobId blobId) {
    return stripeFor(blobId).reader(blobId);
  }

  public WriteChannel writer(BlobInfo blobInfo, BlobWriteOption... options) {
    return stripeFor(blobInfo.getBlobId()).writer(blobInfo, options);
  }

  public boolean delete(BlobId blobId) {
    return stripeFor(blobId).delete(blobId);
  }

  // ---------------------------------------------------------------------------
  // GcsBlobLifecycleCoordinator methods
  // ---------------------------------------------------------------------------

  public Blob get(BlobId blobId) {
    return stripeFor(blobId).get(blobId);
  }

  public Blob create(BlobInfo blobInfo, byte[] content, BlobTargetOption... options) {
    return stripeFor(blobInfo.getBlobId()).create(blobInfo, content, options);
  }

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  @Override
  public void close() throws IOException {
    // implementation ported from `lucene.IOUtils.close(Closeable...)`
    Throwable th = null;
    for (AutoCloseable object : stripes) {
      try {
        if (object != null) {
          object.close();
        }
      } catch (Throwable t) {
        th = IOUtils.useOrSuppress(th, t);
      }
    }

    if (th != null) {
      throw IOUtils.rethrowAlways(th);
    }
  }
}
