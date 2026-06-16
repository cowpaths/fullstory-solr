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

/**
 * Facade over N independent {@link com.google.cloud.storage.Storage} client instances that
 * distributes operations across them by hashing the blob name. Analogous to how {@link BlockCache}
 * stripes across multiple {@link Cache} partitions to reduce per-client gRPC channel contention.
 *
 * <p>The number of stripes must be a power of two. Each operation is routed to {@code
 * stripes[name.hashCode() & mask]}, so the same blob name always lands on the same client —
 * preserving any connection-reuse benefit while spreading load across channels.
 *
 * <p>Only the methods actually used by {@link GCSDirectory} and {@link GcsBlobLifecycleCoordinator}
 * are exposed.
 */
public final class Storage implements Closeable {

  private final com.google.cloud.storage.Storage[] stripes;
  private final int mask;

  /**
   * @param stripes independent {@link com.google.cloud.storage.Storage} instances; length must be a
   *     power of two
   */
  public Storage(com.google.cloud.storage.Storage[] stripes) {
    if (stripes.length == 0 || Integer.bitCount(stripes.length) != 1) {
      throw new IllegalArgumentException(
          "stripes.length must be a power of two, got " + stripes.length);
    }
    this.stripes = stripes.clone();
    this.mask = stripes.length - 1;
  }

  /** Convenience factory for single-stripe use (tests, simple deployments). */
  public static Storage of(com.google.cloud.storage.Storage storage) {
    return new Storage(new com.google.cloud.storage.Storage[] {storage});
  }

  private com.google.cloud.storage.Storage stripeFor(String name) {
    return stripes[name.hashCode() & mask];
  }

  /** Returns the first stripe, e.g. for probe operations that only need one client. */
  public com.google.cloud.storage.Storage first() {
    return stripes[0];
  }

  // ---------------------------------------------------------------------------
  // GCSDirectory methods
  // ---------------------------------------------------------------------------

  public ReadChannel reader(BlobId blobId) {
    return stripeFor(blobId.getName()).reader(blobId);
  }

  public WriteChannel writer(BlobInfo blobInfo, BlobWriteOption... options) {
    return stripeFor(blobInfo.getName()).writer(blobInfo, options);
  }

  public boolean delete(BlobId blobId) {
    return stripeFor(blobId.getName()).delete(blobId);
  }

  // ---------------------------------------------------------------------------
  // GcsBlobLifecycleCoordinator methods
  // ---------------------------------------------------------------------------

  public Blob get(BlobId blobId) {
    return stripeFor(blobId.getName()).get(blobId);
  }

  public Blob create(BlobInfo blobInfo, byte[] content, BlobTargetOption... options) {
    return stripeFor(blobInfo.getName()).create(blobInfo, content, options);
  }

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  @Override
  public void close() throws IOException {
    IOException first = null;
    for (com.google.cloud.storage.Storage stripe : stripes) {
      try {
        stripe.close();
      } catch (Exception e) {
        if (first == null) first = e instanceof IOException ? (IOException) e : new IOException(e);
      }
    }
    if (first != null) throw first;
  }
}
