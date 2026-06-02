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
import com.google.cloud.RestorableState;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;

/**
 * Test-only subclass of {@link GCSDirectory} that replaces the resumable-upload write path with a
 * simple buffered {@link Storage#create(BlobInfo, byte[], Storage.BlobTargetOption[])} call.
 *
 * <p>The GCS client's resumable-upload path ({@code storage.writer()}) builds {@code Content-Range}
 * headers via {@code String.format("%d", ...)} which is locale-sensitive. Under non-Latin locales
 * (e.g. Thai), this produces non-ASCII digits (e.g. {@code bytes ๐-๖๑/๖๒}) that {@link
 * com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper}'s {@code FakeStorageRpc} cannot
 * parse (its regex only matches ASCII {@code \d+}), causing a 404. This override bypasses the
 * resumable-upload path entirely: it accumulates compressed bytes in a heap buffer and commits them
 * atomically via {@code storage.create(BlobInfo, byte[])} on close.
 */
public class LocalGCSDirectory extends GCSDirectory {

  LocalGCSDirectory(
      Path localPath,
      String bucket,
      Storage storage,
      BlockCache cache,
      Cache<ReadChannel, Cache.Node<ReadChannel>> channelPool,
      ExecutorService ioExec,
      boolean useAsyncIO,
      DirectBufferPool bufferPool)
      throws IOException {
    super(localPath, bucket, storage, cache, channelPool, ioExec, useAsyncIO, bufferPool, null);
  }

  @Override
  protected WriteChannel openWriteChannel(BlobInfo blobInfo) {
    return new BufferingWriteChannel(storage, blobInfo);
  }

  private static final class BufferingWriteChannel implements WriteChannel {
    private final Storage storage;
    private final BlobInfo blobInfo;
    private final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    private boolean open = true;

    BufferingWriteChannel(Storage storage, BlobInfo blobInfo) {
      this.storage = storage;
      this.blobInfo = blobInfo;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
      int n = src.remaining();
      if (n > 0) {
        byte[] bytes = new byte[n];
        src.get(bytes);
        buf.write(bytes, 0, n);
      }
      return n;
    }

    @Override
    public boolean isOpen() {
      return open;
    }

    @Override
    public void close() throws IOException {
      if (open) {
        open = false;
        storage.create(blobInfo, buf.toByteArray());
      }
    }

    @Override
    public void setChunkSize(int chunkSize) {}

    @Override
    public RestorableState<WriteChannel> capture() {
      throw new UnsupportedOperationException();
    }
  }
}
