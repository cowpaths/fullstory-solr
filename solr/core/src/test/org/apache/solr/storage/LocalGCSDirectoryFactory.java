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
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Locale;
import java.util.concurrent.ExecutorService;

/**
 * Test-only subclass of {@link GCSDirectoryFactory} that uses an in-memory GCS mock backed by
 * {@link LocalStorageHelper}.
 *
 * <p>A single {@link Storage} instance is shared across the JVM via a static field, ensuring all
 * cores on the same node have a coherent view of the blob store (matching the singleton pattern
 * used by {@code LocalStorageGCSBackupRepository}).
 *
 * <p>To use in tests, set:
 *
 * <pre>
 *   -Psolr.directoryFactory=org.apache.solr.storage.LocalGCSDirectoryFactory
 *   -Psolr.gcsDirectory.bucket=gcs-directory-test
 * </pre>
 */
public class LocalGCSDirectoryFactory extends GCSDirectoryFactory {

  private static volatile Storage storage;

  @Override
  protected Storage initStorage() {
    if (storage == null) {
      synchronized (LocalGCSDirectoryFactory.class) {
        if (storage == null) {
          // FakeStorageRpc uses a static SimpleDateFormat initialized at class-load time.
          // Under non-Latin locales (e.g. Thai), SimpleDateFormat produces non-ASCII digits that
          // DateTime.parseRfc3339() cannot parse. Force Locale.ROOT so the static formatter is
          // initialized with ASCII digits.
          Locale saved = Locale.getDefault();
          Locale.setDefault(Locale.ROOT);
          try {
            storage = LocalStorageHelper.customOptions(false).getService();
          } finally {
            Locale.setDefault(saved);
          }
        }
      }
    }
    return storage;
  }

  @Override
  protected GCSDirectory newGCSDirectory(
      Path localPath,
      String bucket,
      Storage gcsStorage,
      BlockCache cache,
      Cache<ReadChannel, Cache.Node<ReadChannel>> channelPool,
      ExecutorService ioExec,
      boolean useAsyncIO,
      DirectBufferPool bufferPool)
      throws IOException {
    return new LocalGCSDirectory(
        localPath, bucket, gcsStorage, cache, channelPool, ioExec, useAsyncIO, bufferPool);
  }
}
