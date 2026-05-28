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

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

/**
 * Test/dev subclass of {@link GCSDirectoryFactory} that routes GCS operations to a local GCS
 * emulator (e.g. {@code gcloud beta emulators storage} or any GCS-compatible emulator).
 *
 * <p>Unlike {@link LocalGCSDirectoryFactory}, this uses a real HTTP transport, so it exercises the
 * full {@link AsyncGCSWriteHelper} write path and byte-range reads over the wire. gRPC is not used
 * here because gcsemu (and most GCS emulators) only support the HTTP/JSON API.
 *
 * <p><b>Locale sensitivity:</b> the GCS Java client builds {@code Content-Range} headers via {@code
 * String.format("%d", ...)}, which is locale-sensitive. Under non-Latin locales (e.g. Thai) the
 * header contains non-ASCII digits that most GCS emulators cannot parse. Always specify a Latin
 * locale when running tests against this factory, e.g. {@code -Ptests.locale=en} (or {@code root}).
 *
 * <p>Usage:
 *
 * <pre>
 *   -Psolr.directoryFactory=org.apache.solr.storage.EmulatorGCSDirectoryFactory
 *   -Psolr.gcsDirectory.bucket=my-test-bucket
 *   # optional, defaults to http://localhost:4443
 *   -Psolr.gcsDirectory.emulatorHost=http://localhost:4443
 * </pre>
 *
 * <p>The bucket named by {@code solr.gcsDirectory.bucket} must already exist in the emulator, or
 * the emulator must be configured to auto-create buckets.
 */
public class EmulatorGCSDirectoryFactory extends GCSDirectoryFactory {

  private static final String DEFAULT_EMULATOR_HOST = "http://localhost:4443";

  @Override
  protected Storage initStorage() {
    if (!String.format("%d", 0).equals("0")) {
      throw new IllegalStateException(
          "EmulatorGCSDirectoryFactory requires a Latin locale: the GCS client builds "
              + "locale-sensitive Content-Range headers that most emulators cannot parse "
              + "with non-ASCII digits. Run with -Ptests.locale=en (or root).");
    }
    String host = System.getProperty("solr.gcsDirectory.emulatorHost", DEFAULT_EMULATOR_HOST);
    return StorageOptions.newBuilder()
        .setHost(host)
        .setProjectId("emulator-project")
        .setCredentials(NoCredentials.getInstance())
        .build()
        .getService();
  }
}
