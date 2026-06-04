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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.GrpcStorageOptions;
import com.google.cloud.storage.Storage;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;

/**
 * Test/dev subclass of {@link GCSDirectoryFactory} that authenticates via <a
 * href="https://cloud.google.com/docs/authentication/application-default-credentials">Application
 * Default Credentials</a> (ADC), allowing tests to run against a real (non-production) GCS bucket
 * using local user credentials.
 *
 * <p>Before running, authenticate with:
 *
 * <pre>
 *   gcloud auth application-default login
 * </pre>
 *
 * <p>Usage:
 *
 * <pre>
 *   -Psolr.directoryFactory=org.apache.solr.storage.ADCGCSDirectoryFactory
 *   -Psolr.gcsDirectory.bucket=my-dev-bucket
 *   -Psolr.gcsDirectory.project=my-gcp-project
 * </pre>
 */
public class ADCGCSDirectoryFactory extends GCSDirectoryFactory {

  @Override
  protected Storage initStorage() {
    String project = System.getProperty("solr.gcsDirectory.project");
    if (project == null || project.isEmpty()) {
      throw new IllegalArgumentException(
          "ADCGCSDirectoryFactory requires a GCP project ID via 'solr.gcsDirectory.project'.");
    }
    try {
      GoogleCredentials credentials =
          GoogleCredentials.getApplicationDefault()
              .createScoped(
                  Collections.singleton("https://www.googleapis.com/auth/devstorage.read_write"));
      return GrpcStorageOptions.newBuilder()
          .setAttemptDirectPath(true)
          .setCredentials(credentials)
          .setProjectId(project)
          .build()
          .getService();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to load application-default GCS credentials", e);
    }
  }
}
