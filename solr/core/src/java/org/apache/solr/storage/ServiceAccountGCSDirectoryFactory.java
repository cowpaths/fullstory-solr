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
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.GrpcStorageOptions;
import com.google.cloud.storage.Storage;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

/**
 * Production subclass of {@link GCSDirectoryFactory} that authenticates to GCS using a service
 * account key file (JSON format).
 *
 * <p>The key file path and GCP project ID are read from {@code solrconfig.xml} or system
 * properties:
 *
 * <ul>
 *   <li>{@code serviceAccountKeyFile} / {@code solr.gcsDirectory.serviceAccountKeyFile} — path to
 *       the service account JSON key file
 *   <li>{@code project} / {@code solr.gcsDirectory.project} — GCP project ID
 * </ul>
 *
 * <p>Example {@code solrconfig.xml} snippet:
 *
 * <pre>{@code
 * <directoryFactory name="DirectoryFactory"
 *                   class="org.apache.solr.storage.ServiceAccountGCSDirectoryFactory">
 *   <str name="bucket">my-solr-index-bucket</str>
 *   <str name="project">my-gcp-project</str>
 *   <str name="serviceAccountKeyFile">/etc/solr/gcs-key.json</str>
 * </directoryFactory>
 * }</pre>
 */
public class ServiceAccountGCSDirectoryFactory extends GCSDirectoryFactory {

  private String keyFilePath;
  private String project;

  @Override
  public void init(NamedList<?> args) {
    SolrParams params = args.toSolrParams();
    keyFilePath =
        params.get(
            "serviceAccountKeyFile",
            System.getProperty("solr.gcsDirectory.serviceAccountKeyFile", ""));
    if (keyFilePath.isEmpty()) {
      throw new IllegalArgumentException(
          "ServiceAccountGCSDirectoryFactory requires a key file path via the "
              + "'serviceAccountKeyFile' param or 'solr.gcsDirectory.serviceAccountKeyFile' "
              + "system property.");
    }
    project = params.get("project", System.getProperty("solr.gcsDirectory.project", ""));
    if (project.isEmpty()) {
      throw new IllegalArgumentException(
          "ServiceAccountGCSDirectoryFactory requires a GCP project ID via the "
              + "'project' param or 'solr.gcsDirectory.project' system property.");
    }
    super.init(args);
  }

  @Override
  protected Storage initStorage() {
    try (InputStream is = Files.newInputStream(Path.of(keyFilePath))) {
      GoogleCredentials credentials =
          ServiceAccountCredentials.fromStream(is)
              .createScoped(
                  Collections.singleton("https://www.googleapis.com/auth/devstorage.read_write"));
      return GrpcStorageOptions.newBuilder()
          .setAttemptDirectPath(true)
          .setCredentials(credentials)
          .setProjectId(project)
          .build()
          .getService();
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to load GCS service account key from: " + keyFilePath, e);
    }
  }
}
