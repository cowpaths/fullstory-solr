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
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.lang.ref.WeakReference;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.StandardDirectoryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link org.apache.solr.core.DirectoryFactory} that stores compressed index data in Google Cloud
 * Storage and keeps only lightweight offset-manifest files on the local filesystem, via {@link
 * GCSDirectory}.
 *
 * <p>Node-level resources (executor, block cache, buffer pool) are shared across all index
 * directories on the same node via {@link CoreContainer#getObjectCache()}.
 *
 * <p>This factory never uses application-default GCS credentials. Subclasses must override {@link
 * #initStorage()} to supply a {@link Storage} instance — either a real configured client (with an
 * explicit service account key) or an in-memory mock (see {@code LocalGCSDirectoryFactory} for
 * tests).
 *
 * <p>Configuration parameters (via {@code solrconfig.xml} or system properties):
 *
 * <ul>
 *   <li>{@code bucket} / {@code solr.gcsDirectory.bucket} — GCS bucket name
 *   <li>{@code blockCacheKilobytes} / {@code solr.gcsDirectory.blockCacheKilobytes} — decompressed
 *       block cache size in KiB (default: 1 GiB)
 *   <li>{@code useAsyncIO} — whether to use double-buffered async GCS writes (default: true)
 * </ul>
 */
public class GCSDirectoryFactory extends StandardDirectoryFactory {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static {
    // TODO: replace with StorageOptions.Builder#setApplicationName once the API is available.
    // The google-api-java-client layer warns when no application name is set; suppress that
    // specific message while leaving other warnings from this class visible.
    java.util.logging.Logger.getLogger(
            "com.google.api.client.googleapis.services.AbstractGoogleClient")
        .setFilter(r -> !r.getMessage().startsWith("Application name is not set"));
  }

  /** Write buffer size for double-buffered GCS uploads. 256 KB matches GCS chunk alignment. */
  static final int GCS_WRITE_BUFFER_SIZE = 256 * 1024;

  /** Default block cache size: 1 GiB expressed in KiB. */
  private static final long DEFAULT_BLOCK_CACHE_KILOBYTES = 1L << 20;

  /**
   * Default cap on concurrently open GCS {@link com.google.cloud.ReadChannel}s across all files.
   */
  private static final int DEFAULT_MAX_OPEN_CHANNELS = 4096;

  private NodeLevelGCSDirectoryState nodeLevelState;

  /** Non-null only when this instance owns (and must close) the node-level state. */
  private NodeLevelGCSDirectoryState ownNodeLevelState;

  private WeakReference<CoreContainer> cc;
  private String bucket;
  private boolean useAsyncIO;

  @Override
  public void initCoreContainer(CoreContainer cc) {
    super.initCoreContainer(cc);
    this.cc = new WeakReference<>(cc);
  }

  /** Node-level resources shared across all {@link GCSDirectory} instances on this node. */
  public static final class NodeLevelGCSDirectoryState implements Closeable {
    private final java.util.concurrent.ExecutorService ioExec =
        ExecutorUtil.newMDCAwareCachedThreadPool("gcsIOExec");
    private final BlockCache blockCache;
    private final Storage storage;
    private final Cache<ReadChannel, Cache.Node<ReadChannel>> channelPool;

    /**
     * Buffer pool for the double-buffered GCS write path. Buffers are 256 KB, 4-KiB aligned (no
     * DirectIO requirement, but page-aligned is harmless and avoids any platform surprises).
     */
    private final DirectBufferPool bufferPool;

    /**
     * Metadata bucket for {@link GcsBlobLifecycleCoordinator}; derived as {@code bucket + "-meta"}.
     */
    private final String metadataBucket;

    /**
     * Non-null when ZooKeeper is available and {@code solr.gcsDirectory.useZkCoordinator=true};
     * null otherwise.
     */
    private final SolrZkClient zkClient;

    NodeLevelGCSDirectoryState(
        BlockCache blockCache, Storage storage, String metadataBucket, ZkController zkController) {
      this.blockCache = blockCache;
      this.storage = storage;
      this.channelPool = new Cache<>(new ReadChannel[DEFAULT_MAX_OPEN_CHANNELS], true);
      this.bufferPool = new DirectBufferPool(GCS_WRITE_BUFFER_SIZE, 4096, 1);
      this.metadataBucket = metadataBucket;
      this.zkClient = zkController != null ? zkController.getZkClient() : null;
    }

    /**
     * Returns a {@link GCSDirectory.BlobLifecycleCoordinator} scoped to the given local index
     * directory. The {@code refId} is a deterministic UUID derived from the final path element
     * (directory name only), so it is stable across parent-directory migrations and unique per
     * replica even when co-located.
     *
     * <p>Uses {@link ZkBlobLifecycleCoordinator} when {@code
     * solr.gcsDirectory.useZkCoordinator=true} and ZooKeeper is available; otherwise uses {@link
     * GcsBlobLifecycleCoordinator}.
     */
    GCSDirectory.BlobLifecycleCoordinator createBlobCoordinator(Path localPath) throws IOException {
      UUID refId =
          UUID.nameUUIDFromBytes(
              localPath.getFileName().toString().getBytes(StandardCharsets.UTF_8));
      if (Boolean.getBoolean("solr.gcsDirectory.useZkCoordinator") && zkClient != null) {
        return new ZkBlobLifecycleCoordinator(zkClient, refId);
      }
      return new GcsBlobLifecycleCoordinator(storage, metadataBucket, refId);
    }

    @Override
    @SuppressWarnings("try")
    public void close() throws IOException {
      try (AutoCloseable c1 = storage;
          Closeable c2 = blockCache) {
        ExecutorUtil.shutdownAndAwaitTermination(ioExec);
      } catch (IOException e) {
        throw e;
      } catch (Exception e) {
        log.error("error closing {}", NodeLevelGCSDirectoryState.class, e);
      }
    }
  }

  @Override
  public void init(NamedList<?> args) {
    // Reinstall on every init: SLF4JBridgeHandler.install() calls LogManager.reset() which wipes
    // logger-level configuration (including the filter set in the static block above).
    java.util.logging.Logger.getLogger(
            "com.google.api.client.googleapis.services.AbstractGoogleClient")
        .setFilter(r -> !r.getMessage().startsWith("Application name is not set"));
    SolrParams params = args.toSolrParams();
    bucket = params.get("bucket", System.getProperty("solr.gcsDirectory.bucket", ""));
    if (bucket.isEmpty()) {
      throw new IllegalArgumentException(
          "GCSDirectoryFactory requires a bucket name via the 'bucket' param "
              + "or the 'solr.gcsDirectory.bucket' system property.");
    }
    useAsyncIO = params.getBool("useAsyncIO", true);

    final long blockCacheBytes =
        params.getLong(
                "blockCacheKilobytes",
                Long.getLong(
                    "solr.gcsDirectory.blockCacheKilobytes", DEFAULT_BLOCK_CACHE_KILOBYTES))
            * 1024L;

    final Storage storage = initStorage();

    final Path blockCacheBackingFile =
        Path.of(System.getProperty("java.io.tmpdir"))
            .resolve("solr-gcs-block-cache-" + UUID.randomUUID() + ".tmp");

    if (this.cc != null) {
      CoreContainer cc = this.cc.get();
      this.cc = null;
      assert cc != null;
      final String metadataBucket = bucket + "-meta";
      final ZkController zkController = cc.getZkController();
      nodeLevelState =
          cc.getObjectCache()
              .computeIfAbsent(
                  "nodeLevelGCSDirectoryState",
                  NodeLevelGCSDirectoryState.class,
                  k -> {
                    try {
                      return new NodeLevelGCSDirectoryState(
                          new BlockCache(blockCacheBytes, blockCacheBackingFile),
                          storage,
                          metadataBucket,
                          zkController);
                    } catch (IOException e) {
                      throw new UncheckedIOException(e);
                    }
                  });
    } else {
      try {
        nodeLevelState =
            new NodeLevelGCSDirectoryState(
                new BlockCache(blockCacheBytes, blockCacheBackingFile),
                storage,
                bucket + "-meta",
                null);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      ownNodeLevelState = nodeLevelState;
    }

    super.init(args);
  }

  /**
   * Creates the {@link Storage} instance used for all GCS operations.
   *
   * <p>This implementation throws {@link UnsupportedOperationException} — production deployments
   * must subclass and override this method to supply a properly credentialed {@link Storage}
   * instance. This factory intentionally does <em>not</em> call {@code
   * GoogleCredentials.getApplicationDefault()} or any other ambient-credential mechanism.
   *
   * <p>For in-memory testing, use {@code LocalGCSDirectoryFactory} (in the test sources), which
   * overrides this method with a {@code LocalStorageHelper}-backed singleton.
   */
  protected Storage initStorage() {
    throw new UnsupportedOperationException(
        "GCSDirectoryFactory does not use application-default GCS credentials. "
            + "Subclass and override initStorage() to provide a configured Storage instance, "
            + "or use LocalGCSDirectoryFactory for in-memory testing.");
  }

  @Override
  protected Directory create(String path, LockFactory lockFactory, DirContext dirContext)
      throws IOException {
    return newGCSDirectory(
        Path.of(path),
        bucket,
        nodeLevelState.storage,
        nodeLevelState.blockCache,
        nodeLevelState.channelPool,
        nodeLevelState.ioExec,
        useAsyncIO,
        nodeLevelState.bufferPool);
  }

  protected GCSDirectory newGCSDirectory(
      Path localPath,
      String bucket,
      Storage storage,
      BlockCache cache,
      Cache<ReadChannel, Cache.Node<ReadChannel>> channelPool,
      ExecutorService ioExec,
      boolean useAsyncIO,
      DirectBufferPool bufferPool)
      throws IOException {
    return new GCSDirectory(
        localPath,
        bucket,
        storage,
        cache,
        channelPool,
        ioExec,
        useAsyncIO,
        bufferPool,
        nodeLevelState.createBlobCoordinator(localPath));
  }

  @Override
  public boolean isPersistent() {
    return true;
  }

  @Override
  @SuppressWarnings("try")
  public void close() throws IOException {
    try (NodeLevelGCSDirectoryState close = ownNodeLevelState) {
      super.close();
    }
  }
}
