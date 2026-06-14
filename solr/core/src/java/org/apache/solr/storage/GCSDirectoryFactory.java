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

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.lang.ref.WeakReference;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.IntSupplier;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.LockFactory;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.EnvUtils;
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
 * #initStorage(SolrParams)} to supply a {@link Storage} instance — either a real configured client
 * (with an explicit service account key) or an in-memory mock (see {@code LocalGCSDirectoryFactory}
 * for tests).
 *
 * <p>Configuration parameters (via {@code solrconfig.xml} or system properties):
 *
 * <ul>
 *   <li>{@code solr.gcsDirectory.bucket} — GCS bucket name (system property)
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

  /** Block cache size in KiB; node-level, must be set via sysprop before JVM startup. */
  private static final long BLOCK_CACHE_KILOBYTES =
      Long.getLong("solr.gcsDirectory.blockCacheKilobytes", 1L << 20);

  /**
   * Path to an existing file to use as the block cache backing store. Mutually exclusive with
   * {@code solr.gcsDirectory.blockCacheKilobytes}; the file's current size determines cache
   * capacity.
   */
  private static final String BLOCK_CACHE_PATH =
      EnvUtils.getProperty("solr.gcsDirectory.blockCachePath", "");

  /**
   * Default cap on concurrently open GCS {@link com.google.cloud.ReadChannel}s across all files.
   */
  private static final int DEFAULT_MAX_OPEN_CHANNELS =
      EnvUtils.getPropertyAsInteger("solr.gcsDirectory.maxOpenChannels", 256);

  /** GCS bucket name; node-level, must be set via sysprop before JVM startup. */
  protected static final String BUCKET = EnvUtils.getProperty("solr.gcsDirectory.bucket", "");

  private NodeLevelGCSDirectoryState nodeLevelState;

  /** Non-null only when this instance owns (and must close) the node-level state. */
  private NodeLevelGCSDirectoryState ownNodeLevelState;

  private WeakReference<CoreContainer> cc;
  private boolean useAsyncIO;

  @Override
  public void initCoreContainer(CoreContainer cc) {
    super.initCoreContainer(cc);
    this.cc = new WeakReference<>(cc);
  }

  @Override
  public void move(Directory fromDir, Directory toDir, String fileName, IOContext ioContext)
      throws IOException {
    super.move(fromDir, toDir, fileName, ioContext);
    if (toDir instanceof GCSDirectory) {
      ((GCSDirectory) toDir).discover(fileName);
    }
  }

  private static final int MAX_CONCURRENT_PINNED = 4096;

  interface PinSemaphore {
    void register(GCSDirectory.NodeRefStruct instance);
  }

  static PinSemaphore defaultMaxPinned(BlockCache cache) {
    int nPartitions = pinSemaphoreNPartitions();
    int slotsPerPartition = MAX_CONCURRENT_PINNED / nPartitions;
    @SuppressWarnings({"unchecked", "rawtypes"})
    Cache<GCSDirectory.NodeRefStruct, Cache.Node<GCSDirectory.NodeRefStruct>>[] partitions =
        new Cache[nPartitions];
    final IntSupplier idx;
    if (nPartitions == 1) {
      idx = () -> 0;
    } else {
      idx = () -> ThreadLocalRandom.current().nextInt(nPartitions);
    }
    List<GCSDirectory.NodeRefStruct> dummy =
        Arrays.asList(new GCSDirectory.NodeRefStruct[slotsPerPartition]);
    for (int i = 0; i < nPartitions; i++) {
      partitions[i] = new Cache<>(dummy, false);
    }
    return instance -> {
      for (; ; ) {
        Cache<GCSDirectory.NodeRefStruct, Cache.Node<GCSDirectory.NodeRefStruct>> p =
            partitions[idx.getAsInt()];
        Cache.Node<GCSDirectory.NodeRefStruct> permit =
            p.acquireNode(
                (evicted) -> {
                  if (evicted != null) {
                    while (!evicted.outOfBandUnpin(cache)) {
                      // assumption is that individual reads will complete quickly.
                      Thread.yield();
                    }
                  }
                  return instance;
                });
        if (permit == null) {
          Thread.yield(); // all busy; no deadlock possible, so progress is guaranteed
        } else {
          assert permit.getValue() == instance;
          p.unpin(permit, false);
          return;
        }
      }
    };
  }

  private static int pinSemaphoreNPartitions() {
    // Largest power of two <= availableProcessors, capped so each partition retains at least
    // that many slots (ensuring the pool stays meaningfully sized per partition).
    int n = Integer.highestOneBit(Math.max(1, Runtime.getRuntime().availableProcessors()));
    return n <= MAX_CONCURRENT_PINNED / n ? n : 1;
  }

  /** Node-level resources shared across all {@link GCSDirectory} instances on this node. */
  public static final class NodeLevelGCSDirectoryState implements Closeable {
    /** Poison pill: when the drain task sees this it exits, allowing the executor to shut down. */
    private static final Runnable REGISTER_POISON = () -> {};

    /**
     * Node-level queue of pending {@link GCSDirectory.BlobLifecycleCoordinator#registerBatch} and
     * GCS delete tasks. Shared across all {@link GCSDirectory} instances on this node; enqueued by
     * {@code sync()} and {@code GCSIndexInput.close()}. Drained in FIFO order by a single task
     * running on {@link #ioExec}. Using node-level lifecycle means the drain task outlives
     * individual directory closes, so tasks enqueued after a directory is closed are still
     * processed. On shutdown, {@link #close()} enqueues {@link #REGISTER_POISON} to flush all
     * pending tasks before the executor terminates.
     */
    final BlockingQueue<Runnable> registerQueue = new ArrayBlockingQueue<>(4096);

    private final java.util.concurrent.ExecutorService ioExec =
        ExecutorUtil.newMDCAwareCachedThreadPool("gcsIOExec");
    private final BlockCache blockCache;
    private final Storage storage;
    private final Semaphore channelSemaphore = new Semaphore(DEFAULT_MAX_OPEN_CHANNELS);
    private final PinSemaphore acquirePinPermit;

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

    /**
     * The Solr core root directory (from {@link CoreContainer#getCoreRootDirectory()}); used to
     * bound the upward search for {@code core.properties} in {@link
     * #refIdFromCoreProperties(Path)}. Null when no {@link CoreContainer} is available (e.g. in
     * standalone tests), in which case the search falls back to a depth limit.
     */
    private final Path coreRootDirectory;

    /**
     * True when the GCS backend supports single-request multipart uploads ({@code
     * storage.create(BlobInfo, byte[], ...)}); false when only resumable uploads are supported
     * (e.g. some emulators). Detected once at construction time via a probe write.
     */
    private final boolean useMultipartUpload;

    NodeLevelGCSDirectoryState(
        BlockCache blockCache,
        Storage storage,
        String metadataBucket,
        ZkController zkController,
        Path coreRootDirectory)
        throws IOException {
      this.blockCache = blockCache;
      this.acquirePinPermit = defaultMaxPinned(blockCache);
      this.storage = storage;
      this.bufferPool = new DirectBufferPool(GCS_WRITE_BUFFER_SIZE, 4096, 1);
      this.metadataBucket = metadataBucket;
      this.zkClient = zkController != null ? zkController.getZkClient() : null;
      this.coreRootDirectory = coreRootDirectory;
      this.useMultipartUpload = probeMultipartUpload(storage, metadataBucket);
      ioExec.submit(
          () -> {
            while (true) {
              Runnable task;
              try {
                task = registerQueue.take();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
              }
              if (task == REGISTER_POISON) break;
              try {
                task.run();
              } catch (Throwable th) {
                log.warn("exception in gcs-register task", th);
              }
            }
          });
    }

    /**
     * Probes whether the GCS backend supports multipart uploads by writing and deleting a tiny
     * sentinel object. Returns {@code true} on success; {@code false} if a {@link StorageException}
     * is thrown (e.g. when running against an emulator that only supports resumable uploads).
     */
    private static boolean probeMultipartUpload(Storage storage, String metadataBucket) {
      String probeName = ".multipart-probe-" + UUID.randomUUID();
      BlobInfo probe = BlobInfo.newBuilder(BlobId.of(metadataBucket, probeName)).build();
      try {
        storage.create(probe, "probe".getBytes(StandardCharsets.UTF_8));
        storage.delete(BlobId.of(metadataBucket, probeName));
        log.info(
            "GCS metadata bucket supports multipart upload; using multipart for metadata writes");
        return true;
      } catch (StorageException e) {
        log.info(
            "GCS metadata bucket does not support multipart upload ({}); falling back to resumable upload",
            e.getMessage());
        return false;
      }
    }

    /**
     * Returns a {@link GCSDirectory.BlobLifecycleCoordinator} scoped to the given local index
     * directory.
     *
     * <p>The {@code refId} is a deterministic UUID that uniquely identifies this replica's instance
     * of {@code localPath}. It is derived preferentially from the {@code name} and {@code
     * coreNodeName} properties in the nearest {@code core.properties} file found by walking up from
     * {@code localPath}, combined with the path relative to that file's directory. This makes the
     * refId stable across moves of the Solr data root. Falls back to the full absolute path if no
     * {@code core.properties} is found (e.g. in tests or standalone deployments without a standard
     * directory layout).
     *
     * <p><b>Warning (fallback path only):</b> if the absolute-path fallback is in use and the Solr
     * data directory is relocated, refIds will change. Blobs whose old refIds are never released
     * will be orphaned and never garbage-collected, requiring a manual migration.
     *
     * <p>Uses {@link ZkBlobLifecycleCoordinator} when {@code
     * solr.gcsDirectory.useZkCoordinator=true} and ZooKeeper is available; otherwise uses {@link
     * GcsBlobLifecycleCoordinator}.
     */
    GCSDirectory.BlobLifecycleCoordinator createBlobCoordinator(Path localPath) throws IOException {
      UUID refId = refIdFromCoreProperties(localPath);
      if (refId == null) {
        refId =
            UUID.nameUUIDFromBytes(
                localPath.toAbsolutePath().normalize().toString().getBytes(StandardCharsets.UTF_8));
      }
      if (EnvUtils.getPropertyAsBool("solr.gcsDirectory.useZkCoordinator", false)
          && zkClient != null) {
        return new ZkBlobLifecycleCoordinator(zkClient, refId);
      }
      return new GcsBlobLifecycleCoordinator(storage, metadataBucket, refId, useMultipartUpload);
    }

    /**
     * Derives a stable, replica-unique refId from the nearest {@code core.properties} file found by
     * walking up from {@code localPath}. Returns {@code null} if no {@code core.properties} is
     * found or if it contains neither a {@code name} nor a {@code coreNodeName} property.
     */
    private UUID refIdFromCoreProperties(Path localPath) throws IOException {
      if (coreRootDirectory == null) return null;
      Path root = coreRootDirectory.toAbsolutePath().normalize();
      Path normalized = localPath.toAbsolutePath().normalize();
      if (!normalized.startsWith(root)) return null;
      // Walk up from localPath, stopping before coreRootDirectory itself (core.properties lives
      // in a direct subdirectory of it, not in coreRootDirectory itself).
      for (Path dir = normalized; !dir.equals(root); dir = dir.getParent()) {
        Path corePropsPath = dir.resolve("core.properties");
        if (Files.isRegularFile(corePropsPath)) {
          Properties props = new Properties();
          try (InputStream in = Files.newInputStream(corePropsPath)) {
            props.load(in);
          }
          String name = props.getProperty("name", "");
          String coreNodeName = props.getProperty("coreNodeName", "");
          if (name.isEmpty() && coreNodeName.isEmpty()) {
            return null;
          }
          String relPath = dir.relativize(normalized).toString();
          String key = name + "\n" + coreNodeName + "\n" + relPath;
          return UUID.nameUUIDFromBytes(key.getBytes(StandardCharsets.UTF_8));
        }
      }
      return null;
    }

    @Override
    @SuppressWarnings("try")
    public void close() throws IOException {
      // Signal the drain task to exit after processing all pending items, then shut down.
      try {
        registerQueue.put(REGISTER_POISON);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.warn(
            "interrupted while enqueuing REGISTER_POISON; some register/delete tasks may be lost");
      }
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
    if (BUCKET.isEmpty()) {
      throw new IllegalArgumentException(
          "GCSDirectoryFactory requires a bucket name via the 'solr.gcsDirectory.bucket' "
              + "system property.");
    }
    useAsyncIO = params.getBool("useAsyncIO", true);

    boolean hasPath = !BLOCK_CACHE_PATH.isEmpty();
    boolean hasSizeExplicit = System.getProperty("solr.gcsDirectory.blockCacheKilobytes") != null;
    if (hasPath && hasSizeExplicit) {
      throw new IllegalArgumentException(
          "solr.gcsDirectory.blockCachePath and solr.gcsDirectory.blockCacheKilobytes "
              + "are mutually exclusive.");
    }

    if (this.cc != null) {
      CoreContainer cc = this.cc.get();
      this.cc = null;
      assert cc != null;
      final String metadataBucket = BUCKET + "-meta";
      final ZkController zkController = cc.getZkController();
      final Path coreRootDirectory = cc.getCoreRootDirectory();
      nodeLevelState =
          cc.getObjectCache()
              .computeIfAbsent(
                  "nodeLevelGCSDirectoryState",
                  NodeLevelGCSDirectoryState.class,
                  k -> {
                    final Storage storage = initStorage(params); // TODO: race here.
                    try {
                      return new NodeLevelGCSDirectoryState(
                          buildBlockCache(),
                          storage,
                          metadataBucket,
                          zkController,
                          coreRootDirectory);
                    } catch (IOException e) {
                      throw new UncheckedIOException(e);
                    }
                  });
    } else {
      try {
        nodeLevelState =
            new NodeLevelGCSDirectoryState(
                buildBlockCache(), initStorage(params), BUCKET + "-meta", null, null);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      ownNodeLevelState = nodeLevelState;
    }

    super.init(args);
  }

  private static BlockCache buildBlockCache() throws IOException {
    if (!BLOCK_CACHE_PATH.isEmpty()) {
      return new BlockCache(Path.of(BLOCK_CACHE_PATH));
    } else {
      Path backingFile =
          Path.of(EnvUtils.getProperty("java.io.tmpdir"))
              .resolve("solr-gcs-block-cache-" + UUID.randomUUID() + ".tmp");
      return new BlockCache(BLOCK_CACHE_KILOBYTES * 1024L, backingFile);
    }
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
  protected Storage initStorage(SolrParams params) {
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
        BUCKET,
        nodeLevelState.storage,
        nodeLevelState.blockCache,
        nodeLevelState.ioExec,
        nodeLevelState.acquirePinPermit,
        useAsyncIO,
        nodeLevelState.bufferPool);
  }

  protected GCSDirectory newGCSDirectory(
      Path localPath,
      String bucket,
      Storage storage,
      BlockCache cache,
      ExecutorService ioExec,
      PinSemaphore acquirePinPermit,
      boolean useAsyncIO,
      DirectBufferPool bufferPool)
      throws IOException {
    return new GCSDirectory(
        localPath,
        bucket,
        storage,
        cache,
        nodeLevelState.channelSemaphore,
        ioExec,
        acquirePinPermit,
        useAsyncIO,
        bufferPool,
        nodeLevelState.createBlobCoordinator(localPath),
        nodeLevelState.registerQueue);
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

  @Override
  @SuppressWarnings("try")
  protected synchronized void removeDirectory(CacheValue cacheValue) throws IOException {
    try (Closeable c = () -> super.removeDirectory(cacheValue)) {
      Directory d = FilterDirectory.unwrap(cacheValue.directory);
      if (d instanceof GCSDirectory) {
        ((GCSDirectory) d).onDirectoryRemove();
      }
    } catch (NoSuchFileException ex) {
      // swallow this. Depending on the order of Directory removal, a parent directory
      // may have removed us first. In any event, the file's not there, which is what
      // we wanted anyway.
    }
  }
}
