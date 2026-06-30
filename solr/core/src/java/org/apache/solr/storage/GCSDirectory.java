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

import static org.apache.solr.storage.CompressingDirectory.BLOCK_SIZE_ESTIMATE;
import static org.apache.solr.storage.CompressingDirectory.COMPRESSION_BLOCK_MASK_LOW;
import static org.apache.solr.storage.CompressingDirectory.COMPRESSION_BLOCK_SHIFT;
import static org.apache.solr.storage.CompressingDirectory.COMPRESSION_BLOCK_SIZE;
import static org.apache.solr.storage.CompressingDirectory.COMPRESSION_BLOCK_TYPE;
import static org.apache.solr.storage.CompressingDirectory.COMPRESSION_TYPE;

import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.StorageException;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.internal.hppc.IntCursor;
import org.apache.lucene.internal.hppc.LongArrayList;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBufferGuard;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MappedByteBufferIndexInputProvider;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.ThreadInterruptedException;
import org.apache.lucene.util.compress.LZ4;
import org.apache.solr.common.util.EnvUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Lucene {@link org.apache.lucene.store.Directory} that stores compressed index data in Google
 * Cloud Storage and keeps only lightweight offset-manifest files on the local filesystem.
 *
 * <p>Write path: compressed LZ4 blocks are streamed sequentially to a GCS object via a resumable
 * upload ({@link AsyncGCSWriteHelper}). On close, a small local <em>offset file</em> is written
 * containing the file length, the GCS object name (UUID), the total compressed byte count, and the
 * ZInt delta-encoded compressed block sizes — everything needed to reconstruct byte ranges for
 * cache-miss GCS reads.
 *
 * <p>Read path: the offset file is parsed once on open to build an in-memory {@code blockOffsets[]}
 * array. Reads are served from the shared {@link BlockCache} when possible; on a cache miss a
 * byte-range GET is issued to GCS, the block is decompressed, and the result is placed in the
 * cache.
 *
 * <p>Rename: only the local offset file is renamed; the GCS object (identified by UUID) requires no
 * GCS operation.
 *
 * <p>Delete: the local offset file and the GCS object are both removed.
 *
 * <p>Local file format: for GCS-backed files the local file contains three sections, in order:
 *
 * <pre>
 *   [tailLen bytes]     uncompressed tail block (last partial block; absent when length is an exact
 *                       multiple of COMPRESSION_BLOCK_SIZE); tailLen = length % COMPRESSION_BLOCK_SIZE
 *   [variable bytes]    ZInt delta-encoded compressed block sizes for the GCS portion (blocks 0..N-2)
 *   [52 bytes trailer]  fixed metadata, fields little-endian:
 *     [1 byte]   compressionBlockType id
 *     [1 byte]   compressionType id
 *     [2 bytes]  reserved
 *     [16 bytes] GCS blob UUID (MSB then LSB)
 *     [16 bytes] segment UUID (MSB then LSB)
 *     [8 bytes]  logical (uncompressed) file length
 *     [8 bytes]  total compressed bytes in the GCS object (blocks 0..N-2 only)
 * </pre>
 *
 * <p>Local-only files (flush segments or any file whose uncompressed length is less than one block)
 * contain the raw uncompressed data followed by {@link #LOCAL_ONLY_SENTINEL} (8 bytes) as the very
 * last bytes. Detection reads the last 8 bytes first; if they equal the sentinel the file is
 * local-only, otherwise the full 52-byte trailer is parsed.
 */
public class GCSDirectory extends SizeAwareDirectory {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static Directory rawDirectoryView(Directory dir) {
    Directory unwrapped = FilterDirectory.unwrap(dir);
    if (unwrapped instanceof GCSDirectory) {
      return ((GCSDirectory) unwrapped).rawDirectoryView();
    } else {
      return dir;
    }
  }

  private Directory rawDirectoryView() {
    return new Directory() {
      @Override
      public long fileLength(String name) throws IOException {
        return GCSDirectory.super.onDiskFileLength(name);
      }

      @Override
      public IndexInput openInput(String name, IOContext context) throws IOException {
        return GCSDirectory.super.openInput(name, context);
      }

      @Override
      public void close() throws IOException {
        // no-op
      }

      @Override
      public String[] listAll() throws IOException {
        return GCSDirectory.this.listAll();
      }

      @Override
      public void deleteFile(String name) throws IOException {
        GCSDirectory.this.deleteFile(name);
      }

      @Override
      public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return GCSDirectory.this.createOutputDirect(name, context);
      }

      @Override
      public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
          throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public void sync(Collection<String> names) throws IOException {
        GCSDirectory.this.sync(names);
      }

      @Override
      public void syncMetaData() throws IOException {
        GCSDirectory.this.syncMetaData();
      }

      @Override
      public void rename(String source, String dest) throws IOException {
        GCSDirectory.this.rename(source, dest);
      }

      @Override
      public Lock obtainLock(String name) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public Set<String> getPendingDeletions() throws IOException {
        throw new UnsupportedOperationException();
      }
    };
  }

  /**
   * Seam for ZooKeeper-backed distributed blob lifecycle management.
   *
   * <p>When {@code null} (single-node or no-ZK mode), blobs are deleted immediately on the local
   * node. When non-null, the coordinator is responsible for distributed refcounting: blobs are only
   * physically deleted once all replicas have released their reference to the segment batch.
   */
  public interface BlobLifecycleCoordinator {
    /**
     * Registers a new segment batch. Called after {@code sync()} persists the file set for a
     * segment. The coordinator records the mapping of {@code segUUID → blobUUIDs} in ZK and
     * increments the refcount for this node.
     */
    void registerBatch(UUID segUUID, Collection<UUID> blobUUIDs) throws IOException;

    /**
     * Releases this node's reference to the given segment batch. Returns the full set of blob UUIDs
     * for the batch (from ZK), which the caller should delete if the refcount reached zero. Returns
     * an empty collection if other nodes still hold references (nothing to delete yet).
     */
    Collection<BlobId> release(UUID segUUID, String bucket) throws IOException;
  }

  // Offset file header size: 8 (length) + 4 (blockType/comprType/reserved) + 16 (blobUUID)
  // + 16 (segUUID) + 8 (gcsObjectSize) = 52 bytes.
  static final int OFFSET_FILE_HEADER_SIZE = 52;

  /**
   * GCS resumable-upload chunk size. 8 MiB amortizes HTTP round-trip overhead effectively;
   * throughput gains plateau well below the client default of 15 MiB.
   */
  private static final int GCS_WRITE_CHUNK_SIZE = 8 * 1024 * 1024;

  /**
   * Sentinel written as the last 8 bytes of a local-only file. Derived from a UUID so it is
   * astronomically unlikely to collide with a valid {@code gcsObjectSize} field that closes a
   * GCS-backed trailer. Files ending with this value bypass GCS entirely and are served directly
   * from the local MMap file (all bytes except the final 8).
   */
  static final long LOCAL_ONLY_SENTINEL =
      UUID.nameUUIDFromBytes(
              "org.apache.solr.storage.GCSDirectory#LOCAL_ONLY_SENTINEL"
                  .getBytes(java.nio.charset.StandardCharsets.UTF_8))
          .getMostSignificantBits();

  private static final ByteBufferGuard.BufferCleaner UNMAP = unmapHack();

  /** Obtains Lucene's unmap hack for explicit {@link java.nio.MappedByteBuffer} cleanup. */
  private static ByteBufferGuard.BufferCleaner unmapHack() {
    Object hack = MappedByteBufferIndexInputProvider.unmapHackImpl();
    if (hack instanceof ByteBufferGuard.BufferCleaner) {
      return (ByteBufferGuard.BufferCleaner) hack;
    }
    throw new UnsupportedOperationException("MappedByteBuffer unmap not available on this JVM");
  }

  private final String bucket;
  protected final Storage storage;
  private final BlockCache cache;
  private final Semaphore channelSemaphore;
  private final ExecutorService ioExec;
  private final boolean useAsyncIO;
  private final DirectBufferPool bufferPool;

  /**
   * Cache-node arrays shared across all root {@link GCSIndexInput} instances opened for the same
   * GCS blob. Keyed by blob UUID so that rename (which only moves the local offset file) requires
   * no map update. Populated by the writer at write-close time; entries are removed on delete.
   */
  private final ConcurrentHashMap<UUID, BlocksStruct> pendingNodes = new ConcurrentHashMap<>();

  private final ConcurrentHashMap<String, SegmentStruct> pendingWrites = new ConcurrentHashMap<>();

  private static final long RECHECK_NANOS = TimeUnit.MINUTES.toNanos(2);

  private static final class BatchValue {
    private final String segName;
    private final Map<UUID, String> blobUUIDs = new ConcurrentHashMap<>();
    private final AtomicLong lastCheck = new AtomicLong(System.nanoTime() - RECHECK_NANOS);

    private BatchValue(String segName) {
      this.segName = segName;
    }

    public boolean valid() {
      long now = System.nanoTime();
      long extant = lastCheck.get();
      return now - extant <= RECHECK_NANOS
          || !lastCheck.compareAndSet(
              extant, now + ThreadLocalRandom.current().nextLong(RECHECK_NANOS));
    }
  }

  private final ConcurrentHashMap<UUID, BatchValue> batched = new ConcurrentHashMap<>();

  /** {@code null} in single-node/no-ZK mode; non-null enables distributed refcounting via ZK. */
  private final BlobLifecycleCoordinator blobCoordinator;

  /**
   * Node-level queue of pending {@link BlobLifecycleCoordinator#registerBatch} and delete calls.
   * Non-null only when {@code blobCoordinator != null}. {@code sync()} enqueues tasks here instead
   * of calling {@code registerBatch} directly, so the {@code SolrIndexWriter} monitor is not held
   * across a blocking GCS write. Drained by a single background thread in {@link
   * GCSDirectoryFactory.NodeLevelGCSDirectoryState}, which outlives individual directory instances.
   */
  private final BlockingQueue<Runnable> registerQueue;

  private static final class SegmentStruct {
    private final UUID segUUID;
    private final String segName;
    private final AtomicReference<ConcurrentHashMap<String, UUID>> pendingFiles =
        new AtomicReference<>();

    private SegmentStruct(UUID segUUID, String segName) {
      this.segUUID = segUUID;
      this.segName = segName;
    }

    private void registerFileUUID(String name, UUID uuid) throws IOException {
      // TODO: check no race condition here?
      ConcurrentHashMap<String, UUID> pendingFiles = this.pendingFiles.get();
      if (pendingFiles == null) {
        pendingFiles = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, UUID> extant =
            this.pendingFiles.compareAndExchange(null, pendingFiles);
        if (extant != null) {
          pendingFiles = extant;
        }
      }
      if (pendingFiles.put(name, uuid) != null) {
        throw new IOException("double-added UUID " + uuid);
      }
    }

    private IndexOutput createOutput(String name, GCSDirectory dir, IOContext context)
        throws IOException {
      return dir.createOutput(name, this, context);
    }
  }

  @SuppressWarnings("try")
  public GCSDirectory(
      Path localPath,
      String bucket,
      Storage storage,
      BlockCache cache,
      Semaphore channelSemaphore,
      ExecutorService ioExec,
      boolean useAsyncIO,
      DirectBufferPool bufferPool,
      BlobLifecycleCoordinator blobCoordinator,
      BlockingQueue<Runnable> registerQueue)
      throws IOException {
    super(localPath, FSLockFactory.getDefault(), 0);
    this.bucket = bucket;
    this.storage = storage;
    this.cache = cache;
    this.channelSemaphore = channelSemaphore;
    this.ioExec = ioExec;
    this.useAsyncIO = useAsyncIO;
    this.bufferPool = bufferPool;
    this.blobCoordinator = blobCoordinator;
    if (blobCoordinator != null) {
      this.registerQueue = registerQueue;
      Map<String, Integer> segOrds = new HashMap<>();
      String[] files = listAll();
      List<Closeable> toClose = new ArrayList<>(files.length);
      for (String file : files) {
        if (!isGcsBacked(file)) {
          if (file.startsWith("segments_")) {
            parseSegOrds(file, segOrds);
          }
          continue;
        }
        Path path = directory.resolve(file);
        byte[] header = readOffsetFileHeader(path);
        if (header == null || isLocalOnlyHeader(header)) {
          continue; // missing, malformed, or local-only flush segment
        }
        UUID segUUID = readSegmentUUID(header);
        UUID blobUUID = readBlobUUID(header);
        batched
            .computeIfAbsent(segUUID, (k) -> new BatchValue(IndexFileNames.parseSegmentName(file)))
            .blobUUIDs
            .put(blobUUID, file);
        pendingWrites.compute(
            IndexFileNames.parseSegmentName(file),
            (segName, struct) -> {
              if (struct == null) {
                return new SegmentStruct(segUUID, segName);
              } else {
                if (!struct.segUUID.equals(segUUID)) {
                  // this should never happen, but we don't want to fail hard, because all we want
                  // here is a loose guarantee and best-effort batching. In building `segments`
                  // here, we're only need to guarantee a (potentially arbitrary) canonical seg UUID
                  // for this Directory instance.
                  log.warn("unexpected segUUID: {} != {}", struct.segUUID, segUUID);
                }
                return struct;
              }
            });
        toClose.add(new GCSIndexInput("gcs:" + file, this, path, header));
      }
      if (!batched.isEmpty()) {
        registerBatches(ioExec, blobCoordinator);
        // `control` limits local resource consumption; this request may submit at most 2 concurrent
        // seg requests, and overall process at most MAX_READ_AHEAD concurrent `ensureLoaded()`
        // requests.
        List<UUID> sorted =
            batched.entrySet().stream()
                .map(
                    (e) -> {
                      Integer ord = segOrds.get(e.getValue().segName);
                      return ord == null
                          ? null
                          : new SegAndOrd(e.getKey(), ord, e.getValue().segName);
                    })
                .filter(Objects::nonNull)
                .sorted(Comparator.comparingInt(a -> a.ord))
                .map((s) -> s.segId)
                .collect(Collectors.toList());
        if (!sorted.isEmpty()) {
          Iterator<UUID> iter = sorted.iterator();
          Runnable onComplete =
              () -> {
                try (Closeable c = () -> IOUtils.close(toClose)) {
                  // best effort, allow "a while" for the last submitted `ensureLoaded()`
                  // tasks to complete. The worst thing that happens if the tasks take longer
                  // than the allotted sleep time to complete is that the tasks will
                  // populate "zombie" `accessMapped`, doing useless work. It's preferable
                  // to simply accept this remote possibility than worry about formally
                  // synchronizing input close with the completion of tasks.
                  Thread.sleep(5000);
                } catch (IOException ex) {
                  throw new UncheckedIOException(ex);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new ThreadInterruptedException(e);
                }
              };
          maybeReadAheadSeg(iter.next(), iter, new ArrayList<>(sorted.size()), onComplete, 2000);
        }
      }
    } else {
      this.registerQueue = null;
    }
  }

  private static final class SegAndOrd {
    private final UUID segId;
    private final int ord;
    private final String segName;

    private SegAndOrd(UUID segId, int ord, String segName) {
      this.segId = segId;
      this.ord = ord;
      this.segName = segName;
    }
  }

  private void parseSegOrds(String file, Map<String, Integer> segOrds) throws IOException {
    SegmentInfos infos = SegmentInfos.readCommit(this, file);
    for (int i = 0; i < infos.size(); i++) {
      segOrds.put(infos.info(i).info.name, i);
    }
  }

  private static final int REGISTER_CONCURRENCY = 8;

  private void registerBatches(ExecutorService ioExec, BlobLifecycleCoordinator blobCoordinator)
      throws IOException {
    final Semaphore control = new Semaphore(REGISTER_CONCURRENCY); // limit our local concurrency
    List<Future<?>> results = new ArrayList<>(batched.size());
    for (Map.Entry<UUID, BatchValue> entry : batched.entrySet()) {
      UUID segUUID = entry.getKey();
      Set<UUID> blobUUIDs = entry.getValue().blobUUIDs.keySet();
      boolean mustRelease = true;
      try {
        if (!control.tryAcquire(5, TimeUnit.SECONDS)) {
          mustRelease = false; // no acquisition
          blobCoordinator.registerBatch(segUUID, blobUUIDs); // execute inline
        } else {
          results.add(
              ioExec.submit(
                  () -> {
                    try {
                      blobCoordinator.registerBatch(segUUID, blobUUIDs);
                      return null;
                    } finally {
                      control.release();
                    }
                  }));
          mustRelease = false; // will be released by the submitted task
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new ThreadInterruptedException(ex);
      } finally {
        if (mustRelease) {
          control.release();
        }
      }
    }
    try {
      // block max 60s until all tasks finish
      long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
      for (Future<?> r : results) {
        long waitNanos = until - System.nanoTime();
        if (waitNanos < 0) {
          throw new TimeoutException();
        }
        r.get(waitNanos, TimeUnit.NANOSECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ThreadInterruptedException(e);
    } catch (ExecutionException e) {
      throw unwrapException(e.getCause());
    } catch (TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  public void discover(String file) throws IOException {
    if (!isGcsBacked(file)) {
      return;
    }
    byte[] header = readOffsetFileHeader(directory.resolve(file));
    if (header == null || isLocalOnlyHeader(header)) {
      return; // missing, malformed, or local-only flush segment
    }
    UUID segUUID = readSegmentUUID(header);
    UUID blobUUID = readBlobUUID(header);
    String segName = IndexFileNames.parseSegmentName(file);
    SegmentStruct segStruct =
        pendingWrites.computeIfAbsent(segName, (k) -> new SegmentStruct(segUUID, k));
    if (!segUUID.equals(segStruct.segUUID)) {
      segStruct =
          pendingWrites.computeIfAbsent(
              segUUID.toString(), (k) -> new SegmentStruct(segUUID, segName));
    }
    segStruct.registerFileUUID(file, blobUUID);
  }

  // ---------------------------------------------------------------------------
  // Directory API
  // ---------------------------------------------------------------------------

  private void removeCachedMappings(UUID blobUUID) {
    BlocksStruct stale = pendingNodes.remove(blobUUID);
    if (stale != null) {
      removeCachedMappings(stale);
    }
  }

  private void removeCachedMappings(BlocksStruct stale) {
    if (stale.refCount.get() != 0) {
      log.warn("unexpected non-zero refcount in removeCachedMappings");
    }
    AtomicReferenceArray<Cache.Node<BlockCache.Val>> staleNodes = stale.accessMapped;
    for (int i = 0; i < staleNodes.length(); i++) {
      Cache.Node<BlockCache.Val> node = staleNodes.getAndSet(i, null);
      if (node != null) cache.close(node);
    }
  }

  @Override
  protected void deleteFile0(String name) throws IOException {
    ensureOpen();
    byte[] header;
    if (isGcsBacked(name)
        && (header = readOffsetFileHeader(directory.resolve(name))) != null
        && !isLocalOnlyHeader(header)) {
      maybeGcsDelete(
          name,
          readBlobUUID(header),
          IndexFileNames.parseSegmentName(name),
          readSegmentUUID(header));
    }
    super.deleteFile0(name);
  }

  private void maybeGcsDelete(String name, UUID blobUUID, String segName, UUID segUUID) {
    // Build a Runnable for the GCS deletion that needs to be deferred if readers are open.
    // In-memory bookkeeping (batched, pendingWrites) runs immediately regardless.
    final Runnable gcsDelete;
    if (blobCoordinator == null) {
      // No coordinator: delete this blob directly, but defer if readers are open.
      gcsDelete =
          () -> {
            removeCachedMappings(blobUUID);
            deleteBlob(blobUUID);
          };
    } else {
      // Remove from pendingFiles if present (file was written but not yet sync'd).
      // Safe to delete immediately — never-sync'd blobs should not have open readers.
      SegmentStruct struct;
      if (name != null
          && (struct = pendingWrites.get(segName)) != null
          && struct.segUUID.equals(segUUID)) {
        ConcurrentHashMap<String, UUID> pending = struct.pendingFiles.get();
        UUID extantBlobUUID = null;
        if (pending != null && blobUUID.equals(extantBlobUUID = pending.remove(name))) {
          // not yet sync'd, delete the blob immediately
          deleteBlob(blobUUID);
          // If this segment was never batched and all pending files are now gone,
          // clean up the SegmentStruct so it doesn't linger in pendingWrites.
          // NOTE: a new segUUID will be assigned if we see this seg prefix again,
          // but that's fine because we're essentially "starting from scratch".
          if (pending.isEmpty() && !batched.containsKey(segUUID)) {
            // TODO (immediately): there's a race condition here.
            pendingWrites.remove(segName, struct);
          }
        } else if (extantBlobUUID != null) {
          log.warn("found unexpected blobUUID: {} != {}", extantBlobUUID, blobUUID);
        }
      }
      gcsDelete = () -> deleteFully(blobUUID, segName, segUUID);
    }
    runOrDeferFullDelete(gcsDelete, blobUUID, segUUID);
  }

  private void runOrDeferFullDelete(Runnable gcsDelete, UUID blobUUID, UUID segUUID) {
    // If there are open readers, defer gcsDelete to the last close(); otherwise run it now.
    // close() keeps a refCount=0 copy entry when pendingDeletion is null (warm re-open cache),
    // so v != null does NOT imply open readers. Return null for stale copy entries to remove
    // them and fall through to the immediate-run path.
    BlocksStruct[] staleToRecycle = new BlocksStruct[1];
    boolean runNow =
        null
            == pendingNodes.computeIfPresent(
                blobUUID,
                (k, v) -> {
                  if (v.refCount.get() == 0) {
                    staleToRecycle[0] = v; // stale copy entry; remove it and run now
                    return null;
                  }
                  // atomically hand off gcsDelete responsibility to `close()` method
                  // NOTE: we cannot clear mappings here (nor `v.copy()`), because there
                  // are still open inputs!
                  v.pendingDeletion = gcsDelete;
                  return v;
                });
    if (staleToRecycle[0] != null) {
      removeCachedMappings(staleToRecycle[0]);
    }
    if (runNow) {
      if (registerQueue == null) {
        gcsDelete.run();
      } else {
        try {
          registerQueue.put(gcsDelete);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          log.error("interrupted while enqueuing release for {}; blobs may be orphaned", segUUID);
        }
      }
    }
  }

  private void deleteFully(UUID blobUUID, String segName, UUID segUUID) {
    removeCachedMappings(blobUUID);
    boolean[] removedFromBatched = new boolean[1];
    batched.compute(
        segUUID,
        (k, v) -> {
          if (v == null) {
            // Not in batched: file was written but never sync'd. Direct delete above.
            return null;
          } else if (v.blobUUIDs.remove(blobUUID) == null) {
            // blob not in batch: file was never sync'd for a partially-sync'd segment.
            return v;
          } else if (v.blobUUIDs.isEmpty()) {
            removedFromBatched[0] = true;
            return null;
          } else {
            return v;
          }
        });
    if (removedFromBatched[0]) {
      // Last local ref to this segment UUID batch is gone.
      pendingWrites.compute(
          segName,
          (k, v) -> {
            if (v == null || !v.segUUID.equals(segUUID)) {
              return v;
            }
            ConcurrentHashMap<String, UUID> extant = v.pendingFiles.get();
            if (extant == null || extant.isEmpty()) {
              return null;
            } else {
              log.warn("unexpected entries left in pendingFiles {}", extant);
              return v;
            }
          });
      Collection<BlobId> toDelete;
      try {
        toDelete = blobCoordinator.release(segUUID, bucket);
      } catch (IOException e) {
        log.error("async release failed for segUUID {}; blobs may be orphaned", segUUID, e);
        return;
      }
      for (BlobId blob : toDelete) {
        try {
          // false results (blob not found) are fine — already gone
          storage.delete(blob);
        } catch (StorageException e) {
          log.warn("Failed to batch-delete GCS blobs {} — it may be orphaned", blob, e);
        }
      }
    }
  }

  @Override
  protected long fileLength0(String name) throws IOException {
    ensureOpen();
    if (!isGcsBacked(name)) {
      return super.fileLength0(name);
    }
    Path offsetFile = directory.resolve(name);
    byte[] header = readOffsetFileHeader(offsetFile);
    if (header == null) {
      throw new NoSuchFileException(name);
    }
    if (isLocalOnlyHeader(header)) {
      return onDiskFileLength(name) - 8;
    }
    // length is at trailer offset 36: blockType+comprType+reserved(4) + blobUUID(16) + segUUID(16)
    ByteArrayDataInput in = new ByteArrayDataInput(header);
    in.skipBytes(36);
    return in.readLong();
  }

  @Override
  protected IndexOutput createOutput0(String name, IOContext context) throws IOException {
    if (!isGcsBacked(name)) {
      return super.createOutput0(name, context);
    }
    // Flush segments are short-lived and frequently replaced, so skip GCS upload for them.
    // A local-only sentinel is prepended so openInput/fileLength/deleteFile can detect them.
    // On merge, Lucene reads these files via openInput() (which strips the sentinel) and writes
    // the merged result via createOutput() with a MERGE context, taking the normal GCS path.
    if (context.context == IOContext.Context.FLUSH) {
      return new LocalOnlyIndexOutput(name, super.createOutput0(name, context));
    }
    return pendingWrites
        .computeIfAbsent(
            IndexFileNames.parseSegmentName(name),
            (segName) -> new SegmentStruct(UUID.randomUUID(), segName))
        .createOutput(name, this, context);
  }

  private IndexOutput createOutputDirect(String name, IOContext context) throws IOException {
    if (!isGcsBacked(name)) {
      return super.createOutput0(name, context);
    }
    return new GCSIndexOutputDirect(pendingWrites, super.createOutput0(name, context));
  }

  private IndexOutput createOutput(String name, SegmentStruct struct, IOContext context)
      throws IOException {
    return new GCSIndexOutput(this, struct, super.createOutput0(name, context));
  }

  @Override
  protected void rename0(String source, String dest) throws IOException {
    if (isGcsBacked(source) != isGcsBacked(dest)) {
      throw new IOException(
          "Rename across GCS/non-GCS boundary not supported: " + source + " -> " + dest);
    }
    super.rename0(source, dest);
  }

  @Override
  public void copyFrom(Directory from, String src, String dest, IOContext context)
      throws IOException {
    Directory rawFrom = rawDirectoryView(from);
    if (rawFrom == from) {
      // Source is not a GCSDirectory (should not happen in practice); fall back to a regular copy
      // which reads content and uploads it to GCS as a new blob.
      super.copyFrom(from, src, dest, context);
      return;
    }
    // Both directories are GCS-backed: copy the raw offset file so the destination reuses
    // the same GCS blob without re-uploading.
    rawDirectoryView(this).copyFrom(rawFrom, src, dest, context);
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    try {
      super.sync(names);
    } finally {
      if (blobCoordinator != null) {
        for (SegmentStruct s : pendingWrites.values()) {
          ConcurrentHashMap<String, UUID> addToManifest = s.pendingFiles.getAndSet(null);
          if (addToManifest == null) continue;
          // NOTE: we cannot do `addToManifest.keySet().retainAll(names)`, because at the point
          // we have acquired `addToManifest`, we hold the only record of these blobs' existence
          // (`s.pendingFiles.getAndSet(null)`). Therefore it's our responsibility to register
          // all of them with `batched` and `blobCoordinator`.
          if (!addToManifest.isEmpty()) {
            Map<UUID, String> blobUUIDs =
                batched.computeIfAbsent(s.segUUID, (k) -> new BatchValue(s.segName)).blobUUIDs;
            for (Map.Entry<String, UUID> e : addToManifest.entrySet()) {
              blobUUIDs.put(e.getValue(), e.getKey());
            }
            UUID segUUID = s.segUUID;
            List<UUID> blobs = List.copyOf(addToManifest.values());
            try {
              registerQueue.put(
                  () -> {
                    try {
                      blobCoordinator.registerBatch(segUUID, blobs);
                    } catch (IOException e) {
                      log.error(
                          "async registerBatch failed for segUUID {}; {} blobs may be orphaned",
                          segUUID,
                          blobs.size(),
                          e);
                    }
                  });
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IOException("interrupted while enqueuing registerBatch for " + segUUID, e);
            }
          }
        }
      }
    }
  }

  @Override
  @SuppressWarnings("try")
  public void close() throws IOException {
    super.close();
  }

  /**
   * Creates the {@link WriteChannel} used to stream compressed data to GCS. Overridable for
   * testing.
   */
  protected WriteChannel openWriteChannel(BlobInfo blobInfo) {
    return storage.writer(blobInfo);
  }

  /**
   * Deletes a GCS blob, logging a warning on failure rather than propagating. A failed delete
   * leaves the blob orphaned (a space leak) but does not affect index correctness. This can happen
   * transiently under load (e.g. emulator connection resets) and may warrant investigation if it
   * persists in production.
   */
  private void deleteBlob(UUID blobUUID) {
    try {
      storage.delete(BlobId.of(bucket, blobUUID.toString()));
    } catch (StorageException e) {
      log.warn("Failed to delete GCS blob {} — it may be orphaned", blobUUID, e);
    }
  }

  /**
   * For files that are <i>not</i> GCS-backed, we can delegate {@link #openInput(String, IOContext)}
   * ~directly to {@link MMapDirectory#openInput(String, IOContext)}, or we can mmap ourselves and
   * create an "always-mapped" local-only {@link GCSIndexInput}. Since we already have all the
   * tooling to trivially support the latter, and it avoids low-level polymorphism, this is probably
   * the right thing to do, but it's sysprop-configurable for evaluation.
   */
  private static final boolean DIRECT_MMAP_DIR_INPUT =
      EnvUtils.getPropertyAsBool("solr.gcs.directMmapDirInput", false);

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    ensureOpen();
    if (!isGcsBacked(name)) {
      if (DIRECT_MMAP_DIR_INPUT) {
        return super.openInput(name, context);
      } else {
        Path path = directory.resolve(name);
        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
          return GCSIndexInput.ofMapped(
              name, this, ch.map(FileChannel.MapMode.READ_ONLY, 0, ch.size()));
        }
      }
    }
    Path offsetFile = directory.resolve(name);
    byte[] header = readOffsetFileHeader(offsetFile);
    if (header == null) {
      throw new NoSuchFileException(name);
    }
    if (isLocalOnlyHeader(header)) {
      long contentLen = onDiskFileLength(name) - 8;
      if (DIRECT_MMAP_DIR_INPUT) {
        IndexInput raw = super.openInput(name, context);
        return raw.slice("local-only:" + name, 0, contentLen);
      } else {
        try (FileChannel ch = FileChannel.open(offsetFile, StandardOpenOption.READ)) {
          return GCSIndexInput.ofMapped(
              "local-only:" + name, this, ch.map(FileChannel.MapMode.READ_ONLY, 0, contentLen));
        }
      }
    }
    return new GCSIndexInput("gcs:" + name, this, offsetFile, header);
  }

  /**
   * In the spirit of {@link FSDirectory} {@code ensureCanRead(String)}; we do our own accounting
   * (for our own purposes) that should be equivalent to {@link FSDirectory} {@code pendingDeletes}.
   */
  protected final void ensureCanRead(Path path, UUID blobUUID, UUID segUUID)
      throws NoSuchFileException {
    BlocksStruct v = pendingNodes.get(blobUUID);
    if (v == null) {
      BatchValue batch = batched.get(segUUID);
      if (batch != null && batch.blobUUIDs.containsKey(blobUUID)) {
        return;
      }
    } else if (v.pendingDeletion == null) {
      return;
    }
    throw new NoSuchFileException(
        "file \"" + path + "\" is pending delete and cannot be opened for read");
  }

  // ---------------------------------------------------------------------------
  // Offset file helpers
  // ---------------------------------------------------------------------------

  private static final String[] NON_GCS_EXTENSIONS =
      new String[] {
        "tmp", // transient, local-only
        "si", // tiny
        "cfe" // tiny
      };

  /**
   * Returns true for files that are backed by a GCS blob with a local offset file. Temp files
   * (*.tmp) and non-segment files (segments_N, pending_segments_N, etc.) are local-only.
   *
   * <p>TODO: this relies on the Lucene convention that all segment files start with {@code _}.
   * Within an index directory this always holds, but GCSDirectoryFactory is a general-purpose
   * factory and may be called for non-index directories (snapshot_metadata, tlog, etc.). Those
   * directories happen not to contain {@code _}-prefixed files today, so routing is correct by
   * accident. A cleaner fix would be to detect non-index directories at factory time and return a
   * plain MMapDirectory instead of a GCSDirectory.
   */
  static boolean isGcsBacked(String name) {
    int extIdx;
    if (name.charAt(0) != '_') {
      return false;
    } else if ((extIdx = name.lastIndexOf('.')) != -1) {
      int from = extIdx + 1;
      for (String s : NON_GCS_EXTENSIONS) {
        if (name.startsWith(s, from) && from + s.length() == name.length()) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Reads the local-file trailer from {@code path} in a single file open. Returns:
   *
   * <ul>
   *   <li>{@code null} — file missing, unreadable, or shorter than 8 bytes
   *   <li>array of exactly 8 bytes whose {@code long} value equals {@link #LOCAL_ONLY_SENTINEL} —
   *       local-only file
   *   <li>array of exactly {@link #OFFSET_FILE_HEADER_SIZE} bytes — GCS-backed file trailer
   * </ul>
   *
   * Use {@link #isLocalOnlyHeader} to distinguish the two non-null cases.
   */
  private static byte[] readOffsetFileHeader(Path path) throws IOException {
    if (!Files.exists(path)) {
      return null;
    }
    try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
      long fileSize = ch.size();
      if (fileSize < 8) return null;
      // Read last 8 bytes to check for the local-only sentinel.
      ByteBuffer sentinelBuf = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
      ch.read(sentinelBuf, fileSize - 8);
      if (sentinelBuf.getLong(0) == LOCAL_ONLY_SENTINEL) {
        return sentinelBuf.array(); // 8 bytes — local-only
      }
      if (fileSize < OFFSET_FILE_HEADER_SIZE) return null;
      // GCS-backed: read the full 52-byte trailer from the end.
      ByteBuffer buf = ByteBuffer.allocate(OFFSET_FILE_HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
      ch.read(buf, fileSize - OFFSET_FILE_HEADER_SIZE);
      return buf.array();
    }
  }

  /** Returns true if {@code header} was read from a local-only file. */
  private static boolean isLocalOnlyHeader(byte[] header) {
    return header.length < OFFSET_FILE_HEADER_SIZE;
  }

  /** Extracts the GCS blob UUID from an already-read GCS-backed trailer. */
  private static UUID readBlobUUID(byte[] header) {
    ByteArrayDataInput in = new ByteArrayDataInput(header);
    in.skipBytes(4); // blockType(1) + comprType(1) + reserved(2)
    return new UUID(in.readLong(), in.readLong());
  }

  /** Extracts the segment UUID from an already-read GCS-backed trailer. */
  private static UUID readSegmentUUID(byte[] header) {
    ByteArrayDataInput in = new ByteArrayDataInput(header);
    in.skipBytes(20); // blockType+comprType+reserved(4) + blobUUID(16)
    return new UUID(in.readLong(), in.readLong());
  }

  // ---------------------------------------------------------------------------
  // LocalOnlyIndexOutput
  // ---------------------------------------------------------------------------

  /**
   * Wraps a local {@link IndexOutput} (from {@link MMapDirectory}) to produce a local-only file
   * that bypasses GCS upload. Appends {@link #LOCAL_ONLY_SENTINEL} as the last 8 bytes on {@link
   * #close()} so that {@link #openInput}, {@link #fileLength}, and {@link #deleteFile} can detect
   * these files by reading the file's final 8 bytes.
   */
  private static final class LocalOnlyIndexOutput extends IndexOutput {

    private final IndexOutput delegate;
    private final CRC32 crc = new CRC32();

    LocalOnlyIndexOutput(String name, IndexOutput delegate) throws IOException {
      super("LocalOnly(name=\"" + name + "\")", name);
      this.delegate = delegate;
    }

    @Override
    public void writeByte(byte b) throws IOException {
      crc.update(b);
      delegate.writeByte(b);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
      crc.update(b, offset, length);
      delegate.writeBytes(b, offset, length);
    }

    @Override
    public long getFilePointer() {
      return delegate.getFilePointer();
    }

    @Override
    public long getChecksum() {
      return crc.getValue();
    }

    @Override
    public void close() throws IOException {
      delegate.writeLong(LOCAL_ONLY_SENTINEL);
      delegate.close();
    }
  }

  // ---------------------------------------------------------------------------
  // GCSIndexOutput
  // ---------------------------------------------------------------------------

  static final class GCSIndexOutputDirect extends IndexOutput
      implements CompressingDirectory.SizeReportingIndexOutput {

    private final ConcurrentHashMap<String, SegmentStruct> registerUUIDs;
    private final IndexOutput localOut;

    // Sliding window of the last OFFSET_FILE_HEADER_SIZE bytes written.
    // trailerBuf[0..trailerBytesBuffered-1] holds them in order (oldest first).
    private final byte[] trailerBuf = new byte[OFFSET_FILE_HEADER_SIZE];
    private int trailerBytesBuffered = 0;
    // Populated in close() after parseTrailer().
    private SegmentStruct segStruct;
    private UUID uuid;

    private long bytesWritten = 0;

    GCSIndexOutputDirect(
        ConcurrentHashMap<String, SegmentStruct> registerUUIDs, IndexOutput localOut) {
      super("GCSIndexOutputDirect(name=\"" + localOut.getName() + "\")", localOut.getName());
      this.registerUUIDs = registerUUIDs;
      this.localOut = localOut;
    }

    private SegmentStruct initSegStruct(UUID segUUID) {
      String segName = IndexFileNames.parseSegmentName(getName());
      SegmentStruct candidate =
          registerUUIDs.computeIfAbsent(segName, (k) -> new SegmentStruct(segUUID, k));
      if (segUUID.equals(candidate.segUUID)) {
        return candidate;
      } else {
        return registerUUIDs.computeIfAbsent(
            segUUID.toString(), (k) -> new SegmentStruct(segUUID, segName));
      }
    }

    /** Slides {@code b[offset..offset+length-1]} through the trailer window. */
    private void offerBytes(byte[] b, int offset, int length) {
      if (length == 0) return;
      if (length >= OFFSET_FILE_HEADER_SIZE) {
        System.arraycopy(
            b, offset + length - OFFSET_FILE_HEADER_SIZE, trailerBuf, 0, OFFSET_FILE_HEADER_SIZE);
        trailerBytesBuffered = OFFSET_FILE_HEADER_SIZE;
      } else {
        int newTotal = trailerBytesBuffered + length;
        if (newTotal <= OFFSET_FILE_HEADER_SIZE) {
          System.arraycopy(b, offset, trailerBuf, trailerBytesBuffered, length);
          trailerBytesBuffered = newTotal;
        } else {
          int shift = newTotal - OFFSET_FILE_HEADER_SIZE;
          System.arraycopy(trailerBuf, shift, trailerBuf, 0, OFFSET_FILE_HEADER_SIZE - length);
          System.arraycopy(b, offset, trailerBuf, OFFSET_FILE_HEADER_SIZE - length, length);
          trailerBytesBuffered = OFFSET_FILE_HEADER_SIZE;
        }
      }
    }

    /**
     * Parses segUUID and blobUUID from the trailing 52 bytes (the new trailer format). The last 8
     * bytes are checked for {@link #LOCAL_ONLY_SENTINEL}; if matched, no registration.
     */
    private void parseTrailer() {
      if (trailerBytesBuffered < 8) return; // too short for even a sentinel
      // Check last 8 bytes for LOCAL_ONLY_SENTINEL.
      ByteBuffer last8 =
          ByteBuffer.wrap(trailerBuf, trailerBytesBuffered - 8, 8).order(ByteOrder.LITTLE_ENDIAN);
      if (last8.getLong(0) == LOCAL_ONLY_SENTINEL) {
        return; // local-only file received via replication; no GCS blob to register
      }
      if (trailerBytesBuffered < OFFSET_FILE_HEADER_SIZE) return; // partial/unknown format
      // Parse 52-byte trailer:
      // blockType(1)+comprType(1)+reserved(2)+blobUUID(16)+segUUID(16)+length(8)+gcsObjectSize(8)
      ByteArrayDataInput hdr = new ByteArrayDataInput(trailerBuf);
      hdr.readByte(); // blockType
      hdr.readByte(); // comprType
      hdr.readShort(); // reserved
      uuid = new UUID(hdr.readLong(), hdr.readLong()); // blobUUID [4–19]
      UUID segUUID = new UUID(hdr.readLong(), hdr.readLong()); // segUUID [20–35]
      // length [36–43] and gcsObjectSize [44–51] not needed here
      segStruct = initSegStruct(segUUID);
    }

    @Override
    public void close() throws IOException {
      try {
        localOut.close();
      } finally {
        parseTrailer();
        // segStruct/uuid are null for local-only files and for files too short to contain a
        // trailer.
        if (segStruct != null) {
          segStruct.registerFileUUID(getName(), uuid);
        }
      }
    }

    @Override
    public long getFilePointer() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getChecksum() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void writeByte(byte b) throws IOException {
      bytesWritten++;
      if (trailerBytesBuffered < OFFSET_FILE_HEADER_SIZE) {
        trailerBuf[trailerBytesBuffered++] = b;
      } else {
        System.arraycopy(trailerBuf, 1, trailerBuf, 0, OFFSET_FILE_HEADER_SIZE - 1);
        trailerBuf[OFFSET_FILE_HEADER_SIZE - 1] = b;
      }
      localOut.writeByte(b);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
      bytesWritten += length;
      offerBytes(b, offset, length);
      localOut.writeBytes(b, offset, length);
    }

    @Override
    public long getBytesWritten() {
      return bytesWritten; // _compressed_ bytes ... so, like, the actual size on disk.
    }
  }

  static final class GCSIndexOutput extends IndexOutput
      implements CompressingDirectory.SizeReportingIndexOutput {

    private final GCSDirectory dir;
    private final byte[] compressBuffer = new byte[COMPRESSION_BLOCK_SIZE];
    private final LZ4.FastCompressionHashTable ht = new LZ4.FastCompressionHashTable();
    private final ByteBuffer preBuffer;
    // GCS write state: all null until the first full block is dumped (lazy start).
    private AsyncGCSWriteHelper writeHelper;
    private ByteBuffer buffer;
    private UUID uuid;
    private final BytesOut blockDeltas = new BytesOut();
    private int prevBlockSize = BLOCK_SIZE_ESTIMATE;

    private final SegmentStruct registerUUID;
    private final IndexOutput localOut;
    private long filePos;
    private long gcsObjectSize;
    private final CRC32 crc = new CRC32();
    private boolean isOpen;
    // Uncompressed bytes of the last (partial) block, captured in flush().
    private byte[] tailBytes;
    private int tailLen;
    // Cache nodes pre-populated during dump(), one per GCS block (null if pool was exhausted).
    // Published into pendingNodes on close() so readers get cache hits instead of GCS fetches.
    private final ArrayList<Cache.Node<BlockCache.Val>> cachedNodes = new ArrayList<>();
    private final LongArrayList blockOffsets = new LongArrayList();

    GCSIndexOutput(GCSDirectory dir, SegmentStruct registerUUID, IndexOutput localOut)
        throws IOException {
      super("GCSIndexOutput(name=\"" + localOut.getName() + "\")", localOut.getName());
      this.dir = dir;
      this.localOut = localOut;
      this.registerUUID = registerUUID;
      preBuffer = ByteBuffer.wrap(compressBuffer);
      isOpen = true;
    }

    /** Opens the GCS write channel and starts the write helper on the first full-block dump. */
    private void ensureGCSStarted() throws IOException {
      if (writeHelper != null) return;
      uuid = UUID.randomUUID();
      WriteChannel gcsChannel =
          dir.openWriteChannel(BlobInfo.newBuilder(BlobId.of(dir.bucket, uuid.toString())).build());
      gcsChannel.setChunkSize(GCS_WRITE_CHUNK_SIZE);
      writeHelper = new AsyncGCSWriteHelper(dir.bufferPool, gcsChannel);
      buffer = writeHelper.init();
      if (dir.useAsyncIO) {
        writeHelper.start(dir.ioExec);
      } else {
        writeHelper.startSync();
      }
    }

    @Override
    public void writeByte(byte b) throws IOException {
      crc.update(b);
      preBuffer.put(b);
      if (!preBuffer.hasRemaining()) dump();
    }

    @Override
    public void writeBytes(byte[] src, int offset, int len) throws IOException {
      crc.update(src, offset, len);
      int toWrite = len;
      while (true) {
        int left = preBuffer.remaining();
        if (left <= toWrite) {
          preBuffer.put(src, offset, left);
          toWrite -= left;
          offset += left;
          dump();
        } else {
          preBuffer.put(src, offset, toWrite);
          break;
        }
      }
    }

    private void dump() throws IOException {
      assert preBuffer.position() == COMPRESSION_BLOCK_SIZE;
      ensureGCSStarted();
      preBuffer.rewind();
      // Pre-warm the block cache with the uncompressed block while it's already in memory,
      // so readers get a cache hit instead of a GCS round-trip.
      Cache.Node<BlockCache.Val> cacheNode = dir.cache.acquireNode();
      if (cacheNode != null) {
        try {
          cacheNode.getPayload().populate(compressBuffer, 0, COMPRESSION_BLOCK_SIZE, dir.cache);
        } finally {
          // release writer's pin; node enters LRU, ready for readers
          dir.cache.unpin(cacheNode, false);
        }
      }
      cachedNodes.add(cacheNode); // null if pool exhausted; slot index == block number
      blockOffsets.add(gcsObjectSize);
      LZ4.compressWithDictionary(compressBuffer, 0, 0, COMPRESSION_BLOCK_SIZE, out, ht);
      int nextBlockSize = out.resetSize();
      gcsObjectSize += nextBlockSize;
      blockDeltas.writeZInt(nextBlockSize - prevBlockSize);
      prevBlockSize = nextBlockSize;
      filePos += COMPRESSION_BLOCK_SIZE;
      preBuffer.clear();
    }

    private void flush() throws IOException {
      preBuffer.flip();
      int rem = preBuffer.remaining();
      if (rem > 0) {
        // Capture the partial last block uncompressed — it stays local, not in GCS.
        filePos += rem;
        tailBytes = new byte[rem];
        preBuffer.get(tailBytes);
        tailLen = rem;
        preBuffer.clear(); // reset so getFilePointer() = filePos (no double-counting)
      }
      if (writeHelper != null) {
        // Flush the GCS portion (full blocks only; tail was not written to buffer).
        writeHelper.flush(buffer, true);
      }
    }

    @Override
    public long getBytesWritten() {
      return gcsObjectSize;
    }

    @Override
    public long getFilePointer() {
      return filePos + preBuffer.position();
    }

    @Override
    public long getChecksum() {
      return crc.getValue();
    }

    @Override
    public void close() throws IOException {
      if (isOpen) {
        isOpen = false;
        AsyncGCSWriteHelper wh = this.writeHelper;
        if (wh != null) {
          try (wh) {
            flush();
          }
        } else {
          flush();
        }
        try (localOut) {
          if (wh == null) {
            // No full blocks were written: entire file fits in less than one block.
            // Store as local-only: raw data + sentinel.
            if (tailLen > 0) {
              localOut.writeBytes(tailBytes, 0, tailLen);
            }
            localOut.writeLong(LOCAL_ONLY_SENTINEL);
          } else {
            // GCS-backed: [tail bytes][block deltas][52-byte trailer]
            if (tailLen > 0) {
              localOut.writeBytes(tailBytes, 0, tailLen);
            }
            localOut.writeBytes(blockDeltas.baos.buf(), 0, blockDeltas.baos.count());
            // 52-byte trailer:
            // blockType(1)+comprType(1)+reserved(2)+blobUUID(16)+segUUID(16)+length(8)+gcsObjectSize(8)
            UUID segUUID = registerUUID.segUUID;
            localOut.writeByte((byte) COMPRESSION_BLOCK_TYPE.id);
            localOut.writeByte((byte) COMPRESSION_TYPE.id);
            localOut.writeShort((short) 0); // reserved
            localOut.writeLong(uuid.getMostSignificantBits());
            localOut.writeLong(uuid.getLeastSignificantBits());
            localOut.writeLong(segUUID.getMostSignificantBits());
            localOut.writeLong(segUUID.getLeastSignificantBits());
            localOut.writeLong(filePos); // logical (uncompressed) file length
            localOut.writeLong(gcsObjectSize); // compressed bytes in GCS (full blocks only)
            // GCS write and local file are both committed; register for sync/batched accounting.
            registerUUID.registerFileUUID(getName(), uuid);
            // Publish pre-warmed cache nodes so readers get cache hits instead of GCS fetches.
            if (filePos > 0) {
              int blockCount = (int) (((filePos - 1) >> COMPRESSION_BLOCK_SHIFT) + 1);
              int gcsBlockCount = tailLen > 0 ? blockCount - 1 : blockCount;
              long[] blockOffsetsArr = new long[blockCount + 1];
              AtomicReferenceArray<Cache.Node<BlockCache.Val>> accessMapped =
                  new AtomicReferenceArray<>(blockCount);
              for (int i = 0; i < gcsBlockCount; i++) {
                accessMapped.set(i, cachedNodes.get(i));
                blockOffsetsArr[i] = blockOffsets.get(i);
              }
              blockOffsetsArr[gcsBlockCount] = gcsObjectSize; // end of last GCS block
              dir.pendingNodes.put(
                  uuid,
                  new BlocksStruct(
                      segUUID,
                      registerUUID.segName,
                      blockOffsetsArr,
                      accessMapped,
                      0,
                      tailLen > 0));
            }
          }
        }
      }
    }

    // SizeTrackingDataOutput: receives LZ4-compressed bytes and writes them into `buffer`.
    private final SizeTrackingDataOutput out = new SizeTrackingDataOutput();

    private void writeBlock() throws IOException {
      buffer.rewind();
      buffer = writeHelper.write(buffer);
    }

    private class SizeTrackingDataOutput extends DataOutput {
      private int size;

      int resetSize() {
        int ret = size;
        size = 0;
        return ret;
      }

      @Override
      public void writeByte(byte b) throws IOException {
        size++;
        ensureCapacity(1);
        buffer.put(b);
      }

      @Override
      public void writeBytes(byte[] b, int offset, int length) throws IOException {
        size += length;
        do {
          int can = ensureCapacity(length);
          buffer.put(b, offset, can);
          offset += can;
          length -= can;
        } while (length > 0);
      }

      private int ensureCapacity(int want) throws IOException {
        int remaining = buffer.remaining();
        if (remaining >= want) return want;
        if (remaining > 0) return remaining;
        writeBlock();
        return Math.min(want, buffer.remaining());
      }
    }
  }

  // ---------------------------------------------------------------------------
  // GCSIndexInput
  // ---------------------------------------------------------------------------

  private static final class BlocksStruct {
    private final UUID segUUID;
    private final String segName;
    private final AtomicInteger refCount;
    private final long[] blockOffsets;
    private final AtomicReferenceArray<Cache.Node<BlockCache.Val>> accessMapped;
    private final boolean hasTail;
    private final CompletableFuture<ByteBuffer> origMapping = new CompletableFuture<>();
    private final ByteBufferGuard guard = new ByteBufferGuard("gcsLocalFile", UNMAP);
    private Runnable pendingDeletion;

    private BlocksStruct(
        UUID segUUID,
        String segName,
        long[] blockOffsets,
        AtomicReferenceArray<Cache.Node<BlockCache.Val>> accessMapped,
        int initialRefCount,
        boolean hasTail) {
      this.segUUID = segUUID;
      this.segName = segName;
      this.blockOffsets = blockOffsets;
      this.accessMapped = accessMapped;
      this.refCount = new AtomicInteger(initialRefCount);
      this.hasTail = hasTail;
      // origMapping is completed by the winner after blockOffsets is fully populated, so that
      // non-winners use origMapping.join() as a publication barrier for blockOffsets.
    }

    private BlocksStruct copy() {
      int len = accessMapped.length();
      AtomicReferenceArray<Cache.Node<BlockCache.Val>> newAccessMapped =
          new AtomicReferenceArray<>(len);
      for (int i = hasTail ? len - 2 : len - 1; i >= 0; i--) {
        newAccessMapped.setPlain(i, accessMapped.getPlain(i)); // plain/best-effort
      }
      BlocksStruct copy =
          new BlocksStruct(segUUID, segName, blockOffsets, newAccessMapped, 0, hasTail);
      // blockOffsets is already populated in the copy; complete immediately so any re-win winner
      // (and non-winners racing a re-win) see the barrier as already cleared.
      if (!hasTail) {
        copy.origMapping.complete(null);
      }
      return copy;
    }
  }

  private void maybeReadAheadSeg(
      UUID segUUID,
      Iterator<UUID> nextSegs,
      List<Runnable> followup,
      Runnable onComplete,
      int timeoutMillis) {
    List<BlockPreloader.SegmentPreloadTask> tasks = new ArrayList<>();
    for (UUID uuid = segUUID; uuid != null; uuid = nextSegs.hasNext() ? nextSegs.next() : null) {
      BatchValue batchValue = batched.get(uuid);
      if (batchValue != null && !batchValue.valid()) {
        UUID segUUIDF = uuid;
        String segName = batchValue.segName;
        Map<UUID, String> blobs = batchValue.blobUUIDs;
        tasks.add(
            (fp) -> {
              String cfeName;
              if (blobs.size() == 1
                  && Files.exists(directory.resolve(cfeName = segName.concat(".cfe")))) {
                // preload the first block of each logical sub-file within the CFS compound
                UUID blob = blobs.keySet().iterator().next();
                BlocksStruct blocks = pendingNodes.get(blob);
                if (blocks != null) {
                  IntArrayList blockIndexes = parseCfeBlockIndexes(cfeName);
                  String blobS = blob.toString();
                  if (fp == null) {
                    for (IntCursor i : blockIndexes) {
                      ensureLoaded(
                          blobS, blocks.accessMapped, blocks.blockOffsets, i.value, timeoutMillis);
                    }
                  } else {
                    Iterator<IntCursor> iter = blockIndexes.iterator();
                    for (int i = 0; i < 4 && iter.hasNext(); i++) {
                      ensureLoaded(
                          blobS,
                          blocks.accessMapped,
                          blocks.blockOffsets,
                          iter.next().value,
                          timeoutMillis);
                    }
                    if (iter.hasNext()) {
                      fp.add(
                          () -> {
                            do {
                              ensureLoaded(
                                  blobS,
                                  blocks.accessMapped,
                                  blocks.blockOffsets,
                                  iter.next().value,
                                  timeoutMillis);
                            } while (iter.hasNext());
                          });
                    }
                  }
                }
              } else {
                if (fp == null) {
                  preloadNonCfs(segUUIDF, blobs, timeoutMillis);
                } else {
                  fp.add(() -> preloadNonCfs(segUUIDF, blobs, timeoutMillis));
                }
              }
            });
      }
    }
    if (!tasks.isEmpty()) {
      BlockPreloader.readAheadSegs(tasks.iterator(), ioExec, followup, onComplete);
    }
  }

  @SuppressWarnings("try")
  private void preloadNonCfs(UUID segUUID, Map<UUID, String> blobs, int timeoutMillis) {
    for (Map.Entry<UUID, String> blob : blobs.entrySet()) {
      UUID blobId = blob.getKey();
      BlocksStruct blocks = pendingNodes.get(blobId);
      // TODO: now that we do this in Directory ctor, `blobs` can probably revert to `Set<UUID>`.
      if (false && blocks == null) {
        // not initialized yet, so we have to force it the hacky way
        BatchValue batch = batched.get(segUUID);
        if (batch != null) {
          try (IndexInput ignore = openInput(batch.blobUUIDs.get(blobId), IOContext.READONCE)) {
            // just to parse offsets and create an entry in `pendingNodes`
          } catch (IOException ex) {
            throw new UncheckedIOException(ex);
          }
          blocks = pendingNodes.get(blobId);
        }
      }
      if (blocks != null) {
        ensureLoaded(blobId.toString(), blocks.accessMapped, blocks.blockOffsets, 0, timeoutMillis);
      }
    }
  }

  /**
   * Parses the local {@code .cfe} entry table and returns the block index (within the CFS GCS blob)
   * of the first block of each logical sub-file. Block indices are computed as {@code subFileOffset
   * >> COMPRESSION_BLOCK_SHIFT}.
   */
  private IntArrayList parseCfeBlockIndexes(String cfeName) {
    return BlockPreloader.parseCfeBlockIndexes(this, cfeName);
  }

  private boolean ensureLoaded(
      String blob,
      AtomicReferenceArray<Cache.Node<BlockCache.Val>> accessMapped,
      long[] blockOffsets,
      int idx,
      int timeoutMillis) {
    return BlockPreloader.ensureLoaded(
        accessMapped,
        blockOffsets,
        idx,
        COMPRESSION_BLOCK_SIZE,
        cache,
        ioExec,
        readAheadPermits,
        timeoutMillis,
        (blockOffset, compressedLen, decompressedLen) ->
            supply(blob, blockOffset, compressedLen, decompressedLen));
  }

  /**
   * Fetches the given compressed block from GCS and decompresses it. Called only on a cache miss.
   *
   * <p>Opens a fresh {@link ReadChannel} for each block, limited to exactly {@code compressedLen}
   * bytes via {@link ReadChannel#limit}, so the gRPC stream ends naturally after the block is
   * consumed. This prevents unbounded pre-parsing of surplus {@code ReadObjectResponse} messages
   * into the zero-copy lifecycle manager's unclosed-streams map.
   */
  private byte[] supply(String blobName, long pos, int compressedLen, int decompressedLen)
      throws IOException {
    byte[] compressed = new byte[compressedLen];
    ByteBuffer compBuf = ByteBuffer.wrap(compressed);
    try {
      channelSemaphore.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("interrupted waiting for GCS channel slot", e);
    }
    try (ReadChannel ch = storage.reader(BlobId.of(bucket, blobName))) {
      ch.setChunkSize(compressedLen); // otherwise client buffer is oversized
      ch.seek(pos);
      ch.limit(pos + compressedLen);
      while (compBuf.hasRemaining()) {
        int n = ch.read(compBuf);
        if (n == -1) {
          throw new EOFException(
              "unexpected EOF in GCS blob "
                  + blobName
                  + " at position "
                  + (pos + compBuf.position()));
        }
      }
    } finally {
      channelSemaphore.release();
    }
    byte[] decompressed = new byte[decompressedLen + 7];
    CompressingDirectory.decompress(compressed, 0, decompressedLen, decompressed, 0);
    return decompressed;
  }

  static final class GCSIndexInput extends CachedCompressedIndexInput {

    private final GCSDirectory dir;

    /** Null for always-mapped inputs and for slices. */
    private final UUID blobUUID;

    /** Null for always-mapped inputs and for slices. */
    private final UUID segUUID;

    /** Null for always-mapped inputs and for slices. */
    private final String blobName;

    /**
     * Non-null only in the root input (not slices). For always-mapped roots: a minimal struct
     * holding guard + origMapping only; for GCS-backed roots: the shared struct from pendingNodes.
     */
    private final BlocksStruct blocksStruct;

    // -------------------------------------------------------------------------
    // Root constructor (GCS-backed file)
    // -------------------------------------------------------------------------

    GCSIndexInput(String resourceDescription, GCSDirectory dir, Path offsetFile, byte[] trailer)
        throws IOException {
      this(resourceDescription, dir, parseRootParams(dir, offsetFile, trailer));
    }

    private static final class RootParams {
      final UUID blobUUID, segUUID;
      final long length;
      final BlocksStruct bs;

      RootParams(UUID blobUUID, UUID segUUID, long length, BlocksStruct bs) {
        this.blobUUID = blobUUID;
        this.segUUID = segUUID;
        this.length = length;
        this.bs = bs;
      }
    }

    /**
     * Parses the 52-byte offset-file trailer, initialises pendingNodes, and returns computed
     * params.
     */
    private static RootParams parseRootParams(GCSDirectory dir, Path offsetFile, byte[] trailer)
        throws IOException {
      // Parse the 52-byte trailer:
      // blockType(1)+comprType(1)+reserved(2)+blobUUID(16)+segUUID(16)+length(8)+gcsObjectSize(8)
      ByteArrayDataInput hdr = new ByteArrayDataInput(trailer);
      int cBlockTypeId = hdr.readByte() & 0xff;
      if (cBlockTypeId != COMPRESSION_BLOCK_TYPE.id) {
        throw new IOException("unrecognized compression block type id: " + cBlockTypeId);
      }
      hdr.readByte(); // compressionType (not yet used)
      hdr.readShort(); // reserved
      UUID blobUUID = new UUID(hdr.readLong(), hdr.readLong());
      UUID segUUID = new UUID(hdr.readLong(), hdr.readLong());
      dir.ensureCanRead(offsetFile, blobUUID, segUUID);
      long length = hdr.readLong();
      long gcsObjectSize = hdr.readLong();

      int tailLen = (int) (length & COMPRESSION_BLOCK_MASK_LOW);
      boolean hasTail = tailLen > 0;
      int blockCount = (int) (((length - 1) >> COMPRESSION_BLOCK_SHIFT) + 1);
      int lastBlockIdx = blockCount - 1;
      int gcsBlockCount = hasTail ? blockCount - 1 : blockCount;

      String segName = IndexFileNames.parseSegmentName(offsetFile.getFileName().toString());
      int[] winType = new int[1];
      BlocksStruct bs =
          dir.pendingNodes.compute(
              blobUUID,
              (k, v) -> {
                if (v == null) {
                  winType[0] = 1;
                  v =
                      new BlocksStruct(
                          segUUID,
                          segName,
                          new long[blockCount + 1],
                          new AtomicReferenceArray<>(blockCount),
                          1,
                          hasTail);
                } else if (v.refCount.getAndIncrement() == 0) {
                  winType[0] = 2;
                }
                return v;
              });
      long[] blockOffsets = bs.blockOffsets;

      switch (winType[0]) {
        case 1:
          {
            // First opener: decode block offsets from local file and publish via origMapping.
            long offsetFileSize;
            ByteBuffer localFileMapped;
            try (FileChannel ch = FileChannel.open(offsetFile, StandardOpenOption.READ)) {
              offsetFileSize = ch.size();
              localFileMapped = ch.map(FileChannel.MapMode.READ_ONLY, 0, offsetFileSize);
            }
            localFileMapped.order(ByteOrder.LITTLE_ENDIAN);
            // Delta bytes sit between the tail region and the 52-byte trailer.
            int deltaEnd = (int) (offsetFileSize - OFFSET_FILE_HEADER_SIZE);
            byte[] deltaBytes = new byte[Math.max(0, deltaEnd - tailLen)];
            if (deltaBytes.length > 0) {
              localFileMapped.position(tailLen);
              localFileMapped.get(deltaBytes);
            }
            // Decode GCS block offsets (gcsBlockCount - 1 deltas; blockOffsets[0] = 0 is implicit).
            ByteArrayDataInput in = new ByteArrayDataInput(deltaBytes);
            long blockOffset = 0;
            int lastBlockSize = BLOCK_SIZE_ESTIMATE;
            blockOffsets[0] = 0;
            for (int i = 1; i < gcsBlockCount; i++) {
              int delta = in.readZInt();
              int nextBlockSize = lastBlockSize + delta;
              blockOffset += nextBlockSize;
              blockOffsets[i] = blockOffset;
              lastBlockSize = nextBlockSize;
            }
            blockOffsets[gcsBlockCount] = gcsObjectSize;
            // Pre-pin the tail and complete origMapping (publication barrier for blockOffsets).
            if (hasTail) {
              ByteBuffer tailBuf = localFileMapped.slice(0, tailLen).order(ByteOrder.LITTLE_ENDIAN);
              Cache.Node<BlockCache.Val> tailNode = dir.cache.createTailNode(tailBuf);
              if (!bs.accessMapped.compareAndSet(lastBlockIdx, null, tailNode)) {
                throw new IllegalStateException("tailNode already set");
              }
              bs.origMapping.complete(localFileMapped);
            } else {
              UNMAP.freeBuffer("quick close localFileMapped", localFileMapped);
              bs.origMapping.complete(null);
            }
            break;
          }
        case 2:
          {
            // refCount==0 re-win: blockOffsets already populated. Re-init tail if needed.
            if (hasTail) {
              ByteBuffer localFileRemapped;
              try (FileChannel ch = FileChannel.open(offsetFile, StandardOpenOption.READ)) {
                localFileRemapped = ch.map(FileChannel.MapMode.READ_ONLY, 0, ch.size());
              }
              localFileRemapped.order(ByteOrder.LITTLE_ENDIAN);
              ByteBuffer tailBuf =
                  localFileRemapped.slice(0, tailLen).order(ByteOrder.LITTLE_ENDIAN);
              Cache.Node<BlockCache.Val> tailNode = dir.cache.createTailNode(tailBuf);
              if (!bs.accessMapped.compareAndSet(lastBlockIdx, null, tailNode)) {
                throw new IllegalStateException("tailNode already set");
              }
              bs.origMapping.complete(localFileRemapped);
            }
            // !hasTail: origMapping already complete(null) from copy(); nothing to do.
            break;
          }
        default:
          {
            // Non-winner: wait for the winner to publish blockOffsets via origMapping.
            bs.origMapping.join();
            break;
          }
      }

      return new RootParams(blobUUID, segUUID, length, bs);
    }

    private GCSIndexInput(String resourceDescription, GCSDirectory dir, RootParams p) {
      super(
          resourceDescription,
          dir.cache,
          p.length,
          p.bs.blockOffsets,
          p.bs.guard,
          p.bs.accessMapped);
      this.dir = dir;
      this.blobUUID = p.blobUUID;
      this.blobName = p.blobUUID.toString();
      this.segUUID = p.segUUID;
      this.blocksStruct = p.bs;
    }

    // -------------------------------------------------------------------------
    // Always-mapped constructor (local-only / non-GCS-backed files)
    // -------------------------------------------------------------------------

    /**
     * Creates an always-mapped {@link GCSIndexInput} for a local file whose entire content is
     * pre-pinned as tail nodes into {@code accessMapped}. Used instead of {@code super.openInput()}
     * so that all {@link IndexInput} call sites remain monomorphic on {@link GCSIndexInput}.
     */
    static GCSIndexInput ofMapped(
        String resourceDescription, GCSDirectory dir, ByteBuffer content) {
      return new GCSIndexInput(resourceDescription, dir, makeAlwaysMappedParams(dir, content));
    }

    private static final class AlwaysMappedParams {
      final long length;
      final BlocksStruct bs;
      final AtomicReferenceArray<Cache.Node<BlockCache.Val>> accessMapped;

      AlwaysMappedParams(
          long length,
          BlocksStruct bs,
          AtomicReferenceArray<Cache.Node<BlockCache.Val>> accessMapped) {
        this.length = length;
        this.bs = bs;
        this.accessMapped = accessMapped;
      }
    }

    private static AlwaysMappedParams makeAlwaysMappedParams(GCSDirectory dir, ByteBuffer content) {
      int len = content.remaining();
      int tailLen = len & COMPRESSION_BLOCK_MASK_LOW;
      boolean hasTail = tailLen > 0;
      int blockCount = len == 0 ? 1 : (((len - 1) >> COMPRESSION_BLOCK_SHIFT) + 1);
      AtomicReferenceArray<Cache.Node<BlockCache.Val>> accessMapped =
          new AtomicReferenceArray<>(blockCount);
      int base = content.position();
      for (int i = 0; i < blockCount; i++) {
        int blockStart = base + i * COMPRESSION_BLOCK_SIZE;
        int blockEnd = i == blockCount - 1 ? base + len : blockStart + COMPRESSION_BLOCK_SIZE;
        ByteBuffer blockBuf = content.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        blockBuf.limit(blockEnd).position(blockStart);
        accessMapped.set(i, dir.cache.createTailNode(blockBuf.slice()));
      }
      BlocksStruct bs = new BlocksStruct(null, null, null, null, 0, hasTail);
      bs.origMapping.complete(content);
      return new AlwaysMappedParams(len, bs, accessMapped);
    }

    private GCSIndexInput(String resourceDescription, GCSDirectory dir, AlwaysMappedParams p) {
      super(
          resourceDescription,
          dir.cache,
          p.length,
          null /*blockOffsets — never accessed for always-mapped*/,
          p.bs.guard,
          p.accessMapped);
      this.dir = dir;
      this.blobUUID = null;
      this.blobName = null;
      this.segUUID = null;
      this.blocksStruct = p.bs;
    }

    // -------------------------------------------------------------------------
    // Slice / clone constructor
    // -------------------------------------------------------------------------

    private GCSIndexInput(
        String resourceDescription, GCSIndexInput parent, long sliceOffset, long sliceLen) {
      super(resourceDescription, parent, sliceOffset, sliceLen);
      this.dir = parent.dir;
      this.blobUUID = parent.blobUUID;
      this.blobName = parent.blobName;
      this.segUUID = parent.segUUID;
      this.blocksStruct = null; // slice does not own the mapping
      maybePreloadSlice();
    }

    // -------------------------------------------------------------------------
    // CachedCompressedIndexInput abstract method implementations
    // -------------------------------------------------------------------------

    @Override
    protected byte[] supply(int blockIdx, long blockOffset, int compressedLen, int decompressedLen)
        throws IOException {
      return dir.supply(blobName, blockOffset, compressedLen, decompressedLen);
    }

    @Override
    protected GCSIndexInput cloneSlice(String description, long sliceOffset, long sliceLen) {
      return new GCSIndexInput(description, this, sliceOffset, sliceLen);
    }

    @Override
    protected ByteBuffer doClose() throws IOException {
      if (blocksStruct == null) return null; // slice — nothing to do

      if (blobUUID == null) {
        // always-mapped root: not registered in pendingNodes; just invalidate and unmap.
        return blocksStruct.origMapping.join();
      }

      @SuppressWarnings({"unchecked", "rawtypes"})
      CompletableFuture<ByteBuffer>[] toUnmap = new CompletableFuture[1];
      Runnable[] deletionToRun = new Runnable[1];
      BlocksStruct[] toRecycle = new BlocksStruct[1];
      dir.pendingNodes.computeIfPresent(
          blobUUID,
          (k, v) -> {
            int outstandingRefs = v.refCount.decrementAndGet();
            if (outstandingRefs == 0) {
              toUnmap[0] = v.hasTail ? v.origMapping : null;
              if (v.pendingDeletion == null) {
                // file has not yet been deleted, and could be re-opened.
                // `v.copy()` keeps non-local (cached) mappings only.
                return v.copy();
              } else {
                // file has been deleted: run deletion and remove entry.
                deletionToRun[0] = v.pendingDeletion;
                toRecycle[0] = v;
                return null;
              }
            } else if (outstandingRefs > 0) {
              return v;
            } else {
              throw new IllegalStateException();
            }
          });
      if (toRecycle[0] != null) {
        dir.removeCachedMappings(toRecycle[0]);
      }
      if (deletionToRun[0] != null) {
        if (dir.registerQueue == null) {
          deletionToRun[0].run();
        } else {
          try {
            // NOTE: this `put()` may block if the queue buffer is full, but that should
            // be ok because root IndexInput are not closed in a latency-sensitive path.
            dir.registerQueue.put(deletionToRun[0]);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("interrupted while enqueuing release for {}; blobs may be orphaned", segUUID);
          }
        }
      }
      return toUnmap[0] == null ? null : toUnmap[0].join();
    }

    // -------------------------------------------------------------------------
    // Hook overrides
    // -------------------------------------------------------------------------

    @Override
    protected boolean ensureBlockLoaded(int blockIdx) {
      return dir.ensureLoaded(blobName, accessMapped, blockOffsets, blockIdx, 0);
    }

    @Override
    protected void onFirstBlockMiss() {
      dir.maybeReadAheadSeg(segUUID, Collections.emptyIterator(), null, null, 0);
    }
  }

  private static final int READ_CHANNEL_HEADROOM =
      GCSDirectoryFactory.DEFAULT_MAX_OPEN_CHANNELS >> 1;
  private final AtomicInteger readaheadPermit = new AtomicInteger(0);
  private final BlockPreloader.Permits readAheadPermits =
      new BlockPreloader.Permits() {
        private final Semaphore sem =
            new Semaphore(CachedCompressedIndexInput.MAX_READ_AHEAD << 1, true);

        @Override
        public boolean tryAcquire(int timeoutMillis) {
          try {
            if (!sem.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS)) {
              return false;
            } else if (readaheadPermit.getAndIncrement() >= READ_CHANNEL_HEADROOM) {
              sem.release();
              readaheadPermit.decrementAndGet();
              return false;
            } else {
              return true;
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ThreadInterruptedException(e);
          }
        }

        @Override
        public void release() {
          sem.release();
          readaheadPermit.decrementAndGet();
        }
      };

  void onDirectoryRemove() throws IOException {
    Map<UUID, Closeable> toRemove = new HashMap<>();

    // FIRST: pendingWrites require filename, if available, so do these first
    for (SegmentStruct seg : pendingWrites.values()) {
      UUID segUUID = seg.segUUID;
      Map<String, UUID> active = seg.pendingFiles.get();
      if (active != null) {
        for (Map.Entry<String, UUID> e : active.entrySet()) {
          toRemove.computeIfAbsent(
              e.getValue(),
              (blobUUID) ->
                  () -> {
                    maybeGcsDelete(e.getKey(), blobUUID, seg.segName, segUUID);
                  });
        }
      }
    }

    // try to close the pending writes first
    IOUtils.closeWhileHandlingException(toRemove.values());

    HashSet<UUID> snapshot = new HashSet<>(toRemove.keySet());

    // SECOND: `batched` has blobUUIDs nested under segUUIDs, so do these second.
    for (Map.Entry<UUID, BatchValue> e : batched.entrySet()) {
      UUID segUUID = e.getKey();
      BatchValue batch = e.getValue();
      String segName = batch.segName;
      for (UUID blobUUID : batch.blobUUIDs.keySet()) {
        toRemove.computeIfAbsent(
            blobUUID,
            (k) ->
                () -> {
                  maybeGcsDelete(null, k, segName, segUUID);
                });
      }
    }

    toRemove.keySet().removeAll(snapshot); // remove the ones we've already done

    try {
      IOUtils.close(toRemove.values()); // sync'd so they really should be there.
    } finally {
      HashSet<Closeable> leftovers = new HashSet<>();
      // THIRD: there really shouldn't be anything left over here, but in case there is ...
      for (Map.Entry<UUID, BlocksStruct> e : pendingNodes.entrySet()) {
        UUID blobUUID = e.getKey();
        if (!toRemove.containsKey(blobUUID) && !snapshot.contains(blobUUID)) {
          BlocksStruct v = e.getValue();
          // inherently deduped by `pendingNodes`.
          leftovers.add(() -> maybeGcsDelete(null, blobUUID, v.segName, v.segUUID));
        }
      }

      IOUtils.closeWhileHandlingException(leftovers); // sync'd so they really should be there.
    }
  }

  private static IOException unwrapException(Throwable t) {
    if (t instanceof IOException) {
      return (IOException) t;
    } else if (t instanceof RuntimeException) {
      throw (RuntimeException) t;
    } else {
      // must be instanceof Error
      throw (Error) t;
    }
  }

  // ---------------------------------------------------------------------------
  // BytesOut helper (same as CompressingDirectory)
  // ---------------------------------------------------------------------------

  private static class BytesOut extends OutputStreamDataOutput {
    private final AccessibleBAOS baos;

    BytesOut() {
      this(new AccessibleBAOS());
    }

    private BytesOut(AccessibleBAOS baos) {
      super(baos);
      this.baos = baos;
    }
  }

  private static class AccessibleBAOS extends ByteArrayOutputStream {
    byte[] buf() {
      return buf;
    }

    int count() {
      return count;
    }
  }
}
