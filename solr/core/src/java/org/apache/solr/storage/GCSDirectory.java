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
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.zip.CRC32;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexFileNames;
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
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.compress.LZ4;
import org.apache.solr.common.util.ExecutorUtil;
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
  private final GCSDirectoryFactory.PinSemaphore acquirePinPermit;

  public static Directory rawDirectoryView(Directory dir) {
    Directory unwrap = dir;
    while (unwrap instanceof FilterDirectory) {
      if (unwrap instanceof GCSDirectory) {
        return ((GCSDirectory) unwrap).rawDirectoryView();
      }
      unwrap = ((FilterDirectory) unwrap).getDelegate();
    }
    return dir;
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

  static final AtomicReferenceArray<BlockCache.Node> EMPTY_ACCESS_MAPPED =
      new AtomicReferenceArray<>(0);

  static final LongBuffer EMPTY_LONGBUFFER = LongBuffer.allocate(0);
  static final IntBuffer EMPTY_INTBUFFER = IntBuffer.allocate(0);
  static final FloatBuffer EMPTY_FLOATBUFFER = FloatBuffer.allocate(0);

  /**
   * Cache-node arrays shared across all root {@link GCSIndexInput} instances opened for the same
   * GCS blob. Keyed by blob UUID so that rename (which only moves the local offset file) requires
   * no map update. Populated by the writer at write-close time; entries are removed on delete.
   */
  private final ConcurrentHashMap<UUID, BlocksStruct> pendingNodes = new ConcurrentHashMap<>();

  private final ConcurrentHashMap<String, SegmentStruct> pendingWrites = new ConcurrentHashMap<>();

  private static final long RECHECK_NANOS = TimeUnit.MINUTES.toNanos(1);

  private static final class BatchValue {
    private final String segName;
    private final Set<UUID> blobUUIDs = ConcurrentHashMap.newKeySet();
    private volatile long lastCheck = 0;

    private BatchValue(String segName) {
      this.segName = segName;
    }

    public boolean valid() {
      long now = System.nanoTime();
      if (now - lastCheck > RECHECK_NANOS) {
        lastCheck = now;
        return false;
      } else {
        return true;
      }
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

  public GCSDirectory(
      Path localPath,
      String bucket,
      Storage storage,
      BlockCache cache,
      Semaphore channelSemaphore,
      ExecutorService ioExec,
      GCSDirectoryFactory.PinSemaphore acquirePinPermit,
      boolean useAsyncIO,
      DirectBufferPool bufferPool,
      BlobLifecycleCoordinator blobCoordinator,
      BlockingQueue<Runnable> registerQueue)
      throws IOException {
    super(localPath, FSLockFactory.getDefault(), 0);
    this.acquirePinPermit = acquirePinPermit;
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
      for (String file : listAll()) {
        if (!isGcsBacked(file)) {
          continue;
        }
        byte[] header = readOffsetFileHeader(directory.resolve(file));
        if (header == null || isLocalOnlyHeader(header)) {
          continue; // missing, malformed, or local-only flush segment
        }
        UUID segUUID = readSegmentUUID(header);
        UUID blobUUID = readBlobUUID(header);
        batched
            .computeIfAbsent(segUUID, (k) -> new BatchValue(IndexFileNames.parseSegmentName(file)))
            .blobUUIDs
            .add(blobUUID);
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
      }
      for (Map.Entry<UUID, BatchValue> entry : batched.entrySet()) {
        blobCoordinator.registerBatch(entry.getKey(), entry.getValue().blobUUIDs);
      }
    } else {
      this.registerQueue = null;
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
    AtomicReferenceArray<BlockCache.Node> staleNodes = stale.accessMapped;
    for (int i = 0; i < staleNodes.length(); i++) {
      BlockCache.Node node = staleNodes.getAndSet(i, null);
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
          } else if (!v.blobUUIDs.remove(blobUUID)) {
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
            batched
                .computeIfAbsent(s.segUUID, (k) -> new BatchValue(s.segName))
                .blobUUIDs
                .addAll(addToManifest.values());
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
    try (Closeable c = super::close) {
      ExecutorUtil.shutdownAndAwaitTermination(readahead);
    }
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

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    ensureOpen();
    if (!isGcsBacked(name)) {
      return super.openInput(name, context);
    }
    Path offsetFile = directory.resolve(name);
    byte[] header = readOffsetFileHeader(offsetFile);
    if (header == null) {
      throw new NoSuchFileException(name);
    }
    if (isLocalOnlyHeader(header)) {
      IndexInput raw = super.openInput(name, context);
      return raw.slice("local-only:" + name, 0, onDiskFileLength(name) - 8);
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
      if (batch != null && batch.blobUUIDs.contains(blobUUID)) {
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
    private final ArrayList<BlockCache.Node> cachedNodes = new ArrayList<>();
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
      BlockCache.Node cacheNode = dir.cache.acquireNode();
      if (cacheNode != null) {
        try {
          cacheNode.populate(compressBuffer, 0, COMPRESSION_BLOCK_SIZE);
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
              AtomicReferenceArray<BlockCache.Node> accessMapped =
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
    private final AtomicReferenceArray<BlockCache.Node> accessMapped;
    private final boolean hasTail;
    private final CompletableFuture<ByteBuffer> origMapping = new CompletableFuture<>();
    private final ByteBufferGuard guard = new ByteBufferGuard("gcsLocalFile", UNMAP);
    private Runnable pendingDeletion;

    private BlocksStruct(
        UUID segUUID,
        String segName,
        long[] blockOffsets,
        AtomicReferenceArray<BlockCache.Node> accessMapped,
        int initialRefCount,
        boolean hasTail) {
      this.segUUID = segUUID;
      this.segName = segName;
      this.blockOffsets = blockOffsets;
      this.accessMapped = accessMapped;
      this.refCount = new AtomicInteger(initialRefCount);
      this.hasTail = hasTail;
      if (!hasTail) {
        origMapping.complete(null);
      }
    }

    private BlocksStruct copy() {
      int len = accessMapped.length();
      AtomicReferenceArray<BlockCache.Node> newAccessMapped = new AtomicReferenceArray<>(len);
      for (int i = hasTail ? len - 2 : len - 1; i >= 0; i--) {
        newAccessMapped.setPlain(i, accessMapped.getPlain(i)); // plain/best-effort
      }
      return new BlocksStruct(segUUID, segName, blockOffsets, newAccessMapped, 0, hasTail);
    }
  }

  private enum State {
    UNPINNING,
    IDLE,
    PINNED
  }

  static final class NodeRefStruct {
    private final AtomicReference<State> state = new AtomicReference<>(State.IDLE);
    private BlockCache.Node currentNode;
    private int currentBlockIdx = -1;
    private boolean readPermitRegistered;
    private int sequentialAccessCount = 0;

    private void localPin() {
      while (!state.compareAndSet(State.IDLE, State.PINNED)) {
        Thread.yield();
      }
    }

    private void localUnpin(GCSDirectoryFactory.PinSemaphore semaphore) {
      if (!state.compareAndSet(State.PINNED, State.IDLE)) {
        throw new IllegalStateException();
      }
    }

    /**
     * Updates the current cached block. Always called from within a {@code localPin()} context (via
     * {@link GCSIndexInput#refill}), so state is already PINNED and direct update is safe.
     */
    private int setCurrentNode(
        BlockCache.Node node, int blockIdx, GCSDirectoryFactory.PinSemaphore semaphore) {
      assert state.get() == State.PINNED; // should only be called from a pinned context
      int extant = currentBlockIdx < 0 ? ~currentBlockIdx : currentBlockIdx;
      currentNode = node;
      currentBlockIdx = blockIdx;
      try {
        if (blockIdx == extant + 1) {
          // sequential access.
          sequentialAccessCount++;
          return node == null ? 0 : sequentialAccessCount;
        } else if (blockIdx < extant) {
          // gone backward, full reset
          sequentialAccessCount = 0;
          return -1;
        } else if (blockIdx == extant) {
          // same idx. This is an unusual case, but can happen if our block gets unpinned
          // and we have to re-acquire.
          return node == null ? 0 : sequentialAccessCount;
        } else {
          // skipped ahead, reset
          return sequentialAccessCount = 0;
        }
      } finally {
        if (node != null && !readPermitRegistered) {
          readPermitRegistered = true;
          semaphore.register(this);
        }
      }
    }

    /**
     * Unpins the current block. Always called from within a {@code localPin()} context (via {@link
     * GCSIndexInput#refill}), so state is already PINNED and direct update is safe.
     */
    private void unpinCurrentBlock(BlockCache blockCache) {
      BlockCache.Node toUnpin = currentNode;
      if (toUnpin != null) {
        currentNode = null;
        currentBlockIdx = ~currentBlockIdx;
        blockCache.unpin(toUnpin);
      }
    }

    /**
     * Unpins the current block from outside a {@code localPin()} context (i.e. from {@link
     * GCSIndexInput#close}). Handles the race with {@link #outOfBandUnpin}.
     */
    private void unpinFor(BlockCache blockCache) {
      switch (state.compareAndExchange(State.IDLE, State.PINNED)) {
        case IDLE:
          doUnpin(blockCache, State.PINNED);
          break;
        case UNPINNING:
          // another thread is unpinning; spin until it's done.
          do {
            Thread.yield();
          } while (state.get() == State.UNPINNING);
          break;
        default:
          throw new IllegalStateException();
      }
    }

    private void doUnpin(BlockCache blockCache, State from) {
      BlockCache.Node toUnpin = currentNode;
      if (toUnpin != null) {
        currentNode = null;
        currentBlockIdx = ~currentBlockIdx;
        blockCache.unpin(toUnpin);
      }
      if (!state.compareAndSet(from, State.IDLE)) {
        throw new IllegalStateException();
      }
    }

    /** This is the only method that may be called from a different thread! */
    boolean outOfBandUnpin(BlockCache blockCache) {
      if (state.compareAndSet(State.IDLE, State.UNPINNING)) {
        // scavenger has evicted our pinned-LRU slot; signal that re-acquire is needed
        readPermitRegistered = false;
        doUnpin(blockCache, State.UNPINNING);
        return true;
      }
      return false;
    }
  }

  static final class GCSIndexInput extends IndexInput implements RandomAccessInput {

    private final GCSDirectory dir;
    private final long length;
    // blockOffsets[i] = GCS byte offset of block i; [gcsBlockCount] = gcsObjectSize sentinel;
    // [blockCount] = 0 (dummy, never used — tail block always hits accessMapped cache).
    private final long[] blockOffsets;
    private final int blockCount; // total logical blocks (GCS blocks + optional tail)
    private final int lastBlockIdx;
    private final int lastBlockDecompressedLen;
    private final UUID blobUUID;
    private final UUID segUUID;
    private final String blobName;
    // Full mmap of the local file; non-null only in the root (not in slices).
    private final BlocksStruct blocksStruct;
    // Guard for explicit unmap of localFileMapped on close. Created in root, shared with all slices
    // so that after root.close() any in-flight slice read through the guard throws instead of
    // producing a SIGSEGV on unmapped native memory (specifically the mmap-backed tail block).
    private final ByteBufferGuard guard;

    private AtomicReferenceArray<BlockCache.Node> accessMapped;

    private final long offset;
    private final long sliceLength;

    private long seekPos = -1;
    private long filePointer = 0;
    private ByteBuffer postBuffer = ByteBuffer.allocate(0);
    private int postBufferBaseline;
    private final NodeRefStruct currentNodeRef = new NodeRefStruct();

    private LongBuffer[] longViews;
    private IntBuffer[] intViews;
    private FloatBuffer[] floatViews;

    @Override
    public void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException {
      // Intentional passthrough: the base class (DataInput) delegates to readBytes(b, offset, len),
      // which we already override efficiently. The useBuffer hint is meaningful only to
      // BufferedIndexInput subclasses that maintain an internal read buffer: useBuffer=false lets
      // them skip refilling that buffer and go directly to readInternal() for small reads. We have
      // no such buffer — our readBytes copies directly from the decompressed block — so the hint
      // is irrelevant.
      super.readBytes(b, offset, len, useBuffer);
    }

    @Override
    protected void readGroupVInt(long[] dst, int offset) throws IOException {
      // Intentional passthrough: readGroupVInt is a specific encoding (one control byte encoding
      // widths for up to 4 integers, then 1-4 bytes each) — not a generic series of vints. It is
      // the per-group hook called in a loop by the public readGroupVInts(), and is used exclusively
      // by HNSW/KNN vector formats (Lucene99HnswVectorsFormat etc.) for neighbour lists. We do not
      // currently use vector fields so this path is not exercised.
      // If vector fields are added, the right fix is to override readGroupVInts() with a single
      // localPin()/localUnpin() around a loop over a private _readGroupVInt() that uses our
      // _readByte()/_readInt() helpers directly.
      super.readGroupVInt(dst, offset);
    }

    @Override
    public int readZInt() throws IOException {
      // don't override this, so long as it's simply a wrapper around `readVInt()`
      return super.readZInt();
    }

    // Root constructor: mmaps the full local file, parses the 52-byte trailer from the end,
    // pre-pins the uncompressed tail block (if any) into accessMapped, and decodes GCS block
    // offsets.
    GCSIndexInput(String resourceDescription, GCSDirectory dir, Path offsetFile, byte[] trailer)
        throws IOException {
      super(resourceDescription);
      this.dir = dir;

      // Parse the 52-byte trailer:
      // blockType(1)+comprType(1)+reserved(2)+blobUUID(16)+segUUID(16)+length(8)+gcsObjectSize(8)
      ByteArrayDataInput hdr = new ByteArrayDataInput(trailer);
      int cBlockTypeId = hdr.readByte() & 0xff;
      if (cBlockTypeId != COMPRESSION_BLOCK_TYPE.id) {
        throw new IOException("unrecognized compression block type id: " + cBlockTypeId);
      }
      hdr.readByte(); // compressionType (not yet used)
      hdr.readShort(); // reserved
      blobUUID = new UUID(hdr.readLong(), hdr.readLong());
      segUUID = new UUID(hdr.readLong(), hdr.readLong());
      dir.ensureCanRead(offsetFile, blobUUID, segUUID);
      blobName = blobUUID.toString();
      length = hdr.readLong();
      long gcsObjectSize = hdr.readLong();

      // Tail: last partial block, stored uncompressed at offset 0 in the local file.
      int tailLen =
          (int) (length & COMPRESSION_BLOCK_MASK_LOW); // = length % COMPRESSION_BLOCK_SIZE
      boolean hasTail = tailLen > 0;

      blockCount = (int) (((length - 1) >> COMPRESSION_BLOCK_SHIFT) + 1);
      lastBlockIdx = blockCount - 1;
      lastBlockDecompressedLen = hasTail ? tailLen : COMPRESSION_BLOCK_SIZE;

      // TODO: pretty sure all/most of this could be deduped by `weMap[0]`, below
      // gcsBlockCount = full blocks in GCS (all but the tail when hasTail).
      int gcsBlockCount = hasTail ? blockCount - 1 : blockCount;
      // blockOffsets[0..gcsBlockCount] = GCS start offsets; [gcsBlockCount] = gcsObjectSize
      // sentinel.
      // [blockCount] is the dummy entry for the tail (default 0, never used — tail always
      // cache-hits).
      long[] blockOffsetsLocal = new long[blockCount + 1];

      // Mmap the full local file once.
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
      ByteArrayDataInput in = new ByteArrayDataInput(deltaBytes);

      // Decode GCS block offsets (gcsBlockCount - 1 deltas; blockOffsets[0] = 0 is implicit).
      long blockOffset = 0;
      int lastBlockSize = BLOCK_SIZE_ESTIMATE;
      blockOffsetsLocal[0] = 0;
      for (int i = 1; i < gcsBlockCount; i++) {
        int delta = in.readZInt();
        int nextBlockSize = lastBlockSize + delta;
        blockOffset += nextBlockSize;
        blockOffsetsLocal[i] = blockOffset;
        lastBlockSize = nextBlockSize;
      }
      blockOffsetsLocal[gcsBlockCount] = gcsObjectSize;

      boolean[] weMap = new boolean[1];
      this.blocksStruct =
          dir.pendingNodes.compute(
              blobUUID,
              (k, v) -> {
                if (v == null) {
                  weMap[0] = true;
                  String segName =
                      IndexFileNames.parseSegmentName(offsetFile.getFileName().toString());
                  v =
                      new BlocksStruct(
                          segUUID,
                          segName,
                          blockOffsetsLocal,
                          new AtomicReferenceArray<>(blockCount),
                          1,
                          hasTail);
                } else if (v.refCount.getAndIncrement() == 0) {
                  weMap[0] = true;
                }
                return v;
              });
      this.blockOffsets = blocksStruct.blockOffsets;
      this.accessMapped = blocksStruct.accessMapped;
      this.guard = blocksStruct.guard;

      // Pre-pin the tail block into accessMapped[lastBlockIdx] as a synthetic always-pinned node
      // backed directly by the mmap slice — no GCS fetch or decompression ever needed for it.
      if (hasTail) {
        if (weMap[0]) {
          try {
            ByteBuffer tailBuf = localFileMapped.slice(0, tailLen).order(ByteOrder.LITTLE_ENDIAN);
            BlockCache.Node tailNode = dir.cache.createTailNode(tailBuf);
            if (!accessMapped.compareAndSet(lastBlockIdx, null, tailNode)) {
              throw new IllegalStateException("tailNode already set");
            }
            blocksStruct.origMapping.complete(localFileMapped);
          } catch (Throwable t) {
            blocksStruct.origMapping.completeExceptionally(t);
            throw t;
          }
        } else {
          UNMAP.freeBuffer("quick close localFileMapped", localFileMapped);
          blocksStruct.origMapping.join(); // wait until initialized
        }
      } else {
        UNMAP.freeBuffer("quick close localFileMapped", localFileMapped);
      }

      this.offset = 0;
      this.sliceLength = length;
    }

    // Clone / slice constructor: shares immutable state from parent; does not own the mmap.
    private GCSIndexInput(
        String resourceDescription, GCSIndexInput parent, long offset, long length) {
      super(resourceDescription);
      this.dir = parent.dir;
      this.length = parent.length;
      this.blockOffsets = parent.blockOffsets;
      this.blockCount = parent.blockCount;
      this.lastBlockIdx = parent.lastBlockIdx;
      this.lastBlockDecompressedLen = parent.lastBlockDecompressedLen;
      this.blobUUID = parent.blobUUID;
      this.blobName = parent.blobName;
      this.segUUID = parent.segUUID;
      this.blocksStruct = null; // slice does not own the mapping
      this.guard = parent.guard; // shared: root.close() invalidates all slices
      this.accessMapped = parent.accessMapped;
      this.offset = parent.offset + offset;
      this.seekPos = this.offset;
      this.filePointer = this.offset;
      this.sliceLength = length;
      this.postBuffer = ByteBuffer.allocate(0);
    }

    // ---------------------------------------------------------------------------
    // Cache interaction
    // ---------------------------------------------------------------------------

    /**
     * Fetches the given compressed block from GCS and decompresses it. Called only on a cache miss.
     *
     * <p>Opens a fresh {@link ReadChannel} for each block, limited to exactly {@code compressedLen}
     * bytes via {@link ReadChannel#limit}, so the gRPC stream ends naturally after the block is
     * consumed. This prevents unbounded pre-parsing of surplus {@code ReadObjectResponse} messages
     * into the zero-copy lifecycle manager's unclosed-streams map.
     */
    private ByteBuffer supply(String blobName, long pos, int compressedLen, int decompressedLen)
        throws IOException {
      byte[] compressed = new byte[compressedLen];
      ByteBuffer compBuf = ByteBuffer.wrap(compressed);
      try {
        dir.channelSemaphore.acquire();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("interrupted waiting for GCS channel slot", e);
      }
      try (ReadChannel ch = dir.storage.reader(BlobId.of(dir.bucket, blobName))) {
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
        dir.channelSemaphore.release();
      }
      byte[] decompressed = new byte[decompressedLen + 7];
      CompressingDirectory.decompress(compressed, 0, decompressedLen, decompressed, 0);
      return ByteBuffer.wrap(decompressed, 0, decompressedLen);
    }

    // ---------------------------------------------------------------------------
    // Block navigation
    // ---------------------------------------------------------------------------

    private void localPin() throws IOException {
      currentNodeRef.localPin();
      long pos = seekPos;
      if (pos != -1) {
        seekPos = -1;
        actualSeek(pos);
      } else if (currentNodeRef.currentBlockIdx < 0) {
        actualSeek(filePointer);
      }
    }

    private void actualSeek(final long pos) throws IOException {
      filePointer = pos;
      int blockIdx = (int) (pos >> COMPRESSION_BLOCK_SHIFT);
      if (blockIdx != currentNodeRef.currentBlockIdx) initBlock(blockIdx);
      postBuffer.position(postBufferBaseline + (int) (pos & COMPRESSION_BLOCK_MASK_LOW));
    }

    private void initBlock(int blockIdx) throws IOException {
      if (blockIdx > lastBlockIdx) throw new EOFException();
      long blockOffset = blockOffsets[blockIdx];
      int compressedLen = (int) (blockOffsets[blockIdx + 1] - blockOffset);
      refill(blockOffset, compressedLen, blockIdx);
    }

    private void refill() throws IOException {
      int blockIdx = currentNodeRef.currentBlockIdx + 1;
      if (blockIdx > lastBlockIdx) throw new EOFException();
      long blockOffset = blockOffsets[blockIdx];
      int compressedLen = (int) (blockOffsets[blockIdx + 1] - blockOffset);
      refill(blockOffset, compressedLen, blockIdx);
    }

    private static final int MAX_READ_AHEAD = 16;

    private int readAheadTo = 0;

    private static int readAhead(int sequentialAccessCount) {
      // determines how aggressively we ramp up read-ahead.
      return sequentialAccessCount << 1;
    }

    private void setCurrentNode(BlockCache.Node node, int blockIdx) {
      int seqAcccessCount = currentNodeRef.setCurrentNode(node, blockIdx, dir.acquirePinPermit);
      switch (seqAcccessCount) {
        case -1:
          // reset, no read-ahead
          readAheadTo = 0;
          return;
        case 0:
          // moved forward, init to current new position if necessary
          if (blockIdx > readAheadTo) {
            readAheadTo = blockIdx;
          }
          return;
      }
      int newReadAheadTo;
      if (seqAcccessCount > 0
          && (newReadAheadTo =
                  Math.min(
                      accessMapped.length() - 1,
                      blockIdx + Math.min(MAX_READ_AHEAD, readAhead(seqAcccessCount))))
              > readAheadTo) {
        for (int i = readAheadTo + 1; i <= newReadAheadTo; i++) {
          if (!ensureLoaded(blobName, accessMapped, blockOffsets, i)) {
            newReadAheadTo = i - 1;
            break;
          }
        }
        readAheadTo = newReadAheadTo;
      }
    }

    private boolean ensureLoaded(
        String blob,
        AtomicReferenceArray<BlockCache.Node> accessMapped,
        long[] blockOffsets,
        int idx) {
      BlockCache.Node extant = accessMapped.get(idx);
      if ((extant == null || !extant.pinnable()) && dir.acquireReadaheadPermit()) {
        BlockCache.Node toPopulate = dir.cache.acquireNode();
        if (toPopulate == null) {
          dir.releaseReadaheadPermit();
          return false;
        }
        try {
          dir.readahead.submit(
              () -> {
                try {
                  if (accessMapped.compareAndSet(idx, extant, toPopulate)) {
                    long blockOffset = blockOffsets[idx];
                    int compressedLen = (int) (blockOffsets[idx + 1] - blockOffset);
                    populateBuf(
                        blob, blockOffset, compressedLen, idx, COMPRESSION_BLOCK_SIZE, toPopulate);
                    // NOTE: don't unpin in `finally` block! `populateBuf` already unpins the
                    // node upon Exception
                    dir.cache.unpin(toPopulate, false);
                  } else {
                    dir.cache.close(toPopulate, true);
                  }
                } finally {
                  dir.releaseReadaheadPermit();
                }
                return null;
              });
        } catch (Throwable t) {
          // TODO: ensure this won't double-release with submitted task
          dir.releaseReadaheadPermit();
          throw t;
        }
      }
      return true;
    }

    private void refill(final long pos, final int compressedLen, final int blockIdx)
        throws IOException {
      int decompressedLen =
          blockIdx == lastBlockIdx ? lastBlockDecompressedLen : COMPRESSION_BLOCK_SIZE;
      currentNodeRef.unpinCurrentBlock(dir.cache);

      BlockCache.Node node;
      BlockCache.Node cached = accessMapped.get(blockIdx);
      if (cached != null && dir.cache.pin(cached)) {
        // Cache hit (or in-flight by the winning thread — join() blocks until populated).
        ByteBuffer buf;
        try {
          buf = cached.join();
        } catch (CompletionException e) {
          dir.cache.unpin(cached);
          throw unwrapException(e.getCause());
        }
        setCurrentNode(cached, blockIdx);
        postBuffer = buf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        postBuffer.clear().limit(decompressedLen);
        postBufferBaseline = 0;
        longViews = null;
        intViews = null;
        floatViews = null;
        return;
      } else if ((node = dir.cache.acquireNode()) != null) {
        // cache miss
        BlockCache.Node extant = accessMapped.compareAndExchange(blockIdx, cached, node);
        if (extant == cached) {
          // We won the race: fetch from GCS and populate the node.
          maybeReadAheadSeg();
          ByteBuffer buf =
              populateBuf(blobName, pos, compressedLen, blockIdx, decompressedLen, node);
          setCurrentNode(node, blockIdx);
          postBuffer = buf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        } else {
          // Another thread won the race; wait for its result.
          dir.cache.close(node, true);
          if (dir.cache.pin(extant)) {
            ByteBuffer buf;
            try {
              buf = extant.join();
            } catch (CompletionException e) {
              dir.cache.unpin(extant);
              throw unwrapException(e.getCause());
            }
            setCurrentNode(extant, blockIdx);
            postBuffer = buf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
          } else {
            // Serve uncached.
            uncached(pos, compressedLen, blockIdx, decompressedLen);
            return;
          }
        }
        postBuffer.clear().limit(decompressedLen);
        postBufferBaseline = 0;
        longViews = null;
        intViews = null;
        floatViews = null;
        return;
      }

      // Serve uncached.
      uncached(pos, compressedLen, blockIdx, decompressedLen);
    }

    private void maybeReadAheadSeg() {
      BatchValue batchValue = dir.batched.get(segUUID);
      if (batchValue == null || batchValue.valid()) {
        return;
      }
      String segName = batchValue.segName;
      Set<UUID> blobs = batchValue.blobUUIDs;
      dir.readahead.submit(
          () -> {
            Path cfePath;
            if (blobs.size() == 1
                && Files.exists(cfePath = dir.directory.resolve(segName.concat(".cfe")))) {
              UUID blob = blobs.iterator().next();
              BlocksStruct blocks = dir.pendingNodes.get(blob);
              if (blocks != null) {
                IntArrayList blockIndexes = parseCfeBlockIndexes(cfePath);
                for (IntCursor i : blockIndexes) {
                  ensureLoaded(blob.toString(), blocks.accessMapped, blocks.blockOffsets, i.value);
                }
              }
              // preload the first block of each logical file
            } else {
              for (UUID blob : blobs) {
                BlocksStruct blocks = dir.pendingNodes.get(blob);
                if (blocks != null) {
                  ensureLoaded(blob.toString(), blocks.accessMapped, blocks.blockOffsets, 0);
                }
              }
            }
          });
    }

    /**
     * Parses the local {@code .cfe} entry table and returns the block index (within the CFS GCS
     * blob) of the first block of each logical sub-file. Block indices are computed as {@code
     * subFileOffset >> COMPRESSION_BLOCK_SHIFT}.
     */
    private IntArrayList parseCfeBlockIndexes(Path cfePath) {
      IntArrayList blockIndexes = new IntArrayList(16);
      try (IndexInput cfeIn = dir.openInput(cfePath.getFileName().toString(), IOContext.READONCE)) {
        // Skip index header without depending on the codec name (version-agnostic).
        if (CodecUtil.readBEInt(cfeIn) != CodecUtil.CODEC_MAGIC) return blockIndexes;
        cfeIn.readString(); // codec name — discard
        CodecUtil.readBEInt(cfeIn); // version — discard
        cfeIn.skipBytes(16); // segment ID (StringHelper.ID_LENGTH)
        cfeIn.skipBytes(cfeIn.readByte() & 0xFF); // suffix bytes (empty for Lucene90, but generic)
        int n = cfeIn.readVInt();
        int last = -1;
        for (int i = 0; i < n; i++) {
          cfeIn.readString(); // sub-file name — discard
          long offset = cfeIn.readLong(); // byte offset of this sub-file within the .cfs data
          cfeIn.readLong(); // length — discard
          int offsetBlock = (int) (offset >> COMPRESSION_BLOCK_SHIFT);
          if (offsetBlock > last) {
            blockIndexes.add(offsetBlock);
            last = offsetBlock;
          }
        }
      } catch (IOException e) {
        log.debug("readAheadSeg: failed to parse {}", cfePath, e);
      }
      return blockIndexes;
    }

    private ByteBuffer populateBuf(
        String blob,
        long pos,
        int compressedLen,
        int blockIdx,
        int decompressedLen,
        BlockCache.Node node)
        throws IOException {
      ByteBuffer buf;
      try {
        ByteBuffer heapBuf = supply(blob, pos, compressedLen, decompressedLen);
        buf =
            node.populate(
                heapBuf.array(), heapBuf.arrayOffset() + heapBuf.position(), decompressedLen);
      } catch (Throwable t) {
        node.completeExceptionally(t);
        accessMapped.compareAndSet(blockIdx, node, null);
        dir.cache.unpin(node);
        dir.cache.close(node);
        throw unwrapException(t);
      }
      return buf;
    }

    private void uncached(long pos, int compressedLen, int blockIdx, int decompressedLen)
        throws IOException {
      ByteBuffer heapBuf = supply(blobName, pos, compressedLen, decompressedLen);
      setCurrentNode(null, blockIdx);
      postBuffer = heapBuf;
      postBufferBaseline = heapBuf.position();
      heapBuf.order(ByteOrder.LITTLE_ENDIAN);
      longViews = null;
      intViews = null;
      floatViews = null;
    }

    // ---------------------------------------------------------------------------
    // RandomAccessInput
    // ---------------------------------------------------------------------------

    private byte _readByte(final long pos) throws IOException {
      final long absolutePos = pos + offset;
      final int blockIdx = (int) (absolutePos >> COMPRESSION_BLOCK_SHIFT);
      if (blockIdx != currentNodeRef.currentBlockIdx) initBlock(blockIdx);
      return guard.getByte(
          postBuffer, postBufferBaseline + (int) (absolutePos & COMPRESSION_BLOCK_MASK_LOW));
    }

    private int _readInt(final long pos) throws IOException {
      final long absolutePos = pos + offset;
      final int blockIdx = (int) (absolutePos >> COMPRESSION_BLOCK_SHIFT);
      if (blockIdx != currentNodeRef.currentBlockIdx) initBlock(blockIdx);
      final int localPos = postBufferBaseline + (int) (absolutePos & COMPRESSION_BLOCK_MASK_LOW);
      if (postBuffer.limit() - localPos >= Integer.BYTES) {
        return guard.getInt(postBuffer, localPos);
      }
      return ((_readByte(pos + 3) & 0xFF) << 24)
          | ((_readByte(pos + 2) & 0xFF) << 16)
          | ((_readByte(pos + 1) & 0xFF) << 8)
          | (_readByte(pos) & 0xFF);
    }

    private long _readLong(final long pos) throws IOException {
      final long absolutePos = pos + offset;
      final int blockIdx = (int) (absolutePos >> COMPRESSION_BLOCK_SHIFT);
      if (blockIdx != currentNodeRef.currentBlockIdx) initBlock(blockIdx);
      final int localPos = postBufferBaseline + (int) (absolutePos & COMPRESSION_BLOCK_MASK_LOW);
      if (postBuffer.limit() - localPos >= Long.BYTES) {
        return guard.getLong(postBuffer, localPos);
      }
      return ((_readByte(pos + 7) & 0xFFL) << 56)
          | ((_readByte(pos + 6) & 0xFFL) << 48)
          | ((_readByte(pos + 5) & 0xFFL) << 40)
          | ((_readByte(pos + 4) & 0xFFL) << 32)
          | ((_readByte(pos + 3) & 0xFFL) << 24)
          | ((_readByte(pos + 2) & 0xFFL) << 16)
          | ((_readByte(pos + 1) & 0xFFL) << 8)
          | (_readByte(pos) & 0xFFL);
    }

    @Override
    public byte readByte(final long pos) throws IOException {
      currentNodeRef.localPin();
      try {
        return _readByte(pos);
      } finally {
        currentNodeRef.localUnpin(dir.acquirePinPermit);
      }
    }

    @Override
    public short readShort(final long pos) throws IOException {
      currentNodeRef.localPin();
      try {
        final long absolutePos = pos + offset;
        final int blockIdx = (int) (absolutePos >> COMPRESSION_BLOCK_SHIFT);
        if (blockIdx != currentNodeRef.currentBlockIdx) initBlock(blockIdx);
        final int localPos = postBufferBaseline + (int) (absolutePos & COMPRESSION_BLOCK_MASK_LOW);
        if (postBuffer.limit() - localPos >= Short.BYTES) {
          return guard.getShort(postBuffer, localPos);
        }
        return (short) (((_readByte(pos + 1) & 0xFF) << 8) | (_readByte(pos) & 0xFF));
      } finally {
        currentNodeRef.localUnpin(dir.acquirePinPermit);
      }
    }

    @Override
    public int readInt(final long pos) throws IOException {
      currentNodeRef.localPin();
      try {
        return _readInt(pos);
      } finally {
        currentNodeRef.localUnpin(dir.acquirePinPermit);
      }
    }

    @Override
    public long readLong(final long pos) throws IOException {
      currentNodeRef.localPin();
      try {
        return _readLong(pos);
      } finally {
        currentNodeRef.localUnpin(dir.acquirePinPermit);
      }
    }

    // ---------------------------------------------------------------------------
    // Sequential IndexInput
    // ---------------------------------------------------------------------------

    @Override
    public byte readByte() throws IOException {
      localPin();
      try {
        if (!postBuffer.hasRemaining()) refill();
        filePointer++;
        return guard.getByte(postBuffer);
      } finally {
        currentNodeRef.localUnpin(dir.acquirePinPermit);
      }
    }

    @Override
    public void readBytes(byte[] dst, int offset, int len) throws IOException {
      localPin();
      try {
        filePointer += len;
        int left = postBuffer.remaining();
        while (left < len) {
          guard.getBytes(postBuffer, dst, offset, left);
          len -= left;
          offset += left;
          refill();
          left = postBuffer.remaining();
        }
        guard.getBytes(postBuffer, dst, offset, len);
      } finally {
        currentNodeRef.localUnpin(dir.acquirePinPermit);
      }
    }

    // ---------------------------------------------------------------------------
    // Sequential reads: short, int, long, vint, vlong, string
    // (must be overridden here to avoid per-byte localPin/localUnpin in base class)
    // ---------------------------------------------------------------------------

    /** Read next byte within a localPin() context; refills block if at boundary. */
    private byte _readByte(final int remaining) throws IOException {
      if (remaining == 0) refill();
      filePointer++;
      return guard.getByte(postBuffer);
    }

    @Override
    public short readShort() throws IOException {
      localPin();
      try {
        final int remaining = postBuffer.remaining();
        if (remaining >= Short.BYTES) {
          filePointer += Short.BYTES;
          return guard.getShort(postBuffer);
        }
        final byte b1 = _readByte(remaining);
        final byte b2 = _readByte(remaining - 1);
        return (short) (((b2 & 0xFF) << 8) | (b1 & 0xFF));
      } finally {
        currentNodeRef.localUnpin(dir.acquirePinPermit);
      }
    }

    @Override
    public int readInt() throws IOException {
      localPin();
      try {
        return _readInt(postBuffer.remaining());
      } finally {
        currentNodeRef.localUnpin(dir.acquirePinPermit);
      }
    }

    @Override
    public long readLong() throws IOException {
      localPin();
      try {
        return _readLong(postBuffer.remaining());
      } finally {
        currentNodeRef.localUnpin(dir.acquirePinPermit);
      }
    }

    @Override
    public int readVInt() throws IOException {
      localPin();
      try {
        return _readVInt(postBuffer.remaining());
      } finally {
        currentNodeRef.localUnpin(dir.acquirePinPermit);
      }
    }

    private int _readVInt(int remaining) throws IOException {
      byte b;
      if (remaining <= Integer.BYTES) {
        b = _readByte(remaining);
        if (b >= 0) return b;
        int i = b & 0x7F;
        b = _readByte(--remaining);
        i |= (b & 0x7F) << 7;
        if (b >= 0) return i;
        b = _readByte(--remaining);
        i |= (b & 0x7F) << 14;
        if (b >= 0) return i;
        b = _readByte(--remaining);
        i |= (b & 0x7F) << 21;
        if (b >= 0) return i;
        b = _readByte(--remaining);
        i |= (b & 0x0F) << 28;
        if ((b & 0xF0) == 0) return i;
      } else {
        b = guard.getByte(postBuffer);
        filePointer++;
        if (b >= 0) return b;
        int i = b & 0x7F;
        b = guard.getByte(postBuffer);
        filePointer++;
        i |= (b & 0x7F) << 7;
        if (b >= 0) return i;
        b = guard.getByte(postBuffer);
        filePointer++;
        i |= (b & 0x7F) << 14;
        if (b >= 0) return i;
        b = guard.getByte(postBuffer);
        filePointer++;
        i |= (b & 0x7F) << 21;
        if (b >= 0) return i;
        b = guard.getByte(postBuffer);
        filePointer++;
        i |= (b & 0x0F) << 28;
        if ((b & 0xF0) == 0) return i;
      }
      throw new IOException("Invalid vInt detected (too many bits)");
    }

    @Override
    public long readVLong() throws IOException {
      localPin();
      try {
        return _readVLong(false);
      } finally {
        currentNodeRef.localUnpin(dir.acquirePinPermit);
      }
    }

    @Override
    public long readZLong() throws IOException {
      localPin();
      try {
        return BitUtil.zigZagDecode(_readVLong(true));
      } finally {
        currentNodeRef.localUnpin(dir.acquirePinPermit);
      }
    }

    private long _readVLong(final boolean allowNegative) throws IOException {
      int remaining = postBuffer.remaining();
      byte b;
      long i;
      if (remaining <= (allowNegative ? Long.BYTES + 1 : Long.BYTES)) {
        b = _readByte(remaining);
        if (b >= 0) return b;
        i = b & 0x7FL;
        b = _readByte(--remaining);
        i |= (b & 0x7FL) << 7;
        if (b >= 0) return i;
        b = _readByte(--remaining);
        i |= (b & 0x7FL) << 14;
        if (b >= 0) return i;
        b = _readByte(--remaining);
        i |= (b & 0x7FL) << 21;
        if (b >= 0) return i;
        b = _readByte(--remaining);
        i |= (b & 0x7FL) << 28;
        if (b >= 0) return i;
        b = _readByte(--remaining);
        i |= (b & 0x7FL) << 35;
        if (b >= 0) return i;
        b = _readByte(--remaining);
        i |= (b & 0x7FL) << 42;
        if (b >= 0) return i;
        b = _readByte(--remaining);
        i |= (b & 0x7FL) << 49;
        if (b >= 0) return i;
        b = _readByte(--remaining);
        i |= (b & 0x7FL) << 56;
        if (b >= 0) return i;
        if (!allowNegative) {
          throw new IOException("Invalid vLong detected (negative values disallowed)");
        }
        b = _readByte(--remaining);
      } else {
        b = guard.getByte(postBuffer);
        filePointer++;
        if (b >= 0) return b;
        i = b & 0x7FL;
        b = guard.getByte(postBuffer);
        filePointer++;
        i |= (b & 0x7FL) << 7;
        if (b >= 0) return i;
        b = guard.getByte(postBuffer);
        filePointer++;
        i |= (b & 0x7FL) << 14;
        if (b >= 0) return i;
        b = guard.getByte(postBuffer);
        filePointer++;
        i |= (b & 0x7FL) << 21;
        if (b >= 0) return i;
        b = guard.getByte(postBuffer);
        filePointer++;
        i |= (b & 0x7FL) << 28;
        if (b >= 0) return i;
        b = guard.getByte(postBuffer);
        filePointer++;
        i |= (b & 0x7FL) << 35;
        if (b >= 0) return i;
        b = guard.getByte(postBuffer);
        filePointer++;
        i |= (b & 0x7FL) << 42;
        if (b >= 0) return i;
        b = guard.getByte(postBuffer);
        filePointer++;
        i |= (b & 0x7FL) << 49;
        if (b >= 0) return i;
        b = guard.getByte(postBuffer);
        filePointer++;
        i |= (b & 0x7FL) << 56;
        if (b >= 0) return i;
        if (!allowNegative) {
          throw new IOException("Invalid vLong detected (negative values disallowed)");
        }
        b = guard.getByte(postBuffer);
        filePointer++;
      }
      i |= (b & 0x7FL) << 63;
      if (b == 0 || b == 1) return i;
      throw new IOException("Invalid vLong detected (more than 64 bits)");
    }

    @Override
    public String readString() throws IOException {
      localPin();
      try {
        return _readString();
      } finally {
        currentNodeRef.localUnpin(dir.acquirePinPermit);
      }
    }

    public String _readString() throws IOException {
      final int length = _readVInt(postBuffer.remaining());
      final byte[] bytes = new byte[length];
      final int left = postBuffer.remaining();
      filePointer += length;
      if (left < length) {
        slowReadBytes(bytes, 0, length, left);
      } else {
        guard.getBytes(postBuffer, bytes, 0, length);
      }
      return new String(bytes, 0, length, StandardCharsets.UTF_8);
    }

    private void slowReadBytes(final byte[] dst, int offset, int toRead, int left)
        throws IOException {
      do {
        guard.getBytes(postBuffer, dst, offset, left);
        toRead -= left;
        offset += left;
        refill();
        left = postBuffer.remaining();
      } while (left < toRead);
      guard.getBytes(postBuffer, dst, offset, toRead);
    }

    @Override
    public Map<String, String> readMapOfStrings() throws IOException {
      localPin();
      try {
        final int count = _readVInt(postBuffer.remaining());
        switch (count) {
          case 0:
            return Collections.emptyMap();
          case 1:
            return Collections.singletonMap(_readString(), _readString());
          default:
            final Map<String, String> map =
                count > 10 ? CollectionUtil.newHashMap(count) : new TreeMap<>();
            for (int i = count; i > 0; i--) {
              map.put(_readString(), _readString());
            }
            return Collections.unmodifiableMap(map);
        }
      } finally {
        currentNodeRef.localUnpin(dir.acquirePinPermit);
      }
    }

    @Override
    public Set<String> readSetOfStrings() throws IOException {
      localPin();
      try {
        final int count = _readVInt(postBuffer.remaining());
        switch (count) {
          case 0:
            return Collections.emptySet();
          case 1:
            return Collections.singleton(_readString());
          default:
            final Set<String> set = count > 10 ? CollectionUtil.newHashSet(count) : new TreeSet<>();
            for (int i = count; i > 0; i--) {
              set.add(_readString());
            }
            return Collections.unmodifiableSet(set);
        }
      } finally {
        currentNodeRef.localUnpin(dir.acquirePinPermit);
      }
    }

    // ---------------------------------------------------------------------------
    // Bulk typed reads with buffer views
    // ---------------------------------------------------------------------------

    private int _readInt(final int remaining) throws IOException {
      if (remaining >= Integer.BYTES) {
        filePointer += Integer.BYTES;
        return guard.getInt(postBuffer);
      }
      // Cross-block: use _readByte to avoid per-byte localPin/localUnpin overhead.
      final byte b0 = _readByte(remaining);
      final byte b1 = _readByte(remaining - 1);
      final byte b2 = _readByte(remaining - 2);
      final byte b3 = _readByte(remaining - 3);
      return ((b3 & 0xFF) << 24) | ((b2 & 0xFF) << 16) | ((b1 & 0xFF) << 8) | (b0 & 0xFF);
    }

    private long _readLong(final int remaining) throws IOException {
      if (remaining >= Long.BYTES) {
        filePointer += Long.BYTES;
        return guard.getLong(postBuffer);
      }
      final byte b0 = _readByte(remaining);
      final byte b1 = _readByte(remaining - 1);
      final byte b2 = _readByte(remaining - 2);
      final byte b3 = _readByte(remaining - 3);
      final byte b4 = _readByte(remaining - 4);
      final byte b5 = _readByte(remaining - 5);
      final byte b6 = _readByte(remaining - 6);
      final byte b7 = _readByte(remaining - 7);
      return ((b7 & 0xFFL) << 56)
          | ((b6 & 0xFFL) << 48)
          | ((b5 & 0xFFL) << 40)
          | ((b4 & 0xFFL) << 32)
          | ((b3 & 0xFFL) << 24)
          | ((b2 & 0xFFL) << 16)
          | ((b1 & 0xFFL) << 8)
          | (b0 & 0xFFL);
    }

    @Override
    public void readLongs(final long[] dst, final int offset, final int length) throws IOException {
      localPin();
      try {
        if (longViews == null) {
          longViews = initLongViews();
        }
        final int remaining = postBuffer.remaining();
        final long bytesRequested = (long) length << 3;
        if (remaining < bytesRequested) {
          dst[offset] = _readLong(remaining);
          for (int i = 1; i < length; i++) {
            dst[offset + i] = _readLong(postBuffer.remaining());
          }
        } else {
          final int position = postBuffer.position();
          guard.getLongs(longViews[position & 0x07].position(position >>> 3), dst, offset, length);
          filePointer += bytesRequested;
          postBuffer.position(position + (int) bytesRequested);
        }
      } finally {
        currentNodeRef.localUnpin(dir.acquirePinPermit);
      }
    }

    @Override
    public void readInts(final int[] dst, final int offset, final int length) throws IOException {
      localPin();
      try {
        if (intViews == null) {
          intViews = initIntViews();
        }
        final int remaining = postBuffer.remaining();
        final long bytesRequested = (long) length << 2;
        if (remaining < bytesRequested) {
          dst[offset] = _readInt(remaining);
          for (int i = 1; i < length; i++) {
            dst[offset + i] = _readInt(postBuffer.remaining());
          }
        } else {
          final int position = postBuffer.position();
          guard.getInts(intViews[position & 0x03].position(position >>> 2), dst, offset, length);
          filePointer += bytesRequested;
          postBuffer.position(position + (int) bytesRequested);
        }
      } finally {
        currentNodeRef.localUnpin(dir.acquirePinPermit);
      }
    }

    @Override
    public void readFloats(final float[] dst, final int offset, final int length)
        throws IOException {
      localPin();
      try {
        if (floatViews == null) {
          floatViews = initFloatViews();
        }
        final int remaining = postBuffer.remaining();
        final long bytesRequested = (long) length << 2;
        if (remaining < bytesRequested) {
          dst[offset] = Float.intBitsToFloat(_readInt(remaining));
          for (int i = 1; i < length; i++) {
            dst[offset + i] = Float.intBitsToFloat(_readInt(postBuffer.remaining()));
          }
        } else {
          final int position = postBuffer.position();
          guard.getFloats(
              floatViews[position & 0x03].position(position >>> 2), dst, offset, length);
          filePointer += bytesRequested;
          postBuffer.position(position + (int) bytesRequested);
        }
      } finally {
        currentNodeRef.localUnpin(dir.acquirePinPermit);
      }
    }

    private LongBuffer[] initLongViews() {
      final LongBuffer[] ret = new LongBuffer[Long.BYTES];
      final ByteBuffer template = postBuffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
      final int lim = postBuffer.limit();
      for (int i = Long.BYTES - 1; i >= 0; i--) {
        ret[i] = i < lim ? template.position(i).asLongBuffer() : EMPTY_LONGBUFFER;
      }
      return ret;
    }

    private IntBuffer[] initIntViews() {
      final IntBuffer[] ret = new IntBuffer[Integer.BYTES];
      final ByteBuffer template = postBuffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
      final int lim = postBuffer.limit();
      for (int i = Integer.BYTES - 1; i >= 0; i--) {
        ret[i] = i < lim ? template.position(i).asIntBuffer() : EMPTY_INTBUFFER;
      }
      return ret;
    }

    private FloatBuffer[] initFloatViews() {
      final FloatBuffer[] ret = new FloatBuffer[Float.BYTES];
      final ByteBuffer template = postBuffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
      final int lim = postBuffer.limit();
      for (int i = Float.BYTES - 1; i >= 0; i--) {
        ret[i] = i < lim ? template.position(i).asFloatBuffer() : EMPTY_FLOATBUFFER;
      }
      return ret;
    }

    // ---------------------------------------------------------------------------
    // IndexInput lifecycle
    // ---------------------------------------------------------------------------

    @Override
    public long getFilePointer() {
      return filePointer - offset;
    }

    @Override
    public void seek(long pos) throws IOException {
      seekPos = pos + offset;
      filePointer = pos + offset;
    }

    @Override
    public long length() {
      return sliceLength;
    }

    @Override
    public IndexInput slice(String sliceDescription, long sliceOffset, long sliceLength)
        throws IOException {
      if (sliceOffset < 0 || sliceLength < 0 || sliceOffset + sliceLength > this.sliceLength) {
        throw new IllegalArgumentException("slice out of bounds");
      }
      return new GCSIndexInput(
          getFullSliceDescription(sliceDescription), this, sliceOffset, sliceLength);
    }

    @Override
    public GCSIndexInput clone() {
      GCSIndexInput clone = new GCSIndexInput(toString(), this, 0, sliceLength);
      try {
        clone.seek(getFilePointer());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      return clone;
    }

    @Override
    public void close() throws IOException {
      try {
        if (accessMapped == null) return;

        unsetBuffers();

        if (blocksStruct == null) return;

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
                  // `v.copy()` keeps non-local (cached) mappings only. Until our file
                  // is actually deleted, the cached mappings could be used again if
                  // someone reopens the file.
                  return v.copy();
                } else {
                  // file has been deleted (pendingDeletion != null), and we're now
                  // closing the last open input. So set deletion to run, and delete
                  // the entry (return null).
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
        if (toUnmap[0] != null) {
          // tell the guard to invalidate and later unmap the bytebuffers (if supported):
          guard.invalidateAndUnmap(toUnmap[0].join());
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
              log.error(
                  "interrupted while enqueuing release for {}; blobs may be orphaned", segUUID);
            }
          }
        }
      } finally {
        unsetBuffers();
      }
    }

    private void unsetBuffers() {
      accessMapped = null;
      currentNodeRef.unpinFor(dir.cache);
      currentNodeRef.readPermitRegistered = false;
      postBuffer = null;
      floatViews = null;
      intViews = null;
      longViews = null;
    }
  }

  private final ExecutorService readahead = ExecutorUtil.newMDCAwareCachedThreadPool("readahead");
  private static final int READ_CHANNEL_HEADROOM =
      GCSDirectoryFactory.DEFAULT_MAX_OPEN_CHANNELS >> 1;
  private final AtomicInteger readaheadPermit = new AtomicInteger(0);

  private boolean acquireReadaheadPermit() {
    if (readaheadPermit.getAndIncrement() >= READ_CHANNEL_HEADROOM) {
      readaheadPermit.decrementAndGet();
      return false;
    } else {
      return true;
    }
  }

  private void releaseReadaheadPermit() {
    readaheadPermit.decrementAndGet();
  }

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
      for (UUID blobUUID : batch.blobUUIDs) {
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
