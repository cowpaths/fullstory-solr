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
import static org.apache.solr.storage.CompressingDirectory.COMPRESSION_BLOCK_SHIFT;
import static org.apache.solr.storage.CompressingDirectory.COMPRESSION_BLOCK_SIZE;
import static org.apache.solr.storage.CompressingDirectory.DirectIOIndexOutput.HEADER_SIZE;
import static org.apache.solr.storage.CompressingDirectory.readLengthFromHeader;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.StampedLock;
import java.util.zip.CRC32;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.internal.hppc.IntCursor;
import org.apache.lucene.internal.hppc.LongArrayList;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBufferGuard;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MappedByteBufferIndexInputProvider;
import org.apache.lucene.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link MMapDirectory} that serves reads from a {@link BlockCache}-backed decompression layer
 * when the underlying access file is absent. Compressed blocks are decompressed on demand and
 * cached in the shared {@link BlockCache}; cached blocks are pinned for the duration of each read
 * and evicted by the LRU when the pool is exhausted.
 */
public class AccessDirectory2 extends MMapDirectory {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Determines chunk size for mmapping files. {@code 1} yields 1 GiB chunks, but this may be set
   * higher to stress-test buffer boundaries (as low as 15, which yields the min chunk size of 64
   * KiB, equal to {@link CompressingDirectory#COMPRESSION_BLOCK_SIZE}).
   */
  private static final int MAP_BUF_DIVIDE_SHIFT = 1;

  public static final int MAX_MAP_SIZE = Integer.MIN_VALUE >>> MAP_BUF_DIVIDE_SHIFT;
  public static final int MAX_MAP_MASK = MAX_MAP_SIZE - 1;
  public static final int MAX_MAP_SHIFT = Integer.numberOfTrailingZeros(MAX_MAP_SIZE);

  private final long uuidMsb;
  private final long uuidLsb;

  private final Path compressedPath;
  private final BlockCache cache;
  private final ExecutorService ioExec;

  /**
   * Ref-counted wrapper around a shared {@code accessMapped} array. The map itself holds a +1 ref;
   * each root {@link AD2IndexInput} open holds a +1 ref. Nodes are released only when the count
   * reaches zero, so readers that opened a file before {@link #deleteFile} was called continue to
   * get cache hits until they close.
   */
  private static final class NodesEntry {
    private final AtomicLongArray nodes;
    private final AtomicInteger refCount = new AtomicInteger(1); // +1 for pendingNodes map itself

    NodesEntry(AtomicLongArray nodes) {
      this.nodes = nodes;
    }

    void acquire() {
      if (refCount.getAndIncrement() == 0) {
        throw new AlreadyClosedException("attempt to open ref after file deletion");
      }
    }

    void release(BlockCache cache) {
      if (refCount.decrementAndGet() == 0) {
        for (int i = nodes.length() - 1; i >= 0; i--) {
          long node = nodes.getAndSet(i, BlockCache.NULL_HANDLE);
          if (node != BlockCache.NULL_HANDLE) cache.close(node);
        }
      }
    }
  }

  /**
   * Shared {@code accessMapped} arrays pre-populated by {@link WriteThroughOutput} at write-close
   * time. All root {@link AD2IndexInput} instances opened for the same file share the same array,
   * so they all see the same cached blocks. Entries are removed in {@link #deleteFile} or {@link
   * #rename}; nodes are released when the last open root input closes.
   */
  private final HashMap<String, NodesEntry> pendingNodes = new HashMap<>();

  private UUID uuidForFile(String name) {
    if (uuidMsb == 0 && uuidLsb == 0) {
      // No directory-level UUID, fallback to absolutePath string
      return BlockCache.rawMd5UUID(
          compressedPath.resolve(name).toAbsolutePath().normalize().toString());
    } else {
      ByteBuffer hash = BlockCache.rawMd5(name);
      return new UUID(uuidMsb ^ hash.getLong(), uuidLsb ^ hash.getLong());
    }
  }

  @SuppressWarnings("try")
  public AccessDirectory2(
      Path coreRootDirectory,
      Path path,
      LockFactory lockFactory,
      Path compressedPath,
      BlockCache cache,
      ExecutorService ioExec)
      throws IOException {
    super(path, lockFactory);
    UUID uuid = BlockCache.refIdFromCoreProperties(coreRootDirectory, compressedPath, true);
    if (uuid == null) {
      this.uuidMsb = 0;
      this.uuidLsb = 0;
    } else {
      this.uuidMsb = uuid.getMostSignificantBits();
      this.uuidLsb = uuid.getLeastSignificantBits();
    }
    this.compressedPath = compressedPath;
    this.cache = cache;
    this.ioExec = ioExec;

    // Scan existing compressed files, open inputs, and group by segment.
    Map<String, List<Map.Entry<String, AD2IndexInput>>> segToInputs = new HashMap<>();
    List<String> segmentsFiles = new ArrayList<>();
    List<AD2IndexInput> priorityInputs = new ArrayList<>();

    String[] files = compressedPath.toFile().list();
    if (files == null) {
      files = new String[0];
    }
    List<IndexInput> toClose = new ArrayList<>(files.length);
    for (String file : files) {
      if (file.endsWith(".tmp")) continue;
      AD2IndexInput input = (AD2IndexInput) openInput(file, IOContext.DEFAULT);
      toClose.add(input);
      if (file.startsWith("segments_")) {
        segmentsFiles.add(file);
        priorityInputs.add(input);
      } else if (file.endsWith(".si")) {
        priorityInputs.add(input);
      } else {
        String segName = IndexFileNames.parseSegmentName(file);
        segToInputs
            .computeIfAbsent(segName, k -> new ArrayList<>())
            .add(new AbstractMap.SimpleImmutableEntry<>(file, input));
      }
    }

    boolean submitted = false;
    try {
      // Issue MADV_WILLNEED hints inline so the kernel starts I/O immediately,
      // before the async preload task begins decompressing.
      // Rough segment ordering by generation (oldest first) — no file I/O needed.
      final String[] roughSegOrder = new String[segToInputs.size()];
      {
        long[] segIds = new long[roughSegOrder.length];
        int i = 0;
        for (String seg : segToInputs.keySet()) {
          segIds[i++] = Long.parseLong(seg.substring(1), Character.MAX_RADIX);
        }
        Arrays.sort(segIds);
        for (i = segIds.length - 1; i >= 0; i--) {
          roughSegOrder[i] = "_".concat(Long.toString(segIds[i], Character.MAX_RADIX));
        }
      }

      // Priority files: hint all blocks (small, fully read).
      for (AD2IndexInput in : priorityInputs) {
        int blockCount = in.blockOffsets.length - 1;
        if (blockCount > 0) {
          in.hintCompressedRange(0, blockCount - 1);
        }
      }
      // CFS/CFE boundary blocks, then other files' boundary blocks — in rough segment order.
      for (String segName : roughSegOrder) {
        for (Map.Entry<String, AD2IndexInput> e : segToInputs.get(segName)) {
          String name = e.getKey();
          if (name.endsWith(".cfe")) {
            AD2IndexInput in = e.getValue();
            int blockCount = in.blockOffsets.length - 1;
            if (blockCount > 0) in.hintCompressedRange(0, blockCount - 1);
          } else if (name.endsWith(".cfs")) {
            AD2IndexInput in = e.getValue();
            int lastIdx = in.blockOffsets.length - 2;
            if (lastIdx <= 1) {
              in.hintCompressedRange(0, lastIdx);
            } else {
              in.hintCompressedRange(0, 0);
              in.hintCompressedRange(lastIdx, lastIdx);
            }
          }
        }
      }
      for (String segName : roughSegOrder) {
        for (Map.Entry<String, AD2IndexInput> e : segToInputs.get(segName)) {
          String name = e.getKey();
          if (!name.endsWith(".cfs") && !name.endsWith(".cfe")) {
            AD2IndexInput in = e.getValue();
            int lastIdx = in.blockOffsets.length - 2;
            if (lastIdx <= 1) {
              in.hintCompressedRange(0, lastIdx);
            } else {
              in.hintCompressedRange(0, 0);
              in.hintCompressedRange(lastIdx, lastIdx);
            }
          }
        }
      }

      // Load CFE boundary blocks inline and parse sub-file block indices.
      // CFE files are small (1-2 blocks); loading them synchronously lets us
      // know which CFS blocks to hint before the async task starts.
      HashMap<String, IntArrayList> cfsBlockIndexes = new HashMap<>();
      for (String segName : roughSegOrder) {
        AD2IndexInput cfeInput = null;
        for (Map.Entry<String, AD2IndexInput> e : segToInputs.get(segName)) {
          if (e.getKey().endsWith(".cfe")) {
            cfeInput = e.getValue();
            break;
          }
        }
        if (cfeInput != null) {
          int cfeBlockCount = cfeInput.blockOffsets.length - 1;
          for (int i = 0; i < cfeBlockCount; i++) {
            AD2IndexInput.loadBlock(cfeInput, i);
          }
          IntArrayList blockIndexes =
              BlockPreloader.parseCfeBlockIndexes(cfeInput, Integer.MAX_VALUE);
          if (!blockIndexes.isEmpty()) {
            cfsBlockIndexes.put(segName, blockIndexes);
            // Find the CFS input and hint sub-file compressed ranges, coalescing adjacent blocks.
            for (Map.Entry<String, AD2IndexInput> e : segToInputs.get(segName)) {
              if (e.getKey().endsWith(".cfs")) {
                AD2IndexInput cfsInput = e.getValue();
                int rangeStart = blockIndexes.get(0);
                int rangeEnd = rangeStart;
                for (int i = 1, size = blockIndexes.size(); i < size; i++) {
                  int val = blockIndexes.get(i);
                  if (val == rangeEnd + 1) {
                    rangeEnd = val;
                  } else {
                    cfsInput.hintCompressedRange(rangeStart, rangeEnd);
                    rangeStart = val;
                    rangeEnd = val;
                  }
                }
                cfsInput.hintCompressedRange(rangeStart, rangeEnd);
                break;
              }
            }
          }
        }
      }

      // Phase 1: priority files (segments_N, .si) — all blocks (small, fully read)
      for (AD2IndexInput in : priorityInputs) {
        for (int idx = 0, blockCount = in.blockOffsets.length - 1; idx < blockCount; idx++) {
          AD2IndexInput.loadBlock(in, idx);
        }
      }

      // Order segments by ordinal from segments_N (best-effort; data is now cache-hot)
      List<String> segOrder = new ArrayList<>(segToInputs.keySet());
      if (!segmentsFiles.isEmpty() && !segOrder.isEmpty()) {
        Map<String, Integer> segOrds = new HashMap<>();
        for (String segmentsFile : segmentsFiles) {
          try {
            SegmentInfos infos = SegmentInfos.readCommit(this, segmentsFile);
            for (int i = infos.size() - 1; i >= 0; i--) {
              segOrds.put(infos.info(i).info.name, i);
            }
          } catch (Exception e) {
            // best-effort; proceed with unordered segments
          }
        }
        segOrder.sort(Comparator.comparingInt(s -> segOrds.getOrDefault(s, Integer.MAX_VALUE)));
      }

      // Phase 2: CFS boundary blocks (CFE already loaded inline)
      for (String seg : segOrder) {
        for (Map.Entry<String, AD2IndexInput> e : segToInputs.get(seg)) {
          if (e.getKey().endsWith(".cfs")) {
            AD2IndexInput in = e.getValue();
            AD2IndexInput.loadBlock(in, 0);
            int lastIdx = in.blockOffsets.length - 2;
            if (lastIdx > 0) AD2IndexInput.loadBlock(in, lastIdx);
          }
        }
      }

      // Phase 3: all logical file boundary blocks in segment order —
      // non-CFS files and CFS sub-files interleaved.
      for (String seg : segOrder) {
        List<Map.Entry<String, AD2IndexInput>> inputs = segToInputs.get(seg);
        AD2IndexInput cfsInput = null;
        for (Map.Entry<String, AD2IndexInput> e : inputs) {
          String name = e.getKey();
          if (name.endsWith(".cfs")) {
            cfsInput = e.getValue();
          } else if (!name.endsWith(".cfe")) {
            // non-CFS file: load boundary blocks
            AD2IndexInput in = e.getValue();
            AD2IndexInput.loadBlock(in, 0);
            int lastIdx = in.blockOffsets.length - 2;
            if (lastIdx > 0) AD2IndexInput.loadBlock(in, lastIdx);
          }
        }
        IntArrayList blockIndexes = cfsBlockIndexes.get(seg);
        if (cfsInput != null && blockIndexes != null) {
          for (IntCursor cursor : blockIndexes) {
            AD2IndexInput.loadBlock(cfsInput, cursor.value);
          }
        }
      }

      // Defer closing inputs: wait an ample amount of time so that the main code loads its own
      // copies of inputs and can inherit the structures we've prepopulated. If we close too
      // early, refCount will drop to 0, triggering cleanup, and all our work is wasted!
      ioExec.submit(
          () -> {
            try {
              Thread.sleep(30_000);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              IOUtils.close(toClose);
            }
            return null;
          });
      submitted = true;
    } finally {
      if (!submitted) {
        IOUtils.close(toClose);
      }
    }
  }

  /** Stores the shared {@code accessMapped} array for {@code name}, pre-populated by the writer. */
  void storeNodes(String name, AtomicLongArray sharedAccessMapped) {
    NodesEntry extant;
    synchronized (pendingNodes) {
      extant = pendingNodes.putIfAbsent(name, new NodesEntry(sharedAccessMapped));
    }
    if (extant != null) {
      log.warn("unexpected extant `accessMapped` entry");
    }
  }

  @Override
  public void deleteFile(String name) throws IOException {
    NodesEntry stale;
    synchronized (pendingNodes) {
      stale = pendingNodes.remove(name);
    }
    if (stale != null) {
      stale.release(cache);
    }
    if (name.endsWith(".tmp")) {
      super.deleteFile(name);
    }
  }

  @Override
  public void rename(String source, String dest) throws IOException {
    synchronized (pendingNodes) {
      NodesEntry entry = pendingNodes.get(source);
      if (entry == null) {
        throw new NoSuchFileException(source);
      } else if (pendingNodes.putIfAbsent(dest, entry) != null) {
        throw new FileAlreadyExistsException(dest);
      } else if (!pendingNodes.remove(source, entry)) {
        throw new IOException("source entry unexpectedly changed: " + source);
      }
    }
    if (source.endsWith(".tmp")) {
      super.rename(source, dest);
    }
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    List<String> present = null;
    for (String name : names) {
      if (Files.exists(getDirectory().resolve(name))) {
        if (present == null) present = new ArrayList<>();
        present.add(name);
      }
    }
    if (present != null) super.sync(present);
  }

  @Override
  public long fileLength(String name) throws IOException {
    if (name.endsWith(".tmp")) {
      return super.fileLength(name);
    } else {
      ensureOpen();
      return readLengthFromHeader(compressedPath.resolve(name));
    }
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    if (name.endsWith(".tmp")) {
      return super.openInput(name, context);
    }
    NodesEntry sharedEntry;
    synchronized (pendingNodes) {
      sharedEntry = pendingNodes.get(name);
      if (sharedEntry != null) {
        sharedEntry.acquire(); // +1 for this root reader
      }
    }
    // TODO: if `context.mergeInfo != null` we should preload the entire contents
    return new AD2IndexInput(compressedPath.resolve(name), this, sharedEntry, pendingNodes);
  }

  private static ByteBufferGuard.BufferCleaner unmapHack() {
    Object hack = MappedByteBufferIndexInputProvider.unmapHackImpl();
    if (hack instanceof ByteBufferGuard.BufferCleaner) {
      return (ByteBufferGuard.BufferCleaner) hack;
    } else {
      throw new UnsupportedOperationException("unmap not available");
    }
  }

  static final class AD2IndexInput extends CachedCompressedIndexInput {

    private final ExecutorService ioExec;
    private final ByteBufferGuard compressedGuard;
    private final ByteBuffer[] compressed;
    private final long[] compressedBaseAddresses;
    private final boolean isRoot;

    private final BlockPreloader.BlockSupplier blockSupplier;

    // non-null for root inputs with tracked nodes; null otherwise
    private final NodesEntry nodesEntry;

    // Guards compressed ByteBuffer lifetime. Root holds write lock on close; supply calls hold
    // read lock. StampedLock.tryReadLock() returns 0 while a write lock is waiting, so in-flight
    // supply calls drain before invalidateAndUnmap runs, and new tasks see 0 and throw rather than
    // touching freed memory. Null for clones and sentinel (they share root's lock via
    // blockSupplier).
    private final StampedLock supplyLock;

    // -------------------------------------------------------------------------
    // Root constructor (via this() delegation)
    // -------------------------------------------------------------------------

    AD2IndexInput(
        Path source,
        AccessDirectory2 dir,
        NodesEntry sharedEntry,
        HashMap<String, NodesEntry> pendingNodes)
        throws IOException {
      this(
          "lazy:" + source,
          dir,
          parseRootParams(
              source, sharedEntry, pendingNodes, dir.uuidForFile(source.getFileName().toString())),
          source.getFileName().toString().endsWith(".cfs") ? null : Boolean.TRUE);
    }

    private AD2IndexInput() {
      super("done_sentinel");
      this.ioExec = null;
      this.compressedGuard = null;
      this.compressed = null;
      this.compressedBaseAddresses = null;
      this.isRoot = false;
      this.blockSupplier = null;
      this.nodesEntry = null;
      this.supplyLock = null;
    }

    private static final class RootParams {
      final long length;
      final long[] blockOffsets;
      final ByteBuffer[] compressed;
      final AtomicLongArray accessMapped;
      final NodesEntry entry; // null for empty files
      final UUID blobUUID;

      RootParams(
          long length,
          long[] blockOffsets,
          ByteBuffer[] compressed,
          AtomicLongArray accessMapped,
          NodesEntry entry,
          UUID blobUUID) {
        this.length = length;
        this.blockOffsets = blockOffsets;
        this.compressed = compressed;
        this.accessMapped = accessMapped;
        this.entry = entry;
        this.blobUUID = blobUUID;
      }
    }

    private static RootParams parseRootParams(
        Path source,
        NodesEntry sharedEntry,
        HashMap<String, NodesEntry> pendingNodes,
        UUID blobUUID)
        throws IOException {
      try (FileChannel channel = FileChannel.open(source, StandardOpenOption.READ)) {
        long compressedFileSize = channel.size();
        ByteBuffer[] compressed =
            new ByteBuffer[Math.toIntExact(((compressedFileSize - 1) >> MAX_MAP_SHIFT) + 1)];
        long pos = 0;
        long limit = MAX_MAP_SIZE;
        for (int i = 0, lim = compressed.length; i < lim; i++) {
          int size = (int) (Math.min(limit, compressedFileSize) - pos);
          compressed[i] = channel.map(FileChannel.MapMode.READ_ONLY, pos, size);
          pos = limit;
          limit += MAX_MAP_SIZE;
        }

        long size = channel.size();
        if (size == 0) {
          return new RootParams(0, null, compressed, null, null, null);
        }

        ByteBuffer initial = compressed[0];
        long length = initial.getLong(0);
        if (length >> COMPRESSION_BLOCK_SHIFT > Integer.MAX_VALUE) {
          throw new IllegalArgumentException(
              "file too long " + Long.toHexString(length) + ", " + source);
        }
        int blockDeltaFooterSize = initial.getInt(Long.BYTES);
        int cBlockTypeId = initial.get(HEADER_SIZE - Integer.BYTES) & 0xff;
        if (cBlockTypeId != CompressingDirectory.COMPRESSION_BLOCK_TYPE.id) {
          throw new IllegalArgumentException(
              "unrecognized compression block type id: " + cBlockTypeId);
        }
        int cTypeId = initial.get(HEADER_SIZE - Integer.BYTES + 1) & 0xff;
        if (cTypeId != CompressingDirectory.COMPRESSION_TYPE.id) {
          throw new IllegalArgumentException("unrecognized compression type id: " + cTypeId);
        }
        byte[] footer = new byte[blockDeltaFooterSize];
        long blockDeltaFooterOffset = size - blockDeltaFooterSize;
        channel.read(ByteBuffer.wrap(footer), blockDeltaFooterOffset);
        ByteArrayDataInput in = new ByteArrayDataInput(footer);

        long blockOffset = HEADER_SIZE;
        int lastBlockSize = BLOCK_SIZE_ESTIMATE;
        int blockCount = (int) (((length - 1) >> COMPRESSION_BLOCK_SHIFT) + 1);
        long[] blockOffsets = new long[blockCount + 1];
        blockOffsets[0] = blockOffset;
        for (int i = 1; i < blockCount; i++) {
          int delta = in.readZInt();
          int nextBlockSize = lastBlockSize + delta;
          blockOffset += nextBlockSize;
          blockOffsets[i] = blockOffset;
          lastBlockSize = nextBlockSize;
        }
        blockOffsets[blockCount] = blockDeltaFooterOffset;

        NodesEntry entry;
        if (sharedEntry != null) {
          // refCount already incremented by openInput before the constructor call
          if (sharedEntry.nodes.length() != blockCount) {
            throw new IllegalArgumentException("block count mismatch");
          }
          entry = sharedEntry;
        } else {
          // sharedEntry==null: either no pendingNodes entry, or a race — putIfAbsent decides winner
          AtomicLongArray localAccessMapped = new AtomicLongArray(blockCount);
          NodesEntry newEntry = new NodesEntry(localAccessMapped); // refCount=1 (for map)
          NodesEntry existing;
          synchronized (pendingNodes) {
            existing = pendingNodes.putIfAbsent(source.getFileName().toString(), newEntry);
          }
          if (existing == null) {
            newEntry.acquire(); // +1 for this reader
            entry = newEntry;
          } else {
            existing.acquire(); // +1 for this reader
            entry = existing;
          }
        }

        return new RootParams(length, blockOffsets, compressed, entry.nodes, entry, blobUUID);
      }
    }

    private AD2IndexInput(
        String description, AccessDirectory2 dir, RootParams p, Boolean logicalRoot) {
      super(
          description,
          dir.cache,
          p.blobUUID,
          p.length,
          p.blockOffsets,
          new ByteBufferGuard("ad2-decompressed", unmapHack()),
          p.accessMapped,
          logicalRoot);
      this.ioExec = dir.ioExec;
      this.compressedGuard = new ByteBufferGuard("ad2-compressed", unmapHack());
      this.compressed = p.compressed;
      long[] addrs = new long[p.compressed.length];
      for (int i = 0; i < addrs.length; i++) {
        addrs[i] = dir.cache.baseAddress(p.compressed[i]);
      }
      this.compressedBaseAddresses = addrs;
      this.isRoot = true;
      this.nodesEntry = p.entry;
      this.supplyLock = new StampedLock();
      blockSupplier =
          (blockOffset, compressedLen, decompressedLen) -> {
            long stamp = supplyLock.tryReadLock();
            if (stamp == 0) throw new IOException("compressed buffers closed: " + this);
            try {
              return supplyFromBuffers(
                  compressed, compressedGuard, blockOffset, compressedLen, decompressedLen);
            } finally {
              supplyLock.unlockRead(stamp);
            }
          };
    }

    // -------------------------------------------------------------------------
    // Slice / clone constructor
    // -------------------------------------------------------------------------

    private AD2IndexInput(
        String description, AD2IndexInput parent, long sliceOffset, long sliceLen) {
      super(
          description,
          parent,
          sliceOffset,
          sliceLen,
          parent.logicalRoot == null ? Boolean.TRUE : Boolean.FALSE);
      this.ioExec = parent.ioExec;
      this.compressedGuard = parent.compressedGuard;
      this.compressed = parent.compressed;
      this.compressedBaseAddresses = parent.compressedBaseAddresses;
      this.isRoot = false;
      this.nodesEntry = null;
      this.supplyLock = parent.supplyLock;
      blockSupplier = parent.blockSupplier;
    }

    // -------------------------------------------------------------------------
    // CachedCompressedIndexInput abstract method implementations
    // -------------------------------------------------------------------------

    @Override
    @SuppressWarnings("ReferenceEquality")
    protected byte[] supply(int blockIdx, long blockOffset, int compressedLen, int decompressedLen)
        throws IOException {
      if (logicalRoot == Boolean.FALSE && seqAccessCount > 0) {
        int batchSize = Math.min(seqAccessCount, MAX_READ_AHEAD);
        int target = Math.min(blockIdx + batchSize, sliceLastBlockIdx);
        if (target > readAheadTo) {
          // Expand the readahead window from readAheadTo outward.
          // Stop at the first pinnable block (its compressed data won't be read).
          int hintFrom = readAheadTo + 1;
          int hintTo = -1;
          int newReadAheadTo = target;
          AtomicLongArray am = accessMapped;
          for (int i = hintFrom; i <= target; i++) {
            long extant = am.get(i);
            if (extant != BlockCache.NULL_HANDLE && cache.pinnable(extant)) {
              newReadAheadTo = i;
              break;
            }
            hintTo = i;
          }
          if (hintTo >= hintFrom) {
            hintCompressedRange(hintFrom, hintTo);
          }
          readAheadTo = newReadAheadTo;
        }
      } else {
        // No readahead, but still hint the current block so the kernel reads its
        // full compressed range in one I/O rather than demand-faulting page by page.
        hintCompressedRange(blockIdx, blockIdx);
      }
      return supplyFromBuffers(
          compressed, compressedGuard, blockOffset, compressedLen, decompressedLen);
    }

    /** Issues MADV_WILLNEED on compressed data for blocks [fromIdx, toIdx] inclusive. */
    private void hintCompressedRange(int fromIdx, int toIdx) {
      long from = blockOffsets[fromIdx];
      long to = blockOffsets[toIdx + 1];
      int regionIdx = (int) (from >> MAX_MAP_SHIFT);
      int regionEnd = (int) (to >> MAX_MAP_SHIFT);
      if (regionIdx == regionEnd) {
        long addr = compressedBaseAddresses[regionIdx] + (from & MAX_MAP_MASK);
        cache.willneed(addr, to - from);
      } else {
        long pos = from;
        for (int r = regionIdx; r <= regionEnd; r++) {
          long regionStart = (long) r << MAX_MAP_SHIFT;
          long segStart = Math.max(pos, regionStart);
          long segEnd = Math.min(to, regionStart + MAX_MAP_SIZE);
          long addr = compressedBaseAddresses[r] + (segStart & MAX_MAP_MASK);
          cache.willneed(addr, segEnd - segStart);
          pos = segEnd;
        }
      }
    }

    /**
     * Loads block {@code idx} of {@code in} into the cache if not already present. Returns {@code
     * false} if the cache is full (caller should stop issuing further loads); {@code true}
     * otherwise.
     */
    private static boolean loadBlock(AD2IndexInput in, int idx) throws IOException {
      long extant = in.accessMapped.get(idx);
      if (extant != BlockCache.NULL_HANDLE && in.cache.pinnable(extant)) return true;
      long[] nodeHandle = new long[1];
      BlockCache.Val toPopulateVal = in.cache.acquireNode(nodeHandle, in.blobUUID, idx);
      if (toPopulateVal == null) return false;
      long toPopulate = nodeHandle[0];
      if (in.accessMapped.compareAndSet(idx, extant, toPopulate)) {
        if (toPopulateVal.isPopulated()) {
          in.cache.recordWarmStartHit();
        } else {
          long blockOffset = in.blockOffsets[idx];
          int compressedLen = (int) (in.blockOffsets[idx + 1] - blockOffset);
          BlockPreloader.populateBuf(
              blockOffset,
              compressedLen,
              idx,
              in.decompressedLenFor(idx),
              toPopulate,
              toPopulateVal,
              in.accessMapped,
              in.cache,
              in.blockSupplier,
              in.blobUUID);
        }
        in.cache.unpin(toPopulate, false);
      } else {
        in.cache.close(toPopulate, toPopulateVal);
      }
      return true;
    }

    private static byte[] supplyFromBuffers(
        ByteBuffer[] bufs,
        ByteBufferGuard compressedGuard,
        long blockOffset,
        int compressedLen,
        int decompressedLen)
        throws IOException {
      final byte[] preBuffer = new byte[compressedLen];
      final byte[] decompressBuffer = new byte[decompressedLen + 7]; // +7 for decompressor headroom
      ByteBuffer bb =
          bufs[(int) (blockOffset >> MAX_MAP_SHIFT)]
              .duplicate()
              .position((int) (blockOffset & MAX_MAP_MASK));
      int readOffset = 0;
      int left = bb.remaining();
      int toRead = compressedLen;
      while (left < toRead) {
        compressedGuard.getBytes(bb, preBuffer, readOffset, left);
        toRead -= left;
        readOffset += left;
        blockOffset += left;
        bb =
            bufs[(int) (blockOffset >> MAX_MAP_SHIFT)]
                .duplicate()
                .position((int) (blockOffset & MAX_MAP_MASK));
        left = bb.remaining();
      }
      compressedGuard.getBytes(bb, preBuffer, readOffset, toRead);
      CompressingDirectory.decompress(preBuffer, 0, decompressedLen, decompressBuffer, 0);
      return decompressBuffer;
    }

    @Override
    protected AD2IndexInput cloneSlice(String description, long sliceOffset, long sliceLen) {
      return new AD2IndexInput(description, this, sliceOffset, sliceLen);
    }

    @Override
    protected ByteBuffer doClose() throws IOException {
      if (!isRoot) return null;
      if (nodesEntry != null) {
        nodesEntry.release(cache);
      }
      // Acquire exclusive lock and never release — drains in-flight supplyFromBuffers calls,
      // and all subsequent tryReadLock() calls return 0, preventing access to freed memory.
      supplyLock.writeLock();
      compressedGuard.invalidateAndUnmap(compressed);
      return null;
    }
  }

  // ---------------------------------------------------------------------------
  // Write-through output: captures uncompressed blocks into the BlockCache at write time.
  // ---------------------------------------------------------------------------

  /**
   * An {@link IndexOutput} that forwards all writes to a delegate (the {@link CompressingDirectory}
   * output) while simultaneously capturing each {@link
   * CompressingDirectory#COMPRESSION_BLOCK_SIZE}-byte chunk into a {@link BlockCache.Val}. When
   * closed, the collected nodes are handed to {@link AccessDirectory2#storeNodes} so that the first
   * {@link #openInput} call on this file finds a warm cache.
   *
   * <p>This avoids writing an uncompressed copy to the access-path {@link MMapDirectory}.
   */
  static final class WriteThroughOutput extends IndexOutput {

    private final IndexOutput delegate;
    private final AccessDirectory2 dir;
    private final String name;
    private final byte[] blockBuf = new byte[COMPRESSION_BLOCK_SIZE];
    private int blockBufPos = 0;
    private final LongArrayList nodeSlots = new LongArrayList();
    private final CRC32 crc = new CRC32();

    WriteThroughOutput(String name, IndexOutput delegate, AccessDirectory2 dir) {
      super("WriteThroughOutput(" + name + ")", name);
      this.delegate = delegate;
      this.dir = dir;
      this.name = name;
    }

    @Override
    public void writeByte(byte b) throws IOException {
      blockBuf[blockBufPos++] = b;
      if (blockBufPos == COMPRESSION_BLOCK_SIZE) {
        captureBlock(COMPRESSION_BLOCK_SIZE);
      }
      crc.update(b);
      delegate.writeByte(b);
    }

    @Override
    public void writeBytes(byte[] src, int offset, int len) throws IOException {
      int remaining = len;
      int srcOff = offset;
      while (remaining > 0) {
        int space = COMPRESSION_BLOCK_SIZE - blockBufPos;
        int copy = Math.min(space, remaining);
        System.arraycopy(src, srcOff, blockBuf, blockBufPos, copy);
        blockBufPos += copy;
        srcOff += copy;
        remaining -= copy;
        if (blockBufPos == COMPRESSION_BLOCK_SIZE) {
          captureBlock(COMPRESSION_BLOCK_SIZE);
        }
      }
      crc.update(src, offset, len);
      delegate.writeBytes(src, offset, len);
    }

    private void captureBlock(int len) {
      long[] nodeHandle = new long[1];
      BlockCache.Val nodeVal = dir.cache.acquireNode(nodeHandle);
      long node = nodeHandle[0];
      if (nodeVal != null) {
        try {
          nodeVal.populate(blockBuf, 0, len, dir.uuidForFile(name), nodeSlots.size(), dir.cache);
        } finally {
          dir.cache.unpin(node, false);
        }
        dir.cache.recordPrepopulated();
      }
      nodeSlots.add(node); // NULL_HANDLE = cache exhausted; cold on first read
      blockBufPos = 0;
    }

    @Override
    public long getFilePointer() {
      return delegate.getFilePointer();
    }

    @Override
    public long getChecksum() throws IOException {
      return crc.getValue();
    }

    @Override
    public void close() throws IOException {
      try (delegate) {
        if (blockBufPos > 0) {
          captureBlock(blockBufPos); // partial last block
        }
        AtomicLongArray sharedAccessMapped = new AtomicLongArray(nodeSlots.size());
        for (int i = 0; i < nodeSlots.size(); i++) {
          sharedAccessMapped.set(i, nodeSlots.get(i));
        }
        dir.storeNodes(name, sharedAccessMapped);
      }
    }
  }
}
