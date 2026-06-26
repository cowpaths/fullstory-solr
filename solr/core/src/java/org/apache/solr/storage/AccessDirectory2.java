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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.zip.CRC32;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBufferGuard;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MappedByteBufferIndexInputProvider;

/**
 * An {@link MMapDirectory} that serves reads from a {@link BlockCache}-backed decompression layer
 * when the underlying access file is absent. Compressed blocks are decompressed on demand and
 * cached in the shared {@link BlockCache}; cached blocks are pinned for the duration of each read
 * and evicted by the LRU when the pool is exhausted.
 */
public class AccessDirectory2 extends MMapDirectory {

  /**
   * Determines chunk size for mmapping files. {@code 1} yields 1 GiB chunks, but this may be set
   * higher to stress-test buffer boundaries (as low as 15, which yields the min chunk size of 64
   * KiB, equal to {@link CompressingDirectory#COMPRESSION_BLOCK_SIZE}).
   */
  private static final int MAP_BUF_DIVIDE_SHIFT = 1;

  public static final int MAX_MAP_SIZE = Integer.MIN_VALUE >>> MAP_BUF_DIVIDE_SHIFT;
  public static final int MAX_MAP_MASK = MAX_MAP_SIZE - 1;
  public static final int MAX_MAP_SHIFT = Integer.numberOfTrailingZeros(MAX_MAP_SIZE);

  private final Path compressedPath;
  final BlockCache cache;

  /**
   * Shared {@code accessMapped} arrays pre-populated by {@link WriteThroughOutput} at write-close
   * time. All root {@link AD2IndexInput} instances opened for the same file share the same array,
   * so they all see the same cached blocks. Entries are removed (and nodes released) in {@link
   * #deleteFile} or {@link #rename}.
   */
  private final HashMap<String, AtomicReferenceArray<Cache.Node<BlockCache.Val>>> pendingNodes =
      new HashMap<>();

  public AccessDirectory2(Path path, LockFactory lockFactory, Path compressedPath, BlockCache cache)
      throws IOException {
    super(path, lockFactory);
    this.compressedPath = compressedPath;
    this.cache = cache;
  }

  /** Stores the shared {@code accessMapped} array for {@code name}, pre-populated by the writer. */
  void storeNodes(
      String name, AtomicReferenceArray<Cache.Node<BlockCache.Val>> sharedAccessMapped) {
    synchronized (pendingNodes) {
      pendingNodes.put(name, sharedAccessMapped);
    }
  }

  @Override
  public void deleteFile(String name) throws IOException {
    AtomicReferenceArray<Cache.Node<BlockCache.Val>> stale;
    synchronized (pendingNodes) {
      stale = pendingNodes.remove(name);
    }
    if (stale != null) {
      for (int i = 0; i < stale.length(); i++) {
        Cache.Node<BlockCache.Val> node = stale.getAndSet(i, null);
        if (node != null) cache.close(node); // best-effort; no-op if pinned by active reader
      }
    }
    if (name.endsWith(".tmp")) {
      super.deleteFile(name);
    }
  }

  @Override
  public void rename(String source, String dest) throws IOException {
    synchronized (pendingNodes) {
      AtomicReferenceArray<Cache.Node<BlockCache.Val>> nodes = pendingNodes.remove(source);
      if (nodes != null) pendingNodes.put(dest, nodes);
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
    AtomicReferenceArray<Cache.Node<BlockCache.Val>> sharedAccessMapped;
    synchronized (pendingNodes) {
      sharedAccessMapped = pendingNodes.get(name); // get, not remove: shared across all root opens
    }
    return new AD2IndexInput(compressedPath.resolve(name), cache, sharedAccessMapped, pendingNodes);
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

    private final ByteBufferGuard compressedGuard;
    private final ByteBuffer[] compressed;
    private final boolean isRoot;

    // -------------------------------------------------------------------------
    // Root constructor (via this() delegation)
    // -------------------------------------------------------------------------

    AD2IndexInput(
        Path source,
        BlockCache cache,
        AtomicReferenceArray<Cache.Node<BlockCache.Val>> sharedAccessMapped,
        HashMap<String, AtomicReferenceArray<Cache.Node<BlockCache.Val>>> pendingNodes)
        throws IOException {
      this("lazy:" + source, cache, parseRootParams(source, sharedAccessMapped, pendingNodes));
    }

    private static final class RootParams {
      final long length;
      final long[] blockOffsets;
      final ByteBuffer[] compressed;
      final AtomicReferenceArray<Cache.Node<BlockCache.Val>> accessMapped;

      RootParams(
          long length,
          long[] blockOffsets,
          ByteBuffer[] compressed,
          AtomicReferenceArray<Cache.Node<BlockCache.Val>> accessMapped) {
        this.length = length;
        this.blockOffsets = blockOffsets;
        this.compressed = compressed;
        this.accessMapped = accessMapped;
      }
    }

    private static RootParams parseRootParams(
        Path source,
        AtomicReferenceArray<Cache.Node<BlockCache.Val>> sharedAccessMapped,
        HashMap<String, AtomicReferenceArray<Cache.Node<BlockCache.Val>>> pendingNodes)
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
          return new RootParams(0, null, compressed, new AtomicReferenceArray<>(0));
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

        AtomicReferenceArray<Cache.Node<BlockCache.Val>> accessMapped;
        if (sharedAccessMapped != null) {
          if (sharedAccessMapped.length() != blockCount) {
            throw new IllegalArgumentException("block count mismatch");
          }
          accessMapped = sharedAccessMapped;
        } else {
          AtomicReferenceArray<Cache.Node<BlockCache.Val>> localAccessMapped =
              new AtomicReferenceArray<>(blockCount);
          AtomicReferenceArray<Cache.Node<BlockCache.Val>> existing;
          synchronized (pendingNodes) {
            existing = pendingNodes.putIfAbsent(source.getFileName().toString(), localAccessMapped);
          }
          accessMapped = existing == null ? localAccessMapped : existing;
        }

        return new RootParams(length, blockOffsets, compressed, accessMapped);
      }
    }

    private AD2IndexInput(String description, BlockCache cache, RootParams p) {
      super(
          description,
          cache,
          p.length,
          p.blockOffsets,
          new ByteBufferGuard("ad2-decompressed", unmapHack()),
          p.accessMapped);
      this.compressedGuard = new ByteBufferGuard("ad2-compressed", unmapHack());
      this.compressed = p.compressed;
      this.isRoot = true;
    }

    // -------------------------------------------------------------------------
    // Slice / clone constructor
    // -------------------------------------------------------------------------

    private AD2IndexInput(
        String description, AD2IndexInput parent, long sliceOffset, long sliceLen) {
      super(description, parent, sliceOffset, sliceLen);
      this.compressedGuard = parent.compressedGuard;
      ByteBuffer[] parentCompressed = parent.compressed;
      ByteBuffer[] dup = new ByteBuffer[parentCompressed.length];
      for (int i = parentCompressed.length - 1; i >= 0; i--) {
        dup[i] = parentCompressed[i].duplicate();
      }
      this.compressed = dup;
      this.isRoot = false;
    }

    // -------------------------------------------------------------------------
    // CachedCompressedIndexInput abstract method implementations
    // -------------------------------------------------------------------------

    @Override
    protected ByteBuffer supply(
        int blockIdx, long blockOffset, int compressedLen, int decompressedLen) throws IOException {
      final byte[] preBuffer = new byte[compressedLen];
      final byte[] decompressBuffer = new byte[decompressedLen + 7]; // +7 for decompressor headroom
      ByteBuffer bb =
          compressed[(int) (blockOffset >> MAX_MAP_SHIFT)].position(
              (int) (blockOffset & MAX_MAP_MASK));
      int readOffset = 0;
      int left = bb.remaining();
      int toRead = compressedLen;
      while (left < toRead) {
        compressedGuard.getBytes(bb, preBuffer, readOffset, left);
        toRead -= left;
        readOffset += left;
        blockOffset += left;
        bb =
            compressed[(int) (blockOffset >> MAX_MAP_SHIFT)].position(
                (int) (blockOffset & MAX_MAP_MASK));
        left = bb.remaining();
      }
      compressedGuard.getBytes(bb, preBuffer, readOffset, toRead);
      CompressingDirectory.decompress(preBuffer, 0, decompressedLen, decompressBuffer, 0);
      return ByteBuffer.wrap(decompressBuffer, 0, decompressedLen);
    }

    @Override
    protected AD2IndexInput cloneSlice(String description, long sliceOffset, long sliceLen) {
      return new AD2IndexInput(description, this, sliceOffset, sliceLen);
    }

    @Override
    protected ByteBuffer doClose() throws IOException {
      if (!isRoot) return null;
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
    private final ArrayList<Cache.Node<BlockCache.Val>> nodeSlots = new ArrayList<>();
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
      Cache.Node<BlockCache.Val> node = dir.cache.acquireNode();
      if (node != null) {
        node.getPayload().populate(blockBuf, 0, len, dir.cache);
      }
      nodeSlots.add(node); // null = cache exhausted; cold on first read
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
        AtomicReferenceArray<Cache.Node<BlockCache.Val>> sharedAccessMapped =
            new AtomicReferenceArray<>(nodeSlots.size());
        for (int i = 0; i < nodeSlots.size(); i++) {
          sharedAccessMapped.set(i, nodeSlots.get(i));
        }
        // Unpin all nodes so they are evictable (inserted at LRU head) before publishing.
        for (int i = 0; i < sharedAccessMapped.length(); i++) {
          Cache.Node<BlockCache.Val> node = sharedAccessMapped.get(i);
          if (node != null) dir.cache.unpin(node);
        }
        dir.storeNodes(name, sharedAccessMapped);
      }
    }
  }
}
