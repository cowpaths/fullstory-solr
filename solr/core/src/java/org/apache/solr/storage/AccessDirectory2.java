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
import static org.apache.solr.storage.CompressingDirectory.DirectIOIndexOutput.HEADER_SIZE;
import static org.apache.solr.storage.CompressingDirectory.readLengthFromHeader;

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
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
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
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Path compressedPath;
  final BlockCache cache;

  /**
   * Shared {@code accessMapped} arrays pre-populated by {@link WriteThroughOutput} at write-close
   * time. All root {@link LazyLoadInput} instances opened for the same file share the same array,
   * so they all see the same cached blocks. Entries are removed (and nodes released) in {@link
   * #deleteFile} or {@link #rename}.
   */
  private final HashMap<String, AtomicReferenceArray<BlockCache.Node>> pendingNodes =
      new HashMap<>();

  public AccessDirectory2(Path path, LockFactory lockFactory, Path compressedPath, BlockCache cache)
      throws IOException {
    super(path, lockFactory);
    this.compressedPath = compressedPath;
    this.cache = cache;
  }

  /** Stores the shared {@code accessMapped} array for {@code name}, pre-populated by the writer. */
  void storeNodes(String name, AtomicReferenceArray<BlockCache.Node> sharedAccessMapped) {
    synchronized (pendingNodes) {
      pendingNodes.put(name, sharedAccessMapped);
    }
  }

  @Override
  public void deleteFile(String name) throws IOException {
    AtomicReferenceArray<BlockCache.Node> stale;
    synchronized (pendingNodes) {
      stale = pendingNodes.remove(name);
    }
    if (stale != null) {
      for (int i = 0; i < stale.length(); i++) {
        BlockCache.Node node = stale.getAndSet(i, null);
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
      AtomicReferenceArray<BlockCache.Node> nodes = pendingNodes.remove(source);
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
    return readLengthFromHeader(compressedPath.resolve(name));
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    if (name.endsWith(".tmp")) {
      return super.openInput(name, context);
    }
    AtomicReferenceArray<BlockCache.Node> sharedAccessMapped;
    synchronized (pendingNodes) {
      sharedAccessMapped = pendingNodes.get(name); // get, not remove: shared across all root opens
    }
    return new LazyLoadInput(compressedPath.resolve(name), cache, sharedAccessMapped, pendingNodes);
  }

  private static ByteBufferGuard.BufferCleaner unmapHack() {
    Object hack = MappedByteBufferIndexInputProvider.unmapHackImpl();
    if (hack instanceof ByteBufferGuard.BufferCleaner) {
      return (ByteBufferGuard.BufferCleaner) hack;
    } else {
      throw new UnsupportedOperationException("unmap not available");
    }
  }

  private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);

  static final class LazyLoadInput extends IndexInput implements RandomAccessInput {

    private final ByteBufferGuard compressedGuard;
    private final boolean isClone;
    private final long length;
    private final long[] blockOffsets;
    private final int blockCount;
    private final int lastBlockIdx;
    private final int lastBlockDecompressedLen;
    private ByteBuffer[] mapped;

    private final AtomicReferenceArray<BlockCache.Node> accessMapped;

    private final BlockCache cache;

    private final long offset;
    private final long sliceLength;

    private long seekPos = -1;
    private long filePointer = 0;
    private ByteBuffer postBuffer = EMPTY;
    private int postBufferBaseline;
    private int currentBlockIdx = -1;
    private BlockCache.Node currentNode;

    private static final AtomicReferenceArray<BlockCache.Node> EMPTY_ACCESS_MAPPED =
        new AtomicReferenceArray<>(0);

    private static final LongBuffer EMPTY_LONGBUFFER = LongBuffer.allocate(0);
    private static final IntBuffer EMPTY_INTBUFFER = IntBuffer.allocate(0);
    private static final FloatBuffer EMPTY_FLOATBUFFER = FloatBuffer.allocate(0);
    private LongBuffer[] longViews;
    private IntBuffer[] intViews;
    private FloatBuffer[] floatViews;

    // ---------------------------------------------------------------------------
    // Root constructor: opens and mmaps the compressed source file.
    // ---------------------------------------------------------------------------

    LazyLoadInput(
        Path source,
        BlockCache cache,
        AtomicReferenceArray<BlockCache.Node> sharedAccessMapped,
        HashMap<String, AtomicReferenceArray<BlockCache.Node>> pendingNodes)
        throws IOException {
      super("lazy:" + source);
      this.cache = cache;
      this.compressedGuard = new ByteBufferGuard("compressedGuard", unmapHack());
      this.isClone = false;

      try (FileChannel channel = FileChannel.open(source, StandardOpenOption.READ)) {
        long compressedFileSize = channel.size();
        mapped = new ByteBuffer[Math.toIntExact(((compressedFileSize - 1) >> MAX_MAP_SHIFT) + 1)];
        long pos = 0;
        long limit = MAX_MAP_SIZE;
        for (int i = 0, lim = mapped.length; i < lim; i++) {
          int size = (int) (Math.min(limit, compressedFileSize) - pos);
          mapped[i] = channel.map(FileChannel.MapMode.READ_ONLY, pos, size);
          pos = limit;
          limit += MAX_MAP_SIZE;
        }

        long size = channel.size();
        if (size == 0) {
          length = 0;
          blockOffsets = null;
          blockCount = 0;
          lastBlockIdx = -1;
          lastBlockDecompressedLen = 0;
          accessMapped = EMPTY_ACCESS_MAPPED;
        } else {
          ByteBuffer initial = mapped[0];
          length = initial.getLong(0);
          if (length >> COMPRESSION_BLOCK_SHIFT > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                "file too long " + Long.toHexString(length) + ", " + source);
          }
          int blockDeltaFooterSize = initial.getInt(Long.BYTES);
          int cTypeId = initial.get(HEADER_SIZE - Integer.BYTES) & 0xff;
          if (cTypeId != CompressingDirectory.COMPRESSION_TYPE.id) {
            throw new IllegalArgumentException("unrecognized compression type id: " + cTypeId);
          }
          int cBlockTypeId = initial.get(HEADER_SIZE - Integer.BYTES + 1) & 0xff;
          if (cBlockTypeId != CompressingDirectory.COMPRESSION_TYPE.id) {
            throw new IllegalArgumentException(
                "unrecognized compression block type id: " + cBlockTypeId);
          }
          byte[] footer = new byte[blockDeltaFooterSize];
          long blockDeltaFooterOffset = size - blockDeltaFooterSize;
          channel.read(ByteBuffer.wrap(footer), blockDeltaFooterOffset);
          ByteArrayDataInput in = new ByteArrayDataInput(footer);

          long blockOffset = HEADER_SIZE;
          int lastBlockSize = BLOCK_SIZE_ESTIMATE;
          blockCount = (int) (((length - 1) >> COMPRESSION_BLOCK_SHIFT) + 1);
          blockOffsets = new long[blockCount + 1];
          lastBlockIdx = blockCount - 1;
          lastBlockDecompressedLen = (int) (((length - 1) & COMPRESSION_BLOCK_MASK_LOW) + 1);
          blockOffsets[0] = blockOffset;
          for (int i = 1; i < blockCount; i++) {
            int delta = in.readZInt();
            int nextBlockSize = lastBlockSize + delta;
            blockOffset += nextBlockSize;
            blockOffsets[i] = blockOffset;
            lastBlockSize = nextBlockSize;
          }
          blockOffsets[blockCount] = blockDeltaFooterOffset;

          if (sharedAccessMapped != null) {
            if (sharedAccessMapped.length() != blockCount) {
              throw new IllegalArgumentException("block count mismatch");
            }
            // Re-use the shared array from WriteThroughOutput: nodes are already unpinned
            // (evictable), and all root opens of this file share the same slots.
            this.accessMapped = sharedAccessMapped;
          } else {
            AtomicReferenceArray<BlockCache.Node> localAccessMapped =
                new AtomicReferenceArray<>(blockCount);
            AtomicReferenceArray<BlockCache.Node> existing;
            synchronized (pendingNodes) {
              existing =
                  pendingNodes.putIfAbsent(source.getFileName().toString(), localAccessMapped);
            }
            this.accessMapped = existing == null ? localAccessMapped : existing;
          }
        }
      }

      this.offset = 0;
      this.sliceLength = length;
    }

    // ---------------------------------------------------------------------------
    // Clone / slice constructor: shares accessMapped and metadata with parent.
    // ---------------------------------------------------------------------------

    private LazyLoadInput(
        String resourceDescription, LazyLoadInput parent, long offset, long length) {
      super(resourceDescription);
      this.cache = parent.cache;
      this.compressedGuard = parent.compressedGuard;
      this.isClone = true;
      this.length = parent.length;
      this.blockOffsets = parent.blockOffsets;
      this.blockCount = parent.blockCount;
      this.lastBlockIdx = parent.lastBlockIdx;
      this.lastBlockDecompressedLen = parent.lastBlockDecompressedLen;
      this.accessMapped = parent.accessMapped;
      this.offset = parent.offset + offset;
      this.seekPos = this.offset;
      this.sliceLength = length;
      ByteBuffer[] parentMapped = parent.mapped;
      this.mapped = new ByteBuffer[parentMapped.length];
      for (int i = parentMapped.length - 1; i >= 0; i--) {
        mapped[i] = parentMapped[i].duplicate();
      }
    }

    // ---------------------------------------------------------------------------
    // Cache interaction helpers
    // ---------------------------------------------------------------------------

    private void unpinCurrent() {
      if (currentNode != null) {
        cache.unpin(currentNode);
        currentNode = null;
      }
    }

    // ---------------------------------------------------------------------------
    // Block navigation
    // ---------------------------------------------------------------------------

    private void actualSeek(final long pos) throws IOException {
      filePointer = pos;
      final int blockIdx = (int) (pos >> COMPRESSION_BLOCK_SHIFT);
      if (blockIdx != currentBlockIdx) {
        initBlock(blockIdx);
      }
      postBuffer.position(postBufferBaseline + (int) (pos & COMPRESSION_BLOCK_MASK_LOW));
    }

    private void initBlock(int blockIdx) throws IOException {
      // NOTE: keep this small so it can be inlined.
      if (blockIdx > lastBlockIdx) {
        throw new EOFException();
      }
      final long blockOffset = blockOffsets[blockIdx];
      final int compressedLen = (int) (blockOffsets[blockIdx + 1] - blockOffset);
      refill(blockOffset, compressedLen, blockIdx);
    }

    private void refill() throws IOException {
      final int blockIdx = currentBlockIdx + 1;
      if (blockIdx > lastBlockIdx) {
        throw new EOFException();
      }
      final long blockOffset = blockOffsets[blockIdx];
      final int compressedLen = (int) (blockOffsets[blockIdx + 1] - blockOffset);
      refill(blockOffset, compressedLen, blockIdx);
    }

    /**
     * Reads {@code compressedLen} bytes from the compressed mmap at {@code pos}, decompresses them,
     * and returns a heap ByteBuffer containing the decompressed block.
     */
    private ByteBuffer supply(long pos, int compressedLen, int decompressedLen) throws IOException {
      final byte[] preBuffer = new byte[compressedLen];
      final byte[] decompressBuffer = new byte[decompressedLen + 7]; // +7 for decompressor headroom
      ByteBuffer bb = mapped[(int) (pos >> MAX_MAP_SHIFT)].position((int) (pos & MAX_MAP_MASK));
      int readOffset = 0;
      int left = bb.remaining();
      int toRead = compressedLen;
      while (left < toRead) {
        compressedGuard.getBytes(bb, preBuffer, readOffset, left);
        toRead -= left;
        readOffset += left;
        pos += left;
        bb = mapped[(int) (pos >> MAX_MAP_SHIFT)].position((int) (pos & MAX_MAP_MASK));
        left = bb.remaining();
      }
      compressedGuard.getBytes(bb, preBuffer, readOffset, toRead);
      CompressingDirectory.decompress(preBuffer, 0, decompressedLen, decompressBuffer, 0);
      return ByteBuffer.wrap(decompressBuffer, 0, decompressedLen);
    }

    /**
     * Loads block {@code blockIdx} into {@code postBuffer}, either from the cache (cache hit), a
     * freshly decompressed block placed into the cache (cache miss + cache), or an uncached heap
     * buffer (cache exhausted).
     *
     * <p>On a cache miss, the node is published to {@code accessMapped} while still holding the
     * acquire pin (refCount=1). Concurrent readers who find the node via {@code accessMapped} will
     * call {@link BlockCache#pin}, incrementing the refCount further. The pin is held as {@code
     * currentNode} until the block changes or this input is closed.
     */
    private void refill(final long pos, final int compressedLen, final int blockIdx)
        throws IOException {
      final int decompressedLen =
          blockIdx == lastBlockIdx ? lastBlockDecompressedLen : COMPRESSION_BLOCK_SIZE;
      unpinCurrent();

      // --- Cache hit: try to re-pin existing node ---
      final BlockCache.Node cached = accessMapped.get(blockIdx);
      if (cached != null && cache.pin(cached)) {
        currentNode = cached;
        postBuffer = cached.join().duplicate().order(ByteOrder.LITTLE_ENDIAN);
        postBuffer.clear().limit(decompressedLen);
        postBufferBaseline = 0;
        currentBlockIdx = blockIdx;
        longViews = null;
        intViews = null;
        floatViews = null;
        return;
      }

      // --- Cache miss: decompress ---
      final ByteBuffer heapBuf = supply(pos, compressedLen, decompressedLen);

      // --- Try to cache the decompressed block ---
      final BlockCache.Node node = cache.acquireNode();
      if (node != null) {
        // Publish while still holding the acquire pin (refCount=1); readers who find the node via
        // accessMapped will call pin(), which correctly increments from 1.
        ByteBuffer buf =
            node.populate(
                heapBuf.array(), heapBuf.arrayOffset() + heapBuf.position(), decompressedLen);
        accessMapped.set(blockIdx, node);
        currentNode = node;
        postBuffer = buf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        postBuffer.clear().limit(decompressedLen);
        postBufferBaseline = 0;
        currentBlockIdx = blockIdx;
        longViews = null;
        intViews = null;
        floatViews = null;
        return;
      }

      // --- Serve uncached from heap buffer ---
      currentNode = null;
      postBuffer = heapBuf;
      postBufferBaseline = heapBuf.position(); // = 0
      heapBuf.order(ByteOrder.LITTLE_ENDIAN);
      currentBlockIdx = blockIdx;
      longViews = null;
      intViews = null;
      floatViews = null;
    }

    // ---------------------------------------------------------------------------
    // RandomAccessInput
    // ---------------------------------------------------------------------------

    @Override
    public byte readByte(final long pos) throws IOException {
      final long absolutePos = pos + offset;
      final int blockIdx = (int) (absolutePos >> COMPRESSION_BLOCK_SHIFT);
      if (blockIdx != currentBlockIdx) {
        initBlock(blockIdx);
      }
      return postBuffer.get(postBufferBaseline + (int) (absolutePos & COMPRESSION_BLOCK_MASK_LOW));
    }

    @Override
    public short readShort(final long pos) throws IOException {
      final long absolutePos = pos + offset;
      final int blockIdx = (int) (absolutePos >> COMPRESSION_BLOCK_SHIFT);
      if (blockIdx != currentBlockIdx) {
        initBlock(blockIdx);
      }
      final int limit = postBuffer.limit();
      final int localPos = postBufferBaseline + (int) (absolutePos & COMPRESSION_BLOCK_MASK_LOW);
      final int remaining = limit - localPos;
      if (remaining < Short.BYTES) {
        return slowReadShort(remaining, localPos);
      } else {
        return postBuffer.getShort(localPos);
      }
    }

    @Override
    public int readInt(final long pos) throws IOException {
      final long absolutePos = pos + offset;
      final int blockIdx = (int) (absolutePos >> COMPRESSION_BLOCK_SHIFT);
      if (blockIdx != currentBlockIdx) {
        initBlock(blockIdx);
      }
      final int limit = postBuffer.limit();
      final int localPos = postBufferBaseline + (int) (absolutePos & COMPRESSION_BLOCK_MASK_LOW);
      final int remaining = limit - localPos;
      if (remaining < Integer.BYTES) {
        return slowReadInt(remaining, localPos);
      } else {
        return postBuffer.getInt(localPos);
      }
    }

    @Override
    public long readLong(final long pos) throws IOException {
      final long absolutePos = pos + offset;
      final int blockIdx = (int) (absolutePos >> COMPRESSION_BLOCK_SHIFT);
      if (blockIdx != currentBlockIdx) {
        initBlock(blockIdx);
      }
      final int limit = postBuffer.limit();
      final int localPos = postBufferBaseline + (int) (absolutePos & COMPRESSION_BLOCK_MASK_LOW);
      final int remaining = limit - localPos;
      if (remaining < Long.BYTES) {
        return slowReadLong(remaining, localPos);
      } else {
        return postBuffer.getLong(localPos);
      }
    }

    // ---------------------------------------------------------------------------
    // Sequential IndexInput reads
    // ---------------------------------------------------------------------------

    @Override
    public byte readByte() throws IOException {
      final long pos = seekPos;
      if (pos != -1) {
        seekPos = -1;
        actualSeek(pos);
      }
      return _readByte(postBuffer.hasRemaining() ? 1 : 0);
    }

    private byte _readByte(final int remaining) throws IOException {
      if (remaining == 0) {
        refill();
      }
      filePointer++;
      return postBuffer.get();
    }

    @Override
    public void readBytes(byte[] dst, int offset, int len) throws IOException {
      final long pos = seekPos;
      if (pos != -1) {
        seekPos = -1;
        actualSeek(pos);
      }
      final int left = postBuffer.remaining();
      filePointer += len;
      if (left < len) {
        slowReadBytes(dst, offset, len, left);
      } else {
        postBuffer.get(dst, offset, len);
      }
    }

    private void slowReadBytes(final byte[] dst, int offset, int toRead, int left)
        throws IOException {
      do {
        postBuffer.get(dst, offset, left);
        toRead -= left;
        offset += left;
        refill();
        left = postBuffer.remaining();
      } while (left < toRead);
      postBuffer.get(dst, offset, toRead);
    }

    @Override
    public short readShort() throws IOException {
      final long pos = seekPos;
      if (pos != -1) {
        seekPos = -1;
        actualSeek(pos);
      }
      final int remaining = postBuffer.remaining();
      if (remaining < Short.BYTES) {
        return slowReadShort(remaining);
      } else {
        filePointer += Short.BYTES;
        return postBuffer.getShort();
      }
    }

    private short slowReadShort(final int remaining) throws IOException {
      final byte b1 = _readByte(remaining);
      final byte b2 = _readByte(remaining - 1);
      return (short) (((b2 & 0xFF) << 8) | (b1 & 0xFF));
    }

    private short slowReadShort(final int remaining, final int pos) throws IOException {
      assert remaining == 1;
      final byte b1 = postBuffer.get(pos);
      refill();
      final byte b2 = postBuffer.get(postBufferBaseline);
      return (short) (((b2 & 0xFF) << 8) | (b1 & 0xFF));
    }

    @Override
    public int readInt() throws IOException {
      final long pos = seekPos;
      if (pos != -1) {
        seekPos = -1;
        actualSeek(pos);
      }
      final int remaining = postBuffer.remaining();
      if (remaining < Integer.BYTES) {
        return slowReadInt(remaining);
      } else {
        filePointer += Integer.BYTES;
        return postBuffer.getInt();
      }
    }

    private int _readInt(final int remaining) throws IOException {
      if (remaining < Integer.BYTES) {
        return slowReadInt(remaining);
      } else {
        filePointer += Integer.BYTES;
        return postBuffer.getInt();
      }
    }

    private int slowReadInt(int remaining) throws IOException {
      final byte b1 = _readByte(remaining);
      final byte b2 = _readByte(--remaining);
      final byte b3 = _readByte(--remaining);
      final byte b4 = _readByte(--remaining);
      return ((b4 & 0xFF) << 24) | ((b3 & 0xFF) << 16) | ((b2 & 0xFF) << 8) | (b1 & 0xFF);
    }

    private int _readInt(final int remaining, final int pos) throws IOException {
      if (remaining < Integer.BYTES) {
        return slowReadInt(remaining, pos);
      } else {
        return postBuffer.getInt(pos);
      }
    }

    private int slowReadInt(int remaining, int pos) throws IOException {
      assert remaining > 0;
      final byte b1 = postBuffer.get(pos++);
      if (--remaining == 0) {
        refill();
        pos = postBufferBaseline;
      }
      final byte b2 = postBuffer.get(pos++);
      if (--remaining == 0) {
        refill();
        pos = postBufferBaseline;
      }
      final byte b3 = postBuffer.get(pos++);
      if (--remaining == 0) {
        refill();
        pos = postBufferBaseline;
      }
      final byte b4 = postBuffer.get(pos);
      return ((b4 & 0xFF) << 24) | ((b3 & 0xFF) << 16) | ((b2 & 0xFF) << 8) | (b1 & 0xFF);
    }

    @Override
    public long readLong() throws IOException {
      final long pos = seekPos;
      if (pos != -1) {
        seekPos = -1;
        actualSeek(pos);
      }
      final int remaining = postBuffer.remaining();
      if (remaining < Long.BYTES) {
        return (_readInt(remaining) & 0xFFFFFFFFL)
            | (((long) _readInt(postBuffer.remaining())) << 32);
      } else {
        filePointer += Long.BYTES;
        return postBuffer.getLong();
      }
    }

    public long _readLong(final int remaining) throws IOException {
      if (remaining < Long.BYTES) {
        return (_readInt(remaining) & 0xFFFFFFFFL)
            | (((long) _readInt(postBuffer.remaining())) << 32);
      } else {
        filePointer += Long.BYTES;
        return postBuffer.getLong();
      }
    }

    private long slowReadLong(final int remaining, final int pos) throws IOException {
      final long l1 = _readInt(remaining, pos);
      final long l2;
      if (remaining < Integer.BYTES) {
        // the first _readInt will have refilled the buffer, so we adjust here
        l2 = _readInt(Integer.BYTES, postBufferBaseline + (Integer.BYTES - remaining));
      } else if (remaining == Integer.BYTES) {
        // aligned, so we can refill directly
        refill();
        l2 = postBuffer.getInt(postBufferBaseline);
      } else {
        // the first _readInt will _not_ have refilled the buffer, so proceed normally
        l2 = _readInt(remaining - Integer.BYTES, pos + Integer.BYTES);
      }
      return (l1 & 0xFFFFFFFFL) | (l2 << 32);
    }

    @Override
    public int readVInt() throws IOException {
      final long pos = seekPos;
      if (pos != -1) {
        seekPos = -1;
        actualSeek(pos);
      }
      return _readVInt(postBuffer.remaining());
    }

    public int _readVInt(int remaining) throws IOException {
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
        // Warning: the next ands use 0x0F / 0xF0 - beware copy/paste errors:
        i |= (b & 0x0F) << 28;
        if ((b & 0xF0) == 0) return i;
      } else {
        b = postBuffer.get();
        filePointer++;
        if (b >= 0) return b;
        int i = b & 0x7F;
        b = postBuffer.get();
        filePointer++;
        i |= (b & 0x7F) << 7;
        if (b >= 0) return i;
        b = postBuffer.get();
        filePointer++;
        i |= (b & 0x7F) << 14;
        if (b >= 0) return i;
        b = postBuffer.get();
        filePointer++;
        i |= (b & 0x7F) << 21;
        if (b >= 0) return i;
        b = postBuffer.get();
        filePointer++;
        // Warning: the next ands use 0x0F / 0xF0 - beware copy/paste errors:
        i |= (b & 0x0F) << 28;
        if ((b & 0xF0) == 0) return i;
      }
      throw new IOException("Invalid vInt detected (too many bits)");
    }

    @Override
    public long readVLong() throws IOException {
      return readVLong(false);
    }

    @Override
    public long readZLong() throws IOException {
      return BitUtil.zigZagDecode(readVLong(true));
    }

    private long readVLong(final boolean allowNegative) throws IOException {
      final long pos = seekPos;
      if (pos != -1) {
        seekPos = -1;
        actualSeek(pos);
      }
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
        b = postBuffer.get();
        filePointer++;
        if (b >= 0) return b;
        i = b & 0x7FL;
        b = postBuffer.get();
        filePointer++;
        i |= (b & 0x7FL) << 7;
        if (b >= 0) return i;
        b = postBuffer.get();
        filePointer++;
        i |= (b & 0x7FL) << 14;
        if (b >= 0) return i;
        b = postBuffer.get();
        filePointer++;
        i |= (b & 0x7FL) << 21;
        if (b >= 0) return i;
        b = postBuffer.get();
        filePointer++;
        i |= (b & 0x7FL) << 28;
        if (b >= 0) return i;
        b = postBuffer.get();
        filePointer++;
        i |= (b & 0x7FL) << 35;
        if (b >= 0) return i;
        b = postBuffer.get();
        filePointer++;
        i |= (b & 0x7FL) << 42;
        if (b >= 0) return i;
        b = postBuffer.get();
        filePointer++;
        i |= (b & 0x7FL) << 49;
        if (b >= 0) return i;
        b = postBuffer.get();
        filePointer++;
        i |= (b & 0x7FL) << 56;
        if (b >= 0) return i;
        if (!allowNegative) {
          throw new IOException("Invalid vLong detected (negative values disallowed)");
        }
        b = postBuffer.get();
        filePointer++;
      }
      i |= (b & 0x7FL) << 63;
      if (b == 0 || b == 1) return i;
      throw new IOException("Invalid vLong detected (more than 64 bits)");
    }

    @Override
    public void readLongs(final long[] dst, final int offset, final int length) throws IOException {
      final long pos = seekPos;
      if (pos != -1) {
        seekPos = -1;
        actualSeek(pos);
      }
      if (longViews == null) {
        longViews = initLongViews();
      }
      final int remaining = postBuffer.remaining();
      final long bytesRequested = (long) length << 3;
      if (remaining < bytesRequested) {
        dst[offset] = _readLong(remaining);
        for (int i = 1; i < length; ++i) {
          dst[offset + i] = _readLong(postBuffer.remaining());
        }
      } else {
        final int position = postBuffer.position();
        longViews[position & 0x07].position(position >>> 3).get(dst, offset, length);
        filePointer += bytesRequested;
        postBuffer.position(position + (int) bytesRequested);
      }
    }

    @Override
    public void readInts(final int[] dst, final int offset, final int length) throws IOException {
      final long pos = seekPos;
      if (pos != -1) {
        seekPos = -1;
        actualSeek(pos);
      }
      if (intViews == null) {
        intViews = initIntViews();
      }
      final int remaining = postBuffer.remaining();
      final long bytesRequested = (long) length << 2;
      if (remaining < bytesRequested) {
        dst[offset] = _readInt(remaining);
        for (int i = 1; i < length; ++i) {
          dst[offset + i] = _readInt(postBuffer.remaining());
        }
      } else {
        final int position = postBuffer.position();
        intViews[position & 0x03].position(position >>> 2).get(dst, offset, length);
        filePointer += bytesRequested;
        postBuffer.position(position + (int) bytesRequested);
      }
    }

    @Override
    public void readFloats(final float[] dst, final int offset, final int length)
        throws IOException {
      final long pos = seekPos;
      if (pos != -1) {
        seekPos = -1;
        actualSeek(pos);
      }
      if (floatViews == null) {
        floatViews = initFloatViews();
      }
      final int remaining = postBuffer.remaining();
      final long bytesRequested = (long) length << 2;
      if (remaining < bytesRequested) {
        dst[offset] = Float.intBitsToFloat(_readInt(remaining));
        for (int i = 1; i < length; ++i) {
          dst[offset + i] = Float.intBitsToFloat(_readInt(postBuffer.remaining()));
        }
      } else {
        final int position = postBuffer.position();
        floatViews[position & 0x03].position(position >>> 2).get(dst, offset, length);
        filePointer += bytesRequested;
        postBuffer.position(position + (int) bytesRequested);
      }
    }

    private LongBuffer[] initLongViews() {
      final LongBuffer[] ret = new LongBuffer[Long.BYTES];
      final ByteBuffer template = postBuffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
      final int lim = postBuffer.limit();
      for (int i = Long.BYTES - 1; i >= 0; i--) {
        if (i < lim) {
          ret[i] = template.position(i).asLongBuffer();
        } else {
          ret[i] = EMPTY_LONGBUFFER;
        }
      }
      return ret;
    }

    private IntBuffer[] initIntViews() {
      final IntBuffer[] ret = new IntBuffer[Integer.BYTES];
      final ByteBuffer template = postBuffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
      final int lim = postBuffer.limit();
      for (int i = Integer.BYTES - 1; i >= 0; i--) {
        if (i < lim) {
          ret[i] = template.position(i).asIntBuffer();
        } else {
          ret[i] = EMPTY_INTBUFFER;
        }
      }
      return ret;
    }

    private FloatBuffer[] initFloatViews() {
      final FloatBuffer[] ret = new FloatBuffer[Float.BYTES];
      final ByteBuffer template = postBuffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
      final int lim = postBuffer.limit();
      for (int i = Integer.BYTES - 1; i >= 0; i--) {
        if (i < lim) {
          ret[i] = template.position(i).asFloatBuffer();
        } else {
          ret[i] = EMPTY_FLOATBUFFER;
        }
      }
      return ret;
    }

    @Override
    public String readString() throws IOException {
      final long pos = seekPos;
      if (pos != -1) {
        seekPos = -1;
        actualSeek(pos);
      }
      return _readString();
    }

    public String _readString() throws IOException {
      final int length = _readVInt(postBuffer.remaining());
      final byte[] bytes = new byte[length];
      final int left = postBuffer.remaining();
      filePointer += length;
      if (left < length) {
        slowReadBytes(bytes, 0, length, left);
      } else {
        postBuffer.get(bytes, 0, length);
      }
      return new String(bytes, 0, length, StandardCharsets.UTF_8);
    }

    @Override
    public Map<String, String> readMapOfStrings() throws IOException {
      final long pos = seekPos;
      if (pos != -1) {
        seekPos = -1;
        actualSeek(pos);
      }
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
    }

    @Override
    public Set<String> readSetOfStrings() throws IOException {
      final long pos = seekPos;
      if (pos != -1) {
        seekPos = -1;
        actualSeek(pos);
      }
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
    }

    // ---------------------------------------------------------------------------
    // Lifecycle
    // ---------------------------------------------------------------------------

    @Override
    public void close() throws IOException {
      try {
        if (mapped == null) return;
        final ByteBuffer[] bufs = mapped;
        unsetBuffers();

        unpinCurrent();

        if (isClone) return;

        // Slots belong to the shared accessMapped in pendingNodes; leave them intact for other
        // roots. deleteFile() is responsible for draining them when the file is truly gone.
        compressedGuard.invalidateAndUnmap(bufs);
      } finally {
        unsetBuffers();
      }
    }

    private void unsetBuffers() {
      mapped = null;
      currentBlockIdx = -1;
      postBuffer = null;
      floatViews = null;
      intViews = null;
      longViews = null;
    }

    @Override
    public long getFilePointer() {
      return (seekPos == -1 ? filePointer : seekPos) - offset;
    }

    @Override
    public void seek(final long pos) throws IOException {
      seekPos = offset + pos; // defer the actual seek
    }

    @Override
    public long length() {
      return sliceLength;
    }

    @Override
    public IndexInput clone() {
      try {
        IndexInput ret = new LazyLoadInput("clone", this, 0, sliceLength);
        ret.seek(getFilePointer());
        return ret;
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      return new LazyLoadInput(sliceDescription, this, offset, length);
    }
  }

  // ---------------------------------------------------------------------------
  // Write-through output: captures uncompressed blocks into the BlockCache at write time.
  // ---------------------------------------------------------------------------

  /**
   * An {@link IndexOutput} that forwards all writes to a delegate (the {@link CompressingDirectory}
   * output) while simultaneously capturing each {@link
   * CompressingDirectory#COMPRESSION_BLOCK_SIZE}-byte chunk into a {@link BlockCache.Node}. When
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
    private final ArrayList<BlockCache.Node> nodeSlots = new ArrayList<>();
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
      BlockCache.Node node = dir.cache.acquireNode();
      if (node != null) {
        node.populate(blockBuf, 0, len);
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
        AtomicReferenceArray<BlockCache.Node> sharedAccessMapped =
            new AtomicReferenceArray<>(nodeSlots.size());
        for (int i = 0; i < nodeSlots.size(); i++) {
          sharedAccessMapped.set(i, nodeSlots.get(i));
        }
        // Unpin all nodes so they are evictable (inserted at LRU head) before publishing.
        for (int i = 0; i < sharedAccessMapped.length(); i++) {
          BlockCache.Node node = sharedAccessMapped.get(i);
          if (node != null) dir.cache.unpin(node);
        }
        dir.storeNodes(name, sharedAccessMapped);
      }
    }
  }
}
