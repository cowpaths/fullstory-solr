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

import static org.apache.solr.storage.CompressingDirectory.COMPRESSION_BLOCK_MASK_LOW;
import static org.apache.solr.storage.CompressingDirectory.COMPRESSION_BLOCK_SHIFT;
import static org.apache.solr.storage.CompressingDirectory.COMPRESSION_BLOCK_SIZE;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;
import org.apache.lucene.store.ByteBufferGuard;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base for {@link IndexInput} implementations backed by a {@link BlockCache} of
 * decompressed blocks. Subclasses supply compressed block bytes (via {@link #supply}) and handle
 * lifecycle management specific to their storage backend.
 *
 * <p>Reads are served from {@link #accessMapped}: a per-file {@link AtomicLongArray} of opaque
 * {@link BlockCache} handles keyed by block index. On a cache miss, {@link #supply} is invoked to
 * fetch and decompress the block, which is then inserted into the cache for subsequent hits.
 *
 * <p>Threading: each instance (root or slice) is accessed by at most one thread at a time via
 * {@link NodeRefStruct} local-pin/unpin. The {@link #accessMapped} array is shared across all
 * slices and clones; concurrent access is mediated by compare-and-exchange.
 */
abstract class CachedCompressedIndexInput extends IndexInput implements RandomAccessInput {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // Shared empty view sentinels for aligned bulk reads (readLongs/readInts/readFloats).
  private static final LongBuffer EMPTY_LONGBUFFER = LongBuffer.allocate(0);
  private static final IntBuffer EMPTY_INTBUFFER = IntBuffer.allocate(0);
  private static final FloatBuffer EMPTY_FLOATBUFFER = FloatBuffer.allocate(0);

  protected final BlockCache cache;
  // Total logical (decompressed) length of the underlying file.
  private final long length;
  // blockOffsets[i] = compressed byte offset of block i within the backend storage;
  // blockOffsets[blockCount] = total compressed size sentinel. Null for always-mapped inputs
  // (where all blocks are pre-pinned as tail nodes and no compressed reads occur).
  protected final long[] blockOffsets;
  private final int blockCount;
  private final int lastBlockIdx;
  private final int lastBlockDecompressedLen;
  // Guard for unmap; shared across all slices/clones so root.close() invalidates all in-flight
  // reads.
  private final ByteBufferGuard guard;
  // Shared per-file array; null'd on close.
  protected AtomicLongArray accessMapped;

  private final long offset; // absolute start offset of this slice within the file
  private final long sliceLength;
  private final int sliceFirstBlockIdx;
  private final int sliceLastBlockIdx;

  private long seekPos = -1;
  private ByteBuffer postBuffer = ByteBuffer.allocate(0);
  private int postBufferBaseline;
  private NodeRefStruct currentNodeRef;

  private LongBuffer[] longViews;
  private IntBuffer[] intViews;
  private FloatBuffer[] floatViews;

  static final int MAX_READ_AHEAD = 16;
  private int readAheadTo;

  // ---------------------------------------------------------------------------
  // Abstract methods
  // ---------------------------------------------------------------------------

  /**
   * Supply decompressed bytes for block {@code blockIdx}. Called on a cache miss (and for uncached
   * reads when the cache is full). Returns a {@link ByteBuffer} positioned at the start of the
   * decompressed content, with exactly {@code decompressedLen} bytes remaining.
   *
   * @param blockIdx the zero-based block index
   * @param blockOffset the compressed byte offset of this block within the backend storage
   * @param compressedLen the number of compressed bytes to read from the backend
   * @param decompressedLen the expected decompressed block size
   */
  protected abstract byte[] supply(
      int blockIdx, long blockOffset, int compressedLen, int decompressedLen) throws IOException;

  /**
   * Create a clone or slice of this input. The returned instance shares {@link #accessMapped} and
   * {@link #blockOffsets} with the parent but does not own the backend mapping.
   *
   * @param description the resource description for the new input
   * @param sliceOffset the offset within this input's logical file space
   * @param sliceLen the length of the slice
   */
  protected abstract CachedCompressedIndexInput cloneSlice(
      String description, long sliceOffset, long sliceLen);

  /**
   * Release backend-specific resources owned by this root input. Not called for slices (which do
   * not own the mapping). Called inside a {@code try-finally} by {@link #close()}; {@link
   * #unsetBuffers()} always executes in the {@code finally} block regardless of exceptions here.
   */
  protected abstract ByteBuffer doClose() throws IOException;

  // ---------------------------------------------------------------------------
  // Protected hook methods (no-op defaults)
  // ---------------------------------------------------------------------------

  /**
   * Returns the decompressed byte length for block {@code blockIdx}: {@link
   * CompressingDirectory#COMPRESSION_BLOCK_SIZE} for all blocks except the last, where it is the
   * remaining tail length.
   */
  protected final int decompressedLenFor(int blockIdx) {
    return blockIdx == lastBlockIdx ? lastBlockDecompressedLen : COMPRESSION_BLOCK_SIZE;
  }

  /**
   * Attempt to pre-load block {@code blockIdx} asynchronously. Returns {@code true} if loading was
   * initiated or the block is already present; {@code false} to signal that read-ahead should stop
   * (e.g., thread-pool or semaphore saturation). Default: no-op, always returns {@code false}.
   */
  protected boolean ensureBlockLoaded(int blockIdx) {
    return false;
  }

  protected final int sliceLastBlockIdx() {
    return sliceLastBlockIdx;
  }

  /**
   * Called when block 0 is first fetched from the backend (a cache-miss win on block 0). Intended
   * for segment-level read-ahead triggers. Default: no-op.
   */
  protected void onFirstBlockMiss() {}

  /**
   * Returns a directly-owned (pre-mapped, cache-bypassing) {@link ByteBuffer} for the given block
   * index, or {@code null} if the block should be served via the normal cache path. The returned
   * buffer must be a fresh independent view (i.e. a duplicate), positioned at 0, with {@link
   * ByteOrder#LITTLE_ENDIAN} byte order, and with limit set to the block's decompressed length.
   * Default: always {@code null}.
   */
  protected ByteBuffer ownedBufferFor(int blockIdx) {
    return null;
  }

  // ---------------------------------------------------------------------------
  // Root constructor
  // ---------------------------------------------------------------------------

  /** dummy/sentinel ctor */
  protected CachedCompressedIndexInput(String resourceDescription) {
    super(resourceDescription);
    this.cache = null;
    this.currentNodeRef = null;
    this.length = -1;
    this.blockOffsets = null;
    this.guard = null;
    this.blockCount = -1;
    this.lastBlockIdx = -1;
    this.lastBlockDecompressedLen = -1;
    this.offset = -1;
    this.sliceLength = -1;
    this.sliceFirstBlockIdx = -1;
    this.sliceLastBlockIdx = -1;
  }

  /**
   * Root constructor. Computes block-count and last-block-length from {@code length}.
   *
   * @param blockOffsets compressed block offsets array; {@code blockOffsets[blockCount] =
   *     totalCompressedSize}. May be {@code null} for always-mapped inputs.
   */
  protected CachedCompressedIndexInput(
      String resourceDescription,
      BlockCache cache,
      long length,
      long[] blockOffsets,
      ByteBufferGuard guard,
      AtomicLongArray accessMapped) {
    super(resourceDescription);
    this.cache = cache;
    this.currentNodeRef = cache.register(this);
    this.length = length;
    this.blockOffsets = blockOffsets;
    this.guard = guard;
    this.accessMapped = accessMapped;
    int tailLen = (int) (length & COMPRESSION_BLOCK_MASK_LOW);
    boolean hasTail = tailLen > 0;
    this.blockCount = length == 0 ? 1 : (int) (((length - 1) >> COMPRESSION_BLOCK_SHIFT) + 1);
    this.lastBlockIdx = blockCount - 1;
    this.lastBlockDecompressedLen = hasTail ? tailLen : COMPRESSION_BLOCK_SIZE;
    this.offset = 0;
    this.sliceLength = length;
    this.sliceFirstBlockIdx = 0;
    this.readAheadTo = 0;
    this.sliceLastBlockIdx = lastBlockIdx;
  }

  // ---------------------------------------------------------------------------
  // Slice / clone constructor
  // ---------------------------------------------------------------------------

  private static final NodeRefStruct UNINITIALIZED = new NodeRefStruct(null, null, null, null, -2L);

  /**
   * Slice/clone constructor: shares immutable state from parent without owning the backend mapping.
   */
  protected CachedCompressedIndexInput(
      String resourceDescription,
      CachedCompressedIndexInput parent,
      long sliceOffset,
      long sliceLen) {
    super(resourceDescription);
    this.cache = parent.cache;
    this.currentNodeRef = UNINITIALIZED; // lazily registered on first block access
    this.length = parent.length;
    this.blockOffsets = parent.blockOffsets;
    this.blockCount = parent.blockCount;
    this.lastBlockIdx = parent.lastBlockIdx;
    this.lastBlockDecompressedLen = parent.lastBlockDecompressedLen;
    this.guard = parent.guard;
    this.accessMapped = parent.accessMapped;
    this.offset = parent.offset + sliceOffset;
    this.seekPos = this.offset;
    this.sliceLength = sliceLen;
    this.postBuffer = ByteBuffer.allocate(0);
    this.sliceFirstBlockIdx = Math.toIntExact(this.offset >> COMPRESSION_BLOCK_SHIFT);
    this.readAheadTo = sliceFirstBlockIdx;
    this.sliceLastBlockIdx =
        sliceLen == 0
            ? sliceFirstBlockIdx
            : Math.toIntExact((this.offset + sliceLen - 1) >> COMPRESSION_BLOCK_SHIFT);
  }

  /**
   * Heuristic: if this slice spans exactly one block, pre-load it now. Must be called at the end of
   * the subclass slice constructor, after all subclass fields are initialized, to avoid
   * virtual-dispatch-before-initialization bugs.
   */
  protected final void maybePreloadSlice() {
    if (sliceFirstBlockIdx == sliceLastBlockIdx && sliceLength != 0) {
      ensureBlockLoaded(sliceFirstBlockIdx);
    }
  }

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  @Override
  public final void close() throws IOException {
    if (accessMapped == null) return;
    unsetBuffers();
    try {
      ByteBuffer toUnmap = doClose();
      if (toUnmap != null) {
        guard.invalidateAndUnmap(toUnmap);
      }
    } finally {
      unsetBuffers();
    }
  }

  private NodeRefStruct nodeRef() {
    NodeRefStruct ref = currentNodeRef;
    if (ref == UNINITIALIZED) {
      return currentNodeRef = cache.register(this);
    } else {
      return ref;
    }
  }

  private void unsetBuffers() {
    accessMapped = null;
    currentNodeRef.closeFor(cache);
    postBuffer = null;
    floatViews = null;
    intViews = null;
    longViews = null;
  }

  @Override
  public final long getFilePointer() {
    if (seekPos != -1) return seekPos - offset;
    int blockIdx = currentNodeRef.currentBlockIdx;
    if (blockIdx < 0) blockIdx = ~blockIdx;
    return ((long) blockIdx << COMPRESSION_BLOCK_SHIFT)
        + (postBuffer.position() - postBufferBaseline)
        - offset;
  }

  @Override
  public final void seek(long pos) throws IOException {
    seekPos = pos + offset;
  }

  @Override
  public final long length() {
    return sliceLength;
  }

  @Override
  public final IndexInput slice(String sliceDescription, long sliceOffset, long sliceLength)
      throws IOException {
    if (sliceOffset < 0 || sliceLength < 0 || sliceOffset + sliceLength > this.sliceLength) {
      throw new IllegalArgumentException("slice out of bounds");
    }
    return cloneSlice(getFullSliceDescription(sliceDescription), sliceOffset, sliceLength);
  }

  @Override
  public CachedCompressedIndexInput clone() {
    CachedCompressedIndexInput clone = cloneSlice(toString(), 0, sliceLength);
    try {
      clone.seek(getFilePointer());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return clone;
  }

  // ---------------------------------------------------------------------------
  // Block navigation
  // ---------------------------------------------------------------------------

  private void initPositional() throws IOException {
    long pos = seekPos;
    if (pos != -1) {
      seekPos = -1;
      actualSeek(pos);
    }
  }

  private void actualSeek(final long pos) throws IOException {
    int blockIdx = (int) (pos >> COMPRESSION_BLOCK_SHIFT);
    if (blockIdx != currentNodeRef.currentBlockIdx) initBlock(blockIdx);
    postBuffer.position(postBufferBaseline + (int) (pos & COMPRESSION_BLOCK_MASK_LOW));
  }

  private void initBlockSeq() throws IOException {
    initBlock(currentNodeRef.currentBlockIdx + 1);
  }

  private void initBlock(int blockIdx) throws IOException {
    if (blockIdx > lastBlockIdx) throw new EOFException();
    ByteBuffer owned = ownedBufferFor(blockIdx);
    if (owned != null) {
      setCurrentNode(BlockCache.NULL_HANDLE, blockIdx, 0, 0);
      postBuffer = owned;
      postBufferBaseline = 0;
      longViews = null;
      intViews = null;
      floatViews = null;
      return;
    }
    long cached = accessMapped.get(blockIdx);
    boolean uninitialized;
    BlockCache.Val cachedVal = null;
    if ((uninitialized = cached == BlockCache.NULL_HANDLE)
        || (cachedVal = cache.pin(cached)) == null) {
      if (!uninitialized) {
        cache.recordFailedPin();
      }
      cacheMiss(cached, blockIdx);
    } else {
      cacheHit(blockIdx, cached, cachedVal, 0);
    }
  }

  private void cacheHit(int blockIdx, long cached, BlockCache.Val cachedVal, int type)
      throws IOException {
    ByteBuffer buf;
    long loadNanos;
    try {
      // long start = System.nanoTime();
      buf = cachedVal.join(cache);
      loadNanos = 0; // /System.nanoTime() - start;
    } catch (CompletionException e) {
      cache.unpin(cached);
      throw unwrapException(e.getCause());
    }
    setCurrentNode(cached, blockIdx, type, loadNanos);
    postBuffer = buf.duplicate().order(ByteOrder.LITTLE_ENDIAN).position(0);
    postBufferBaseline = 0;
    longViews = null;
    intViews = null;
    floatViews = null;
  }

  private static int readAheadCount(int sequentialAccessCount) {
    return sequentialAccessCount << 1;
  }

  private void setCurrentNode(long node, int blockIdx, int type, long loadNanos) {
    int seqAccessCount = nodeRef().setCurrentNode(node, blockIdx, cache);
    //    System.out.println("XXX type="+ type + ", " + seqAccessCount + ", " + blockIdx+"/"+
    //        accessMapped.length()+", "+ TimeUnit.NANOSECONDS.toMillis(loadNanos) + "ms, " + this);
    switch (seqAccessCount) {
      case -1:
        // gone backward, no read-ahead
        readAheadTo = sliceFirstBlockIdx;
        return;
      case 0:
        // moved forward but not sequential, update high-water mark
        if (blockIdx > readAheadTo) {
          readAheadTo = blockIdx;
        }
        return;
    }
    if (type == 0) {
      if (blockIdx > readAheadTo) {
        readAheadTo = blockIdx;
      }
      return;
    }
    int newReadAheadTo;
    if (seqAccessCount > 0
        && (newReadAheadTo =
                Math.min(
                    sliceLastBlockIdx,
                    blockIdx + Math.min(MAX_READ_AHEAD, readAheadCount(seqAccessCount))))
            > readAheadTo) {
      for (int i = readAheadTo + 1; i <= newReadAheadTo; i++) {
        if (!ensureBlockLoaded(i)) {
          newReadAheadTo = i - 1;
          break;
        }
      }
      readAheadTo = newReadAheadTo;
    }
  }

  private static final AtomicInteger IDS = new AtomicInteger();
  private static final ConcurrentHashMap<List<StackTraceElement>, Map.Entry<Integer, AtomicLong>>
      CALL_STACKS = new ConcurrentHashMap<>();

  private static String stackTraceId() {
    Exception ex = new Exception();
    List<StackTraceElement> callStack = Arrays.asList(ex.getStackTrace());
    boolean[] added = new boolean[1];
    Map.Entry<Integer, AtomicLong> e =
        CALL_STACKS.computeIfAbsent(
            callStack,
            (k) -> {
              added[0] = true;
              return new AbstractMap.SimpleImmutableEntry<>(
                  IDS.getAndIncrement(), new AtomicLong());
            });
    if (VERBOSE && added[0] && log.isInfoEnabled()) {
      log.info("XXX callStack {}", e.getKey(), ex);
    }
    return "callStack="
        + e.getKey()
        + "/"
        + CALL_STACKS.size()
        + " : count="
        + e.getValue().incrementAndGet();
  }

  private static final boolean VERBOSE = false;

  private void cacheMiss(final long cached, final int blockIdx) throws IOException {
    long blockOffset = blockOffsets[blockIdx];
    int compressedLen = (int) (blockOffsets[blockIdx + 1] - blockOffset);
    int decompressedLen =
        blockIdx == lastBlockIdx ? lastBlockDecompressedLen : COMPRESSION_BLOCK_SIZE;

    long[] nodeHandle = new long[1];
    BlockCache.Val nodeVal = cache.acquireNode(nodeHandle);
    if (nodeVal != null) {
      long node = nodeHandle[0];
      long extant = accessMapped.compareAndExchange(blockIdx, cached, node);
      if (extant == cached) {
        // We won the race: fetch from backend and populate the node.
        if (blockIdx == 0) onFirstBlockMiss();
        // long start = System.nanoTime();
        cache.recordDecompressionDemand();
        if (VERBOSE && log.isInfoEnabled()) {
          log.info(
              "demand {} of {}/{}/{} {} [{}]",
              blockIdx,
              sliceFirstBlockIdx,
              sliceLastBlockIdx,
              lastBlockIdx,
              this,
              stackTraceId());
        }
        ByteBuffer buf;
        try {
          byte[] heapBuf = supply(blockIdx, blockOffset, compressedLen, decompressedLen);
          buf = nodeVal.populate(heapBuf, 0, decompressedLen, cache);
        } catch (Throwable t) {
          nodeVal.completeExceptionally(t);
          accessMapped.compareAndSet(blockIdx, node, BlockCache.NULL_HANDLE);
          cache.unpin(node);
          cache.close(node);
          throw unwrapException(t);
        }
        setCurrentNode(node, blockIdx, 2, 0 /* System.nanoTime() - start */);
        postBuffer = buf.duplicate().order(ByteOrder.LITTLE_ENDIAN).position(0);
        postBufferBaseline = 0;
        longViews = null;
        intViews = null;
        floatViews = null;
        return;
      } else {
        // Another thread won the race; wait for its result.
        cache.close(node, true);
        BlockCache.Val extantVal = cache.pin(extant);
        if (extantVal != null) {
          cache.recordCasRaceLoss();
          cacheHit(blockIdx, extant, extantVal, 1);
          return;
        }
      }
    }
    // Serve uncached (cache full or node race lost with no cached result).
    // long start = System.nanoTime();
    cache.recordDecompressionDemand();
    if (VERBOSE && log.isInfoEnabled()) {
      log.info(
          "demand HEAP {} of {}/{}/{} {} [{}]",
          blockIdx,
          sliceFirstBlockIdx,
          sliceLastBlockIdx,
          lastBlockIdx,
          this,
          stackTraceId());
    }
    ByteBuffer heapBuf =
        ByteBuffer.wrap(
            supply(blockIdx, blockOffset, compressedLen, decompressedLen), 0, decompressedLen);
    setCurrentNode(BlockCache.NULL_HANDLE, blockIdx, 3, 0 /* System.nanoTime() - start */);
    postBuffer = heapBuf;
    postBufferBaseline = heapBuf.position();
    heapBuf.order(ByteOrder.LITTLE_ENDIAN);
    longViews = null;
    intViews = null;
    floatViews = null;
  }

  // ---------------------------------------------------------------------------
  // DataInput overrides
  // ---------------------------------------------------------------------------

  @Override
  public void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException {
    // Intentional passthrough: the base class (DataInput) delegates to readBytes(b, offset, len),
    // which we already override efficiently. The useBuffer hint is meaningful only to
    // BufferedIndexInput subclasses that maintain an internal read buffer. We have no such buffer.
    super.readBytes(b, offset, len, useBuffer);
  }

  @Override
  protected void readGroupVInt(long[] dst, int offset) throws IOException {
    // Intentional passthrough: readGroupVInt is a specific encoding used exclusively by HNSW/KNN
    // vector formats for neighbour lists. We do not currently use vector fields so this path is
    // not exercised. If vector fields are added, override readGroupVInts() instead with a single
    // localPin()/localUnpin() around a loop over a private _readGroupVInt().
    super.readGroupVInt(dst, offset);
  }

  @Override
  public int readZInt() throws IOException {
    // don't override this, so long as it's simply a wrapper around `readVInt()`
    return super.readZInt();
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
    return _readByte(pos);
  }

  @Override
  public short readShort(final long pos) throws IOException {
    final long absolutePos = pos + offset;
    final int blockIdx = (int) (absolutePos >> COMPRESSION_BLOCK_SHIFT);
    if (blockIdx != currentNodeRef.currentBlockIdx) initBlock(blockIdx);
    final int localPos = postBufferBaseline + (int) (absolutePos & COMPRESSION_BLOCK_MASK_LOW);
    if (postBuffer.limit() - localPos >= Short.BYTES) {
      return guard.getShort(postBuffer, localPos);
    }
    return (short) (((_readByte(pos + 1) & 0xFF) << 8) | (_readByte(pos) & 0xFF));
  }

  @Override
  public int readInt(final long pos) throws IOException {
    return _readInt(pos);
  }

  @Override
  public long readLong(final long pos) throws IOException {
    return _readLong(pos);
  }

  // ---------------------------------------------------------------------------
  // Sequential IndexInput
  // ---------------------------------------------------------------------------

  @Override
  public byte readByte() throws IOException {
    initPositional();
    if (!postBuffer.hasRemaining()) initBlockSeq();
    return guard.getByte(postBuffer);
  }

  @Override
  public void readBytes(byte[] dst, int offset, int len) throws IOException {
    initPositional();
    int left = postBuffer.remaining();
    while (left < len) {
      guard.getBytes(postBuffer, dst, offset, left);
      len -= left;
      offset += left;
      initBlockSeq();
      left = postBuffer.remaining();
    }
    guard.getBytes(postBuffer, dst, offset, len);
  }

  // ---------------------------------------------------------------------------
  // Sequential reads: short, int, long, vint, vlong, string
  // (must be overridden here to avoid per-byte localPin/localUnpin in base class)
  // ---------------------------------------------------------------------------

  /** Read next byte within a localPin() context; refills block if at boundary. */
  private byte _readByte(final int remaining) throws IOException {
    if (remaining == 0) initBlockSeq();
    return guard.getByte(postBuffer);
  }

  @Override
  public short readShort() throws IOException {
    initPositional();
    final int remaining = postBuffer.remaining();
    if (remaining >= Short.BYTES) {
      return guard.getShort(postBuffer);
    }
    final byte b1 = _readByte(remaining);
    final byte b2 = _readByte(remaining - 1);
    return (short) (((b2 & 0xFF) << 8) | (b1 & 0xFF));
  }

  @Override
  public int readInt() throws IOException {
    initPositional();
    return _readInt(postBuffer.remaining());
  }

  @Override
  public long readLong() throws IOException {
    initPositional();
    return _readLong(postBuffer.remaining());
  }

  @Override
  public int readVInt() throws IOException {
    initPositional();
    return _readVInt(postBuffer.remaining());
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
      if (b >= 0) return b;
      int i = b & 0x7F;
      b = guard.getByte(postBuffer);
      i |= (b & 0x7F) << 7;
      if (b >= 0) return i;
      b = guard.getByte(postBuffer);
      i |= (b & 0x7F) << 14;
      if (b >= 0) return i;
      b = guard.getByte(postBuffer);
      i |= (b & 0x7F) << 21;
      if (b >= 0) return i;
      b = guard.getByte(postBuffer);
      i |= (b & 0x0F) << 28;
      if ((b & 0xF0) == 0) return i;
    }
    throw new IOException("Invalid vInt detected (too many bits)");
  }

  @Override
  public long readVLong() throws IOException {
    initPositional();
    return _readVLong(false);
  }

  @Override
  public long readZLong() throws IOException {
    initPositional();
    return BitUtil.zigZagDecode(_readVLong(true));
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
      if (b >= 0) return b;
      i = b & 0x7FL;
      b = guard.getByte(postBuffer);
      i |= (b & 0x7FL) << 7;
      if (b >= 0) return i;
      b = guard.getByte(postBuffer);
      i |= (b & 0x7FL) << 14;
      if (b >= 0) return i;
      b = guard.getByte(postBuffer);
      i |= (b & 0x7FL) << 21;
      if (b >= 0) return i;
      b = guard.getByte(postBuffer);
      i |= (b & 0x7FL) << 28;
      if (b >= 0) return i;
      b = guard.getByte(postBuffer);
      i |= (b & 0x7FL) << 35;
      if (b >= 0) return i;
      b = guard.getByte(postBuffer);
      i |= (b & 0x7FL) << 42;
      if (b >= 0) return i;
      b = guard.getByte(postBuffer);
      i |= (b & 0x7FL) << 49;
      if (b >= 0) return i;
      b = guard.getByte(postBuffer);
      i |= (b & 0x7FL) << 56;
      if (b >= 0) return i;
      if (!allowNegative) {
        throw new IOException("Invalid vLong detected (negative values disallowed)");
      }
      b = guard.getByte(postBuffer);
    }
    i |= (b & 0x7FL) << 63;
    if (b == 0 || b == 1) return i;
    throw new IOException("Invalid vLong detected (more than 64 bits)");
  }

  @Override
  public String readString() throws IOException {
    initPositional();
    return _readString();
  }

  private String _readString() throws IOException {
    final int length = _readVInt(postBuffer.remaining());
    final byte[] bytes = new byte[length];
    final int left = postBuffer.remaining();
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
      initBlockSeq();
      left = postBuffer.remaining();
    } while (left < toRead);
    guard.getBytes(postBuffer, dst, offset, toRead);
  }

  @Override
  public Map<String, String> readMapOfStrings() throws IOException {
    initPositional();
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
    initPositional();
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
  // Bulk typed reads with buffer views
  // ---------------------------------------------------------------------------

  private int _readInt(final int remaining) throws IOException {
    if (remaining >= Integer.BYTES || refillAtBoundary(remaining)) {
      return guard.getInt(postBuffer);
    }
    // Cross-block: use _readByte to avoid per-byte localPin/localUnpin overhead.
    final byte b0 = _readByte(remaining);
    final byte b1 = _readByte(remaining - 1);
    final byte b2 = _readByte(remaining - 2);
    final byte b3 = _readByte(remaining - 3);
    return ((b3 & 0xFF) << 24) | ((b2 & 0xFF) << 16) | ((b1 & 0xFF) << 8) | (b0 & 0xFF);
  }

  private boolean refillAtBoundary(int remaining) throws IOException {
    if (remaining == 0) {
      initBlockSeq();
      return true;
    } else {
      return false;
    }
  }

  private long _readLong(final int remaining) throws IOException {
    if (remaining >= Long.BYTES || refillAtBoundary(remaining)) {
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
    initPositional();
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
      postBuffer.position(position + (int) bytesRequested);
    }
  }

  @Override
  public void readInts(final int[] dst, final int offset, final int length) throws IOException {
    initPositional();
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
      postBuffer.position(position + (int) bytesRequested);
    }
  }

  @Override
  public void readFloats(final float[] dst, final int offset, final int length) throws IOException {
    initPositional();
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
      guard.getFloats(floatViews[position & 0x03].position(position >>> 2), dst, offset, length);
      postBuffer.position(position + (int) bytesRequested);
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
  // Utility
  // ---------------------------------------------------------------------------

  static IOException unwrapException(Throwable t) {
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
  // NodeRefStruct
  // ---------------------------------------------------------------------------

  /**
   * Per-{@link CachedCompressedIndexInput}-instance struct tracking the currently pinned {@link
   * BlockCache.Val} and sequential-access statistics.
   */
  static final class NodeRefStruct extends WeakReference<IndexInput> {

    private long currentNode = BlockCache.NULL_HANDLE;
    // Positive = current block index; negative (~idx) = block index with no live pin.
    private int currentBlockIdx = -1;
    private int sequentialAccessCount = 0;

    // for cleanup upon GC
    private final LongAdder outstandingRefs;
    private final Cache.Node<?> remove; // non-null on Cache fallback path
    // Cache3 primary path: (partIdx << 32 | slot); -1L if using Cache fallback.
    private final long cache3Handle;

    NodeRefStruct(
        IndexInput referrent,
        ReferenceQueue<? super IndexInput> q,
        LongAdder outstandingRefs,
        Cache.Node<?> remove,
        long cache3Handle) {
      super(referrent, q);
      this.outstandingRefs = outstandingRefs;
      this.remove = remove;
      this.cache3Handle = cache3Handle;
    }

    /** Updates the current cached block. */
    private int setCurrentNode(long node, int blockIdx, BlockCache cache) {
      int extant = currentBlockIdx < 0 ? ~currentBlockIdx : currentBlockIdx;
      long toUnpin = currentNode;
      if (toUnpin != BlockCache.NULL_HANDLE) {
        cache.unpin(toUnpin);
      }
      currentNode = node;
      currentBlockIdx = blockIdx;
      if (blockIdx == extant + 1) {
        // sequential access.
        sequentialAccessCount++;
        return node == BlockCache.NULL_HANDLE ? 0 : sequentialAccessCount;
      } else if (blockIdx < extant) {
        // gone backward, full reset
        sequentialAccessCount = 0;
        return -1;
      } else if (blockIdx == extant) {
        // same idx — re-acquire after eviction
        return node == BlockCache.NULL_HANDLE ? 0 : sequentialAccessCount;
      } else {
        // skipped ahead, reset
        return sequentialAccessCount = 0;
      }
    }

    /** Unpins the current block on {@link CachedCompressedIndexInput#close()}. */
    void closeFor(BlockCache blockCache) {
      boolean claimed;
      switch ((int) cache3Handle) {
        case -2:
          return; // UNINITIALIZED: never registered, nothing to do
        case -1:
          claimed = Cache.tryRemove(remove);
          break;
        default:
          claimed = blockCache.releaseHoldRef(cache3Handle);
      }
      if (claimed) {
        outstandingRefs.decrement();
        long toUnpin = currentNode;
        if (toUnpin != BlockCache.NULL_HANDLE) {
          currentNode = BlockCache.NULL_HANDLE;
          currentBlockIdx = ~currentBlockIdx;
          blockCache.unpin(toUnpin);
        }
      }
    }
  }
}
