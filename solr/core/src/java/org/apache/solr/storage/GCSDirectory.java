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
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.zip.CRC32;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.compress.LZ4;

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
 * <p>Offset file format (all fields big-endian):
 *
 * <pre>
 *   [8 bytes]  logical (uncompressed) file length
 *   [1 byte]   compressionBlockType id
 *   [1 byte]   compressionType id
 *   [2 bytes]  reserved
 *   [8 bytes]  GCS blob UUID most-significant bits
 *   [8 bytes]  GCS blob UUID least-significant bits
 *   [8 bytes]  total compressed bytes in the GCS object
 *   [remaining bytes]  ZInt delta-encoded compressed block sizes
 * </pre>
 */
public class GCSDirectory extends FSDirectory {

  // Offset file header size: 8 (length) + 4 (blockType/comprType/reserved) + 16 (UUID) + 8
  // (gcsObjectSize) = 36 bytes.
  static final int OFFSET_FILE_HEADER_SIZE = 36;

  private final String bucket;
  final Storage storage;
  private final BlockCache cache;
  final Cache<ReadChannel, Cache.Node<ReadChannel>> channelPool;
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
  private final ConcurrentHashMap<UUID, AtomicReferenceArray<BlockCache.Node>> pendingNodes =
      new ConcurrentHashMap<>();

  public GCSDirectory(
      Path localPath,
      String bucket,
      Storage storage,
      BlockCache cache,
      Cache<ReadChannel, Cache.Node<ReadChannel>> channelPool,
      ExecutorService ioExec,
      boolean useAsyncIO,
      DirectBufferPool bufferPool)
      throws IOException {
    super(localPath, FSLockFactory.getDefault());
    this.bucket = bucket;
    this.storage = storage;
    this.cache = cache;
    this.channelPool = channelPool;
    this.ioExec = ioExec;
    this.useAsyncIO = useAsyncIO;
    this.bufferPool = bufferPool;
  }

  // ---------------------------------------------------------------------------
  // Directory API
  // ---------------------------------------------------------------------------

  @Override
  public void deleteFile(String name) throws IOException {
    ensureOpen();
    Path offsetFile = directory.resolve(name);
    UUID blobUUID = readBlobUUID(offsetFile);
    if (blobUUID != null) {
      AtomicReferenceArray<BlockCache.Node> stale = pendingNodes.remove(blobUUID);
      if (stale != null) {
        for (int i = 0; i < stale.length(); i++) {
          BlockCache.Node node = stale.getAndSet(i, null);
          if (node != null) cache.close(node);
        }
      }
      storage.delete(BlobId.of(bucket, blobUUID.toString()));
    }
    if (Files.exists(offsetFile)) {
      Files.delete(offsetFile);
    }
  }

  @Override
  public long fileLength(String name) throws IOException {
    ensureOpen();
    Path offsetFile = directory.resolve(name);
    byte[] header = readOffsetFileHeader(offsetFile);
    if (header == null) throw new NoSuchFileException(name);
    return new ByteArrayDataInput(header).readLong();
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    return new GCSIndexOutput(this, super.createOutput(name, context));
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
      throws IOException {
    return new GCSIndexOutput(this, super.createTempOutput(prefix, suffix, context));
  }

  /**
   * Creates the {@link WriteChannel} used to stream compressed data to GCS. Overridable for
   * testing.
   */
  protected WriteChannel openWriteChannel(BlobInfo blobInfo) {
    return storage.writer(blobInfo);
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    ensureOpen();
    ensureCanRead(name);
    Path offsetFile = directory.resolve(name);
    if (!Files.exists(offsetFile)) throw new NoSuchFileException(name);
    return new GCSIndexInput("gcs:" + name, this, offsetFile);
  }

  // ---------------------------------------------------------------------------
  // Offset file helpers
  // ---------------------------------------------------------------------------

  /** Returns the first {@link #OFFSET_FILE_HEADER_SIZE} bytes, or null if file is too small. */
  private static byte[] readOffsetFileHeader(Path path) throws IOException {
    if (!Files.exists(path) || Files.size(path) < OFFSET_FILE_HEADER_SIZE) return null;
    try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
      ByteBuffer buf = ByteBuffer.allocate(OFFSET_FILE_HEADER_SIZE);
      while (buf.hasRemaining()) ch.read(buf);
      return buf.array();
    }
  }

  /** Reads the GCS blob UUID from the offset file, or null if the file is missing/malformed. */
  private static UUID readBlobUUID(Path path) throws IOException {
    byte[] header = readOffsetFileHeader(path);
    if (header == null) return null;
    ByteArrayDataInput in = new ByteArrayDataInput(header);
    in.skipBytes(12); // skip fileLength(8) + blockType(1) + compressionType(1) + reserved(2)
    return new UUID(in.readLong(), in.readLong());
  }

  // ---------------------------------------------------------------------------
  // GCSIndexOutput
  // ---------------------------------------------------------------------------

  static final class GCSIndexOutput extends IndexOutput
      implements CompressingDirectory.SizeReportingIndexOutput {

    private final GCSDirectory dir;
    private final byte[] compressBuffer = new byte[COMPRESSION_BLOCK_SIZE];
    private final LZ4.FastCompressionHashTable ht = new LZ4.FastCompressionHashTable();
    private final ByteBuffer preBuffer;
    private final AsyncGCSWriteHelper writeHelper;
    private ByteBuffer buffer;
    private final BytesOut blockDeltas = new BytesOut();
    private int prevBlockSize = BLOCK_SIZE_ESTIMATE;

    private final UUID uuid;
    private final IndexOutput localOut;
    private long filePos;
    private long gcsObjectSize;
    private final CRC32 crc = new CRC32();
    private boolean isOpen;

    GCSIndexOutput(GCSDirectory dir, IndexOutput localOut) throws IOException {
      super("GCSIndexOutput(name=\"" + localOut.getName() + "\")", localOut.getName());
      this.dir = dir;
      this.localOut = localOut;
      this.uuid = UUID.randomUUID();
      String blobName = uuid.toString();
      WriteChannel gcsChannel =
          dir.openWriteChannel(BlobInfo.newBuilder(BlobId.of(dir.bucket, blobName)).build());
      writeHelper = new AsyncGCSWriteHelper(dir.bufferPool, gcsChannel);
      buffer = writeHelper.init();
      preBuffer = ByteBuffer.wrap(compressBuffer);
      if (dir.useAsyncIO) {
        writeHelper.start(dir.ioExec);
      } else {
        writeHelper.startSync();
      }
      isOpen = true;
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
      preBuffer.rewind();
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
      int preBufferRemaining = preBuffer.remaining();
      if (preBufferRemaining > 0) {
        filePos += preBufferRemaining;
        LZ4.compressWithDictionary(compressBuffer, 0, 0, preBufferRemaining, out, ht);
        gcsObjectSize += out.resetSize();
      }
      // Flush compressed data to GCS (this does NOT include block deltas).
      writeHelper.flush(buffer, true);
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
        try (writeHelper) {
          flush();
        }
        // Write header + delta-encoded block sizes into the local offset file.
        try (localOut) {
          localOut.writeLong(filePos);
          localOut.writeByte((byte) COMPRESSION_BLOCK_TYPE.id);
          localOut.writeByte((byte) COMPRESSION_TYPE.id);
          localOut.writeShort((short) 0); // reserved
          localOut.writeLong(uuid.getMostSignificantBits());
          localOut.writeLong(uuid.getLeastSignificantBits());
          localOut.writeLong(gcsObjectSize);
          localOut.writeBytes(blockDeltas.baos.buf(), 0, blockDeltas.baos.count());
        }
        // Pre-populate cache-node array for readers of this file.
        if (filePos > 0) {
          int blockCount = (int) (((filePos - 1) >> COMPRESSION_BLOCK_SHIFT) + 1);
          dir.pendingNodes.put(uuid, new AtomicReferenceArray<>(blockCount));
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

  private static final long REOPEN_THRESHOLD = COMPRESSION_BLOCK_SIZE >> 1;

  static final class GCSIndexInput extends IndexInput implements RandomAccessInput {

    private final GCSDirectory dir;
    private final long length;
    private final long[] blockOffsets; // blockOffsets[i] = byte offset of block i in GCS object
    private final int blockCount;
    private final int lastBlockIdx;
    private final int lastBlockDecompressedLen;
    private final String blobName;

    private final AtomicReferenceArray<BlockCache.Node> accessMapped;

    private final long offset;
    private final long sliceLength;

    private long seekPos = -1;
    private long filePointer = 0;
    private ByteBuffer postBuffer = ByteBuffer.allocate(0);
    private int postBufferBaseline;
    private int currentBlockIdx = -1;
    private BlockCache.Node currentNode;

    private LongBuffer[] longViews;
    private IntBuffer[] intViews;
    private FloatBuffer[] floatViews;

    // Pool node for the active channel; null when the pool was exhausted (direct fallback).
    private Cache.Node<ReadChannel> channelNode;
    // Active ReadChannel, whether pool-backed or opened directly as a one-off fallback.
    // Null between supply() calls in the direct-fallback case (closed at the end of each supply()).
    private ReadChannel channel;
    private long channelPos = -1; // current byte offset of channel in the GCS object

    // Root constructor: parses the offset file.
    GCSIndexInput(String resourceDescription, GCSDirectory dir, Path offsetFile)
        throws IOException {
      super(resourceDescription);
      this.dir = dir;

      byte[] header = readOffsetFileHeader(offsetFile);
      if (header == null || header.length < OFFSET_FILE_HEADER_SIZE) {
        length = 0;
        blockOffsets = null;
        blockCount = 0;
        lastBlockIdx = -1;
        lastBlockDecompressedLen = 0;
        blobName = null;
        accessMapped = EMPTY_ACCESS_MAPPED;
        offset = 0;
        sliceLength = 0;
        return;
      }

      ByteArrayDataInput hdr = new ByteArrayDataInput(header);
      length = hdr.readLong();
      int cBlockTypeId = hdr.readByte() & 0xff;
      if (cBlockTypeId != COMPRESSION_BLOCK_TYPE.id) {
        throw new IOException("unrecognized compression block type id: " + cBlockTypeId);
      }
      hdr.readByte(); // compressionType (not yet used)
      hdr.readShort(); // reserved
      UUID blobUUID = new UUID(hdr.readLong(), hdr.readLong());
      blobName = blobUUID.toString();
      long gcsObjectSize = hdr.readLong();

      // Delta bytes occupy everything after the fixed header.
      long offsetFileSize = Files.size(offsetFile);
      int blockDeltaFooterSize = (int) (offsetFileSize - OFFSET_FILE_HEADER_SIZE);
      byte[] footer = new byte[blockDeltaFooterSize];
      try (FileChannel ch = FileChannel.open(offsetFile, StandardOpenOption.READ)) {
        ByteBuffer footerBuf = ByteBuffer.wrap(footer);
        ch.read(footerBuf, OFFSET_FILE_HEADER_SIZE);
      }
      ByteArrayDataInput in = new ByteArrayDataInput(footer);

      blockCount = (int) (((length - 1) >> COMPRESSION_BLOCK_SHIFT) + 1);
      blockOffsets = new long[blockCount + 1];
      lastBlockIdx = blockCount - 1;
      lastBlockDecompressedLen = (int) (((length - 1) & COMPRESSION_BLOCK_MASK_LOW) + 1);

      // Decode block offsets: blockOffsets[0] = 0 (GCS objects have no header prefix).
      long blockOffset = 0;
      int lastBlockSize = BLOCK_SIZE_ESTIMATE;
      blockOffsets[0] = 0;
      for (int i = 1; i < blockCount; i++) {
        int delta = in.readZInt();
        int nextBlockSize = lastBlockSize + delta;
        blockOffset += nextBlockSize;
        blockOffsets[i] = blockOffset;
        lastBlockSize = nextBlockSize;
      }
      blockOffsets[blockCount] = gcsObjectSize;

      AtomicReferenceArray<BlockCache.Node> local = new AtomicReferenceArray<>(blockCount);
      AtomicReferenceArray<BlockCache.Node> existing =
          dir.pendingNodes.putIfAbsent(blobUUID, local);
      this.accessMapped = existing == null ? local : existing;

      this.offset = 0;
      this.sliceLength = length;
    }

    // Clone / slice constructor.
    private GCSIndexInput(
        String resourceDescription, GCSIndexInput parent, long offset, long length) {
      super(resourceDescription);
      this.dir = parent.dir;
      this.length = parent.length;
      this.blockOffsets = parent.blockOffsets;
      this.blockCount = parent.blockCount;
      this.lastBlockIdx = parent.lastBlockIdx;
      this.lastBlockDecompressedLen = parent.lastBlockDecompressedLen;
      this.blobName = parent.blobName;
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

    private void unpinCurrent() {
      if (currentNode != null) {
        dir.cache.unpin(currentNode);
        currentNode = null;
      }
    }

    /**
     * Fetches the given compressed block from GCS and decompresses it. Called only on a cache miss.
     *
     * <p>Reuses a persistent {@link ReadChannel} across sequential block fetches to avoid per-block
     * connection overhead. The channel is seeked (triggering a reconnect) only when the target
     * position is not reachable by a short forward skip.
     */
    private ByteBuffer supply(long pos, int compressedLen, int decompressedLen) throws IOException {
      byte[] compressed = new byte[compressedLen];
      ByteBuffer compBuf = ByteBuffer.wrap(compressed);
      try {
        fillBuffer(pos, compBuf);
      } finally {
        if (channelNode != null) {
          dir.channelPool.unpin(channelNode);
        } else if (channel != null) {
          try {
            channel.close();
          } catch (Exception ignored) {
          }
          channel = null;
        }
      }
      byte[] decompressed = new byte[decompressedLen + 7];
      CompressingDirectory.decompress(compressed, 0, decompressedLen, decompressed, 0);
      return ByteBuffer.wrap(decompressed, 0, decompressedLen);
    }

    /**
     * Acquires a channel (from the pool if available, otherwise a direct one-off) seeked to {@code
     * pos}, updating {@link #channelNode} and {@link #channel}.
     */
    private void openChannelAt(long pos) throws IOException {
      channelNode =
          dir.channelPool.acquireNode(
              ch -> {
                if (ch != null) {
                  try {
                    ch.close();
                  } catch (Exception ignored) {
                  }
                }
                ReadChannel c = dir.storage.reader(BlobId.of(dir.bucket, blobName));
                c.setChunkSize(COMPRESSION_BLOCK_SIZE);
                return c;
              });
      if (channelNode != null) {
        channel = channelNode.value;
      } else {
        // Pool exhausted: open a transient channel for this supply() call only.
        channel = dir.storage.reader(BlobId.of(dir.bucket, blobName));
        channel.setChunkSize(COMPRESSION_BLOCK_SIZE);
      }
      channel.seek(pos);
      channelPos = pos;
    }

    /**
     * Fills {@code dst} completely starting at {@code startPos}.
     *
     * <p>Handles channel acquisition and positioning (seek or short forward skip) before reading.
     * If the channel was re-pinned from the pool it may have been idle long enough for the
     * transport to time out; in that case one reopen is attempted on the first failing operation.
     * Note that successful reads do not clear the stale flag: buffered data may be served without a
     * network call, so a failure can surface at any point during the block read. A brand-new
     * channel opened via {@link #openChannelAt} is never retried.
     */
    private void fillBuffer(long startPos, ByteBuffer dst) throws IOException {
      final long resumePos = startPos + dst.position();

      // Acquire / re-pin the channel. isStale is true when we re-pinned an existing channel
      // that may have been idle long enough for the transport layer to time out.
      boolean isStale;
      if (channel == null || (channelNode != null && !dir.channelPool.pin(channelNode))) {
        openChannelAt(resumePos);
        isStale = false;
      } else {
        isStale = true;
        if (resumePos != channelPos) {
          long distance = resumePos - channelPos;
          try {
            if (distance > 0 && distance < REOPEN_THRESHOLD) {
              // Short forward skip: read and discard rather than paying for a reconnect.
              ByteBuffer skip = ByteBuffer.allocate((int) distance);
              while (skip.hasRemaining()) {
                if (channel.read(skip) == -1) {
                  throw new EOFException(
                      "unexpected EOF in GCS blob " + blobName + " while skipping to " + resumePos);
                }
              }
              channelPos = resumePos;
            } else {
              channel.seek(resumePos);
              channelPos = resumePos;
              // seek() is lazy (no network call); isStale remains true until first read succeeds.
            }
            // (skip path only: isStale cleared below after actual reads confirmed the transport)
          } catch (IOException e) {
            // First operation on a potentially-stale channel failed: reopen once.
            releaseAndReopenAt(resumePos);
            isStale = false; // fresh channel; no further retries
          }
        }
      }

      while (dst.hasRemaining()) {
        int n;
        try {
          n = channel.read(dst);
        } catch (IOException e) {
          if (!isStale) {
            throw new IOException("GCS read failed at " + (startPos + dst.position()), e);
          }
          // First read on a potentially-stale channel: reopen once.
          releaseAndReopenAt(startPos + dst.position());
          isStale = false; // fresh channel; no further retries
          continue;
        }
        if (n == -1) {
          throw new EOFException(
              "unexpected EOF in GCS blob "
                  + blobName
                  + " at position "
                  + (startPos + dst.position()));
        }
      }
      channelPos = startPos + dst.position();
    }

    /**
     * Releases the current channel back to the pool (or closes it directly) and opens a fresh one.
     */
    private void releaseAndReopenAt(long pos) throws IOException {
      if (channelNode != null) {
        dir.channelPool.unpin(channelNode);
        dir.channelPool.close(channelNode);
        channelNode = null;
      } else if (channel != null) {
        try {
          channel.close();
        } catch (Exception ignored) {
        }
      }
      channel = null;
      openChannelAt(pos);
    }

    // ---------------------------------------------------------------------------
    // Block navigation
    // ---------------------------------------------------------------------------

    private void actualSeek(final long pos) throws IOException {
      filePointer = pos;
      int blockIdx = (int) (pos >> COMPRESSION_BLOCK_SHIFT);
      if (blockIdx != currentBlockIdx) initBlock(blockIdx);
      postBuffer.position(postBufferBaseline + (int) (pos & COMPRESSION_BLOCK_MASK_LOW));
    }

    private void initBlock(int blockIdx) throws IOException {
      if (blockIdx > lastBlockIdx) throw new EOFException();
      long blockOffset = blockOffsets[blockIdx];
      int compressedLen = (int) (blockOffsets[blockIdx + 1] - blockOffset);
      refill(blockOffset, compressedLen, blockIdx);
    }

    private void refill() throws IOException {
      int blockIdx = currentBlockIdx + 1;
      if (blockIdx > lastBlockIdx) throw new EOFException();
      long blockOffset = blockOffsets[blockIdx];
      int compressedLen = (int) (blockOffsets[blockIdx + 1] - blockOffset);
      refill(blockOffset, compressedLen, blockIdx);
    }

    private void refill(final long pos, final int compressedLen, final int blockIdx)
        throws IOException {
      int decompressedLen =
          blockIdx == lastBlockIdx ? lastBlockDecompressedLen : COMPRESSION_BLOCK_SIZE;
      unpinCurrent();

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
        currentNode = cached;
        postBuffer = buf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        postBuffer.clear().limit(decompressedLen);
        postBufferBaseline = 0;
        currentBlockIdx = blockIdx;
        longViews = null;
        intViews = null;
        floatViews = null;
        return;
      } else if ((node = dir.cache.acquireNode()) != null) {
        // cache miss
        BlockCache.Node extant = accessMapped.compareAndExchange(blockIdx, cached, node);
        if (extant == cached) {
          // We won the race: fetch from GCS and populate the node.
          ByteBuffer buf;
          try {
            ByteBuffer heapBuf = supply(pos, compressedLen, decompressedLen);
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
          currentNode = node;
          postBuffer = buf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        } else {
          // Another thread won the race; wait for its result.
          dir.cache.unpin(node);
          dir.cache.close(node);
          if (dir.cache.pin(extant)) {
            ByteBuffer buf;
            try {
              buf = extant.join();
            } catch (CompletionException e) {
              dir.cache.unpin(extant);
              throw unwrapException(e.getCause());
            }
            currentNode = extant;
            postBuffer = buf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
          } else {
            // Serve uncached.
            uncached(pos, compressedLen, blockIdx, decompressedLen);
            return;
          }
        }
        postBuffer.clear().limit(decompressedLen);
        postBufferBaseline = 0;
        currentBlockIdx = blockIdx;
        longViews = null;
        intViews = null;
        floatViews = null;
        return;
      }

      // Serve uncached.
      uncached(pos, compressedLen, blockIdx, decompressedLen);
    }

    private void uncached(long pos, int compressedLen, int blockIdx, int decompressedLen)
        throws IOException {
      ByteBuffer heapBuf = supply(pos, compressedLen, decompressedLen);
      currentNode = null;
      postBuffer = heapBuf;
      postBufferBaseline = heapBuf.position();
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
      long absolutePos = pos + offset;
      int blockIdx = (int) (absolutePos >> COMPRESSION_BLOCK_SHIFT);
      if (blockIdx != currentBlockIdx) initBlock(blockIdx);
      return postBuffer.get(postBufferBaseline + (int) (absolutePos & COMPRESSION_BLOCK_MASK_LOW));
    }

    @Override
    public short readShort(final long pos) throws IOException {
      long absolutePos = pos + offset;
      int blockIdx = (int) (absolutePos >> COMPRESSION_BLOCK_SHIFT);
      if (blockIdx != currentBlockIdx) initBlock(blockIdx);
      int localPos = postBufferBaseline + (int) (absolutePos & COMPRESSION_BLOCK_MASK_LOW);
      if (postBuffer.limit() - localPos < Short.BYTES) {
        return (short) (((readByte(pos + 1) & 0xFF) << 8) | (readByte(pos) & 0xFF));
      }
      return postBuffer.getShort(localPos);
    }

    @Override
    public int readInt(final long pos) throws IOException {
      long absolutePos = pos + offset;
      int blockIdx = (int) (absolutePos >> COMPRESSION_BLOCK_SHIFT);
      if (blockIdx != currentBlockIdx) initBlock(blockIdx);
      int localPos = postBufferBaseline + (int) (absolutePos & COMPRESSION_BLOCK_MASK_LOW);
      if (postBuffer.limit() - localPos < Integer.BYTES) {
        return ((readByte(pos + 3) & 0xFF) << 24)
            | ((readByte(pos + 2) & 0xFF) << 16)
            | ((readByte(pos + 1) & 0xFF) << 8)
            | (readByte(pos) & 0xFF);
      }
      return postBuffer.getInt(localPos);
    }

    @Override
    public long readLong(final long pos) throws IOException {
      return (readInt(pos) & 0xFFFFFFFFL) | ((long) readInt(pos + 4) << 32);
    }

    // ---------------------------------------------------------------------------
    // Sequential IndexInput
    // ---------------------------------------------------------------------------

    @Override
    public byte readByte() throws IOException {
      long pos = seekPos;
      if (pos != -1) {
        seekPos = -1;
        actualSeek(pos);
      }
      if (!postBuffer.hasRemaining()) refill();
      filePointer++;
      return postBuffer.get();
    }

    @Override
    public void readBytes(byte[] dst, int offset, int len) throws IOException {
      long pos = seekPos;
      if (pos != -1) {
        seekPos = -1;
        actualSeek(pos);
      }
      filePointer += len;
      int left = postBuffer.remaining();
      while (left < len) {
        postBuffer.get(dst, offset, left);
        len -= left;
        offset += left;
        refill();
        left = postBuffer.remaining();
      }
      postBuffer.get(dst, offset, len);
    }

    // ---------------------------------------------------------------------------
    // Bulk typed reads with buffer views
    // ---------------------------------------------------------------------------

    private int _readInt(final int remaining) throws IOException {
      if (remaining >= Integer.BYTES) {
        filePointer += Integer.BYTES;
        return postBuffer.getInt();
      }
      // Cross-block: read byte-by-byte (little-endian).
      int b0 = readByte() & 0xFF;
      int b1 = readByte() & 0xFF;
      int b2 = readByte() & 0xFF;
      int b3 = readByte() & 0xFF;
      return (b3 << 24) | (b2 << 16) | (b1 << 8) | b0;
    }

    private long _readLong(final int remaining) throws IOException {
      if (remaining >= Long.BYTES) {
        filePointer += Long.BYTES;
        return postBuffer.getLong();
      }
      return (_readInt(remaining) & 0xFFFFFFFFL)
          | (((long) _readInt(postBuffer.remaining())) << 32);
    }

    @Override
    public void readLongs(final long[] dst, final int offset, final int length) throws IOException {
      long pos = seekPos;
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
        for (int i = 1; i < length; i++) {
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
      long pos = seekPos;
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
        for (int i = 1; i < length; i++) {
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
      long pos = seekPos;
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
        for (int i = 1; i < length; i++) {
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
      unpinCurrent();
      ReadChannel c = channel;
      channel = null;
      Cache.Node<ReadChannel> cn = channelNode;
      channelNode = null;
      if (cn != null) {
        assert c == cn.value;
        // recycle slot to pool tail; channel closed lazily on eviction
        if (dir.channelPool.close(cn)) {
          cn.value.close();
        }
      } else if (c != null) {
        c.close();
      }
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
