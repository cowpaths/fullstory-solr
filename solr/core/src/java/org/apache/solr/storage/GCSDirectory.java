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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.DataOutput;
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
public class GCSDirectory extends BaseDirectory {

  // Offset file header size: 8 (length) + 4 (blockType/comprType/reserved) + 16 (UUID) + 8
  // (gcsObjectSize) = 36 bytes.
  static final int OFFSET_FILE_HEADER_SIZE = 36;

  private final Path localPath;
  private final String bucket;
  final Storage storage;
  private final BlockCache cache;
  private final ExecutorService ioExec;
  private final boolean useAsyncIO;
  private final DirectBufferPool bufferPool;
  private final AtomicLong tempFileCounter = new AtomicLong();

  @SuppressWarnings({"unchecked", "rawtypes"})
  static final AtomicReference<BlockCache.Node>[] EMPTY_ACCESS_MAPPED = new AtomicReference[0];

  /**
   * Cache-node arrays shared across all root {@link GCSIndexInput} instances opened for the same
   * file. Populated by the writer at write-close time; entries are removed on delete or rename.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private final ConcurrentHashMap<String, AtomicReference<BlockCache.Node>[]> pendingNodes =
      new ConcurrentHashMap<>();

  public GCSDirectory(
      Path localPath,
      String bucket,
      Storage storage,
      BlockCache cache,
      ExecutorService ioExec,
      boolean useAsyncIO,
      DirectBufferPool bufferPool)
      throws IOException {
    super(FSLockFactory.getDefault());
    this.localPath = localPath;
    this.bucket = bucket;
    this.storage = storage;
    this.cache = cache;
    this.ioExec = ioExec;
    this.useAsyncIO = useAsyncIO;
    this.bufferPool = bufferPool;
    Files.createDirectories(localPath);
  }

  // ---------------------------------------------------------------------------
  // Directory API
  // ---------------------------------------------------------------------------

  @Override
  public String[] listAll() throws IOException {
    ensureOpen();
    try (var stream = Files.list(localPath)) {
      return stream
          .map(p -> p.getFileName().toString())
          .filter(name -> !name.equals("write.lock"))
          .sorted()
          .toArray(String[]::new);
    }
  }

  @Override
  public void deleteFile(String name) throws IOException {
    ensureOpen();
    @SuppressWarnings({"unchecked", "rawtypes"})
    AtomicReference<BlockCache.Node>[] stale = pendingNodes.remove(name);
    if (stale != null) {
      for (AtomicReference<BlockCache.Node> slot : stale) {
        BlockCache.Node node = slot.getAndSet(null);
        if (node != null) cache.close(node);
      }
    }
    Path offsetFile = localPath.resolve(name);
    if (Files.exists(offsetFile)) {
      String blobName = readBlobName(offsetFile);
      if (blobName != null) {
        storage.delete(BlobId.of(bucket, blobName));
      }
      Files.delete(offsetFile);
    }
  }

  @Override
  public long fileLength(String name) throws IOException {
    ensureOpen();
    Path offsetFile = localPath.resolve(name);
    byte[] header = readOffsetFileHeader(offsetFile);
    if (header == null) return 0;
    return ByteBuffer.wrap(header).getLong(0);
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    ensureOpen();
    return new GCSIndexOutput(name, localPath.resolve(name));
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
      throws IOException {
    ensureOpen();
    while (true) {
      String name =
          prefix + "_" + Long.toString(tempFileCounter.getAndIncrement(), 36) + suffix + ".tmp";
      Path path = localPath.resolve(name);
      try {
        Files.createFile(path); // atomically claim the name
        Files.delete(path); // GCSIndexOutput will create the offset file at close
        return new GCSIndexOutput(name, path);
      } catch (java.nio.file.FileAlreadyExistsException e) {
        // retry with next counter value
      }
    }
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    ensureOpen();
    for (String name : names) {
      Path p = localPath.resolve(name);
      if (Files.exists(p)) {
        try (FileChannel ch = FileChannel.open(p, StandardOpenOption.READ)) {
          ch.force(true);
        }
      }
    }
  }

  @Override
  public void syncMetaData() throws IOException {
    ensureOpen();
    try (FileChannel ch = FileChannel.open(localPath, StandardOpenOption.READ)) {
      ch.force(true);
    }
  }

  @Override
  public void rename(String source, String dest) throws IOException {
    ensureOpen();
    AtomicReference<BlockCache.Node>[] nodes = pendingNodes.remove(source);
    if (nodes != null) pendingNodes.put(dest, nodes);
    Files.move(localPath.resolve(source), localPath.resolve(dest));
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    ensureOpen();
    @SuppressWarnings({"unchecked", "rawtypes"})
    AtomicReference<BlockCache.Node>[] shared = pendingNodes.get(name);
    return new GCSIndexInput("gcs:" + name, localPath.resolve(name), shared);
  }

  @Override
  public Set<String> getPendingDeletions() {
    return Collections.emptySet();
  }

  @Override
  public void close() throws IOException {
    isOpen = false;
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

  /** Reads the GCS blob UUID string from the offset file, or null if the file is empty/missing. */
  private static String readBlobName(Path path) throws IOException {
    byte[] header = readOffsetFileHeader(path);
    if (header == null) return null;
    ByteBuffer buf = ByteBuffer.wrap(header);
    long uuidMsb = buf.getLong(12);
    long uuidLsb = buf.getLong(20);
    return new UUID(uuidMsb, uuidLsb).toString();
  }

  private static void writeOffsetFile(
      Path path, long fileLength, UUID uuid, long gcsObjectSize, byte[] deltaBytes)
      throws IOException {
    ByteBuffer header = ByteBuffer.allocate(OFFSET_FILE_HEADER_SIZE);
    header.putLong(fileLength);
    header.put((byte) COMPRESSION_BLOCK_TYPE.id);
    header.put((byte) COMPRESSION_TYPE.id);
    header.putShort((short) 0); // reserved
    header.putLong(uuid.getMostSignificantBits());
    header.putLong(uuid.getLeastSignificantBits());
    header.putLong(gcsObjectSize);
    header.flip();
    try (FileChannel ch =
        FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
      while (header.hasRemaining()) ch.write(header);
      ByteBuffer body = ByteBuffer.wrap(deltaBytes);
      while (body.hasRemaining()) ch.write(body);
      ch.force(true);
    }
  }

  // ---------------------------------------------------------------------------
  // GCSIndexOutput
  // ---------------------------------------------------------------------------

  final class GCSIndexOutput extends IndexOutput
      implements CompressingDirectory.SizeReportingIndexOutput {

    private final byte[] compressBuffer = new byte[COMPRESSION_BLOCK_SIZE];
    private final LZ4.FastCompressionHashTable ht = new LZ4.FastCompressionHashTable();
    private final ByteBuffer preBuffer;
    private final AsyncGCSWriteHelper writeHelper;
    private ByteBuffer buffer;
    private final BytesOut blockDeltas = new BytesOut();
    private int prevBlockSize = BLOCK_SIZE_ESTIMATE;

    private final UUID uuid;
    private final Path offsetFilePath;
    private long filePos;
    private long gcsObjectSize;
    private final CRC32 crc = new CRC32();
    private boolean isOpen;

    GCSIndexOutput(String name, Path offsetFilePath) throws IOException {
      super("GCSIndexOutput(name=\"" + name + "\")", name);
      this.offsetFilePath = offsetFilePath;
      this.uuid = UUID.randomUUID();
      String blobName = uuid.toString();
      WriteChannel gcsChannel =
          storage.writer(BlobInfo.newBuilder(BlobId.of(bucket, blobName)).build());
      writeHelper = new AsyncGCSWriteHelper(bufferPool, gcsChannel);
      buffer = writeHelper.init();
      preBuffer = ByteBuffer.wrap(compressBuffer);
      if (useAsyncIO) {
        writeHelper.start(ioExec);
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
        // Write the local offset file: header + UUID + gcsObjectSize + delta-encoded block sizes.
        byte[] deltaBytes = blockDeltas.toByteArray();
        writeOffsetFile(offsetFilePath, filePos, uuid, gcsObjectSize, deltaBytes);
        // Pre-populate cache-node array for readers of this file.
        if (filePos > 0) {
          int blockCount = (int) (((filePos - 1) >> COMPRESSION_BLOCK_SHIFT) + 1);
          @SuppressWarnings({"unchecked", "rawtypes"})
          AtomicReference<BlockCache.Node>[] nodes = new AtomicReference[blockCount];
          for (int i = 0; i < blockCount; i++) nodes[i] = new AtomicReference<>();
          pendingNodes.put(getName(), nodes);
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

  final class GCSIndexInput extends IndexInput implements RandomAccessInput {

    private final boolean isClone;
    private final long length;
    private final long[] blockOffsets; // blockOffsets[i] = byte offset of block i in GCS object
    private final int blockCount;
    private final int lastBlockIdx;
    private final int lastBlockDecompressedLen;
    private final String blobName;

    private final AtomicReference<BlockCache.Node>[] accessMapped;

    private final long offset;
    private final long sliceLength;

    private long seekPos = -1;
    private long filePointer = 0;
    private ByteBuffer postBuffer = ByteBuffer.allocate(0);
    private int postBufferBaseline;
    private int currentBlockIdx = -1;
    private BlockCache.Node currentNode;

    // Root constructor: parses the offset file.
    @SuppressWarnings("unchecked")
    GCSIndexInput(
        String resourceDescription,
        Path offsetFile,
        AtomicReference<BlockCache.Node>[] sharedAccessMapped)
        throws IOException {
      super(resourceDescription);
      this.isClone = false;

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

      ByteBuffer hdr = ByteBuffer.wrap(header);
      length = hdr.getLong(0);
      int cBlockTypeId = hdr.get(8) & 0xff;
      if (cBlockTypeId != COMPRESSION_BLOCK_TYPE.id) {
        throw new IOException("unrecognized compression block type id: " + cBlockTypeId);
      }
      long uuidMsb = hdr.getLong(12);
      long uuidLsb = hdr.getLong(20);
      blobName = new UUID(uuidMsb, uuidLsb).toString();
      long gcsObjectSize = hdr.getLong(28);

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

      if (sharedAccessMapped != null) {
        if (sharedAccessMapped.length != blockCount) {
          throw new IllegalArgumentException("block count mismatch");
        }
        this.accessMapped = sharedAccessMapped;
      } else {
        @SuppressWarnings({"unchecked", "rawtypes"})
        AtomicReference<BlockCache.Node>[] local = new AtomicReference[blockCount];
        for (int i = 0; i < blockCount; i++) local[i] = new AtomicReference<>();
        AtomicReference<BlockCache.Node>[] existing =
            pendingNodes.putIfAbsent(offsetFile.getFileName().toString(), local);
        this.accessMapped = existing == null ? local : existing;
      }

      this.offset = 0;
      this.sliceLength = length;
    }

    // Clone / slice constructor.
    private GCSIndexInput(
        String resourceDescription, GCSIndexInput parent, long offset, long length) {
      super(resourceDescription);
      this.isClone = true;
      this.length = parent.length;
      this.blockOffsets = parent.blockOffsets;
      this.blockCount = parent.blockCount;
      this.lastBlockIdx = parent.lastBlockIdx;
      this.lastBlockDecompressedLen = parent.lastBlockDecompressedLen;
      this.blobName = parent.blobName;
      this.accessMapped = parent.accessMapped;
      this.offset = parent.offset + offset;
      this.seekPos = this.offset;
      this.sliceLength = length;
      this.postBuffer = ByteBuffer.allocate(0);
    }

    // ---------------------------------------------------------------------------
    // Cache interaction
    // ---------------------------------------------------------------------------

    private void unpinCurrent() {
      if (currentNode != null) {
        cache.unpin(currentNode);
        currentNode = null;
      }
    }

    /**
     * Issues a byte-range GET to GCS for the given compressed block and decompresses it. This is
     * called only on a cache miss.
     */
    private ByteBuffer supply(long pos, int compressedLen, int decompressedLen) throws IOException {
      byte[] compressed = new byte[compressedLen];
      ByteBuffer compBuf = ByteBuffer.wrap(compressed);
      try (ReadChannel reader = storage.reader(BlobId.of(bucket, blobName))) {
        reader.seek(pos);
        while (compBuf.hasRemaining()) reader.read(compBuf);
      }
      byte[] decompressed = new byte[decompressedLen + 7];
      CompressingDirectory.decompress(compressed, 0, decompressedLen, decompressed, 0);
      return ByteBuffer.wrap(decompressed, 0, decompressedLen);
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

      // Cache hit.
      BlockCache.Node cached = accessMapped[blockIdx].get();
      if (cached != null && cache.pin(cached)) {
        currentNode = cached;
        postBuffer = cached.buf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        postBuffer.clear().limit(decompressedLen);
        postBufferBaseline = 0;
        currentBlockIdx = blockIdx;
        return;
      }

      // Cache miss: GCS byte-range read + decompress.
      ByteBuffer heapBuf = supply(pos, compressedLen, decompressedLen);

      // Try to cache the decompressed block.
      BlockCache.Node node = cache.acquireNode();
      if (node != null) {
        node.buf.clear();
        node.buf.put(heapBuf.array(), heapBuf.arrayOffset() + heapBuf.position(), decompressedLen);
        accessMapped[blockIdx].set(node);
        currentNode = node;
        postBuffer = node.buf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        postBuffer.clear().limit(decompressedLen);
        postBufferBaseline = 0;
        currentBlockIdx = blockIdx;
        return;
      }

      // Serve uncached.
      currentNode = null;
      postBuffer = heapBuf;
      postBufferBaseline = heapBuf.position();
      heapBuf.order(ByteOrder.LITTLE_ENDIAN);
      currentBlockIdx = blockIdx;
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
      GCSIndexInput clone = (GCSIndexInput) super.clone();
      clone.unpinCurrent();
      return new GCSIndexInput(toString(), this, 0, sliceLength);
    }

    @Override
    public void close() throws IOException {
      unpinCurrent();
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

    byte[] toByteArray() {
      return Arrays.copyOf(baos.buf(), baos.count());
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
