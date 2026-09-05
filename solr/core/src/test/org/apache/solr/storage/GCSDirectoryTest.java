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

import static org.apache.solr.storage.CompressingDirectory.COMPRESSION_BLOCK_SIZE;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.common.util.ExecutorUtil;

public class GCSDirectoryTest extends SolrTestCaseJ4 {

  private static final String BUCKET = EnvUtils.getProperty("solr.gcsDirectory.bucket");

  private Storage storage;
  private BlockCache cache;
  private ExecutorService ioExec;
  private DirectBufferPool bufferPool;
  private GCSDirectory dir;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    String factoryClass = EnvUtils.getProperty("solr.directoryFactory");
    if (factoryClass != null) {
      Class<?> clazz = Class.forName(factoryClass);
      GCSDirectoryFactory factory;
      if (GCSDirectoryFactory.class.isAssignableFrom(clazz)) {
        factory = (GCSDirectoryFactory) clazz.getDeclaredConstructor().newInstance();
      } else {
        factory = new LocalGCSDirectoryFactory();
      }
      storage = factory.initStorage(new ModifiableSolrParams());
    } else {
      storage = new LocalGCSDirectoryFactory().initStorage(new ModifiableSolrParams());
    }
    Path tmpDir = createTempDir();
    // Cache sized to hold a handful of blocks; intentionally smaller than the largest test files
    // so that cache-miss GCS reads are exercised.
    cache = new BlockCache(8L * COMPRESSION_BLOCK_SIZE, tmpDir.resolve("cache.tmp"));
    ioExec = ExecutorUtil.newMDCAwareCachedThreadPool("test-gcs-io");
    bufferPool = new DirectBufferPool(GCSDirectoryFactory.GCS_WRITE_BUFFER_SIZE, 4096, 1);
    dir =
        new GCSDirectory(
            tmpDir.resolve("index"),
            BUCKET,
            storage,
            cache,
            new java.util.concurrent.Semaphore(Integer.MAX_VALUE),
            ioExec,
            false,
            bufferPool,
            null,
            null);
  }

  @Override
  public void tearDown() throws Exception {
    dir.close();
    cache.close();
    storage.close();
    ExecutorUtil.shutdownAndAwaitTermination(ioExec);
    super.tearDown();
  }

  /**
   * Deterministic sweep of raw-data sizes at 0, ±1 around each compression-block boundary up to
   * several blocks, and ±1 around the GCS write-buffer boundary. With random (incompressible) data
   * the compressed size closely tracks the raw size, so this also exercises the region where
   * compressed data crosses a GCS buffer flush.
   */
  public void testRoundTripAtBlockBoundaries() throws IOException {
    int buf = GCSDirectoryFactory.GCS_WRITE_BUFFER_SIZE;
    int[] sizes = {
      0,
      1,
      COMPRESSION_BLOCK_SIZE - 1,
      COMPRESSION_BLOCK_SIZE,
      COMPRESSION_BLOCK_SIZE + 1,
      2 * COMPRESSION_BLOCK_SIZE - 1,
      2 * COMPRESSION_BLOCK_SIZE,
      2 * COMPRESSION_BLOCK_SIZE + 1,
      5 * COMPRESSION_BLOCK_SIZE - 1,
      5 * COMPRESSION_BLOCK_SIZE,
      5 * COMPRESSION_BLOCK_SIZE + 1,
      buf - 1,
      buf,
      buf + 1,
    };
    for (int size : sizes) {
      byte[] data = randomBytes(size);
      String name = "_b" + size + ".dat";
      writeFile(name, data);
      assertArrayEquals("data mismatch at size=" + size, data, readFile(name, size));
      assertEquals("fileLength mismatch at size=" + size, size, dir.fileLength(name));
      dir.deleteFile(name);
    }
  }

  /**
   * Randomized file size to exercise variation in where compressed blocks happen to fall relative
   * to GCS write-buffer boundaries — not controllable deterministically since it depends on the
   * compressed block sizes produced by LZ4.
   */
  public void testRandomSizeRoundTrip() throws IOException {
    int size =
        COMPRESSION_BLOCK_SIZE + random().nextInt(2 * GCSDirectoryFactory.GCS_WRITE_BUFFER_SIZE);
    byte[] data = randomBytes(size);
    writeFile("_rand.dat", data);
    assertArrayEquals(data, readFile("_rand.dat", size));
    assertEquals(size, dir.fileLength("_rand.dat"));
  }

  public void testListAll() throws IOException {
    writeFile("_a.dat", randomBytes(random().nextInt(COMPRESSION_BLOCK_SIZE) + 1));
    writeFile("_b.dat", randomBytes(random().nextInt(COMPRESSION_BLOCK_SIZE) + 1));
    String[] names = dir.listAll();
    Arrays.sort(names);
    assertArrayEquals(new String[] {"_a.dat", "_b.dat"}, names);
  }

  public void testDeleteFile() throws IOException {
    writeFile("_todelete.dat", randomBytes(random().nextInt(COMPRESSION_BLOCK_SIZE) + 1));
    assertEquals(1, dir.listAll().length);
    dir.deleteFile("_todelete.dat");
    assertEquals(0, dir.listAll().length);
  }

  public void testRename() throws IOException {
    int size = random().nextInt(3 * COMPRESSION_BLOCK_SIZE) + 1;
    byte[] data = randomBytes(size);
    writeFile("_before.dat", data);
    dir.rename("_before.dat", "_after.dat");
    assertFalse(Arrays.asList(dir.listAll()).contains("_before.dat"));
    assertArrayEquals(data, readFile("_after.dat", size));
  }

  /** Spot-check random-access reads at arbitrary byte positions across blocks. */
  public void testRandomAccess() throws IOException {
    int size = 2 * COMPRESSION_BLOCK_SIZE + random().nextInt(3 * COMPRESSION_BLOCK_SIZE) + 1;
    byte[] data = randomBytes(size);
    writeFile("_random.dat", data);
    try (IndexInput in = dir.openInput("_random.dat", IOContext.DEFAULT)) {
      RandomAccessInput rai = in.randomAccessSlice(0, in.length());
      for (int i = 0; i < 200; i++) {
        int pos = random().nextInt(size);
        assertEquals("byte mismatch at pos " + pos, data[pos], rai.readByte(pos));
      }
    }
  }

  /** Second open of the same file should read identically (exercises shared pendingNodes). */
  public void testReopenFile() throws IOException {
    int size = COMPRESSION_BLOCK_SIZE + random().nextInt(2 * COMPRESSION_BLOCK_SIZE) + 1;
    byte[] data = randomBytes(size);
    writeFile("_reopen.dat", data);
    assertArrayEquals(data, readFile("_reopen.dat", size));
    assertArrayEquals(data, readFile("_reopen.dat", size));
  }

  /**
   * Verifies that getChecksum() tracks uncompressed bytes correctly: write a codec header + data +
   * footer, then confirm CodecUtil.checksumEntireFile() passes on read-back.
   */
  public void testChecksumRoundTrip() throws IOException {
    int dataSize = random().nextInt(3 * COMPRESSION_BLOCK_SIZE) + 1;
    try (IndexOutput out = dir.createOutput("_checksum.dat", IOContext.DEFAULT)) {
      CodecUtil.writeHeader(out, "TestCodec", 1);
      out.writeBytes(randomBytes(dataSize), 0, dataSize);
      CodecUtil.writeFooter(out);
    }
    try (IndexInput in = dir.openInput("_checksum.dat", IOContext.DEFAULT)) {
      CodecUtil.checksumEntireFile(in); // throws CorruptIndexException on mismatch
    }
  }

  /** Async write path smoke check. */
  public void testAsyncWriteRoundTrip() throws IOException {
    int size = COMPRESSION_BLOCK_SIZE + random().nextInt(4 * COMPRESSION_BLOCK_SIZE) + 1;
    GCSDirectory asyncDir =
        new GCSDirectory(
            createTempDir().resolve("async-index"),
            BUCKET,
            storage,
            cache,
            new java.util.concurrent.Semaphore(Integer.MAX_VALUE),
            ioExec,
            true,
            bufferPool,
            null,
            null);
    try {
      byte[] data = randomBytes(size);
      try (IndexOutput out = asyncDir.createOutput("_async.dat", IOContext.DEFAULT)) {
        out.writeBytes(data, 0, data.length);
      }
      assertArrayEquals(data, readFileFrom(asyncDir, "_async.dat", size));
    } finally {
      asyncDir.close();
    }
  }

  /**
   * Directly tests the unclosedStreams accumulation hypothesis by bypassing GCSDirectory entirely
   * and probing GrpcStorageImpl internals.
   *
   * <p>The hypothesis: gRPC's blocking server-streaming iterator calls {@code request(MAX_VALUE)}
   * upfront, so GCS streams the entire object. On the first {@code iter.next()} call,
   * waitAndDrain() processes ALL already-queued tasks, parsing every received ReadObjectResponse
   * (adding each to unclosedStreams). After consuming only the first response, seeking away cancels
   * the stream — leaving all pre-parsed extras stuck.
   *
   * <p>The test directly opens a ReadChannel, reads a tiny amount, checks unclosedStreams before
   * and after close to distinguish two cases:
   *
   * <ul>
   *   <li>If entries appear after read but before close → pre-parsing is happening; they are stuck
   *       until the Storage is closed (confirming the accumulation mechanism).
   *   <li>If entries appear then disappear on channel close → close() drains them (original
   *       supply()-close fix would have been sufficient).
   *   <li>If entries never appear → parse() is fully lazy (no accumulation regardless of seeks).
   * </ul>
   *
   * <p>Run with -Psolr.directoryFactory=org.apache.solr.storage.ADCGCSDirectoryFactory and a bucket
   * containing at least one object larger than 2 MB (a few gRPC response chunks). Skipped
   * automatically when zero-copy is not active.
   */
  public void testUnclosedStreamsDirectProbe() throws Exception {
    // ---- resolve unclosedStreams via reflection ----
    Field rclmField;
    try {
      rclmField = storage.getClass().getDeclaredField("responseContentLifecycleManager");
    } catch (NoSuchFieldException e) {
      System.out.println("SKIP: " + storage.getClass().getName() + " has no rclm field");
      return;
    }
    rclmField.setAccessible(true);
    Object rclm = rclmField.get(storage);
    System.out.println("rclm class: " + rclm.getClass().getName());

    Field unclosedStreamsField;
    try {
      unclosedStreamsField = rclm.getClass().getDeclaredField("unclosedStreams");
    } catch (NoSuchFieldException e) {
      System.out.println(
          "SKIP: rclm " + rclm.getClass().getName() + " has no unclosedStreams (noop path)");
      return;
    }
    unclosedStreamsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<Object, Object> unclosedStreams = (Map<Object, Object>) unclosedStreamsField.get(rclm);

    // ---- write a large file: enough blocks that compressed data > several gRPC chunks (2 MB ea)
    // ----
    // ~200 * 256 KB = ~50 MB compressed (incompressible random data, so ~50 MB on wire).
    // GCS returns 50 MB / 2 MB = ~25 ReadObjectResponse messages for a no-limit request.
    int blockCount = 200;
    byte[] data = randomBytes(blockCount * COMPRESSION_BLOCK_SIZE);
    writeFile("_probe.dat", data);

    System.out.println("zeroCopyReady class: " + rclm.getClass().getSimpleName());
    System.out.println("baseline unclosedStreams.size() = " + unclosedStreams.size());

    // ---- read block 0 (forces a GCS fetch since cache is cold) ----
    // We use sequential IndexInput.readByte() rather than randomAccessSlice so we can
    // observe the state right after the first GCS supply() call, before any seek.
    try (IndexInput in = dir.openInput("_probe.dat", IOContext.DEFAULT)) {
      // Read one byte at position 0; this triggers supply() for block 0 and opens a gRPC stream
      // for the entire object (no readLimit). waitAndDrain() will process ALL queued tasks.
      in.readByte();
      System.out.println(
          "After readByte(0) [block 0 fetched from GCS]: unclosedStreams.size() = "
              + unclosedStreams.size());

      // Force a second block fetch to trigger a seek (stream cancel + reopen).
      // Seek past the end of block 0.
      in.seek(2L * COMPRESSION_BLOCK_SIZE);
      in.readByte();
      System.out.println(
          "After seek+readByte(block 2) [stream cancelled+reopened]: unclosedStreams.size() = "
              + unclosedStreams.size());
    }
    System.out.println(
        "After IndexInput.close(): unclosedStreams.size() = " + unclosedStreams.size());

    // ---- also test ReadChannel directly (no GCSDirectory layer) ----
    // Write a raw blob that we can read with a plain ReadChannel.
    String testBlob = "unclosed-streams-probe-" + System.currentTimeMillis();
    byte[] blobData = randomBytes(10 * 1024 * 1024); // 10 MB; expect ~5 ReadObjectResponse chunks
    storage.create(
        com.google.cloud.storage.BlobInfo.newBuilder(BUCKET, testBlob).build(), blobData);
    try {
      System.out.println("-- direct ReadChannel probe (10 MB blob) --");
      int before = unclosedStreams.size();

      com.google.cloud.ReadChannel ch =
          storage.reader(com.google.cloud.storage.BlobId.of(BUCKET, testBlob));
      ch.setChunkSize(3098);
      ch.seek(0);
      java.nio.ByteBuffer tiny = java.nio.ByteBuffer.allocate(1024);
      ch.read(tiny);
      System.out.println(
          "After 1 KB read (no seek yet): unclosedStreams.size() = "
              + unclosedStreams.size()
              + "  (delta="
              + (unclosedStreams.size() - before)
              + ")");

      ch.seek(9 * 1024 * 1024); // seek near end — cancels old stream
      ch.read(tiny.clear());
      System.out.println(
          "After seek+read (stream cancelled): unclosedStreams.size() = "
              + unclosedStreams.size()
              + "  (delta="
              + (unclosedStreams.size() - before)
              + ")");

      ch.close();
      System.out.println(
          "After ch.close(): unclosedStreams.size() = "
              + unclosedStreams.size()
              + "  (delta="
              + (unclosedStreams.size() - before)
              + ")");
    } finally {
      storage.delete(com.google.cloud.storage.BlobId.of(BUCKET, testBlob));
    }
  }

  /**
   * Stress reproduction for unclosedStreams accumulation.
   *
   * <p>Pattern per thread: seek → read small amount (opens gRPC stream, window opens) → sleep (gRPC
   * I/O thread pre-parses more responses into unclosedStreams) → seek again (cancels stream,
   * leaving pre-parsed entries permanently stuck). Repeat with many concurrent threads.
   *
   * <p>Run with -Psolr.directoryFactory=org.apache.solr.storage.ADCGCSDirectoryFactory and a real
   * bucket. Skipped automatically when not using gRPC storage.
   */
  public void testUnclosedStreamsStress() throws Exception {
    Field rclmField;
    try {
      rclmField = storage.getClass().getDeclaredField("responseContentLifecycleManager");
    } catch (NoSuchFieldException e) {
      System.out.println("SKIP: " + storage.getClass().getName() + " has no rclm field");
      return;
    }
    rclmField.setAccessible(true);
    Object rclm = rclmField.get(storage);
    Field unclosedStreamsField;
    try {
      unclosedStreamsField = rclm.getClass().getDeclaredField("unclosedStreams");
    } catch (NoSuchFieldException e) {
      System.out.println("SKIP: noop rclm (zero-copy not active)");
      return;
    }
    unclosedStreamsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<Object, Object> unclosedStreams = (Map<Object, Object>) unclosedStreamsField.get(rclm);

    // 50 MB: ~25 gRPC response messages at 2 MB each — enough to pre-parse several per sleep window
    int blobBytes = 50 * 1024 * 1024;
    String blobName = "unclosed-streams-stress-" + System.currentTimeMillis();
    storage.create(
        com.google.cloud.storage.BlobInfo.newBuilder(BUCKET, blobName).build(),
        randomBytes(blobBytes));

    try {
      int numThreads = 16;
      long runMillis = 2 * 60 * 1000L;
      long deadline = System.currentTimeMillis() + runMillis;
      AtomicInteger maxUnclosed = new AtomicInteger(0);
      CountDownLatch done = new CountDownLatch(numThreads);
      ExecutorService exec = Executors.newFixedThreadPool(numThreads);

      for (int t = 0; t < numThreads; t++) {
        exec.submit(
            () -> {
              ThreadLocalRandom rng = ThreadLocalRandom.current();
              com.google.cloud.ReadChannel ch =
                  storage.reader(com.google.cloud.storage.BlobId.of(BUCKET, blobName));
              try {
                ByteBuffer buf = ByteBuffer.allocate(4096);
                while (System.currentTimeMillis() < deadline) {
                  // Stay well away from EOF so the stream has many remaining messages
                  long pos = rng.nextLong(blobBytes / 2);
                  ch.seek(pos);
                  buf.clear();
                  ch.read(buf); // opens gRPC stream, consumes first response, opens flow window

                  // Let gRPC I/O thread pre-parse more responses while we sleep
                  Thread.sleep(rng.nextInt(50, 150));

                  int cur = unclosedStreams.size();
                  int prev = maxUnclosed.getAndUpdate(m -> Math.max(m, cur));
                  if (cur > prev) {
                    System.out.printf(
                        "[%5.1fs] new peak unclosedStreams.size() = %d%n",
                        (System.currentTimeMillis() - (deadline - runMillis)) / 1000.0, cur);
                  }
                  // Next seek will cancel this stream, leaving pre-parsed entries stuck
                }
              } catch (Exception e) {
                System.err.println("stress thread error: " + e);
              } finally {
                try {
                  ch.close();
                } catch (Exception ignored) {
                }
                done.countDown();
              }
            });
      }

      // Reporter thread: print current size every 5 s
      Thread reporter =
          new Thread(
              () -> {
                try {
                  while (!Thread.currentThread().isInterrupted()
                      && System.currentTimeMillis() < deadline) {
                    Thread.sleep(5000);
                    System.out.printf(
                        "[%5.1fs] unclosedStreams.size() = %d  (peak=%d)%n",
                        (System.currentTimeMillis() - (deadline - runMillis)) / 1000.0,
                        unclosedStreams.size(),
                        maxUnclosed.get());
                  }
                } catch (InterruptedException ignored) {
                }
              });
      reporter.setDaemon(true);
      reporter.start();

      done.await();
      reporter.interrupt();
      ExecutorUtil.shutdownAndAwaitTermination(exec);
      System.out.println("Peak unclosedStreams.size()   = " + maxUnclosed.get());
      System.out.println("Final unclosedStreams.size()  = " + unclosedStreams.size());

      Thread.sleep(5000);
      System.out.println("Final2 unclosedStreams.size() = " + unclosedStreams.size());

      storage.delete(com.google.cloud.storage.BlobId.of(BUCKET, blobName));
      storage.close();
      Thread.sleep(5000);
      System.out.println("After storage.close()         = " + unclosedStreams.size());
    } finally {
      // best-effort cleanup in case the block above threw before delete
      try {
        storage.delete(com.google.cloud.storage.BlobId.of(BUCKET, blobName));
      } catch (Exception ignored) {
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private void writeFile(String name, byte[] data) throws IOException {
    try (IndexOutput out = dir.createOutput(name, IOContext.DEFAULT)) {
      out.writeBytes(data, 0, data.length);
    }
    if (random().nextBoolean()) {
      dir.sync(List.of(name));
    }
  }

  private byte[] readFile(String name, int length) throws IOException {
    return readFileFrom(dir, name, length);
  }

  private static byte[] readFileFrom(GCSDirectory d, String name, int length) throws IOException {
    byte[] result = new byte[length];
    try (IndexInput in = d.openInput(name, IOContext.DEFAULT)) {
      in.readBytes(result, 0, length);
    }
    return result;
  }

  private byte[] randomBytes(int len) {
    byte[] b = new byte[len];
    random().nextBytes(b);
    return b;
  }
}
