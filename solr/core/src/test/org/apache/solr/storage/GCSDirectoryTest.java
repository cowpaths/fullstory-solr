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

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.ExecutorUtil;

public class GCSDirectoryTest extends SolrTestCaseJ4 {

  private static final String BUCKET = "gcs-directory-test";

  private Storage storage;
  private BlockCache cache;
  private ExecutorService ioExec;
  private DirectBufferPool bufferPool;
  private GCSDirectory dir;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    storage = LocalStorageHelper.customOptions(false).getService();
    Path tmpDir = createTempDir();
    // Cache sized to hold a handful of blocks; intentionally smaller than the largest test files
    // so that cache-miss GCS reads are exercised.
    cache = new BlockCache(8L * COMPRESSION_BLOCK_SIZE, tmpDir.resolve("cache.tmp"));
    ioExec = ExecutorUtil.newMDCAwareCachedThreadPool("test-gcs-io");
    bufferPool = new DirectBufferPool(GCSDirectoryFactory.GCS_WRITE_BUFFER_SIZE, 4096, 1);
    dir =
        new GCSDirectory(
            tmpDir.resolve("index"), BUCKET, storage, cache, ioExec, false, bufferPool);
  }

  @Override
  public void tearDown() throws Exception {
    dir.close();
    cache.close();
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
      String name = "b" + size + ".dat";
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
    writeFile("rand.dat", data);
    assertArrayEquals(data, readFile("rand.dat", size));
    assertEquals(size, dir.fileLength("rand.dat"));
  }

  public void testListAll() throws IOException {
    writeFile("a.dat", randomBytes(random().nextInt(COMPRESSION_BLOCK_SIZE) + 1));
    writeFile("b.dat", randomBytes(random().nextInt(COMPRESSION_BLOCK_SIZE) + 1));
    String[] names = dir.listAll();
    Arrays.sort(names);
    assertArrayEquals(new String[] {"a.dat", "b.dat"}, names);
  }

  public void testDeleteFile() throws IOException {
    writeFile("todelete.dat", randomBytes(random().nextInt(COMPRESSION_BLOCK_SIZE) + 1));
    assertEquals(1, dir.listAll().length);
    dir.deleteFile("todelete.dat");
    assertEquals(0, dir.listAll().length);
  }

  public void testRename() throws IOException {
    int size = random().nextInt(3 * COMPRESSION_BLOCK_SIZE) + 1;
    byte[] data = randomBytes(size);
    writeFile("before.dat", data);
    dir.rename("before.dat", "after.dat");
    assertFalse(Arrays.asList(dir.listAll()).contains("before.dat"));
    assertArrayEquals(data, readFile("after.dat", size));
  }

  /** Spot-check random-access reads at arbitrary byte positions across blocks. */
  public void testRandomAccess() throws IOException {
    int size = 2 * COMPRESSION_BLOCK_SIZE + random().nextInt(3 * COMPRESSION_BLOCK_SIZE) + 1;
    byte[] data = randomBytes(size);
    writeFile("random.dat", data);
    try (IndexInput in = dir.openInput("random.dat", IOContext.DEFAULT)) {
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
    writeFile("reopen.dat", data);
    assertArrayEquals(data, readFile("reopen.dat", size));
    assertArrayEquals(data, readFile("reopen.dat", size));
  }

  /**
   * Verifies that getChecksum() tracks uncompressed bytes correctly: write a codec header + data +
   * footer, then confirm CodecUtil.checksumEntireFile() passes on read-back.
   */
  public void testChecksumRoundTrip() throws IOException {
    int dataSize = random().nextInt(3 * COMPRESSION_BLOCK_SIZE) + 1;
    try (IndexOutput out = dir.createOutput("checksum.dat", IOContext.DEFAULT)) {
      CodecUtil.writeHeader(out, "TestCodec", 1);
      out.writeBytes(randomBytes(dataSize), 0, dataSize);
      CodecUtil.writeFooter(out);
    }
    try (IndexInput in = dir.openInput("checksum.dat", IOContext.DEFAULT)) {
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
            ioExec,
            true,
            bufferPool);
    try {
      byte[] data = randomBytes(size);
      try (IndexOutput out = asyncDir.createOutput("async.dat", IOContext.DEFAULT)) {
        out.writeBytes(data, 0, data.length);
      }
      assertArrayEquals(data, readFileFrom(asyncDir, "async.dat", size));
    } finally {
      asyncDir.close();
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private void writeFile(String name, byte[] data) throws IOException {
    try (IndexOutput out = dir.createOutput(name, IOContext.DEFAULT)) {
      out.writeBytes(data, 0, data.length);
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
