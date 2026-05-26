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
    cache = new BlockCache(16L * COMPRESSION_BLOCK_SIZE, tmpDir.resolve("cache.tmp"));
    ioExec = ExecutorUtil.newMDCAwareCachedThreadPool("test-gcs-io");
    bufferPool = new DirectBufferPool(GCSDirectoryFactory.GCS_WRITE_BUFFER_SIZE, 4096, 1);
    dir = new GCSDirectory(tmpDir.resolve("index"), BUCKET, storage, cache, ioExec, false, bufferPool);
  }

  @Override
  public void tearDown() throws Exception {
    dir.close();
    cache.close();
    ExecutorUtil.shutdownAndAwaitTermination(ioExec);
    super.tearDown();
  }

  /** Write and read back a file smaller than one compression block. */
  public void testSmallFileRoundTrip() throws IOException {
    byte[] data = randomBytes(100);
    writeFile("small.dat", data);
    assertArrayEquals(data, readFile("small.dat", data.length));
    assertEquals(data.length, dir.fileLength("small.dat"));
  }

  /** Write and read back a file spanning several compression blocks. */
  public void testMultiBlockRoundTrip() throws IOException {
    byte[] data = randomBytes(5 * COMPRESSION_BLOCK_SIZE + 300);
    writeFile("multi.dat", data);
    assertArrayEquals(data, readFile("multi.dat", data.length));
    assertEquals(data.length, dir.fileLength("multi.dat"));
  }

  /** File whose size is an exact multiple of the compression block size. */
  public void testExactBlockBoundaryRoundTrip() throws IOException {
    byte[] data = randomBytes(3 * COMPRESSION_BLOCK_SIZE);
    writeFile("exact.dat", data);
    assertArrayEquals(data, readFile("exact.dat", data.length));
    assertEquals(data.length, dir.fileLength("exact.dat"));
  }

  public void testListAll() throws IOException {
    writeFile("a.dat", randomBytes(50));
    writeFile("b.dat", randomBytes(50));
    String[] names = dir.listAll();
    Arrays.sort(names);
    assertArrayEquals(new String[] {"a.dat", "b.dat"}, names);
  }

  public void testDeleteFile() throws IOException {
    writeFile("todelete.dat", randomBytes(50));
    assertEquals(1, dir.listAll().length);
    dir.deleteFile("todelete.dat");
    assertEquals(0, dir.listAll().length);
  }

  public void testRename() throws IOException {
    byte[] data = randomBytes(200);
    writeFile("before.dat", data);
    dir.rename("before.dat", "after.dat");
    assertFalse(Arrays.asList(dir.listAll()).contains("before.dat"));
    assertArrayEquals(data, readFile("after.dat", data.length));
  }

  /** Spot-check random-access reads at arbitrary byte positions across blocks. */
  public void testRandomAccess() throws IOException {
    byte[] data = randomBytes(3 * COMPRESSION_BLOCK_SIZE + 500);
    writeFile("random.dat", data);
    try (IndexInput in = dir.openInput("random.dat", IOContext.DEFAULT)) {
      RandomAccessInput rai = in.randomAccessSlice(0, in.length());
      for (int i = 0; i < 200; i++) {
        int pos = random().nextInt(data.length);
        assertEquals("byte mismatch at pos " + pos, data[pos], rai.readByte(pos));
      }
    }
  }

  /** Second open of the same file should read identically (exercises shared pendingNodes). */
  public void testReopenFile() throws IOException {
    byte[] data = randomBytes(2 * COMPRESSION_BLOCK_SIZE + 100);
    writeFile("reopen.dat", data);
    assertArrayEquals(data, readFile("reopen.dat", data.length));
    assertArrayEquals(data, readFile("reopen.dat", data.length));
  }

  /** Async write path smoke check. */
  public void testAsyncWriteRoundTrip() throws IOException {
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
      byte[] data = randomBytes(4 * COMPRESSION_BLOCK_SIZE + 77);
      try (IndexOutput out = asyncDir.createOutput("async.dat", IOContext.DEFAULT)) {
        out.writeBytes(data, 0, data.length);
      }
      assertArrayEquals(data, readFileFrom(asyncDir, "async.dat", data.length));
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
