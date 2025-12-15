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
package org.apache.solr.handler;

import static org.apache.solr.handler.ReplicationHandler.CHECKSUM;
import static org.apache.solr.handler.ReplicationHandler.CMD_GET_FILE;
import static org.apache.solr.handler.ReplicationHandler.COMMAND;
import static org.apache.solr.handler.ReplicationHandler.FILE;
import static org.apache.solr.handler.ReplicationHandler.FILE_STREAM;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.util.Random;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;

/**
 * Tests the replication file stream protocol emitted by DirectoryFileStream.write(...).
 * <p>
 * Validates that when the file size is an exact multiple of PACKET_SZ and checksum is enabled, the
 * stream ends with a single 0-length marker and does not include a checksum for the EOF marker.
 */
public class DirectoryFileStreamTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema_latest.xml");
  }

  @Test
  public void testDirectoryFileHandling() throws Exception {
    testWithPackets(2); //2 whole packets
    testWithPackets(2.5); //non-whole packets
  }

  public void testWithPackets(double packetCount) throws Exception {
    final SolrCore core = h.getCore();
    final int packetSize = ReplicationHandler.PACKET_SZ; // 1 MiB

    // Create a file in the active index directory with size that is an exact multiple of PACKET_SZ
    final String fileName = "replication_stream_exact_multiple-" + packetCount + ".bin";
    final int fileSize = (int)(packetCount * packetSize);

    final byte[] content = new byte[fileSize];
    new Random(17L).nextBytes(content);

    final DirectoryFactory df = core.getDirectoryFactory();
    final Directory dir = df.get(core.getIndexDir(), DirectoryFactory.DirContext.DEFAULT, null);
    try (IndexOutput out = dir.createOutput(fileName, DirectoryFactory.IOCONTEXT_NO_CACHE)) {
      out.writeBytes(content, fileSize);
    }

    // Build a replication request to stream this file with checksum enabled
    final SolrQueryRequest req =
        req(
            CommonParams.WT,
            FILE_STREAM,
            COMMAND,
            CMD_GET_FILE,
            FILE,
            fileName,
            CHECKSUM,
            "true");
    final SolrQueryResponse rsp = new SolrQueryResponse();

    final ReplicationHandler handler =
        (ReplicationHandler) core.getRequestHandler(ReplicationHandler.PATH);
    assertNotNull("Replication handler must be registered", handler);

    handler.handleRequestBody(req, rsp);

    final Object writerObj = rsp.getValues().get(FILE_STREAM);
    assertNotNull("Response should contain filestream writer", writerObj);
    assertTrue(
        "filestream must be a RawWriter", writerObj instanceof SolrCore.RawWriter);

    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ((SolrCore.RawWriter) writerObj).write(bos);
    final byte[] streamBytes = bos.toByteArray();
    assertTrue(streamBytes.length > 0);

    // Parse the protocol: [int length][optional long checksum][bytes]* ... [int 0] EOF
    final ByteArrayInputStream bais = new ByteArrayInputStream(streamBytes);
    final DataInputStream dis = new DataInputStream(bais);

    int totalData = 0;

    byte[] downloadedContent = new byte[fileSize];
    while (true) {
      final int len = dis.readInt();
      if (len == 0) {
        break;
      }
      // checksum present when CHECKSUM=true
      dis.readLong();
      final byte[] buf = new byte[len];
      dis.readFully(buf);
      System.arraycopy(buf, 0, downloadedContent, totalData, len);
      totalData += len;
    }

    // After EOF marker, there should be no extra bytes
    assertEquals("No trailing bytes expected after EOF marker", 0, dis.available());
    assertArrayEquals("Downloaded content must match original", content, downloadedContent);


    df.release(dir);
  }
}

