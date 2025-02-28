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
package org.apache.solr.handler.component;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.LBHttp2SolrClient;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.core.CoreContainer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/** Tests specifying a custom ShardHandlerFactory */
public class TestTimeLimitedShardHandler extends SolrTestCaseJ4 {
  @Test
  public void testDetectionNoSlowShard() throws IOException {
    List<String> shards = new ArrayList<>();
    final int SHARD_COUNT = 512;
    for (int i = 0; i < SHARD_COUNT; i ++) {
      String shard = "http://solr-" + (i / 8 + 1) + ":8983/solr/coll_shard" + (i + 1) + "_replica_n" + (i + 1);
      shards.add(shard);
    }

    try (TestFixture fixture = buildTestFixture("solr-shardhandler-timeLimited.xml", Collections.emptyMap(), 100, -1)){
      org.apache.solr.handler.component.ShardHandler handler = fixture.factory.getShardHandler();
      org.apache.solr.handler.component.ShardRequest sreq = new org.apache.solr.handler.component.ShardRequest();
      sreq.actualShards = shards.toArray(new String[0]);
      for (String shard : shards) {
        handler.submit(sreq, shard, new ModifiableSolrParams());
      }

      org.apache.solr.handler.component.ShardResponse response = handler.takeCompletedIncludingErrors();
      assertEquals(SHARD_COUNT, response.getShardRequest().responses.size());

      assertNull(response.getException());
      assertTrue(fixture.slowNodeDetector.getSlowNodes().isEmpty());
    }
  }

//  @Test
//  public void testOneSlowShardBelowLimit() throws IOException {
//    List<String> shards = new ArrayList<>();
//    final int SHARD_COUNT = 16;
//    for (int i = 0; i < SHARD_COUNT; i ++) {
//      String shard = "http://solr-" + (i / 8 + 1) + ":8983/solr/coll_shard" + (i + 1) + "_replica_n" + (i + 1);
//      shards.add(shard);
//    }
//    try (TestFixture fixture = buildTestFixture("solr-shardhandler-timeLimited.xml", Map.of("solr-10:8983/solr/coll_shard_65_replica_n65", 800L), 100)){
//      org.apache.solr.handler.component.ShardHandler handler = fixture.factory.getShardHandler();
//      org.apache.solr.handler.component.ShardRequest sreq = new org.apache.solr.handler.component.ShardRequest();
//
//      sreq.actualShards = shards.toArray(new String[0]);
//      for (String shard : shards) {
//        handler.submit(sreq, shard, new ModifiableSolrParams());
//      }
//
//      org.apache.solr.handler.component.ShardResponse response = handler.takeCompletedIncludingErrors();
//      assertEquals(SHARD_COUNT, response.getShardRequest().responses.size());
//      assertNull(response.getException());
//    }
//  }

  @Test
  public void testDetectionSlowNodes() throws IOException {
    List<String> shards = new ArrayList<>();
    final int SHARD_COUNT = 512;
    Map<String, Long> latenciesByShard = new HashMap<>();
    for (int i = 0; i < SHARD_COUNT; i ++) {
      String shard = "http://solr-" + (i / 8 + 1) + ":8983/solr/coll_shard" + (i + 1) + "_replica_n" + (i + 1);
      shards.add(shard);
      if (i == 10 || i == 11) { //2 slow nodes
        latenciesByShard.put(shard, 12000L);
      }
    }
    try (TestFixture fixture = buildTestFixture("solr-shardhandler-timeLimited.xml", latenciesByShard, 500, -1)){
      org.apache.solr.handler.component.ShardHandler handler = fixture.factory.getShardHandler();
      org.apache.solr.handler.component.ShardRequest sreq = new org.apache.solr.handler.component.ShardRequest();


      sreq.actualShards = shards.toArray(new String[0]);
      for (String shard : shards) {
        handler.submit(sreq, shard, new ModifiableSolrParams());
      }

      org.apache.solr.handler.component.ShardResponse response = handler.takeCompletedIncludingErrors();
      assertEquals(SHARD_COUNT, response.getShardRequest().responses.size());

//      List<Throwable> exceptions = response.getShardRequest().responses.stream().filter(r -> r.getException() != null).map(ShardResponse::getException).collect(Collectors.toList());
      assertNull(response.getException());
      assertEquals(Set.of("http://solr-10:8983", "http://solr-11:8983"), fixture.slowNodeDetector.slowNodes);
    }
  }

//  @Test
//  public void testHalfSlowShards() throws IOException {
//    List<String> shards = new ArrayList<>();
//    final int SHARD_COUNT = 16;
//    Map<String, Long> latenciesByShard = new HashMap<>();
//    for (int i = 0; i < SHARD_COUNT; i ++) {
//      String shard = "http://solr-" + (i / 8 + 1) + ":8983/solr/coll_shard" + (i + 1) + "_replica_n" + (i + 1);
//      shards.add(shard);
//      if (i % 2 == 1) {
//        latenciesByShard.put(shard, 2000L);
//      }
//    }
//    try (TestFixture fixture = buildTestFixture("solr-shardhandler-timeLimited.xml", latenciesByShard, 500)){
//      org.apache.solr.handler.component.ShardHandler handler = fixture.factory.getShardHandler();
//      org.apache.solr.handler.component.ShardRequest sreq = new org.apache.solr.handler.component.ShardRequest();
//
//
//      sreq.actualShards = shards.toArray(new String[0]);
//      for (String shard : shards) {
//        handler.submit(sreq, shard, new ModifiableSolrParams());
//      }
//
//      org.apache.solr.handler.component.ShardResponse response = handler.takeCompletedIncludingErrors();
//      assertEquals(SHARD_COUNT, response.getShardRequest().responses.size());
//
//      List<Throwable> exceptions = response.getShardRequest().responses.stream().filter(r -> r.getException() != null).map(ShardResponse::getException).collect(Collectors.toList());
//      assertEquals(SHARD_COUNT/2, exceptions.size());
//      assertEquals(CancellationException.class, exceptions.get(0).getClass());
//    }
//  }



  private static TestFixture buildTestFixture(String configFile, Map<String, Long> latenciesByUrl, long defaultLatency, long slowNodeTtl) {
    final Path home = SolrTestCaseJ4.TEST_PATH();
    CoreContainer cc = CoreContainer.createAndLoad(home, home.resolve(configFile));
    org.apache.solr.handler.component.TimeLimitedHttpShardHandlerFactory factory = (TimeLimitedHttpShardHandlerFactory) cc.getShardHandlerFactory();
    SlowNodeDetector slowNodeDetector = SlowNodeDetector.build(slowNodeTtl);
    factory.setSlowNodeDetector(slowNodeDetector);
    Http2SolrClient client = new Http2SolrClient.Builder().build();

    ExecutorService executor = ExecutorUtil.newMDCAwareCachedThreadPool(TestTimeLimitedShardHandler.class.getSimpleName());
    factory.loadbalancer = new LBHttp2SolrClient(client, new String[0]) {
      @Override
      public CompletableFuture<Rsp> requestAsync(Req req) {
        long latency = latenciesByUrl.getOrDefault(req.getServers().get(0), defaultLatency);
        return CompletableFuture.supplyAsync(() -> {
          try {
            Thread.sleep(latency);
          } catch (InterruptedException e) {
            //OK
          }
          return new Rsp();
        }, executor);
      }
    };
    return new TestFixture(cc, factory, client, executor, slowNodeDetector);
  }
}

class TestFixture implements Closeable {
  CoreContainer cc;
  TimeLimitedHttpShardHandlerFactory factory;
  Http2SolrClient client;
  ExecutorService executorService;
  SlowNodeDetector slowNodeDetector;

  TestFixture(CoreContainer cc, TimeLimitedHttpShardHandlerFactory factory, Http2SolrClient client, ExecutorService executorService, SlowNodeDetector slowNodeDetector) {
    this.cc = cc;
    this.factory = factory;
    this.client = client;
    this.executorService = executorService;
    this.slowNodeDetector = slowNodeDetector;
  }


  @Override
  public void close() throws IOException {
    if (factory != null) factory.close();
    if (cc != null) cc.shutdown();
    if (client != null) client.close();
    if (executorService != null) {
      ExecutorUtil.shutdownAndAwaitTermination(executorService);
    }
  }
}
