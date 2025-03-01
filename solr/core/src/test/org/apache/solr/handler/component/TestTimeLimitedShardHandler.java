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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
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
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * This ensures stats are collected and reported by ShardHandler to the SlowNodeDetector.
   * <p>
   * For through testing on the SlowNodeDetector, please refer to the corresponding TestSlowNodeDetector
   */
  @Test
  public void testDetectionSlowNodes() throws IOException {
    List<String> shards = new ArrayList<>();
    final int SHARD_COUNT = 512;
    Map<String, Long> latenciesByShard = new HashMap<>();
    for (int i = 0; i < SHARD_COUNT; i ++) {
      int serverIndex = (i / 8 + 1);
      String shard = "http://solr-" + serverIndex + ":8983/solr/coll_shard" + (i + 1) + "_replica_n" + (i + 1);
      shards.add(shard);
      if (serverIndex == 10 || serverIndex == 11) { //2 slow nodes
        latenciesByShard.put(shard, 2000L);
      }
    }
    try (TestFixture fixture = buildTestFixture("solr-shardhandler-timeLimited.xml", latenciesByShard, 100)){
      org.apache.solr.handler.component.ShardHandler handler = fixture.factory.getShardHandler();
      org.apache.solr.handler.component.ShardRequest sreq = new org.apache.solr.handler.component.ShardRequest();


      sreq.actualShards = shards.toArray(new String[0]);
      for (String shard : shards) {
        handler.submit(sreq, shard, new ModifiableSolrParams());
      }

      org.apache.solr.handler.component.ShardResponse response = handler.takeCompletedIncludingErrors();
      assertEquals(SHARD_COUNT, response.getShardRequest().responses.size());

//      List<Throwable> exceptions = response.getShardRequest().responses.stream().filter(r -> r.getException() != null).map(ShardResponse::getException).collect(Collectors.toList());
      assertNull(response.getException()); //no exception, since the slow nodes are not detected yet before execution of this shard request
      assertEquals(Set.of("solr-10:8983", "solr-11:8983"), fixture.slowNodeDetector.getSlowNodes());
    }
  }

  /**
   * This ensures handler execution should not be affected if there are no slow nodes tracked
   */
  @Test
  public void testExecutionNoSlowNodes() throws IOException {
    List<String> shards = new ArrayList<>();
    final int SHARD_COUNT = 512;
    Map<String, Long> latenciesByShard = new HashMap<>();
    for (int i = 0; i < SHARD_COUNT; i ++) {
      int serverIndex = (i / 8 + 1);
      String shard = "http://solr-" + serverIndex + ":8983/solr/coll_shard" + (i + 1) + "_replica_n" + (i + 1);
      shards.add(shard);
    }
    try (TestFixture fixture = buildTestFixture("solr-shardhandler-timeLimited.xml", latenciesByShard, 100)){
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


  /**
   * This ensures slow node execution would time out if it has been detected as slow before
   */
  @Test
  public void testExecutionSlowNodes() throws IOException {
    List<String> shards = new ArrayList<>();
    final int SHARD_COUNT = 512;
    Map<String, Long> latenciesByShard = new HashMap<>();
    for (int i = 0; i < SHARD_COUNT; i ++) {
      int serverIndex = (i / 8 + 1);
      String shard = "http://solr-" + serverIndex + ":8983/solr/coll_shard" + (i + 1) + "_replica_n" + (i + 1);
      if (serverIndex == 10 || serverIndex == 11) { //2 slow nodes
        latenciesByShard.put(shard, 5000L);
      }
      shards.add(shard);
    }
    try (TestFixture fixture = buildTestFixture("solr-shardhandler-timeLimited.xml", latenciesByShard, 100)){
      org.apache.solr.handler.component.ShardHandler handler = fixture.factory.getShardHandler();
      org.apache.solr.handler.component.ShardRequest sreq = new org.apache.solr.handler.component.ShardRequest();
      fixture.slowNodeDetector.setSlowNodes(Set.of("solr-10:8983", "solr-11:8983")); //simulate slow nodes in previous run


      sreq.actualShards = shards.toArray(new String[0]);
      for (String shard : shards) {
        handler.submit(sreq, shard, new ModifiableSolrParams());
      }

      org.apache.solr.handler.component.ShardResponse response = handler.takeCompletedIncludingErrors();
      assertEquals(SHARD_COUNT, response.getShardRequest().responses.size());

      assertTrue(response.getException() instanceof CancellationException);
      List<Throwable> exceptions = response.getShardRequest().responses.stream().filter(r -> r.getException() != null).map(ShardResponse::getException).collect(Collectors.toList());
      assertEquals(2 * 8, exceptions.size()); //2 slow nodes, 8 replicas per node

      assertEquals(Set.of("solr-10:8983", "solr-11:8983"), fixture.slowNodeDetector.getSlowNodes());
    }
  }



  private static TestFixture buildTestFixture(String configFile, Map<String, Long> latenciesByUrl, long defaultLatency) {
    final Path home = SolrTestCaseJ4.TEST_PATH();
    CoreContainer cc = CoreContainer.createAndLoad(home, home.resolve(configFile));
    org.apache.solr.handler.component.TimeLimitedHttpShardHandlerFactory factory = (TimeLimitedHttpShardHandlerFactory) cc.getShardHandlerFactory();
    SlowNodeDetector slowNodeDetector = new SlowNodeDetector.Builder().withSlowNodeTtl(-1).withSlowLatencyThreshold(1000).build();
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
            Thread.currentThread().interrupt();
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
