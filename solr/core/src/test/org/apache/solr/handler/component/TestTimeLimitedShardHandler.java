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
import org.apache.solr.client.solrj.impl.LBSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;

/** Tests specifying a custom ShardHandlerFactory */
public class TestTimeLimitedShardHandler extends SolrTestCaseJ4 {

  @Test
  public void testNoSlowShard() {
    final Path home = TEST_PATH();
    CoreContainer cc = null;
    org.apache.solr.handler.component.ShardHandlerFactory factory = null;
    Http2SolrClient client = null;
    try {
      cc =
          CoreContainer.createAndLoad(
              home, home.resolve("solr-shardhandler-timeLimited.xml"));
      factory = cc.getShardHandlerFactory();

      // test that factory is HttpShardHandlerFactory with expected url reserve fraction
      assertTrue(factory instanceof org.apache.solr.handler.component.TimeLimitedHttpShardHandlerFactory);


      Map<String, Long> latenciesByUrl = Map.of();
      long defaultLatency = 100; //100 ms by default

      final org.apache.solr.handler.component.TimeLimitedHttpShardHandlerFactory testFactory =
          (org.apache.solr.handler.component.TimeLimitedHttpShardHandlerFactory) factory;
      client = new Http2SolrClient.Builder().build();
      testFactory.loadbalancer = new LBHttp2SolrClient(client, new String[0]) {
        @Override
        public CompletableFuture<Rsp> requestAsync(Req req) {
          long latency = latenciesByUrl.getOrDefault(req.getServers().get(0), defaultLatency);
          return CompletableFuture.supplyAsync(() -> {
            try {
              Thread.sleep(latency);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            return new Rsp();
          });
        }
      };

      org.apache.solr.handler.component.ShardHandler handler = testFactory.getShardHandler();

      org.apache.solr.handler.component.ShardRequest sreq = new org.apache.solr.handler.component.ShardRequest();
      List<String> shards = new ArrayList<>();
      for (int i = 0; i < 1024; i ++) {
        String shard = "http://solr-" + (i / 8 + 1) + ":8983/solr/coll_shard" + (i + 1) + "_replica_n" + (i + 1);
        shards.add(shard);
      }
      sreq.actualShards = shards.toArray(new String[0]);
      for (String shard : shards) {
        handler.submit(sreq, shard, new ModifiableSolrParams());
      }


      org.apache.solr.handler.component.ShardResponse response = handler.takeCompletedIncludingErrors();
      //TODO check the response
    } finally {
      if (factory != null) factory.close();
      if (cc != null) cc.shutdown();
      if (client != null) client.close();
    }
  }

}
