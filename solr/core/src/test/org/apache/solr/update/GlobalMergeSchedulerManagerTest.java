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
package org.apache.solr.update;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.index.GlobalConcurrentMergeScheduler;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.ClusterPropertiesListener;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class GlobalMergeSchedulerManagerTest extends SolrTestCaseJ4 {

  private static final String NODE = "solr-c92-8:8986_solr";

  @BeforeClass
  public static void beforeClass() {
    assumeWorkingMockito();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    GlobalMergeSchedulerManager.resetForTesting();
    GlobalConcurrentMergeScheduler.resetForTesting();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    GlobalMergeSchedulerManager.resetForTesting();
    GlobalConcurrentMergeScheduler.resetForTesting();
    super.tearDown();
  }

  @Test
  public void testOverrideAppliesToMatchingNode() throws Exception {
    AtomicReference<ClusterPropertiesListener> listenerRef = new AtomicReference<>();
    GlobalMergeSchedulerManager manager =
        GlobalMergeSchedulerManager.getInstance(mockZkController(null, NODE, listenerRef));
    GlobalConcurrentMergeScheduler cms = manager.getScheduler();
    cms.setMaxMergesAndThreads(6, 1); // solrconfig

    listenerRef.get().onChange(parseJson(overrideJson(NODE)));

    assertTrue(manager.getWatcher().hasActiveOverride());
    assertEquals(Integer.valueOf(8), manager.getWatcher().getOverrideMaxMergeCount());
    assertEquals(8, cms.getMaxMergeCount());
    assertEquals(2, cms.getMaxThreadCount());
  }

  @Test
  public void testOverrideSkippedForNonMatchingNode() throws Exception {
    AtomicReference<ClusterPropertiesListener> listenerRef = new AtomicReference<>();
    GlobalMergeSchedulerManager manager =
        GlobalMergeSchedulerManager.getInstance(mockZkController(null, NODE, listenerRef));
    GlobalConcurrentMergeScheduler cms = manager.getScheduler();
    cms.setMaxMergesAndThreads(6, 1);

    listenerRef.get().onChange(parseJson(overrideJson("other-node:8986_solr")));

    assertFalse(manager.getWatcher().hasActiveOverride());
    assertEquals(6, cms.getMaxMergeCount());
    assertEquals(1, cms.getMaxThreadCount());
  }

  @Test
  public void testOverrideWithoutNodesAppliesEverywhere() throws Exception {
    AtomicReference<ClusterPropertiesListener> listenerRef = new AtomicReference<>();
    String json =
        "{\n"
            + "  \"ext.globalMergeScheduler\": {\n"
            + "    \"maxThreadCount\": 3,\n"
            + "    \"maxMergeCount\": 5\n"
            + "  }\n"
            + "}";
    GlobalMergeSchedulerManager manager =
        GlobalMergeSchedulerManager.getInstance(mockZkController(null, NODE, listenerRef));
    GlobalConcurrentMergeScheduler cms = manager.getScheduler();
    cms.setMaxMergesAndThreads(6, 1);

    listenerRef.get().onChange(parseJson(json));

    assertTrue(manager.getWatcher().hasActiveOverride());
    assertNull(manager.getWatcher().getOverrideNodes());
    assertEquals(5, cms.getMaxMergeCount());
    assertEquals(3, cms.getMaxThreadCount());
  }

  @Test
  public void testClearOverrideRevertsToPriorLimits() throws Exception {
    AtomicReference<ClusterPropertiesListener> listenerRef = new AtomicReference<>();
    GlobalMergeSchedulerManager manager =
        GlobalMergeSchedulerManager.getInstance(mockZkController(null, NODE, listenerRef));
    GlobalConcurrentMergeScheduler cms = manager.getScheduler();
    cms.setMaxMergesAndThreads(6, 1);

    listenerRef.get().onChange(parseJson(overrideJson(NODE)));
    assertEquals(8, cms.getMaxMergeCount());

    listenerRef.get().onChange(Map.of());
    assertFalse(manager.getWatcher().hasActiveOverride());
    assertEquals(6, cms.getMaxMergeCount());
    assertEquals(1, cms.getMaxThreadCount());
  }

  @Test
  public void testInvalidLimitsIgnored() throws Exception {
    AtomicReference<ClusterPropertiesListener> listenerRef = new AtomicReference<>();
    String json =
        "{\n"
            + "  \"ext.globalMergeScheduler\": {\n"
            + "    \"maxThreadCount\": 10,\n"
            + "    \"maxMergeCount\": 2\n"
            + "  }\n"
            + "}";
    GlobalMergeSchedulerManager manager =
        GlobalMergeSchedulerManager.getInstance(mockZkController(null, NODE, listenerRef));
    GlobalConcurrentMergeScheduler cms = manager.getScheduler();
    cms.setMaxMergesAndThreads(6, 1);

    listenerRef.get().onChange(parseJson(json));

    assertFalse(manager.getWatcher().hasActiveOverride());
    assertEquals(6, cms.getMaxMergeCount());
    assertEquals(1, cms.getMaxThreadCount());
  }

  @Test
  public void testSingletonHoldsSameScheduler() throws Exception {
    ZkController zk = mockZkController(null, NODE, null);
    GlobalMergeSchedulerManager a = GlobalMergeSchedulerManager.getInstance(zk);
    GlobalMergeSchedulerManager b = GlobalMergeSchedulerManager.getInstance(zk);
    assertSame(a, b);
    assertSame(a.getScheduler(), b.getScheduler());
  }

  private static String overrideJson(String node) {
    return "{\n"
        + "  \"ext.globalMergeScheduler\": {\n"
        + "    \"maxThreadCount\": 2,\n"
        + "    \"maxMergeCount\": 8,\n"
        + "    \"nodes\": [\""
        + node
        + "\"]\n"
        + "  }\n"
        + "}";
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> parseJson(String jsonString) {
    return (Map<String, Object>) Utils.fromJSON(jsonString.getBytes(Charset.defaultCharset()));
  }

  private static ZkController mockZkController(
      String jsonString, String nodeName, AtomicReference<ClusterPropertiesListener> listenerRef)
      throws InterruptedException, KeeperException {
    ZkStateReader zkStateReader = mockZkStateReader(jsonString, listenerRef);
    ZkController zkController = Mockito.mock(ZkController.class);
    when(zkController.getNodeName()).thenReturn(nodeName);
    when(zkController.getZkStateReader()).thenReturn(zkStateReader);
    return zkController;
  }

  @SuppressWarnings("unchecked")
  private static ZkStateReader mockZkStateReader(
      String jsonString, AtomicReference<ClusterPropertiesListener> listenerRef)
      throws InterruptedException, KeeperException {
    ZkStateReader zkStateReader = Mockito.mock(ZkStateReader.class);
    Mockito.doAnswer(
            invocation -> {
              ClusterPropertiesListener listener = invocation.getArgument(0);
              listener.onChange(
                  jsonString != null
                      ? (Map<String, Object>)
                          Utils.fromJSON(jsonString.getBytes(Charset.defaultCharset()))
                      : Map.of());
              if (listenerRef != null) {
                listenerRef.set(listener);
              }
              return true;
            })
        .when(zkStateReader)
        .registerClusterPropertiesListener(any(ClusterPropertiesListener.class));
    return zkStateReader;
  }
}
