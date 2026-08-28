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
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.update.GlobalMergeSchedulerManager.ConcurrentSemaphore;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class GlobalMergeSchedulerManagerTest extends SolrTestCaseJ4 {

  private static final String NODE = "solr-c92-8:8986_solr";
  private static final int DEFAULT_MAX_GLOBAL = 4;

  @BeforeClass
  public static void beforeClass() {
    assumeWorkingMockito();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    GlobalMergeSchedulerManager.resetForTesting();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    GlobalMergeSchedulerManager.resetForTesting();
    super.tearDown();
  }

  @Test
  public void testCreateSchedulerSharesSemaphore() throws Exception {
    ZkController zk = mockZkController(null, NODE, null);
    GlobalMergeSchedulerManager manager =
        GlobalMergeSchedulerManager.getInstance(zk, defaultInitArgs());
    GlobalConcurrentMergeScheduler a = manager.getScheduler();
    GlobalConcurrentMergeScheduler b = manager.getScheduler();
    assertNotSame(a, b);
    assertSame(manager.getSemaphore(), a.getSemaphore());
    assertSame(a.getSemaphore(), b.getSemaphore());
  }

  @Test
  public void testSemaphoreCapsConcurrentAcquires() throws Exception {
    GlobalMergeSchedulerManager manager =
        GlobalMergeSchedulerManager.getInstance(
            mockZkController(null, NODE, null), defaultInitArgs());
    ConcurrentSemaphore sem = manager.getSemaphore();
    sem.setMaxRunningOverride(1);

    assertTrue(sem.tryAcquire());
    assertFalse(sem.tryAcquire());
    assertEquals(1, sem.getActiveMergeCount());

    sem.release();
    assertEquals(0, sem.getActiveMergeCount());
    assertTrue(sem.tryAcquire());
    sem.release();
    assertEquals(0, sem.getActiveMergeCount());
  }

  @Test
  public void testLoweringMaxRunningShrinksAvailablePermits() throws Exception {
    GlobalMergeSchedulerManager manager =
        GlobalMergeSchedulerManager.getInstance(
            mockZkController(null, NODE, null), defaultInitArgs());
    ConcurrentSemaphore sem = manager.getSemaphore();

    for (int i = 0; i < DEFAULT_MAX_GLOBAL; i++) {
      assertTrue(sem.tryAcquire());
    }
    assertFalse(sem.tryAcquire());

    // shrink while all permits are out: in-flight merges finish, but only 1 can be re-acquired
    sem.setMaxRunningOverride(1);
    assertEquals(1, sem.getMaxRunning());
    for (int i = 0; i < DEFAULT_MAX_GLOBAL; i++) {
      sem.release();
    }
    assertEquals(0, sem.getActiveMergeCount());
    assertTrue(sem.tryAcquire());
    assertFalse(sem.tryAcquire());
    sem.release();
  }

  @Test
  public void testRaisingMaxRunningGrantsMorePermits() throws Exception {
    GlobalMergeSchedulerManager manager =
        GlobalMergeSchedulerManager.getInstance(
            mockZkController(null, NODE, null), defaultInitArgs());
    ConcurrentSemaphore sem = manager.getSemaphore();
    sem.setMaxRunningOverride(1);

    assertTrue(sem.tryAcquire());
    assertFalse(sem.tryAcquire());

    sem.setMaxRunningOverride(3);
    assertTrue(sem.tryAcquire());
    assertTrue(sem.tryAcquire());
    assertFalse(sem.tryAcquire());
    assertEquals(3, sem.getActiveMergeCount());

    sem.release();
    sem.release();
    sem.release();
    assertEquals(0, sem.getActiveMergeCount());
  }

  @Test
  public void testOverrideAppliesMaxRunningForMatchingNode() throws Exception {
    AtomicReference<ClusterPropertiesListener> listenerRef = new AtomicReference<>();
    GlobalMergeSchedulerManager manager =
        GlobalMergeSchedulerManager.getInstance(
            mockZkController(null, NODE, listenerRef), defaultInitArgs());

    listenerRef.get().onChange(parseJson(overrideJson(NODE)));

    assertTrue(manager.getWatcher().hasActiveOverride());
    assertEquals(Integer.valueOf(2), manager.getWatcher().getOverrideMaxThreadCount());
    assertEquals(2, manager.getSemaphore().getMaxRunning());
  }

  @Test
  public void testOverrideSkippedForNonMatchingNode() throws Exception {
    AtomicReference<ClusterPropertiesListener> listenerRef = new AtomicReference<>();
    GlobalMergeSchedulerManager manager =
        GlobalMergeSchedulerManager.getInstance(
            mockZkController(null, NODE, listenerRef), defaultInitArgs());

    listenerRef.get().onChange(parseJson(overrideJson("other-node:8986_solr")));

    assertFalse(manager.getWatcher().hasActiveOverride());
    assertEquals(DEFAULT_MAX_GLOBAL, manager.getSemaphore().getMaxRunning());
  }

  @Test
  public void testOverrideWithoutNodesAppliesEverywhere() throws Exception {
    AtomicReference<ClusterPropertiesListener> listenerRef = new AtomicReference<>();
    String json =
        "{\n"
            + "  \"ext.globalMergeScheduler\": {\n"
            + "    \"maxThreadCount\": 3\n"
            + "  }\n"
            + "}";
    GlobalMergeSchedulerManager manager =
        GlobalMergeSchedulerManager.getInstance(
            mockZkController(null, NODE, listenerRef), defaultInitArgs());

    listenerRef.get().onChange(parseJson(json));

    assertTrue(manager.getWatcher().hasActiveOverride());
    assertNull(manager.getWatcher().getOverrideNodes());
    assertEquals(3, manager.getSemaphore().getMaxRunning());
  }

  @Test
  public void testClearOverrideRestoresDefault() throws Exception {
    AtomicReference<ClusterPropertiesListener> listenerRef = new AtomicReference<>();
    GlobalMergeSchedulerManager manager =
        GlobalMergeSchedulerManager.getInstance(
            mockZkController(null, NODE, listenerRef), defaultInitArgs());

    listenerRef.get().onChange(parseJson(overrideJson(NODE)));
    assertEquals(2, manager.getSemaphore().getMaxRunning());

    listenerRef.get().onChange(Map.of());
    assertFalse(manager.getWatcher().hasActiveOverride());
    assertEquals(DEFAULT_MAX_GLOBAL, manager.getSemaphore().getMaxRunning());
  }

  @Test
  public void testInvalidLimitsIgnored() throws Exception {
    AtomicReference<ClusterPropertiesListener> listenerRef = new AtomicReference<>();
    String json =
        "{\n"
            + "  \"ext.globalMergeScheduler\": {\n"
            + "    \"maxThreadCount\": 0\n"
            + "  }\n"
            + "}";
    GlobalMergeSchedulerManager manager =
        GlobalMergeSchedulerManager.getInstance(
            mockZkController(null, NODE, listenerRef), defaultInitArgs());

    listenerRef.get().onChange(parseJson(json));

    assertFalse(manager.getWatcher().hasActiveOverride());
    assertEquals(DEFAULT_MAX_GLOBAL, manager.getSemaphore().getMaxRunning());
  }

  @Test
  public void testMaxMergeCountAloneIgnored() throws Exception {
    AtomicReference<ClusterPropertiesListener> listenerRef = new AtomicReference<>();
    String json =
        "{\n"
            + "  \"ext.globalMergeScheduler\": {\n"
            + "    \"maxMergeCount\": 8\n"
            + "  }\n"
            + "}";
    GlobalMergeSchedulerManager manager =
        GlobalMergeSchedulerManager.getInstance(
            mockZkController(null, NODE, listenerRef), defaultInitArgs());

    listenerRef.get().onChange(parseJson(json));

    assertFalse(manager.getWatcher().hasActiveOverride());
    assertEquals(DEFAULT_MAX_GLOBAL, manager.getSemaphore().getMaxRunning());
  }

  @Test
  public void testSingletonManager() throws Exception {
    ZkController zk = mockZkController(null, NODE, null);
    NamedList<Object> args = defaultInitArgs();
    GlobalMergeSchedulerManager a = GlobalMergeSchedulerManager.getInstance(zk, args);
    GlobalMergeSchedulerManager b = GlobalMergeSchedulerManager.getInstance(zk, args);
    assertSame(a, b);
    assertSame(a.getSemaphore(), b.getSemaphore());
  }

  private static NamedList<Object> defaultInitArgs() {
    NamedList<Object> args = new NamedList<>();
    args.add("maxGlobalThreadCount", DEFAULT_MAX_GLOBAL);
    return args;
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
