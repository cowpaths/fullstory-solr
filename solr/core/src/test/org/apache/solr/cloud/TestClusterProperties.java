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

package org.apache.solr.cloud;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestClusterProperties extends SolrCloudTestCase {

  private ClusterProperties props;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).configure();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    props = new ClusterProperties(zkClient());
  }

  @Test
  public void testSetPluginClusterProperty() throws Exception {
    String propertyName = ClusterProperties.EXT_PROPRTTY_PREFIX + "pluginA.propertyA";
    CollectionAdminRequest.setClusterProperty(propertyName, "valueA")
        .process(cluster.getSolrClient());
    assertEquals("valueA", props.getClusterProperty(propertyName, null));
  }

  @Test(expected = SolrException.class)
  public void testSetInvalidPluginClusterProperty() throws Exception {
    String propertyName = "pluginA.propertyA";
    CollectionAdminRequest.setClusterProperty(propertyName, "valueA")
        .process(cluster.getSolrClient());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testWatchedNodeProperties() {
    WatchedClusterProperties wcp = new WatchedClusterProperties("node1");
    NamedList<String> listener1 = new NamedList<>();
    NamedList<String> all = new NamedList<>();
    NamedList<String> listener3 = new NamedList<>();
    wcp.watchProperty("p1", (key, value) -> listener1.add(key, value));
    wcp.watchProperty(null, (key, value) -> all.add(key, value));
    BiConsumer<String, String> p3Listener = (key, value) -> listener3.add(key, value);
    wcp.watchProperty("p3", p3Listener);
    wcp.onChange(
        (Map<String, Object>)
            Utils.fromJSONString(
                "{\n"
                    + "  \"watched-node-properties\" :{\n"
                    + "    \"node1\" : {\n"
                    + "      \"p1\" : \"v1\",\n"
                    + "      \"p2\" : \"v2\"\n"
                    + "    }\n"
                    + "  }\n"
                    + "}"));

    assertEquals(1, listener1.size());
    assertEquals("v1", listener1.get("p1"));
    assertEquals(2, all.size());
    assertEquals("v1", all.get("p1"));
    assertEquals("v2", all.get("p2"));
    assertEquals(0, listener3.size());
  }

  static class NodeWatch {
    final WatchedClusterProperties wcp;
    Map<String, NamedList<String>> events = new HashMap<>();

    NodeWatch(String name) {
      wcp = new WatchedClusterProperties(name);
    }

    @SuppressWarnings("unchecked")
    void newProps(String json) {
      events.clear();
      wcp.onChange((Map<String, Object>) Utils.fromJSONString(json));
    }

    NodeWatch listen(String name) {
      wcp.watchProperty(
          name, (k, v) -> events.computeIfAbsent(name, s -> new NamedList<>()).add(k, v));
      return this;
    }

    static final NamedList<String> empty = new NamedList<>();

    int count(String prop) {
      return events.getOrDefault(prop, empty)._size();
    }

    String val(String key) {
      return events.getOrDefault(key, empty).get(key);
    }

    String val(String listenerName, String key) {
      return events.getOrDefault(listenerName, empty).get(key);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testWatchedClusterProperties() {
    NodeWatch n1 = new NodeWatch("node1").listen("p1").listen(null).listen("p3");
    NodeWatch n2 = new NodeWatch("node2").listen("p1").listen(null).listen("p3");
    ;

    String json =
        "{\n"
            + "  \"watched-properties\": {\n"
            + "    \"p1\": \"v1\",\n"
            + "    \"p2\": \"v2\"\n"
            + "  }\n"
            + "}";
    n1.newProps(json);
    n2.newProps(json);

    assertEquals(1, n1.count("p1"));
    assertEquals("v1", n1.val("p1"));
    assertEquals(2, n1.count(null));
    assertEquals("v1", n1.val(null, "p1"));
    assertEquals("v2", n1.val(null, "p2"));
    assertEquals(0, n1.count("p3"));

    assertEquals(1, n2.count("p1"));
    assertEquals("v1", n2.val("p1"));
    assertEquals(2, n2.count(null));
    assertEquals("v1", n2.val(null, "p1"));
    assertEquals("v2", n2.val(null, "p2"));
    assertEquals(0, n2.count("p3"));

    json =
        "{\n"
            + "  \"watched-properties\": {\n"
            + "    \"p1\": \"v1\",\n"
            + "    \"p2\": \"v2\"\n"
            + "    \"p3\": \"v3\"\n"
            + "  }\n"
            + "}";
    n1.newProps(json);
    n2.newProps(json);
    assertEquals(0, n1.count("p1"));
    assertEquals(0, n1.count("p2"));
    assertEquals(0, n2.count("p1"));
    assertEquals(0, n2.count("p2"));
    assertEquals("v3", n1.val("p3"));
    assertEquals("v3", n2.val("p3"));
    json =
        "{\n"
            + "  \"watched-properties\": {\n"
            + "    \"p1\": \"v1\",\n"
            + "    \"p2\": \"v2\"\n"
            + "    \"p3\": \"v3\"\n"
            + "  },\n"
            + "  \"watched-node-properties\" :{\n"
            + "    \"node1\" : {\n"
            + "      \"p1\" : \"v_n1\",\n"
            + "      \"p2\" : \"v_n2\"\n"
            + "    },\n"
            + "  }\n"
            + "}";
    n1.newProps(json);
    n2.newProps(json);
    assertEquals(0, n2.count("p1"));
    assertEquals(0, n2.count(null));

    assertEquals("v_n1", n1.val("p1"));
    assertEquals("v_n1", n1.val(null, "p1"));
    assertEquals("v_n2", n1.val(null, "p2"));
  }
}
