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
package org.apache.solr.search;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class TestFilterStats extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig.xml", "schema_latest.xml");
  }

  @Test
  public void testFiltersStatsAlwaysOn() throws Exception {
    clearIndex();
    assertU(adoc("id", "1", "val_i", "1", "foo_s", "a"));
    assertU(adoc("id", "2", "val_i", "2", "foo_s", "b"));
    assertU(adoc("id", "3", "val_i", "3", "foo_s", "a"));
    assertU(commit());

    // Disable result cache on the main query using local params.
    // First request should MISS for both fqs (empty cache)
    SolrQueryRequest req1 = req("q", "{!cache=false}*:*", "fq", "foo_s:a", "fq", "val_i:[1 TO 3]");
    SolrQueryResponse rsp1 = h.queryAndResponse("/select", req1);
    req1.close();

    List<Map<String, Object>> filterStats1 = parseFilterStats(rsp1);
    assertEquals(2, filterStats1.size());

    Map<String, Object> filter1_0 = filterStats1.get(0);
    assertEquals("foo_s:a", filter1_0.get("key"));
    assertNotNull(filter1_0.get("time"));
    assertEquals(false, filter1_0.get("cacheHit"));
    assertEquals(2, ((Number) filter1_0.get("docSetIdCount")).intValue());

    Map<String, Object> filter1_1 = filterStats1.get(1);
    String key1_1 = (String) filter1_1.get("key");
    assertTrue(key1_1.contains("val_i:[1 TO 3]"));
    assertNotNull(filter1_1.get("time"));
    assertEquals(false, filter1_1.get("cacheHit"));
    assertEquals(3, ((Number) filter1_1.get("docSetIdCount")).intValue());

    // Second identical request should HIT for both
    SolrQueryRequest req2 = req("q", "{!cache=false}*:*", "fq", "foo_s:a", "fq", "val_i:[1 TO 3]");
    SolrQueryResponse rsp2 = h.queryAndResponse("/select", req2);
    req2.close();

    List<Map<String, Object>> filterStats2 = parseFilterStats(rsp2);
    assertEquals(2, filterStats2.size());

    Map<String, Object> filter2_0 = filterStats2.get(0);
    assertEquals("foo_s:a", filter2_0.get("key"));
    assertEquals(true, filter2_0.get("cacheHit"));
    assertEquals(2, ((Number) filter2_0.get("docSetIdCount")).intValue());

    Map<String, Object> filter2_1 = filterStats2.get(1);
    String key2_1 = (String) filter2_1.get("key");
    assertTrue(key2_1.contains("val_i:[1 TO 3]"));
    assertEquals(true, filter2_1.get("cacheHit"));
    assertEquals(3, ((Number) filter2_1.get("docSetIdCount")).intValue());

    // 3rd request, only 2nd fq is identical, so 1st should HIT, 2nd should MISS
    SolrQueryRequest req3 = req("q", "{!cache=false}*:*", "fq", "foo_s:a", "fq", "val_i:[1 TO 2]");
    SolrQueryResponse rsp3 = h.queryAndResponse("/select", req3);
    req3.close();

    List<Map<String, Object>> filterStats3 = parseFilterStats(rsp3);
    assertEquals(2, filterStats3.size());

    Map<String, Object> filter3_0 = filterStats3.get(0);
    assertEquals("foo_s:a", filter3_0.get("key"));
    assertEquals(true, filter3_0.get("cacheHit"));
    assertEquals(2, ((Number) filter3_0.get("docSetIdCount")).intValue());

    Map<String, Object> filter3_1 = filterStats3.get(1);
    String key3_1 = (String) filter3_1.get("key");
    assertTrue(key3_1.contains("val_i:[1 TO 2]"));
    assertEquals(false, filter3_1.get("cacheHit"));
    assertEquals(2, ((Number) filter3_1.get("docSetIdCount")).intValue());
  }

  private List<Map<String, Object>> parseFilterStats(SolrQueryResponse rsp) throws Exception {
    NamedList<Object> toLog = rsp.getToLog();
    String filterStatsJson = (String) toLog.get("filtersStats");
    assertNotNull("filtersStats should be present in toLog", filterStatsJson);

    JSONParser parser = new JSONParser(filterStatsJson);
    Object parsed = ObjectBuilder.getVal(parser);
    assertTrue("filtersStats should be a list", parsed instanceof List);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> filterStats = (List<Map<String, Object>>) parsed;
    return filterStats;
  }
}
