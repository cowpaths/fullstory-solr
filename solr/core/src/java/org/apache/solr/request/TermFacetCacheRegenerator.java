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
package org.apache.solr.request;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.lucene.index.IndexReader.CacheKey;
import org.apache.lucene.util.CollectionUtil;
import org.apache.solr.request.TermFacetCache.SegmentCacheEntry;
import org.apache.solr.search.CacheRegenerator;
import org.apache.solr.search.SegmentMap;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrIndexSearcher;

/** */
public class TermFacetCacheRegenerator implements CacheRegenerator {
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public boolean regenerateItem(
      SolrIndexSearcher newSearcher, SolrCache nc, SolrCache oc, Object oldKey, Object oldVal)
      throws IOException {
    if (((TermFacetCache.FacetCacheKey) oldKey).isCrossDoc()) {
      // domain is defined by cross-doc queries (e.g., {!join}), so per-seg entries
      // cannot be carried over.
      return true;
    }
    Map<CacheKey, SegmentCacheEntry> oldSegmentCache = (Map<CacheKey, SegmentCacheEntry>) oldVal;
    Map<CacheKey, SegmentMap.Segment> newSegments = newSearcher.getSegmentMap().segments;
    Map<CacheKey, SegmentCacheEntry> newSegmentCache =
        CollectionUtil.newHashMap(newSegments.size());
    for (Entry<CacheKey, SegmentCacheEntry> e : oldSegmentCache.entrySet()) {
      CacheKey segmentKey = e.getKey();
      if (newSegments.containsKey(segmentKey)) {
        newSegmentCache.put(segmentKey, e.getValue());
      }
    }
    if (!newSegmentCache.isEmpty()) {
      nc.put(oldKey, Map.copyOf(newSegmentCache));
    }
    return true;
  }
}
