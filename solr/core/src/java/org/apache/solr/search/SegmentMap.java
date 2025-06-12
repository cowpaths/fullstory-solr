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

import com.carrotsearch.hppc.ObjectDoubleHashMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;

public class SegmentMap {

  public static SegmentMap generateSegmentMap(SolrIndexSearcher searcher) {
    final List<LeafReaderContext> leafContexts = searcher.getLeafContexts();
    Map<IndexReader.CacheKey, Segment> segs = new HashMap<>(leafContexts.size());
    int i = 0;
    for (LeafReaderContext ctx : leafContexts) {
      LeafReader r = ctx.reader();
      IndexReader.CacheKey coreKey = r.getCoreCacheHelper().getKey();
      segs.put(
          coreKey,
          new Segment(
              coreKey, r.getReaderCacheHelper().getKey(), ctx.docBase, r.numDocs(), r.maxDoc()));
    }
    DirectoryReader r = searcher.getIndexReader();
    return new SegmentMap(
        r.getReaderCacheHelper().getKey(),
        Collections.unmodifiableMap(segs),
        r.numDocs(),
        r.maxDoc());
  }

  public final IndexReader.CacheKey key;
  public final Map<IndexReader.CacheKey, Segment> segments;
  public final int numDocs;
  public final int maxDoc;
  private final ObjectDoubleHashMap<IndexReader.CacheKey> overlap = new ObjectDoubleHashMap<>();

  private SegmentMap(
      IndexReader.CacheKey key,
      Map<IndexReader.CacheKey, Segment> segments,
      int numDocs,
      int maxDoc) {
    this.key = key;
    this.segments = segments;
    this.numDocs = numDocs;
    this.maxDoc = maxDoc;
  }

  public double registerOverlap(SegmentMap newSearcher) {
    int count = 0;
    final Map<IndexReader.CacheKey, Segment> newSegments = newSearcher.segments;
    for (Segment s : segments.values()) {
      if (newSegments.containsKey(s.coreKey)) {
        count += s.maxDoc;
      }
    }
    double ret = (double) count / newSearcher.maxDoc;
    overlap.put(newSearcher.key, ret);
    return ret;
  }

  public double getOverlap(IndexReader.CacheKey newSearcher) {
    return overlap.getOrDefault(newSearcher, 0);
  }

  public static class Segment {
    public final IndexReader.CacheKey coreKey;
    public final IndexReader.CacheKey readerKey;
    public final int docBase;
    public final int numDocs;
    public final int maxDoc;

    private Segment(
        IndexReader.CacheKey coreKey,
        IndexReader.CacheKey readerKey,
        int docBase,
        int numDocs,
        int maxDoc) {
      this.coreKey = coreKey;
      this.readerKey = readerKey;
      this.docBase = docBase;
      this.numDocs = numDocs;
      this.maxDoc = maxDoc;
    }
  }
}
