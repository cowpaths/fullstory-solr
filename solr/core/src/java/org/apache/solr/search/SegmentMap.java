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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;

public class SegmentMap {

  private static final AtomicLong IDS = new AtomicLong();

  private static String mintId() {
    long raw = IDS.getAndIncrement();
    String inOrder = Long.toUnsignedString(raw, Character.MAX_RADIX);
    return new StringBuilder(inOrder).reverse().toString();
  }

  public static SegmentMap generateSegmentMap(SolrIndexSearcher searcher) {
    final List<LeafReaderContext> leafContexts = searcher.getLeafContexts();
    @SuppressWarnings({"unchecked", "rawtypes"})
    Map.Entry<IndexReader.CacheKey, Segment>[] segs = new Map.Entry[leafContexts.size()];
    int i = 0;
    for (LeafReaderContext ctx : leafContexts) {
      LeafReader r = ctx.reader();
      IndexReader.CacheKey coreKey = r.getCoreCacheHelper().getKey();
      segs[i++] =
          new AbstractMap.SimpleImmutableEntry<>(
              coreKey,
              new Segment(
                  coreKey,
                  r.getReaderCacheHelper().getKey(),
                  ctx.docBase,
                  r.numDocs(),
                  r.maxDoc()));
    }
    DirectoryReader r = searcher.getIndexReader();
    return new SegmentMap(
        r.getReaderCacheHelper().getKey(), Map.ofEntries(segs), r.numDocs(), r.maxDoc());
  }

  /**
   * Uniquely identifies this {@link SegmentMap} within the context of the associated classloader.
   */
  public final String id;

  public final IndexReader.CacheKey key;
  public final Map<IndexReader.CacheKey, Segment> segments;
  public final int numDocs;
  public final int maxDoc;
  private final ConcurrentHashMap<IndexReader.CacheKey, Double> overlap = new ConcurrentHashMap<>();

  private SegmentMap(
      IndexReader.CacheKey key,
      Map<IndexReader.CacheKey, Segment> segments,
      int numDocs,
      int maxDoc) {
    this.id = mintId();
    this.key = key;
    this.segments = segments;
    this.numDocs = numDocs;
    this.maxDoc = maxDoc;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public SegmentMap ignoreSegmentsOneIn(int denominator) {
    List<Map.Entry<IndexReader.CacheKey, Segment>> segs = new ArrayList<>(segments.size());
    Random r = new Random();
    for (Map.Entry<IndexReader.CacheKey, Segment> e : segments.entrySet()) {
      if (r.nextInt(denominator) != 0) {
        segs.add(e);
      }
    }
    return new SegmentMap(null, Map.ofEntries(segs.toArray(new Map.Entry[0])), numDocs, maxDoc);
  }

  public double registerOverlap(SegmentMap other) {
    if (other.key == null) {
      return computeOverlap(other);
    } else {
      return overlap.computeIfAbsent(other.key, (k) -> computeOverlap(other));
    }
  }

  private double computeOverlap(SegmentMap other) {
    int count = 0;
    final Map<IndexReader.CacheKey, Segment> otherSegments = other.segments;
    for (Segment s : segments.values()) {
      if (otherSegments.containsKey(s.coreKey)) {
        count += s.maxDoc;
      }
    }
    return (double) count / other.maxDoc;
  }

  public double getOverlap(IndexReader.CacheKey newSearcher) {
    return overlap.getOrDefault(newSearcher, 0d);
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
