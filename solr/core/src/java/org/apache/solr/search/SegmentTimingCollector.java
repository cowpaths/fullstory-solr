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

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.LeafCollector;
/**
 * Wraps a {@link Collector} to record per-segment (per-leaf) wall time for searches executed via
 * {@link org.apache.lucene.search.IndexSearcher}'s {@code search(Query, Collector)} path (including
 * multi-threaded collector managers).
 */
public final class SegmentTimingCollector extends FilterCollector {

  private final Map<String, SegmentSearchStats> statsBySegmentName;

  public SegmentTimingCollector(Collector in, Map<String, SegmentSearchStats> statsBySegmentName) {
    super(in);
    this.statsBySegmentName =
        Objects.requireNonNull(statsBySegmentName, "statsBySegmentName");
  }

  static String segmentLabel(LeafReaderContext context) {
    LeafReader reader = FilterLeafReader.unwrap(context.reader());
    if (reader instanceof SegmentReader) {
      return ((SegmentReader) reader).getSegmentInfo().info.name;
    }
    return "leaf@" + context.docBase;
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
    final String segmentName = segmentLabel(context);
    final long leafStartNanos = System.nanoTime();
    final LeafCollector inner = super.getLeafCollector(context);
    final SegmentSearchStats stats =
        statsBySegmentName.computeIfAbsent(segmentName, k -> new SegmentSearchStats());

    return new FilterLeafCollector(inner) {
      @Override
      public void finish() throws IOException {
        try {
          super.finish();
        } finally {
          long elapsed = System.nanoTime() - leafStartNanos;
          stats.recordLeafNanos(elapsed);
        }
      }
    };
  }
}
