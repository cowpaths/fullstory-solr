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
import java.util.Arrays;
import java.util.Map;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.FixedBitSet;

public class SegAwareDocSetCache extends SegAwareCache<Query, DocSet> {

  private static final ReconstructorShim<Query, DocSet> FILTER_CACHE_RECONSTRUCTOR_SHIM =
      new ReconstructorShim<>() {
        @Override
        public Query getShimKey(Query key, SegmentMap staleSegs, DocSet staleVal) {
          return new FrankensteinQuery(key, staleSegs.segments, staleVal);
        }
      };

  public SegAwareDocSetCache() {
    super(FILTER_CACHE_RECONSTRUCTOR_SHIM);
  }

  private static class FrankensteinQuery extends Query implements ShimKey<Query> {

    private final Query backing;
    private final Map<IndexReader.CacheKey, SegmentMap.Segment> segs;
    private final DocSet stale;

    private FrankensteinQuery(
        Query backing, Map<IndexReader.CacheKey, SegmentMap.Segment> segs, DocSet stale) {
      this.backing = backing;
      this.segs = segs;
      this.stale = stale;
    }

    @Override
    public Query getUnshimmedKey() {
      return backing;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
      final Weight backingWeight = backing.createWeight(searcher, scoreMode, boost);
      return new Weight(this) {
        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
          return null;
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          final SegmentMap.Segment segment =
              segs.get(context.reader().getCoreCacheHelper().getKey());
          if (segment == null) {
            return backingWeight.scorer(context);
          } else {
            // backed by existing (partially stale) DocSet
            final int docBase = segment.docBase;
            final DocIdSetIterator disi;
            if (stale instanceof SortedIntDocSet) {
              final int[] docs = ((SortedIntDocSet) stale).getDocs();
              final int first = Arrays.binarySearch(docs, docBase);
              disi =
                  new DocIdSetIterator() {
                    final int limit = segment.maxDoc + docBase;
                    int idx = (first < 0 ? ~first : first) - 1;
                    int id = -1;

                    @Override
                    public int docID() {
                      return id == NO_MORE_DOCS ? NO_MORE_DOCS : id - docBase;
                    }

                    @Override
                    public int nextDoc() {
                      if (++idx >= docs.length || (id = docs[idx]) >= limit) {
                        return id = NO_MORE_DOCS;
                      } else {
                        return id - docBase;
                      }
                    }

                    @Override
                    public int advance(int target) {
                      while (nextDoc() < target) {
                        // advance
                      }
                      return id == NO_MORE_DOCS ? NO_MORE_DOCS : id - docBase;
                    }

                    @Override
                    public long cost() {
                      return 0;
                    }
                  };
            } else if (stale instanceof BitDocSet) {
              final FixedBitSet docs = ((BitDocSet) stale).getBits();
              disi =
                  new DocIdSetIterator() {
                    final int limit = segment.maxDoc + docBase;
                    int id = docBase - 1;

                    @Override
                    public int docID() {
                      return id == NO_MORE_DOCS ? NO_MORE_DOCS : id - docBase;
                    }

                    @Override
                    public int nextDoc() {
                      if (++id >= limit || (id = docs.nextSetBit(id)) >= limit) {
                        return id = NO_MORE_DOCS;
                      } else {
                        return id - docBase;
                      }
                    }

                    @Override
                    public int advance(int target) {
                      if (target >= limit || (id = docs.nextSetBit(target)) >= limit) {
                        return id = NO_MORE_DOCS;
                      } else {
                        return id - docBase;
                      }
                    }

                    @Override
                    public long cost() {
                      return 0;
                    }
                  };
            } else {
              throw new IllegalStateException();
            }
            return new ConstantScoreScorer(this, 1f, scoreMode, disi);
          }
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
          return true;
        }
      };
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
      Query rewrittenBacking = backing.rewrite(reader);
      return rewrittenBacking == backing
          ? this
          : new FrankensteinQuery(rewrittenBacking, segs, stale);
    }

    @Override
    public String toString(String field) {
      return FrankensteinQuery.class.getSimpleName() + "{" + backing + "}";
    }

    @Override
    public void visit(QueryVisitor visitor) {
      backing.visit(visitor);
    }

    @Override
    public boolean equals(Object obj) {
      return obj == this
          || (obj instanceof FrankensteinQuery
              && ((FrankensteinQuery) obj).backing.equals(backing));
    }

    @Override
    public int hashCode() {
      return FrankensteinQuery.class.hashCode() ^ backing.hashCode();
    }
  }
}
