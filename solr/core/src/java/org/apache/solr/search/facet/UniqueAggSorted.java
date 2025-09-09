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
package org.apache.solr.search.facet;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.solr.common.util.SimpleOrderedMap;

import java.io.IOException;
import java.util.function.IntFunction;

public class UniqueAggSorted extends StrAggValueSource {
  public static final String UNIQUE_SORTED = "uniqueSorted";

  public UniqueAggSorted(String field) {
    super(UNIQUE_SORTED, field);
  }

  @Override
  public SortedSlotAcc createSlotAcc(FacetContext fcontext, long numDocs, int numSlots)
      throws IOException {
    return new LongSlotAcc(fcontext, numSlots);
  }

  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    return new Merger();
  }

  private static class Merger extends FacetModule.FacetSortableMerger {
    long value = 0;

    @Override
    public void merge(Object facetResult, Context mcontext) {
      if (facetResult != null) {
        value += (long) facetResult;
      }
    }

    @Override
    public Object getMergedResult() {
      return value;
    }

    @Override
    public int compareTo(
        FacetModule.FacetSortableMerger other, FacetRequest.SortDirection direction) {
      return Long.compare(value, ((Merger) other).value);
    }
  }

  static class LongSlotAcc extends SortedSlotAcc {
    Long[] values;
    long lastSortValue;

    public LongSlotAcc(FacetContext fcontext, int numSlots) {
      super(fcontext);
      values = new Long[numSlots];
      lastSortValue = Long.MIN_VALUE;
    }

    @Override
    public void reset() {
      values = new Long[values.length];
      lastSortValue = Long.MIN_VALUE;
    }

    @Override
    public void resize(Resizer resizer) {
      values = resizer.resize(values, null);
    }

    @Override
    public Long getValue(int slot) {
      Long val = values[slot];
      return val != null ? val : 0L;
    }

    @Override
    public int compare(int slotA, int slotB) {
      return Long.compare(getValue(slotA), getValue(slotB));
    }

    @Override
    public Collector collector(LeafReaderContext ctx, int slot, IntFunction<SlotContext> slotContext) {
      return (doc, sortValue) -> {
        if (sortValue != lastSortValue) {
          values[slot] = getValue(slot) + 1;
          lastSortValue = sortValue;
        }
      };
    }
  }
}
