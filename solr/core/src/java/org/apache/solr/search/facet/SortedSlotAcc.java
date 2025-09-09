package org.apache.solr.search.facet;

import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;
import java.util.function.IntFunction;

public abstract class SortedSlotAcc extends SlotAcc {
  @FunctionalInterface
  public interface Collector {
    void collect(int doc, long sortValue);
  }

  public SortedSlotAcc(FacetContext fcontext) {
    super(fcontext);
  }

  @Override
  public final void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
    throw new RuntimeException("not supported");
  }

  public abstract Collector collector(LeafReaderContext ctx, int slot, IntFunction<SlotContext> slotContext);
}
