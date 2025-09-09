package org.apache.solr.search.facet;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.PriorityQueue;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrIndexSearcher;

import java.io.IOException;
import java.util.List;
import java.util.function.IntFunction;

public final class FacetProcessorCollect {
  private static class SortedContext {
    private final DocIdSetIterator disi;
    private final SortedNumericDocValues dvs;
    private final SortedSlotAcc.Collector[] collectors;
    private int doc;
    private long sortValue;
    private long count;

    private SortedContext(DocIdSetIterator disi, SortedNumericDocValues dvs, SortedSlotAcc.Collector[] collectors) {
      this.disi = disi;
      this.dvs = dvs;
      this.collectors = collectors;
    }

    private boolean advance() throws IOException {
      while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        count++;
        if (dvs.advanceExact(doc)) {
          sortValue = dvs.nextValue();
          return true;
        }
      }
      return false;
    }
  }

  public static long collectSorted(SolrIndexSearcher searcher, DocSet docs, int slot, IntFunction<SlotAcc.SlotContext> slotContext, SortedSlotAcc[] sortedAccs) throws IOException {
    final List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
    final PriorityQueue<SortedContext> queue = new PriorityQueue<>(leaves.size()) {
      @Override
      protected boolean lessThan(SortedContext a, SortedContext b) {
        return a.sortValue < b.sortValue;
      }
    };

    long count = 0;
    for (LeafReaderContext ctx : leaves) {
      DocIdSetIterator disi = docs.iterator(ctx);
      if (disi != null) {
        SortedNumericDocValues dvs = DocValues.getSortedNumeric(ctx.reader(), "IndvId");
        SortedSlotAcc.Collector[] collectors = new SortedSlotAcc.Collector[sortedAccs.length];
        for (int c = 0; c < sortedAccs.length; c++) {
          collectors[c] = sortedAccs[c].collector(ctx, slot, slotContext);
        }
        SortedContext sortedCtx = new SortedContext(disi, dvs, collectors);
        if (sortedCtx.advance()) {
          queue.add(sortedCtx);
        } else {
          count += sortedCtx.count;
        }
      }
    }

    while (queue.size() > 0) {
      SortedContext sortedCtx = queue.top();
      for (int c = 0; c < sortedCtx.collectors.length; c++) {
        sortedCtx.collectors[c].collect(sortedCtx.doc, sortedCtx.sortValue);
      }
      if (sortedCtx.advance()) {
        queue.updateTop();
      } else {
        queue.pop();
        count += sortedCtx.count;
      }
    }

    return count;
  }
}
