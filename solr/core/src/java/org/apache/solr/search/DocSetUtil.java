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
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

/**
 * @lucene.experimental
 */
public class DocSetUtil {

  /**
   * The cut-off point for small sets (SortedIntDocSet) vs large sets (BitDocSet)
   *
   * <p>For ridiculously small sets, we'll just use a sorted int[]. {@code maxDoc >>> 6} is a good
   * value if you want to save memory, lower values such as {@code maxDoc >>> 11} should provide
   * faster building but at the expense of using a full bitset even for quite sparse data.
   */
  public static int smallSetSize(int maxDoc) {
    return (maxDoc >> 6) + 5; // The +5 is for better test coverage for small sets
  }

  /**
   * Iterates DocSets to test for equality - slow and for testing purposes only.
   *
   * @lucene.internal
   */
  public static boolean equals(DocSet a, DocSet b) {
    DocIterator iter1 = a.iterator();
    DocIterator iter2 = b.iterator();

    for (; ; ) {
      boolean n1 = iter1.hasNext();
      boolean n2 = iter2.hasNext();
      if (n1 != n2) {
        return false;
      }
      if (!n1) return true; // made it to end
      int d1 = iter1.nextDoc();
      int d2 = iter2.nextDoc();
      if (d1 != d2) {
        return false;
      }
    }
  }

  /**
   * This variant of getDocSet will attempt to do some deduplication on certain DocSets such as
   * DocSets that match numDocs. This means it can return a cached version of the set, and the
   * returned set should not be modified.
   *
   * @lucene.experimental
   */
  public static DocSet getDocSet(DocSetCollector collector, SolrIndexSearcher searcher) {
    final int size = collector.size();
    if (size == searcher.numDocs()) {
      // see comment under similar block in `getDocSet(DocSet, SolrIndexSearcher)`
      return searcher.offerLiveDocs(collector::getDocSet, size);
    }

    return collector.getDocSet();
  }

  /**
   * This variant of getDocSet maps all sets with size numDocs to searcher.getLiveDocs. The returned
   * set should not be modified.
   *
   * @lucene.experimental
   */
  public static DocSet getDocSet(DocSet docs, SolrIndexSearcher searcher) {
    final int size = docs.size();
    if (size == searcher.numDocs()) {
      // if this docset has the same cardinality as liveDocs, return liveDocs instead
      // so this set will be short lived garbage.
      // This could be a very broad query, or some unusual way of running MatchAllDocsQuery?
      // In any event, we already _have_ a viable `liveDocs` DocSet, so offer it to the
      // SolrIndexSearcher, and use whatever canonical `liveDocs` instance the searcher returns
      // (which may or may not be derived from the set that we offered)
      return searcher.offerLiveDocs(() -> docs, size);
    }

    return docs;
  }

  // implementers of DocSetProducer should not call this with themselves or it will result in an
  // infinite loop
  public static DocSet createDocSet(SolrIndexSearcher searcher, Query query, DocSet filter)
      throws IOException {

    if (filter != null) {
      query = QueryUtils.combineQueryAndFilter(query, filter.makeQuery());
    }

    if (query instanceof TermQuery) {
      DocSet set = createDocSet(searcher, ((TermQuery) query).getTerm());
      // assert equals(set, createDocSetGeneric(searcher, query));
      return set;
    } else if (query instanceof DocSetProducer) {
      DocSet set = ((DocSetProducer) query).createDocSet(searcher);
      // assert equals(set, createDocSetGeneric(searcher, query));
      return set;
    } else if (query instanceof MatchAllDocsQuery) {
      DocSet set = searcher.getLiveDocSet();
      // assert equals(set, createDocSetGeneric(searcher, query));
      return set;
    }

    return createDocSetGeneric(searcher, query);
  }

  // code to produce docsets for non-docsetproducer queries
  public static DocSet createDocSetGeneric(SolrIndexSearcher searcher, Query query)
      throws IOException {

    int maxDoc = searcher.getIndexReader().maxDoc();
    DocSetCollector collector = new DocSetCollector(maxDoc);

    // This may throw an ExitableDirectoryReader.ExitingReaderException
    // but we should not catch it here, as we don't know how this DocSet will be used (it could be
    // negated before use) or cached.
    searcher.search(query, collector);

    return getDocSet(collector, searcher);
  }

  public static DocSet createDocSet(SolrIndexSearcher searcher, Term term) throws IOException {
    DirectoryReader reader = searcher.getRawReader(); // raw reader to avoid extra wrapping overhead
    int maxDoc = searcher.getIndexReader().maxDoc();
    int smallSetSize = smallSetSize(maxDoc);

    String field = term.field();
    BytesRef termVal = term.bytes();

    int maxCount = 0;
    int firstReader = -1;
    List<LeafReaderContext> leaves = reader.leaves();
    // use array for slightly higher scanning cost, but fewer memory allocations
    PostingsEnum[] postList = new PostingsEnum[leaves.size()];
    for (LeafReaderContext ctx : leaves) {
      assert leaves.get(ctx.ord) == ctx;
      LeafReader r = ctx.reader();
      Terms t = r.terms(field);
      if (t == null) continue; // field is missing
      TermsEnum te = t.iterator();
      if (te.seekExact(termVal)) {
        maxCount += te.docFreq();
        postList[ctx.ord] = te.postings(null, PostingsEnum.NONE);
        if (firstReader < 0) firstReader = ctx.ord;
      }
    }

    DocSet answer = null;
    if (maxCount == 0) {
      answer = DocSet.empty();
    } else if (maxCount <= smallSetSize) {
      answer = createSmallSet(leaves, postList, maxCount, firstReader);
    } else {
      answer = createBigSet(leaves, postList, maxDoc, firstReader);
    }

    return DocSetUtil.getDocSet(answer, searcher);
  }

  private static DocSet createSmallSet(
      List<LeafReaderContext> leaves, PostingsEnum[] postList, int maxPossible, int firstReader)
      throws IOException {
    IntBuffer[] docs = SortedIntDocSet.allocate(maxPossible);
    int sz = 0;
    for (int i = firstReader; i < postList.length; i++) {
      PostingsEnum postings = postList[i];
      if (postings == null) continue;
      LeafReaderContext ctx = leaves.get(i);
      Bits liveDocs = ctx.reader().getLiveDocs();
      int base = ctx.docBase;
      for (; ; ) {
        int subId = postings.nextDoc();
        if (subId == DocIdSetIterator.NO_MORE_DOCS) break;
        if (liveDocs != null && !liveDocs.get(subId)) continue;
        int globalId = subId + base;
        docs[sz >> SortedIntDocSet.WORDS_SHIFT].put(sz++ & SortedIntDocSet.ARR_MASK, globalId);
      }
    }

    return new SortedIntDocSet(docs, sz);
  }

  private static DocSet createBigSet(
      List<LeafReaderContext> leaves, PostingsEnum[] postList, int maxDoc, int firstReader)
      throws IOException {
    FixedBitSets bits = new FixedBitSets(maxDoc);
    int sz = 0;
    for (int i = firstReader; i < postList.length; i++) {
      PostingsEnum postings = postList[i];
      if (postings == null) continue;
      LeafReaderContext ctx = leaves.get(i);
      Bits liveDocs = ctx.reader().getLiveDocs();
      int base = ctx.docBase;
      for (; ; ) {
        int subId = postings.nextDoc();
        if (subId == DocIdSetIterator.NO_MORE_DOCS) break;
        if (liveDocs != null && !liveDocs.get(subId)) continue;
        bits.set(subId + base);
        sz++;
      }
    }

    BitDocSet docSet = new BitDocSet(bits.parts, sz);

    int smallSetSize = smallSetSize(maxDoc);
    if (sz < smallSetSize) {
      // make this optional?
      DocSet smallSet = toSmallSet(docSet);
      // assert equals(docSet, smallSet);
      return smallSet;
    }

    return docSet;
  }

  public static DocSet toSmallSet(BitDocSet bitSet) {
    int sz = bitSet.size();
    IntBuffer[] docs = SortedIntDocSet.allocate(sz);
    FixedBitSets bs = bitSet.getBits();
    BitDocSet.BitSetsIterator iter = new BitDocSet.BitSetsIterator(bs.parts, bs.length(), sz);
    for (int i = 0; i < sz; i++) {
      docs[i >> SortedIntDocSet.WORDS_SHIFT].put(i & SortedIntDocSet.ARR_MASK, iter.nextDoc());
    }
    return new SortedIntDocSet(docs);
  }

  public static void collectSortedDocSet(DocSet docs, IndexReader reader, Collector collector)
      throws IOException {
    // TODO add SortedDocSet sub-interface and take that.
    // TODO collectUnsortedDocSet: iterate segment, then all docSet per segment.

    final List<LeafReaderContext> leaves = reader.leaves();
    final Iterator<LeafReaderContext> ctxIt = leaves.iterator();
    int segBase = 0;
    int segMax;
    int adjustedMax = 0;
    LeafReaderContext ctx = null;
    LeafCollector leafCollector = null;
    for (DocIterator docsIt = docs.iterator(); docsIt.hasNext(); ) {
      final int doc = docsIt.nextDoc();
      if (doc >= adjustedMax) {
        do {
          ctx = ctxIt.next();
          segBase = ctx.docBase;
          segMax = ctx.reader().maxDoc();
          adjustedMax = segBase + segMax;
        } while (doc >= adjustedMax);
        leafCollector = collector.getLeafCollector(ctx);
      }
      if (doc < segBase) {
        throw new IllegalStateException(
            "algorithm expects sorted DocSet but wasn't: " + docs.getClass());
      }
      leafCollector.collect(doc - segBase); // per-seg collectors
    }
  }

  /**
   * Utility method to copy a specified range of {@link Bits} to a specified offset in a destination
   * {@link FixedBitSet}. This can be useful, e.g., for translating per-segment bits ranges to
   * composite DocSet bits ranges.
   *
   * @param src source Bits
   * @param srcOffset start offset (inclusive) in source Bits
   * @param srcLimit end offset (exclusive) in source Bits
   * @param dest destination FixedBitSet
   * @param destOffset start offset of range in destination
   */
  static void copyTo(
      Bits src, final int srcOffset, int srcLimit, FixedBitSets dest, int destOffset) {
    /*
    NOTE: `adjustedSegDocBase` +1 to compensate for the fact that `segOrd` always has to "read
    ahead" by 1. Adding 1 to set `adjustedSegDocBase` once allows us to use `segOrd` as-is (with
    no "pushback") for both `startIndex` and `endIndex` args to `dest.set(startIndex, endIndex)`
     */
    final int adjustedSegDocBase = destOffset - srcOffset + 1;
    int segOrd = srcLimit;
    do {
      /*
      NOTE: we check deleted range before live range in the outer loop in order to not have
      to explicitly guard against `dest.set(maxDoc, maxDoc)` in the event that the global max doc
      is a delete (this case would trigger a bounds-checking AssertionError in
      `FixedBitSet.set(int, int)`).
       */
      do {
        // consume deleted range
        if (--segOrd < srcOffset) {
          // we're currently in a "deleted" run, so just return; no need to do anything further
          return;
        }
      } while (!src.get(segOrd));
      final int limit = segOrd; // set highest ord (inclusive) of live range
      while (segOrd-- > srcOffset && src.get(segOrd)) {
        // consume live range
      }
      dest.set(adjustedSegDocBase + segOrd, adjustedSegDocBase + limit);
    } while (segOrd > srcOffset);
  }

  private static long clearLow(long v, int numBits) {
    return (v >>> numBits) << numBits;
  }

  private static long clearHigh(long v, int numBits) {
    return (v << numBits) >>> numBits;
  }

  public static void copyBitRange(
      final FixedBitSets src,
      final int srcIdx,
      final FixedBitSets dest,
      final int destIdx,
      final int len) {
    copyBitRange(src.parts, srcIdx, dest.parts, destIdx, len);
  }

  public static void copyBitRange(
      final FixedBitSet[] src,
      final int srcIdx,
      final FixedBitSet[] dest,
      final int destIdx,
      final int len) {
    LongBuffer[] src2 = Arrays.stream(src).map(FixedBitSet::getBits).toArray(LongBuffer[]::new);
    LongBuffer[] dest2 = Arrays.stream(dest).map(FixedBitSet::getBits).toArray(LongBuffer[]::new);
    copyBitRange(src2, srcIdx, dest2, destIdx, len);
  }

  public static void copyBitRange(
      final long[] src, final int srcIdx, final long[] dest, final int destIdx, final int len) {
    copyBitRange(LongBuffer.wrap(src), srcIdx, LongBuffer.wrap(dest), destIdx, len);
  }

  public static void copyBitRange(
      final long[][] src, final int srcIdx, final long[][] dest, final int destIdx, final int len) {
    LongBuffer[] src2 = Arrays.stream(src).map(LongBuffer::wrap).toArray(LongBuffer[]::new);
    LongBuffer[] dest2 = Arrays.stream(dest).map(LongBuffer::wrap).toArray(LongBuffer[]::new);
    copyBitRange(src2, srcIdx, dest2, destIdx, len);
  }

  /**
   * Analogous to {@link #copyBitRange(LongBuffer, int, LongBuffer, int, int)}, but takes
   * 2-dimensional {@code long[]} as input (formatted according to boundaries defined by
   * partitioning scheme of {@link BitDocSet#BIT_SHIFT}.
   */
  public static void copyBitRange(
      final LongBuffer[] src,
      final int srcIdx,
      final LongBuffer[] dest,
      final int destIdx,
      final int len) {
    if (len == 0) return;
    int srcOuterOffset = srcIdx >> BitDocSet.BIT_SHIFT;
    final int destOuterOffset = destIdx >> BitDocSet.BIT_SHIFT;
    int srcInnerOffset = srcIdx & BitDocSet.BLOCK_BIT_MASK;
    int destInnerOffset = destIdx & BitDocSet.BLOCK_BIT_MASK;
    final int len1;
    final int len2;
    LongBuffer srcArr1;
    LongBuffer srcArr2;

    // the array offset of the word for the last "bit" element.
    final int destOuterLimit = (destIdx + len - 1) >> BitDocSet.BIT_SHIFT;

    if (srcInnerOffset <= destInnerOffset) {
      len1 = destInnerOffset - srcInnerOffset;
      len2 = BitDocSet.MAX_BLOCK_BITS - len1;
      srcArr1 = null;
      srcArr2 = src[srcOuterOffset];
    } else {
      len2 = srcInnerOffset - destInnerOffset;
      len1 = BitDocSet.MAX_BLOCK_BITS - len2;
      srcArr1 = src[srcOuterOffset]; // clear out-of-scope bits
      srcArr2 = ++srcOuterOffset < src.length ? src[srcOuterOffset] : null;
    }
    // special handling for the first word, which may be partial
    LongBuffer destArr = dest[destOuterOffset];
    if (srcArr1 == null) {
      copyBitRange(
          srcArr2,
          srcInnerOffset,
          destArr,
          destInnerOffset,
          Math.min(len, BitDocSet.MAX_BLOCK_BITS - destInnerOffset));
    } else if (srcArr2 == null) {
      copyBitRange(
          srcArr1,
          srcInnerOffset,
          destArr,
          destInnerOffset,
          Math.min(len, BitDocSet.MAX_BLOCK_BITS - srcInnerOffset));
    } else {
      int initialLen = BitDocSet.MAX_BLOCK_BITS - srcInnerOffset;
      if (len <= initialLen) {
        copyBitRange(srcArr1, srcInnerOffset, destArr, destInnerOffset, len);
      } else {
        copyBitRange(srcArr1, srcInnerOffset, destArr, destInnerOffset, initialLen);
        copyBitRange(
            srcArr2, 0, destArr, destInnerOffset + initialLen, Math.min(len2, len - initialLen));
      }
    }
    if (destOuterOffset == destOuterLimit) return;

    for (int i = destOuterOffset + 1; i < destOuterLimit; i++) {
      // inner words are guaranteed to not be partial, so this can be very simple
      srcArr1 = srcArr2;
      srcArr2 = src[++srcOuterOffset];
      destArr = dest[i];
      copyBitRange(srcArr1, len2, destArr, 0, len1);
      copyBitRange(srcArr2, 0, destArr, len1, len2);
    }
    srcArr1 = srcArr2;
    srcArr2 = ++srcOuterOffset < src.length ? src[srcOuterOffset] : null;

    // special handling for the last word, which may be partial
    int remainder = ((destIdx + len - 1) & BitDocSet.BLOCK_BIT_MASK) + 1;
    destArr = dest[destOuterLimit];
    if (srcArr2 == null || remainder <= len1) {
      copyBitRange(srcArr1, len2, destArr, 0, remainder);
    } else {
      copyBitRange(srcArr1, len2, destArr, 0, len1);
      copyBitRange(srcArr2, 0, destArr, len1, remainder - len1);
    }
  }

  /**
   * Analogous to {@link System#arraycopy(Object, int, Object, int, int)}, but copies {@code long[]}
   * of the format that is used to back {@link FixedBitSet}, e.g., with offset and length args
   * specified in terms of <i>bit</i> index, not {@code long} word (array index).
   *
   * <p>This method is optimized to avoid inspecting individual bits, instead issuing far fewer
   * instructions and bit-shifting "words" as necessary to align with potentially different word
   * boundaries in the destination array. It is vastly more efficient than analogous bit-by-bit
   * operations -- on the order of ~100x.
   */
  public static void copyBitRange(
      final LongBuffer src,
      final int srcIdx,
      final LongBuffer dest,
      final int destIdx,
      final int len) {
    if (len == 0) return;
    int srcOuterOffset = srcIdx >> 6;
    int destOuterOffset = destIdx >> 6;
    int srcInnerOffset = srcIdx & 63;
    int destInnerOffset = destIdx & 63;
    int rightShift1;
    int leftShift2;
    long srcWord1;
    long srcWord2;

    // the array offset of the word for the last "bit" element.
    final int destOuterLimit = (destIdx + len - 1) >> 6;

    if (srcInnerOffset <= destInnerOffset) {
      leftShift2 = destInnerOffset - srcInnerOffset;
      rightShift1 = Long.SIZE - leftShift2;
      srcWord1 = 0;
      srcWord2 = clearLow(src.get(srcOuterOffset), srcInnerOffset); // clear out-of-scope bits
    } else {
      rightShift1 = srcInnerOffset - destInnerOffset;
      leftShift2 = Long.SIZE - rightShift1;
      srcWord1 = clearLow(src.get(srcOuterOffset), srcInnerOffset); // clear out-of-scope bits
      srcWord2 = ++srcOuterOffset < src.capacity() ? src.get(srcOuterOffset) : 0;
    }
    // NOTE: we have to special-case the `leftShift=0` case because right-shift over too-large
    // values is defined as `shift % Long.SIZE`. i.e., `N >>> 64` does _not_ clear all bits,
    // but rather is equivalent to `N >>> 0` (identity).
    long incoming = (leftShift2 == 0 ? 0 : (srcWord1 >>> rightShift1)) | (srcWord2 << leftShift2);
    long extant = clearHigh(dest.get(destOuterOffset), Long.SIZE - destInnerOffset);
    if (destOuterOffset == destOuterLimit) {
      // very short `len` -- special case
      int remainder = ((destIdx + len - 1) & 63) + 1;
      extant |= clearLow(dest.get(destOuterOffset), remainder);
      incoming = clearHigh(incoming, Long.SIZE - remainder);
      dest.put(destOuterOffset, extant | incoming);
      return;
    }
    // special handling for the first word, which may be partial
    dest.put(destOuterOffset, extant | incoming);

    for (int i = destOuterOffset + 1; i < destOuterLimit; i++) {
      // inner words are guaranteed to not be partial, so this can be very simple
      srcWord1 = srcWord2;
      srcWord2 = src.get(++srcOuterOffset);
      dest.put(i, (leftShift2 == 0 ? 0 : (srcWord1 >>> rightShift1)) | (srcWord2 << leftShift2));
    }
    srcWord1 = srcWord2;
    srcWord2 = ++srcOuterOffset < src.capacity() ? src.get(srcOuterOffset) : 0;

    // special handling for the last word, which may be partial
    int remainder = ((destIdx + len - 1) & 63) + 1;
    extant = clearLow(dest.get(destOuterLimit), remainder);
    incoming = (leftShift2 == 0 ? 0 : (srcWord1 >>> rightShift1)) | (srcWord2 << leftShift2);
    incoming = clearHigh(incoming, Long.SIZE - remainder);
    dest.put(destOuterLimit, extant | incoming);
  }
}
