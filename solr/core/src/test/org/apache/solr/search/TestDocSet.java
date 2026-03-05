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

import static org.apache.solr.search.DocSetUtil.copyBitRange;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorSpecies;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.Version;
import org.apache.solr.SolrTestCase;

/** */
public class TestDocSet extends SolrTestCase {
  Random rand;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    rand = random();
  }

  // test the DocSetCollector
  @SuppressWarnings("BadShiftAmount")
  public void collect(DocSet set, int maxDoc) {
    int smallSetSize = maxDoc >> (64 + 3);
    if (set.size() > 1) {
      if (random().nextBoolean()) {
        smallSetSize = set.size() + random().nextInt(3) - 1; // test the bounds around smallSetSize
      }
    }
    DocSetCollector collector = new DocSetCollector(smallSetSize, maxDoc);

    for (DocIterator i1 = set.iterator(); i1.hasNext(); ) {
      try {
        collector.collect(i1.nextDoc());
      } catch (IOException e) {
        throw new RuntimeException(e); // should be impossible
      }
    }

    DocSet result = collector.getDocSet();
    iter(set, result); // check that they are equal
  }

  public FixedBitSet getRandomSet(int sz, int bitsToSet) {
    FixedBitSet bs = new FixedBitSet(sz);
    if (sz == 0) return bs;
    for (int i = 0; i < bitsToSet; i++) {
      bs.set(rand.nextInt(sz));
    }
    return bs;
  }

  public DocSet getIntDocSet(FixedBitSet bs) {
    int size = bs.cardinality();
    int lim = bs.length();
    SortedIntDocSet.Parts parts = SortedIntDocSet.allocate(size);
    IntBuffer[] docs = parts.arr;
    int doc = -1;
    int i = 0;
    while (++doc < lim && (doc = bs.nextSetBit(doc)) != DocIdSetIterator.NO_MORE_DOCS) {
      docs[i >> SortedIntDocSet.WORDS_SHIFT].put(i++ & SortedIntDocSet.ARR_MASK, doc);
    }
    return new SortedIntDocSet(parts);
  }

  public DocSet getBitDocSet(FixedBitSet bs) {
    return new BitDocSet(partition(bs));
  }

  public DocSlice getDocSlice(FixedBitSet bs) {
    int len = bs.cardinality();
    int[] arr = new int[len + 5];
    arr[0] = 10;
    arr[1] = 20;
    arr[2] = 30;
    arr[arr.length - 1] = 1;
    arr[arr.length - 2] = 2;
    int offset = 3;
    int end = offset + len;

    BitSetIterator iter = new BitSetIterator(bs, 0);
    // put in opposite order... DocLists are not ordered.
    for (int i = end - 1; i >= offset; i--) {
      arr[i] = iter.nextDoc();
    }

    return new DocSlice(offset, len, arr, null, len * 2, 100.0f, TotalHits.Relation.EQUAL_TO);
  }

  public DocSet getDocSet(FixedBitSet bs) {
    switch (rand.nextInt(9)) {
      case 0:
      case 1:
      case 2:
      case 3:
        return getBitDocSet(bs);

      case 4:
        return getIntDocSet(bs);
      case 5:
        return getIntDocSet(bs);
      case 6:
        return getIntDocSet(bs);
      case 7:
        return getIntDocSet(bs);
      case 8:
        return getIntDocSet(bs);
    }
    return null;
  }

  public void checkEqual(FixedBitSet bs, DocSet set) {
    for (int i = 0, lim = set.size(); i < lim; i++) {
      assertEquals(bs.get(i), set.exists(i));
    }
    assertEquals(bs.cardinality(), set.size());
  }

  public void iter(DocSet d1, DocSet d2) {

    DocIterator i1 = d1.iterator();
    DocIterator i2 = d2.iterator();

    assertEquals(i1.hasNext(), i2.hasNext());

    for (; ; ) {
      boolean b1 = i1.hasNext();
      boolean b2 = i2.hasNext();
      assertEquals(b1, b2);
      if (!b1) break;
      assertEquals(i1.nextDoc(), i2.nextDoc());
    }
  }

  private static FixedBitSets partition(FixedBitSet b) {
    int numBits = b.length();
    FixedBitSets ret = new FixedBitSets(numBits);
    int doc = -1;
    while (++doc < numBits && (doc = b.nextSetBit(doc)) != DocIdSetIterator.NO_MORE_DOCS) {
      ret.set(doc);
    }
    return ret;
  }

  protected void doSingle(int maxSize) {
    int sz = rand.nextInt(maxSize + 1);
    int sz2 = rand.nextInt(maxSize);
    FixedBitSet bs1 = getRandomSet(sz, rand.nextInt(sz + 1));
    FixedBitSet bs2 = getRandomSet(sz, rand.nextInt(sz2 + 1));

    DocSet a1 = new BitDocSet(partition(bs1));
    DocSet a2 = new BitDocSet(partition(bs2));
    DocSet b1 = getDocSet(bs1);
    DocSet b2 = getDocSet(bs2);

    checkEqual(bs1, b1);
    checkEqual(bs2, b2);

    iter(a1, b1);
    iter(a2, b2);

    collect(a1, maxSize);
    collect(a2, maxSize);

    FixedBitSet a_and = bs1.clone();
    a_and.and(bs2);
    FixedBitSet a_or = bs1.clone();
    a_or.or(bs2);
    // FixedBitSet a_xor = bs1.clone(); a_xor.xor(bs2);
    FixedBitSet a_andn = bs1.clone();
    a_andn.andNot(bs2);

    checkEqual(a_and, b1.intersection(b2));
    checkEqual(a_or, b1.union(b2));
    checkEqual(a_andn, b1.andNot(b2));

    assertEquals(a_and.cardinality(), b1.intersectionSize(b2));
    assertEquals(a_or.cardinality(), b1.unionSize(b2));
    assertEquals(a_andn.cardinality(), b1.andNotSize(b2));
  }

  public void doMany(int maxSz, int iter) {
    for (int i = 0; i < iter; i++) {
      doSingle(maxSz);
    }
  }

  public void testRandomDocSets() {
    // Make the size big enough to go over certain limits, such as one set
    // being 8 times the size of another in the int set, or going over 2 times
    // 64 bits for the bit doc set.  Smaller sets can hit more boundary conditions though.

    doMany(130, 10000);
    // doMany(130, 1000000);
  }

  public DocSet getRandomDocSet(int n, int maxDoc) {
    FixedBitSet obs = new FixedBitSet(maxDoc);
    int[] a = new int[n];
    for (int i = 0; i < n; i++) {
      for (; ; ) {
        int idx = rand.nextInt(maxDoc);
        if (obs.getAndSet(idx)) continue;
        a[i] = idx;
        break;
      }
    }

    if (n <= smallSetCutoff) {
      if (smallSetType == 0) {
        Arrays.sort(a);
        SortedIntDocSet.Parts parts = SortedIntDocSet.allocate(n);
        IntBuffer[] dimensional = parts.arr;
        for (int i = 0; i < n; i++) {
          dimensional[i >> SortedIntDocSet.WORDS_SHIFT].put(i & SortedIntDocSet.ARR_MASK, a[i]);
        }
        return new SortedIntDocSet(parts);
      }
    }

    return new BitDocSet(partition(obs), n);
  }

  public DocSet[] getRandomSets(int nSets, int minSetSize, int maxSetSize, int maxDoc) {
    DocSet[] sets = new DocSet[nSets];

    for (int i = 0; i < nSets; i++) {
      int sz;
      sz = rand.nextInt(maxSetSize - minSetSize + 1) + minSetSize;
      // different distribution
      // sz = (maxSetSize+1)/(rand.nextInt(maxSetSize)+1) + minSetSize;
      sets[i] = getRandomDocSet(sz, maxDoc);
    }

    return sets;
  }

  public static int smallSetType = 0; // 0==sortedint, 2==FixedBitSet
  public static int smallSetCutoff = 3000;

  /*
  public void testIntersectionSizePerformance() {
    rand=new Random(1);  // make deterministic

    int minBigSetSize=1,maxBigSetSize=30000;
    int minSmallSetSize=1,maxSmallSetSize=30000;
    int nSets=1024;
    int iter=1;
    int maxDoc=1000000;


    smallSetCuttoff = maxDoc>>6; // break even for SortedIntSet is /32... but /64 is better for performance
    // smallSetCuttoff = maxDoc;


    DocSet[] bigsets = getRandomSets(nSets, minBigSetSize, maxBigSetSize, maxDoc);
    DocSet[] smallsets = getRandomSets(nSets, minSmallSetSize, maxSmallSetSize, maxDoc);
    int ret=0;
    long start=System.currentTimeMillis();
    for (int i=0; i<iter; i++) {
      for (DocSet s1 : bigsets) {
        for (DocSet s2 : smallsets) {
          ret += s1.intersectionSize(s2);
        }
      }
    }
    long end=System.currentTimeMillis();
    System.out.println("intersectionSizePerformance="+(end-start)+" ms");
    System.out.println("ret="+ret);
  }
   ***/

  public LeafReader dummyIndexReader(final int maxDoc) {
    return new LeafReader() {
      @Override
      public int maxDoc() {
        return maxDoc;
      }

      @Override
      @Deprecated
      public Fields getTermVectors(int docID) {
        return null;
      }

      @Override
      public TermVectors termVectors() {
        return null;
      }

      @Override
      public int numDocs() {
        return maxDoc;
      }

      @Override
      public FieldInfos getFieldInfos() {
        return FieldInfos.EMPTY;
      }

      @Override
      public Bits getLiveDocs() {
        return null;
      }

      @Override
      public Terms terms(String field) {
        return null;
      }

      @Override
      public NumericDocValues getNumericDocValues(String field) {
        return null;
      }

      @Override
      public BinaryDocValues getBinaryDocValues(String field) {
        return null;
      }

      @Override
      public SortedDocValues getSortedDocValues(String field) {
        return null;
      }

      @Override
      public SortedNumericDocValues getSortedNumericDocValues(String field) {
        return null;
      }

      @Override
      public SortedSetDocValues getSortedSetDocValues(String field) {
        return null;
      }

      @Override
      public NumericDocValues getNormValues(String field) {
        return null;
      }

      @Override
      public PointValues getPointValues(String field) {
        return null;
      }

      @Override
      public FloatVectorValues getFloatVectorValues(String field) {
        return null;
      }

      @Override
      public ByteVectorValues getByteVectorValues(String field) {
        return null;
      }

      @Override
      public StoredFields storedFields() {
        return null;
      }

      @Override
      public void searchNearestVectors(
          String field, float[] target, KnnCollector knnCollector, Bits acceptDocs) {}

      @Override
      public void searchNearestVectors(
          String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs) {}

      @Override
      protected void doClose() {}

      @Override
      @Deprecated
      public void document(int doc, StoredFieldVisitor visitor) {}

      @Override
      public void checkIntegrity() {}

      @Override
      public LeafMetaData getMetaData() {
        return new LeafMetaData(Version.LATEST.major, Version.LATEST, null, true);
      }

      @Override
      public CacheHelper getCoreCacheHelper() {
        return null;
      }

      @Override
      public CacheHelper getReaderCacheHelper() {
        return null;
      }
    };
  }

  public IndexReader dummyMultiReader(int nSeg, int maxDoc) throws IOException {
    if (nSeg == 1 && rand.nextBoolean()) return dummyIndexReader(rand.nextInt(maxDoc));

    IndexReader[] subs = new IndexReader[rand.nextInt(nSeg) + 1];
    for (int i = 0; i < subs.length; i++) {
      subs[i] = dummyIndexReader(rand.nextInt(maxDoc));
    }

    return new MultiReader(subs);
  }

  private static boolean checkNullOrEmpty(DocIdSetIterator[] disis) throws IOException {
    for (DocIdSetIterator disi : disis) {
      if (disi == null) {
        for (DocIdSetIterator check : disis) {
          if (check != null) {
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, check.nextDoc());
          }
        }
        return true;
      }
    }
    return false;
  }

  private static Supplier<DocIdSetIterator> disiSupplier(final DocIdSet docs) {
    return () -> {
      try {
        return docs.iterator();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };
  }

  @SafeVarargs
  private static void populateDisis(
      NoThrowDocIdSetIterator[] disis, Supplier<DocIdSetIterator>... suppliers) {
    for (int i = 0; i < suppliers.length; i++) {
      DocIdSetIterator disi = suppliers[i].get();
      disis[i] = disi == null ? null : new NoThrowDocIdSetIterator(disi);
    }
  }

  private static void populateDocs(
      NoThrowDocIdSetIterator[] disis, int[] docs, ToIntFunction<NoThrowDocIdSetIterator> toDocId) {
    for (int i = 0; i < docs.length; i++) {
      docs[i] = toDocId.applyAsInt(disis[i]);
    }
  }

  private static void assertAll(int expected, int[] docs) {
    for (int doc : docs) {
      assertEquals(expected, doc);
    }
  }

  /**
   * By wrapping exceptions (which we don't expect to have thrown in this context anyway), we allow
   * for more transparent/readable inline functions.
   */
  private static class NoThrowDocIdSetIterator extends DocIdSetIterator {
    private final DocIdSetIterator backing;

    private NoThrowDocIdSetIterator(DocIdSetIterator backing) {
      this.backing = backing;
    }

    @Override
    public int advance(int target) {
      try {
        return backing.advance(target);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public long cost() {
      return backing.cost();
    }

    @Override
    public int docID() {
      return backing.docID();
    }

    @Override
    public int nextDoc() {
      try {
        return backing.nextDoc();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @SafeVarargs
  private void doTestIteratorEqual(Bits bits, Supplier<DocIdSetIterator>... disiSuppliers)
      throws IOException {
    NoThrowDocIdSetIterator[] disis = new NoThrowDocIdSetIterator[disiSuppliers.length];
    int[] docs = new int[disiSuppliers.length];
    populateDisis(disis, disiSuppliers);
    if (checkNullOrEmpty(disis)) {
      // both iterators are empty or null (equivalent), so there's nothing more to check
      return;
    }

    // test for next() equivalence
    final int bitsLength = bits == null ? -1 : bits.length();
    int bitsDoc = -1;
    for (; ; ) {
      populateDocs(disis, docs, (disi) -> disi.nextDoc());
      final int expected = docs[0]; // arbitrarily pick the first as "expected"
      assertAll(expected, docs);
      populateDocs(disis, docs, (disi) -> disi.docID());
      assertAll(expected, docs);
      while (++bitsDoc < expected && bitsDoc < bitsLength) {
        assertFalse(bits.get(bitsDoc));
      }
      if (expected == DocIdSetIterator.NO_MORE_DOCS) break;
      assertTrue(bits.get(expected));
    }

    for (int i = 0; i < 10; i++) {
      // test random skipTo() and next()
      populateDisis(disis, disiSuppliers);
      bitsDoc = -1;
      int doc = -1;
      for (; ; ) {
        final int target;
        if (rand.nextBoolean()) {
          target = doc + 1;
          populateDocs(disis, docs, (disi) -> disi.nextDoc());
        } else {
          target =
              doc
                  + rand.nextInt(10)
                  + 1; // keep in mind future edge cases like probing (increase if necessary)
          populateDocs(disis, docs, (disi) -> disi.advance(target));
        }

        final int expected = docs[0]; // arbitrarily pick the first as "expected"
        assertAll(expected, docs);
        populateDocs(disis, docs, (disi) -> disi.docID());
        assertAll(expected, docs);
        for (int j = target; j < expected && j < bitsLength; j++) {
          assertFalse(bits.get(j));
        }
        if (expected == DocIdSetIterator.NO_MORE_DOCS) break;
        assertTrue(bits.get(expected));
        doc = expected;
      }
    }
  }

  /**
   * Tests equivalence among {@link DocIdSetIterator} instances retrieved from {@link BitDocSet} and
   * {@link SortedIntDocSet} implementations, via {@link DocSet#makeQuery()} and directly via {@link
   * DocSet#iterator(LeafReaderContext)}. Also tests corresponding random-access {@link Bits}
   * instances retrieved via {@link DocSet#makeQuery()}/ {@link DocIdSet#bits()}.
   */
  public void doFilterTest(IndexReader reader) throws IOException {
    IndexReaderContext topLevelContext = reader.getContext();
    FixedBitSet bs = getRandomSet(reader.maxDoc(), rand.nextInt(reader.maxDoc() + 1));
    DocSet a = new BitDocSet(partition(bs));
    DocSet b = getIntDocSet(bs);

    //    Query fa = a.makeQuery();
    //    Query fb = b.makeQuery();

    /* top level filters are no longer supported
    // test top-level
    DocIdSet da = fa.getDocIdSet(topLevelContext);
    DocIdSet db = fb.getDocIdSet(topLevelContext);
    doTestIteratorEqual(da, db);
    ***/

    List<LeafReaderContext> leaves = topLevelContext.leaves();
    // first test in-sequence sub readers
    for (LeafReaderContext readerContext : leaves) {
      // there are various ways that disis can be retrieved for each leafReader; they should all be
      // equivalent.
      doTestIteratorEqual(
          getExpectedBits(a, readerContext),
          () -> a.iterator(readerContext),
          () -> b.iterator(readerContext));
    }

    int nReaders = leaves.size();
    // now test out-of-sequence sub readers
    for (int i = 0; i < nReaders; i++) {
      LeafReaderContext readerContext = leaves.get(rand.nextInt(nReaders));
      doTestIteratorEqual(
          getExpectedBits(a, readerContext),
          () -> a.iterator(readerContext),
          () -> b.iterator(readerContext));
    }
  }

  public void testFilter() throws IOException {
    // keeping these numbers smaller help hit more edge cases
    int maxSeg = 4;
    // increase if future changes add more edge cases (like probing a certain distance in the bin
    // search)
    int maxDoc = 5;
    for (int i = 0; i < 5000; i++) {
      IndexReader r = dummyMultiReader(maxSeg, maxDoc);
      doFilterTest(r);
    }
  }

  private static final int MAX_SRC_SIZE = 130; // push _just_ into 3 `long` "words"

  public void testCopyBitsToRange() {
    // Sanity-check round-tripping.
    for (int i = 0; i < 1000; i++) {
      final int sz = rand.nextInt(MAX_SRC_SIZE);
      final FixedBitSet src = getRandomSet(sz, rand.nextInt(sz + 1));
      final int destSize = sz * 2;
      final int destOffset =
          sz == 0
              ? 0
              : (rand.nextInt(sz) + 1); // +1 here b/c we want nextInt(sz) _inclusive_ of sz.
      final FixedBitSets dest = new FixedBitSets(destSize);
      final FixedBitSets roundTrip = new FixedBitSets(sz);
      DocSetUtil.copyTo(src.asReadOnlyBits(), 0, sz, dest, destOffset);
      DocSetUtil.copyTo(dest.asReadOnlyBits(), destOffset, destOffset + sz, roundTrip, 0);
      assertArrayEquals(partition(src).parts, roundTrip.parts);
    }
  }

  public void testCopyTo() {
    Random r = random();
    // test edge cases
    int[] sizes =
        new int[] {
          0,
          1,
          BitDocSet.MAX_BLOCK_BITS - 1,
          BitDocSet.MAX_BLOCK_BITS,
          BitDocSet.MAX_BLOCK_BITS + 1,
          (BitDocSet.MAX_BLOCK_BITS << 1) - 1,
          BitDocSet.MAX_BLOCK_BITS << 1,
          (BitDocSet.MAX_BLOCK_BITS << 1) + 1
        };
    for (int size : sizes) {
      testCopyTo0(size, r.nextLong());
    }
    // now test big
    for (int i = 0; i < 10; i++) {
      testCopyTo0(100_000_000, r.nextLong());
    }
  }

  private void testCopyTo0(int size, long seed) {
    Random r = new Random(seed);
    long[] options = new long[Long.SIZE];
    for (int i = options.length - 1; i >= 0; i--) {
      options[i] = ~(1L << i);
    }
    long[] vals = new long[FixedBitSet.bits2words(size)];
    if (vals.length > 0) {
      // ghost bits must be clear
      vals[vals.length - 1] = options[r.nextInt(options.length)] & ~(-1L << size);
    }

    for (int i = vals.length - 2; i >= 0; i--) {
      vals[i] = options[r.nextInt(options.length)];
    }
    FixedBitSet src = new FixedBitSet(LongBuffer.wrap(vals), size);
    FixedBitSets dest = new FixedBitSets(size);
    long start = System.nanoTime();
    DocSetUtil.copyTo(src, 0, size, dest, 0);
    System.err.println(
        "copyTo duration: " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + "ms");
    for (int i = size - 1; i >= 0; i--) {
      assertEquals(src.get(i), dest.get(i));
    }
  }

  private Bits getExpectedBits(final DocSet docSet, final LeafReaderContext context) {
    if (context.isTopLevel) {
      return docSet.getBits();
    }

    final int base = context.docBase;
    final int length = context.reader().maxDoc();
    final FixedBitSets bs = docSet.getFixedBitSet();
    return new Bits() {
      @Override
      public boolean get(int index) {
        return bs.get(index + base);
      }

      @Override
      public int length() {
        return length;
      }
    };
  }

  public void testBlah() {
    for (int j = 0; j < 5; j++) {
      long nanos = 0;
      for (int i = 10; i > 0; i--) {
        nanos += testBlah0();
      }
      System.err.println("blah0 "+TimeUnit.NANOSECONDS.toMillis(nanos / 10));
      nanos = 0;
      for (int i = 10; i > 0; i--) {
        nanos += testBlah1();
      }
      System.err.println("blah1 "+TimeUnit.NANOSECONDS.toMillis(nanos / 10));
      nanos = 0;
      for (int i = 10; i > 0; i--) {
        nanos += testBlah1a();
      }
      System.err.println("blah1a "+TimeUnit.NANOSECONDS.toMillis(nanos / 10));
      nanos = 0;
      for (int i = 10; i > 0; i--) {
        nanos += testBlah1b();
      }
      System.err.println("blah1b "+TimeUnit.NANOSECONDS.toMillis(nanos / 10));
      nanos = 0;
      for (int i = 10; i > 0; i--) {
        nanos += testBlah1c();
      }
      System.err.println("blah1c "+TimeUnit.NANOSECONDS.toMillis(nanos / 10));
      nanos = 0;
      for (int i = 10; i > 0; i--) {
        nanos += testBlah2();
      }
      System.err.println("blah2 "+TimeUnit.NANOSECONDS.toMillis(nanos / 10));
    }
    if (true) return;
    System.err.println("blahQ");
    for (int i = 10; i > 0; i--) {
      testBlahQ();
    }
    System.err.println("blah0");
    for (int i = 10; i > 0; i--) {
      testBlah0();
    }
    System.err.println("blah1");
    for (int i = 10; i > 0; i--) {
      testBlah1();
    }
    System.err.println("blah2");
    for (int i = 10; i > 0; i--) {
      testBlah2();
    }
    System.err.println("blah3");
    for (int i = 10; i > 0; i--) {
      testBlah3();
    }
    System.err.println("blah4");
    for (int i = 10; i > 0; i--) {
      testBlah4();
    }
  }

  private static long testBlahQ() {
    int reps = 1000;
    int size = 1 << 22; // 4m
    long[] data = new long[size];
    long start = System.nanoTime();
    for (int i = reps; i > 0; i--) {
      for (int j = size - 1; j >= 0; j--) {
        data[j]++;
      }
    }
    long ret = System.nanoTime() - start;
    System.err.println("\tDONE! "+TimeUnit.NANOSECONDS.toMillis(ret));
    for (int i = size - 1; i >= 0; i--) {
      assertEquals(reps, data[i]);
    }
    return ret;
  }

  private static long testBlah0() {
    int reps = 1000;
    int size = 1 << 22; // 4m
    long[] data = new long[size];
    long start = System.nanoTime();
    for (int i = reps; i > 0; i--) {
      for (int j = size - 1; j >= 0; j--) {
        data[j] += 1;
      }
    }
    long ret = System.nanoTime() - start;
    System.err.println("\tDONE! "+TimeUnit.NANOSECONDS.toMillis(ret));
    for (int i = size - 1; i >= 0; i--) {
      assertEquals(reps, data[i]);
    }
    return ret;
  }

  private static long testBlah1() {
    int reps = 1000;
    int size = 1 << 22; // 4m
    LongBuffer data = LongBuffer.allocate(size);
    long start = System.nanoTime();
    for (int i = reps; i > 0; i--) {
      for (int j = size - 1; j >= 0; j--) {
        data.put(j, data.get(j) + 1);
      }
    }
    long ret = System.nanoTime() - start;
    System.err.println("\tDONE! "+TimeUnit.NANOSECONDS.toMillis(ret));
    for (int i = size - 1; i >= 0; i--) {
      assertEquals(reps, data.get(i));
    }
    return ret;
  }

  private static long testBlah1a() {
    int reps = 1000;
    final int size = 1 << 22; // 4m
    LongBuffer data = LongBuffer.allocate(size);
    long start = System.nanoTime();
    for (int i = reps; i > 0; i--) {
      for (int j = 0; j < size; j++) {
        data.put(j, data.get(j) + 1);
      }
    }
    long ret = System.nanoTime() - start;
    System.err.println("\tDONE! "+TimeUnit.NANOSECONDS.toMillis(ret));
    for (int i = size - 1; i >= 0; i--) {
      assertEquals(reps, data.get(i));
    }
    return ret;
  }

  private static long testBlah1b() {
    int reps = 1000;
    int size = 1 << 22; // 4m
    LongBuffer data = ByteBuffer.allocate(size * Long.BYTES).asLongBuffer();
    long start = System.nanoTime();
    for (int i = reps; i > 0; i--) {
      for (int j = size - 1; j >= 0; j--) {
        data.put(j, data.get(j) + 1);
      }
    }
    long ret = System.nanoTime() - start;
    System.err.println("\tDONE! "+TimeUnit.NANOSECONDS.toMillis(ret));
    for (int i = size - 1; i >= 0; i--) {
      assertEquals(reps, data.get(i));
    }
    return ret;
  }

  @SuppressWarnings("preview")
  private static long testBlah1c() {
    int reps = 1000;
    int size = 1 << 22; // 4m
    ByteBuffer bb = ByteBuffer.allocateDirect(size * Long.BYTES).order(ByteOrder.nativeOrder());
    MemorySegment seg = MemorySegment.ofBuffer(bb);
    LongBuffer data = bb.asLongBuffer();
    long start = System.nanoTime();
    for (int i = reps; i > 0; i--) {
      for (int j = (size - 1) * Long.BYTES; j >= 0; j -= Long.BYTES) {
        long val = (long) VH_LONG.getOpaque(seg, j); // offset in bytes
        VH_LONG.setOpaque(seg, j, val + 1);
      }
    }
    long ret = System.nanoTime() - start;
    System.err.println("\tDONE! "+TimeUnit.NANOSECONDS.toMillis(ret));
    for (int i = size - 1; i >= 0; i--) {
      assertEquals(reps, data.get(i));
    }
    return ret;
  }

  @SuppressWarnings("preview")
  private static final VarHandle VH_LONG = MethodHandles.memorySegmentViewVarHandle(ValueLayout.JAVA_LONG);

  private static long testBlah2() {
    int reps = 1000;
    int size = 1 << 22; // 4m
    LongBuffer data = ByteBuffer.allocateDirect(size * Long.BYTES).asLongBuffer();
    long start = System.nanoTime();
    for (int i = reps; i > 0; i--) {
      for (int j = size - 1; j >= 0; j--) {
        data.put(j, data.get(j) + 1);
      }
    }
    long ret = System.nanoTime() - start;
    System.err.println("\tDONE! "+TimeUnit.NANOSECONDS.toMillis(ret));
    for (int i = size - 1; i >= 0; i--) {
      assertEquals(reps, data.get(i));
    }
    return ret;
  }

  @SuppressWarnings("preview")
  static long testBlah3() {
    int reps = 1000;
    int size = 1 << 22;
    // Use MemorySegment for off-heap performance
    try (Arena arena = Arena.ofShared()) {
      MemorySegment data = arena.allocate(size * 8L, 8L);
      int vectorByteSize = LongVector.SPECIES_PREFERRED.vectorByteSize();
      int alignmentOffset = data.asByteBuffer().alignmentOffset(0, vectorByteSize);
      System.err.println("\tvbs="+vectorByteSize+", aOffset="+alignmentOffset);
      VectorSpecies<Long> SPECIES = LongVector.SPECIES_PREFERRED;
      LongVector plusOne = LongVector.broadcast(SPECIES, 1L);

      long start = System.nanoTime();
      for (int i = 0; i < reps; i++) {
        int limit = SPECIES.loopBound(size);
        int j = 0;
        // Vectorized loop
        for (; j < limit; j += SPECIES.length()) {
          var v = LongVector.fromMemorySegment(SPECIES, data, (long) j * 8, ByteOrder.nativeOrder());
          v.add(plusOne).intoMemorySegment(data, (long) j * 8, ByteOrder.nativeOrder());
        }
        // Tail cleanup (for sizes not divisible by species length)
        for (; j < size; j++) {
          long val = data.get(ValueLayout.JAVA_LONG, (long) j * 8);
          data.set(ValueLayout.JAVA_LONG, (long) j * 8, val + 1);
        }
      }
      long ret = System.nanoTime() - start;
      System.err.println("\tDONE(v1)! " + TimeUnit.NANOSECONDS.toMillis(ret));
      for (int i = size - 1; i >= 0; i--) {
        assertEquals(reps, data.get(ValueLayout.JAVA_LONG, i * 8L));
      }
      return ret;
    }
  }

  @SuppressWarnings("preview")
  private static long testBlah4() {
    int reps = 1000;
    int size = 1 << 22;
    // Use MemorySegment for off-heap performance
    ByteBuffer bb = ByteBuffer.allocateDirect((size + 1) * Long.BYTES).slice(0, size * Long.BYTES).order(ByteOrder.nativeOrder());
    int vectorByteSize = LongVector.SPECIES_PREFERRED.vectorByteSize();
    int alignmentOffset = bb.alignmentOffset(0, vectorByteSize);
    System.err.println("\tvbs="+vectorByteSize+", aOffset="+alignmentOffset);
    LongBuffer lb = bb.asLongBuffer();
    MemorySegment data = MemorySegment.ofBuffer(bb);
    VectorSpecies<Long> SPECIES = LongVector.SPECIES_PREFERRED;
    LongVector plusOne = LongVector.broadcast(SPECIES, 1L);

    long start = System.nanoTime();
    for (int i = 0; i < reps; i++) {
      int limit = SPECIES.loopBound(size);
      int j = 0;
      // Vectorized loop
      for (; j < limit; j += SPECIES.length()) {
        var v = LongVector.fromMemorySegment(SPECIES, data, (long) j * 8, ByteOrder.nativeOrder());
        v.add(plusOne).intoMemorySegment(data, (long) j * 8, ByteOrder.nativeOrder());
      }
      // Tail cleanup (for sizes not divisible by species length)
      for (; j < size; j++) {
        long val = data.get(ValueLayout.JAVA_LONG, (long) j * 8);
        data.set(ValueLayout.JAVA_LONG, (long) j * 8, val + 1);
      }
    }
    long ret = System.nanoTime() - start;
    System.err.println("\tDONE(v2)! " + TimeUnit.NANOSECONDS.toMillis(ret));
    for (int i = size - 1; i >= 0; i--) {
      assertEquals(reps, lb.get(i));
    }
    return ret;
  }

  public void testCopyBitRange() {
    Random r = random();
    int successfulSeeds = 0;
    for (int i = 100 * RANDOM_MULTIPLIER; i > 0; i--) {
      long seed = r.nextLong();
      try {
        assertTrue(
            "failed for inner seed " + Long.toUnsignedString(seed, 16), doTestCopyBitRange(seed));
        successfulSeeds++;
      } catch (Throwable ex) {
        System.err.println("exception for inner seed: " + Long.toUnsignedString(seed, 16));
        throw ex;
      }
    }
    System.err.println("testCopyBitRange successful seed count: " + successfulSeeds);
  }

  public void testCopyBitRangeArr() {
    Random r = random();
    int successfulSeeds = 0;
    for (int i = 100 * RANDOM_MULTIPLIER; i > 0; i--) {
      long seed = r.nextLong();
      try {
        assertTrue(
            "failed for inner seed " + Long.toUnsignedString(seed, 16),
            doTestCopyBitRangeArr(seed));
        successfulSeeds++;
      } catch (Throwable ex) {
        System.err.println("exception for inner seed: " + Long.toUnsignedString(seed, 16));
        throw ex;
      }
    }
    System.err.println("testCopyBitRange successful seed count: " + successfulSeeds);
  }

  /**
   * Tests the correctness of {@link DocSetUtil#copyBitRange(long[], int, long[], int, int)} by
   * copying arbitrary ranges from a source "bitset" array to a dest "bitset" array, and back into
   * their original positions in a new array (essentially a round-trip), and comparing the source
   * and "round-tripped" final array for equality.
   */
  private static boolean doTestCopyBitRange(long seed) {
    Random r = new Random(seed);
    long[] src = new long[r.nextInt(1026) + 1];
    for (int i = src.length - 1; i >= 0; i--) {
      src[i] = r.nextLong();
    }
    int bitLen = src.length * Long.SIZE;
    int boundary2 = r.nextInt(bitLen);
    int boundary1 = r.nextInt(boundary2 + 1);
    int boundary3 = boundary2 + r.nextInt(bitLen - boundary2);
    int[][] regions =
        new int[][] {
          new int[] {0, boundary1},
          new int[] {boundary1, boundary2 - boundary1},
          new int[] {boundary2, boundary3 - boundary2},
          new int[] {boundary3, bitLen - boundary3}
        };
    Integer[] regionIdxs = new Integer[regions.length];
    for (int i = regionIdxs.length - 1; i >= 0; i--) {
      regionIdxs[i] = i;
    }
    Collections.shuffle(Arrays.asList(regionIdxs), r);
    int[][] mappings = new int[regions.length][];
    int newOffset = 0;
    for (int i = 0; i < regions.length; i++) {
      int[] region = regions[regionIdxs[i]];
      mappings[i] = Arrays.copyOf(region, 3);
      mappings[i][2] = newOffset;
      newOffset += region[1];
    }
    long[] dest = new long[src.length];
    Collections.shuffle(Arrays.asList(mappings), r);
    for (int[] mapping : mappings) {
      copyBitRange(src, mapping[0], dest, mapping[2], mapping[1]);
    }
    long[] rtx = new long[src.length];
    Collections.shuffle(Arrays.asList(mappings), r);
    for (int[] mapping : mappings) {
      copyBitRange(dest, mapping[2], rtx, mapping[0], mapping[1]);
    }
    return Arrays.equals(src, rtx);
  }

  /**
   * Tests the correctness of {@link DocSetUtil#copyBitRange(long[], int, long[], int, int)} by
   * copying arbitrary ranges from a source "bitset" array to a dest "bitset" array, and back into
   * their original positions in a new array (essentially a round-trip), and comparing the source
   * and "round-tripped" final array for equality.
   */
  private static boolean doTestCopyBitRangeArr(long seed) {
    Random r = new Random(seed);
    long[] src = new long[r.nextInt(1026) + 1];
    for (int i = src.length - 1; i >= 0; i--) {
      src[i] = r.nextLong();
    }
    int bitLen = src.length * Long.SIZE;
    int boundary2 = r.nextInt(bitLen);
    int boundary1 = r.nextInt(boundary2 + 1);
    int boundary3 = boundary2 + r.nextInt(bitLen - boundary2);
    int[][] regions =
        new int[][] {
          new int[] {0, boundary1},
          new int[] {boundary1, boundary2 - boundary1},
          new int[] {boundary2, boundary3 - boundary2},
          new int[] {boundary3, bitLen - boundary3}
        };
    Integer[] regionIdxs = new Integer[regions.length];
    for (int i = regionIdxs.length - 1; i >= 0; i--) {
      regionIdxs[i] = i;
    }
    Collections.shuffle(Arrays.asList(regionIdxs), r);
    int[][] mappings = new int[regions.length][];
    int newOffset = 0;
    for (int i = 0; i < regions.length; i++) {
      int[] region = regions[regionIdxs[i]];
      mappings[i] = Arrays.copyOf(region, 3);
      mappings[i][2] = newOffset;
      newOffset += region[1];
    }
    FixedBitSets srcArr = new FixedBitSets(src.length << 6);
    int idx = 0;
    for (FixedBitSet a : srcArr.parts) {
      LongBuffer bits = a.getBits();
      for (int i = 0, lim = bits.capacity(); i < lim; i++) {
        bits.put(i, src[idx++]);
      }
    }

    FixedBitSets destArr = new FixedBitSets(src.length << 6);
    Collections.shuffle(Arrays.asList(mappings), r);
    for (int[] mapping : mappings) {
      copyBitRange(srcArr, mapping[0], destArr, mapping[2], mapping[1]);
    }
    FixedBitSets rtx = new FixedBitSets(src.length << 6);
    Collections.shuffle(Arrays.asList(mappings), r);
    for (int[] mapping : mappings) {
      copyBitRange(destArr, mapping[2], rtx, mapping[0], mapping[1]);
    }
    boolean ret = Arrays.deepEquals(srcArr.parts, rtx.parts);
    if (!ret) {
      System.err.println(srcArr.parts.length + " ?= " + rtx.parts.length);
      for (int i = 0; i < srcArr.parts.length; i++) {
        LongBuffer a = srcArr.parts[i].getBits();
        LongBuffer b = rtx.parts[i].getBits();
        System.err.println(
            "\t" + a.capacity() + " ?= " + b.capacity() + " equals?: " + a.equals(b));
        System.err.println("\t\t" + a);
        System.err.println("\t\t" + b);
      }
    }
    return ret;
  }
}
