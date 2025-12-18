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

import java.util.Collection;
import java.util.Collections;
import java.util.NoSuchElementException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * A {@link FixedBitSet} based implementation of a {@link DocSet}. Good for medium/large sets.
 *
 * @since solr 0.9
 */
public class BitDocSet extends DocSet {
  // for the array object inside the FixedBitSet. long[] array won't change alignment, so no need to
  // calculate it.
  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(BitDocSet.class)
          + RamUsageEstimator.shallowSizeOfInstance(FixedBitSet.class)
          + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;

  // TODO consider SparseFixedBitSet alternative

  /**
   * Normally this is set to 17, making for max 1M blocks. May set lower for improved test coverage
   * of edge cases. TODO: maybe set this dynamically according to configured G1HeapRegionSize?
   *
   * <ul>
   *   <li>23 -&gt; 1M blocks
   *   <li>22 -&gt; 512K blocks
   *   <li>21 -&gt; 256K blocks
   *   <li>20 -&gt; 128K blocks
   *   <li>19 -&gt; 64K blocks
   *   <li>18 -&gt; 32K blocks
   *   <li>17 -&gt; 16K blocks
   *   <li>16 -&gt; 8K blocks
   *   <li>15 -&gt; 4K blocks
   *   <li>14 -&gt; 2K blocks
   *   <li>13 -&gt; 1K blocks
   *   <li>12 -&gt; 512b blocks
   *   <li>11 -&gt; 256b blocks
   *   <li>10 -&gt; 128b blocks
   *   <li>9 -&gt; 64b blocks
   *   <li>8 -&gt; 32b blocks
   *   <li>7 -&gt; 16b blocks
   *   <li>6 -&gt; 8b blocks (single long)
   * </ul>
   */
  public static final int BIT_SHIFT;

  private static final int DEFAULT_MAX_BIT_SHIFT = 23; // 1M blocks

  static {
    // here we work based on the assumption that the min and max heap sizes will be the same, and
    // will guide the heap region sizing
    long maxMemory = Runtime.getRuntime().maxMemory();
    int exp = 64 - Long.numberOfLeadingZeros(maxMemory) - 1; // round down to nearest power of 2
    String tmp = System.getProperty("solr.bitdocset.maxwordsshift");
    int maxBitShift;
    if (tmp == null) {
      maxBitShift = DEFAULT_MAX_BIT_SHIFT;
    } else {
      try {
        // 6 is the absolute minimum, corresponding to a single `long`
        maxBitShift = Math.max(6, Integer.parseInt(tmp));
      } catch (Exception e) {
        maxBitShift = DEFAULT_MAX_BIT_SHIFT;
      }
    }
    BIT_SHIFT = Math.min(maxBitShift, Math.max(6, (exp - 11)));
    System.err.println("exp=" + exp);
    long maxBlockBytes = 1L << (BIT_SHIFT - 3);
    System.err.println(
        "MEM="
            + (maxMemory >> 20)
            + "m, maxBlockBytes="
            + maxBlockBytes
            + " ("
            + (maxBlockBytes >> 10)
            + "k) BIT_SHIFT="
            + BIT_SHIFT
            + " / "
            + RamUsageEstimator.humanReadableUnits(
                RamUsageEstimator.sizeOf(new FixedBitSet(1 << BIT_SHIFT))));
  }

  public static final int MAX_BLOCK_BITS = 1 << BIT_SHIFT;
  public static final int BLOCK_BIT_MASK = MAX_BLOCK_BITS - 1;

  private final FixedBitSet[] bits;
  private final FixedBitSets parts;
  int size; // number of docs in the set (cached for perf)

  public BitDocSet() {
    bits = new FixedBitSet[] {new FixedBitSet(64)};
    parts = new FixedBitSets(bits);
  }

  /** Construct a BitDocSet. The capacity of the {@link FixedBitSet} should be at least maxDoc() */
  public BitDocSet(FixedBitSet[] bits) {
    this.bits = bits;
    this.parts = new FixedBitSets(bits);
    size = -1;
  }

  /**
   * Construct a BitDocSet, and provides the number of set bits. The capacity of the {@link
   * FixedBitSet} should be at least maxDoc()
   */
  public BitDocSet(FixedBitSet[] bits, int size) {
    this.bits = bits;
    this.parts = new FixedBitSets(bits);
    this.size = size;
  }

  public BitDocSet(FixedBitSets parts) {
    this.bits = parts.parts;
    this.parts = parts;
    size = -1;
  }

  public BitDocSet(FixedBitSets parts, int size) {
    this.bits = parts.parts;
    this.parts = parts;
    this.size = size;
  }

  private static final int MAX_BLOCK_LONGS = MAX_BLOCK_BITS >> 6;

  public static BitDocSet newInstance(FixedBitSet fixedBitSet) {
    if (fixedBitSet.getBits().capacity() <= MAX_BLOCK_LONGS) {
      return new BitDocSet(new FixedBitSet[] {fixedBitSet});
    } else {
      FixedBitSets ret = new FixedBitSets(fixedBitSet.length());
      FixedBitSet.copyTo(fixedBitSet, ret.parts, FixedBitSet.DEFAULT_MODIFIER);
      return new BitDocSet(ret);
    }
  }

  private static final DocIterator EMPTY_DOC_ITERATOR =
      new DocIterator() {
        @Override
        public int nextDoc() {
          return DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public float score() {
          throw new IllegalStateException();
        }

        @Override
        public boolean hasNext() {
          return false;
        }

        @Override
        public Integer next() {
          throw new NoSuchElementException();
        }
      };

  @Override
  public DocIterator iterator() {
    if (bits.length == 0) {
      return EMPTY_DOC_ITERATOR;
    }
    return new DocIterator() {
      int outerIdx = 0;
      private int base = 0;
      private BitSetIterator iter = new BitSetIterator(bits[0], 0L); // cost is not useful here
      private int last = -1;
      private int pos = initPos();

      @Override
      public boolean hasNext() {
        return pos != DocIdSetIterator.NO_MORE_DOCS;
      }

      @Override
      public Integer next() {
        return nextDoc();
      }

      @Override
      public void remove() {
        bits[last >> BIT_SHIFT].clear(last & BLOCK_BIT_MASK);
      }

      private int initPos() {
        int pos;
        while ((pos = iter.nextDoc()) == DocIdSetIterator.NO_MORE_DOCS) {
          if (++outerIdx < bits.length) {
            iter = new BitSetIterator(bits[outerIdx], 0L);
            base += BitDocSet.MAX_BLOCK_BITS;
          } else {
            break;
          }
        }
        return pos;
      }

      @Override
      public int nextDoc() {
        int old = pos;
        if (old == DocIdSetIterator.NO_MORE_DOCS) {
          return last = DocIdSetIterator.NO_MORE_DOCS;
        } else {
          int ret = base + old;
          pos = initPos();
          return last = ret;
        }
      }

      @Override
      public float score() {
        return 0.0f;
      }
    };
  }

  /**
   * @return the <b>internal</b> {@link FixedBitSet} that should <b>not</b> be modified.
   */
  @Override
  public FixedBitSets getBits() {
    return parts;
  }

  @Override
  protected FixedBitSets getFixedBitSet() {
    return parts;
  }

  @Override
  protected FixedBitSets getFixedBitSetClone() {
    return parts.clone();
  }

  @Override
  public int size() {
    if (size != -1) return size;
    return size = parts.cardinality();
  }

  /**
   * Returns true of the doc exists in the set. Should only be called when doc &lt; {@link
   * FixedBitSet#length()}.
   */
  @Override
  public boolean exists(int doc) {
    return parts.get(doc);
  }

  @Override
  public DocSet intersection(DocSet other) {
    // intersection is overloaded in the smaller DocSets to be more
    // efficient, so dispatch off of it instead.
    if (!(other instanceof BitDocSet)) {
      return other.intersection(this);
    }

    // Default... handle with bitsets.
    FixedBitSets newbits = getFixedBitSetClone();
    newbits.and(((BitDocSet) other).parts);
    return new BitDocSet(newbits.parts);
  }

  public static void and(FixedBitSet[] bitSet, FixedBitSet[] filter) {
    // TODO: mutate first bitset
  }

  @Override
  public int intersectionSize(DocSet other) {
    if (other instanceof BitDocSet) {
      return parts.andCount(((BitDocSet) other).parts);
    } else {
      // they had better not call us back!
      return other.intersectionSize(this);
    }
  }

  @Override
  public boolean intersects(DocSet other) {
    if (other instanceof BitDocSet) {
      return parts.intersects(((BitDocSet) other).parts);
    } else {
      // they had better not call us back!
      return other.intersects(this);
    }
  }

  @Override
  public int unionSize(DocSet other) {
    if (other instanceof BitDocSet) {
      // if we don't know our current size, this is faster than
      // size + other.size - intersection_size
      return parts.unionCount(((BitDocSet) other).parts);
    } else {
      // they had better not call us back!
      return other.unionSize(this);
    }
  }

  @Override
  public int andNotSize(DocSet other) {
    if (other instanceof BitDocSet) {
      // if we don't know our current size, this is faster than
      // size - intersection_size
      return parts.andNotCount(((BitDocSet) other).parts);
    } else {
      return super.andNotSize(other);
    }
  }

  @Override
  public void addAllTo(FixedBitSets target) {
    target.or(parts);
  }

  @Override
  public DocSet andNot(DocSet other) {
    FixedBitSets newbits = getFixedBitSetClone();
    andNot(newbits, other);
    return new BitDocSet(newbits.parts);
  }

  /**
   * Helper method for andNot that takes FixedBitSet and DocSet. This modifies the provided
   * FixedBitSet to remove all bits contained in the DocSet argument -- equivalent to calling
   * a.andNot(b), but modifies the state of the FixedBitSet instead of returning a new FixedBitSet.
   *
   * @param bits FixedBitSet to operate on
   * @param other The DocSet to compare to
   */
  protected static void andNot(FixedBitSets bits, DocSet other) {
    if (other instanceof BitDocSet) {
      bits.andNot(((BitDocSet) other).parts);
    } else {
      DocIterator iter = other.iterator();
      int lim = bits.length();
      while (iter.hasNext()) {
        int doc = iter.nextDoc();
        if (doc < lim) {
          bits.clear(doc);
        }
      }
    }
  }

  public int length() {
    return parts.length();
  }

  static FixedBitSets ensureCapacity(FixedBitSets subject, int numBits) {
    if (numBits == 0 || numBits <= subject.length()) {
      return subject;
    }
    int newLen = ArrayUtil.oversize(numBits, Long.BYTES);
    int lastIdx = (newLen - 1) >> BIT_SHIFT;
    FixedBitSet[] ret = new FixedBitSet[lastIdx + 1];
    FixedBitSet[] bits = subject.parts;
    int len = ((newLen - 1) & BLOCK_BIT_MASK) + 1;
    for (int i = lastIdx; i >= 0; i--) {
      FixedBitSet dest = new FixedBitSet(len);
      if (i < bits.length) {
        dest.or(bits[i]);
      }
      ret[i] = dest;
      len = MAX_BLOCK_BITS;
    }
    return new FixedBitSets(ret);
  }

  @Override
  public DocSet union(DocSet other) {
    FixedBitSets newbits = getFixedBitSetClone();
    if (other instanceof BitDocSet) {
      BitDocSet otherDocSet = (BitDocSet) other;
      newbits = ensureCapacity(newbits, otherDocSet.length());
      newbits.or(otherDocSet.parts);
    } else {
      DocIterator iter = other.iterator();
      int capacity = newbits.length();
      while (iter.hasNext()) {
        int doc = iter.nextDoc();
        if (doc >= capacity) {
          newbits = ensureCapacity(newbits, doc);
          capacity = newbits.length();
        }
        newbits.set(doc);
      }
    }
    return new BitDocSet(newbits.parts);
  }

  @Override
  public BitDocSet clone() {
    return new BitDocSet(bits.clone(), size);
  }

  @Override
  public DocIdSetIterator iterator(LeafReaderContext context) {
    if (context.isTopLevel) {
      int cost;
      switch (size) {
        case -1:
          // size has not been computed; use bits.length() as an upper bound on cost
          final int maxSize = length();
          if (maxSize < 1) {
            return null;
          } else {
            cost = maxSize;
            break;
          }
        case 0:
          return null;
        default:
          // we have an explicit size; use it
          cost = size;
          break;
      }
      return new BitSetsIterator(bits, length(), cost);
    }

    final int maxDoc = context.reader().maxDoc();
    if (maxDoc < 1) {
      // entirely empty segment; verified this actually happens
      return null;
    }

    final int base = context.docBase;
    // `lastSegDoc` is the max doc in this segment, limited to bit set length
    final int lastSegDoc = Math.min(maxDoc, length() - base) - 1;
    final FixedBitSet[] bss = bits;

    return new DocIdSetIterator() {
      int outerIdx = base >> BIT_SHIFT;
      FixedBitSet bs = bss[outerIdx];
      int innerIdx = (base & BLOCK_BIT_MASK) - 1;
      int partitionBase = outerIdx << BIT_SHIFT;
      int segDoc = -1;

      @Override
      public int docID() {
        return segDoc;
      }

      @Override
      public int nextDoc() {
        if (segDoc >= lastSegDoc) return segDoc = NO_MORE_DOCS;
        int nextInner = innerIdx + 1;
        while (nextInner >= MAX_BLOCK_BITS
            || (innerIdx = bs.nextSetBit(nextInner)) == NO_MORE_DOCS) {
          if (++outerIdx < bss.length) {
            bs = bss[outerIdx];
            nextInner = 0;
            partitionBase = outerIdx << BIT_SHIFT;
          } else {
            return segDoc = NO_MORE_DOCS;
          }
        }
        segDoc = (partitionBase + innerIdx) - base;
        if (segDoc > lastSegDoc) {
          segDoc = NO_MORE_DOCS;
        }
        return segDoc;
      }

      @Override
      public int advance(int target) {
        if (target > lastSegDoc) return segDoc = NO_MORE_DOCS;
        int nextGlobal = target + base;
        outerIdx = nextGlobal >> BIT_SHIFT;
        bs = bss[outerIdx];
        partitionBase = outerIdx << BIT_SHIFT;
        int nextInner = nextGlobal & BLOCK_BIT_MASK;
        while (nextInner >= MAX_BLOCK_BITS
            || (innerIdx = bs.nextSetBit(nextInner)) == NO_MORE_DOCS) {
          if (++outerIdx < bss.length) {
            bs = bss[outerIdx];
            nextInner = 0;
            partitionBase = outerIdx << BIT_SHIFT;
          } else {
            return segDoc = NO_MORE_DOCS;
          }
        }
        segDoc = (partitionBase + innerIdx) - base;
        if (segDoc > lastSegDoc) {
          segDoc = NO_MORE_DOCS;
        }
        return segDoc;
      }

      @Override
      public long cost() {
        // we don't want to actually compute cardinality, but
        // if it's already been computed, we use it (pro-rated for the segment)
        if (size != -1) {
          return (long) (size * ((FixedBitSet.bits2words(maxDoc) << 6) / (float) length()));
        } else {
          return maxDoc;
        }
      }
    };
  }

  @Override
  public DocSetQuery makeQuery() {
    return new DocSetQuery(this);
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(bits);
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }

  @Override
  public String toString() {
    return "BitDocSet{"
        + "size="
        + size()
        + ",ramUsed="
        + RamUsageEstimator.humanReadableUnits(ramBytesUsed())
        + '}';
  }

  static class BitSetsIterator extends DocIdSetIterator {
    private final FixedBitSet[] bss;
    private final int max;
    private final long cost;
    int outerIdx;
    FixedBitSet bs;
    int pos;
    int partitionBase;

    public BitSetsIterator(FixedBitSet[] bss, int max, long cost) {
      this.bss = bss;
      this.max = max;
      this.cost = cost;
      outerIdx = 0;
      bs = bss[0];
      pos = -1;
      partitionBase = 0;
    }

    @Override
    public int docID() {
      return pos < max ? pos : NO_MORE_DOCS;
    }

    @Override
    public int nextDoc() {
      int next = pos + 1;
      if (next >= max) {
        pos = max;
        return NO_MORE_DOCS;
      } else {
        while ((pos = bs.nextSetBit(next & BLOCK_BIT_MASK)) == NO_MORE_DOCS) {
          if (++outerIdx < bss.length) {
            bs = bss[outerIdx];
            next = outerIdx << BIT_SHIFT;
            partitionBase = next;
          } else {
            pos = max - partitionBase;
            break;
          }
        }
        pos += partitionBase;
        return pos < max ? pos : NO_MORE_DOCS;
      }
    }

    @Override
    public int advance(int target) {
      if (target >= max) {
        pos = max;
        return NO_MORE_DOCS;
      } else {
        while ((pos = bs.nextSetBit(target & BLOCK_BIT_MASK)) == NO_MORE_DOCS) {
          if (++outerIdx < bss.length) {
            bs = bss[outerIdx];
            target = outerIdx << BIT_SHIFT;
            partitionBase = target;
          } else {
            pos = max - partitionBase;
            break;
          }
          pos += partitionBase;
        }
        return pos < max ? pos : NO_MORE_DOCS;
      }
    }

    @Override
    public long cost() {
      return cost;
    }
  }
}
