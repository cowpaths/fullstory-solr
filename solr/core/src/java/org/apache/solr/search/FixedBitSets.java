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

import static org.apache.solr.search.BitDocSet.BIT_SHIFT;
import static org.apache.solr.search.BitDocSet.BLOCK_BIT_MASK;
import static org.apache.solr.search.BitDocSet.MAX_BLOCK_BITS;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * A {@link FixedBitSet} based implementation of a {@link DocSet}. Good for medium/large sets.
 *
 * @since solr 0.9
 */
public class FixedBitSets implements Bits, Accountable {
  // for the array object inside the FixedBitSet. long[] array won't change alignment, so no need to
  // calculate it.
  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(FixedBitSets.class);

  private static final FixedBitSet.Modifier DEFAULT_MODIFIER =
      FixedBitSet.DEFAULT_MODIFIER.partitioned(BIT_SHIFT);

  static FixedBitSet.Modifier MODIFIER = DEFAULT_MODIFIER;

  private static final Object SYNC = new Object();

  private static int registeredCount = 0;

  public static <T extends FixedBitSet.Modifier> T registerModifier(IOSupplier<T> register)
      throws IOException {
    synchronized (SYNC) {
      for (; ; ) {
        if (MODIFIER == DEFAULT_MODIFIER) {
          registeredCount = 1;
          T ret = register.get();
          MODIFIER = ret;
          SYNC.notifyAll();
          return ret;
        } else {
          @SuppressWarnings("unchecked")
          T ret = (T) MODIFIER;
          if (registeredCount == 0) {
            try {
              SYNC.wait();
            } catch (InterruptedException e) {
              throw new ThreadInterruptedException(e);
            }
          } else {
            registeredCount++;
            return ret;
          }
        }
      }
    }
  }

  public static boolean unregisterModifier(FixedBitSet.Modifier m, Closeable close)
      throws IOException {
    synchronized (SYNC) {
      if (MODIFIER != m) {
        throw new IllegalStateException();
      }
      if (--registeredCount == 0) {
        try (close) {
          MODIFIER = DEFAULT_MODIFIER;
        }
        SYNC.notifyAll();
        return true;
      } else {
        return false;
      }
    }
  }

  public final FixedBitSet[] parts;
  private int cachedLength = -1;

  public FixedBitSets(int numBits) {
    if (numBits == 0) {
      this.parts = new FixedBitSet[0];
      this.cachedLength = 0;
      return;
    }
    int lastIdx = (numBits - 1) >> BIT_SHIFT;
    this.parts = new FixedBitSet[lastIdx + 1];
    ByteBuffer[] bb = MODIFIER.allocateBytesArr(FixedBitSet.bits2words(numBits) << 3, this.parts);
    int len = ((numBits - 1) & BLOCK_BIT_MASK) + 1;
    for (int i = lastIdx; i >= 0; i--) {
      parts[i] = new FixedBitSet(bb[i].asLongBuffer(), len);
      len = MAX_BLOCK_BITS;
    }
  }

  FixedBitSets(FixedBitSet[] parts) {
    this.parts = parts;
  }

  private FixedBitSets(FixedBitSets template) {
    FixedBitSet[] otherParts = template.parts;
    this.parts = new FixedBitSet[otherParts.length];
    int numBits = template.length();
    ByteBuffer[] bb = MODIFIER.allocateBytesArr(FixedBitSet.bits2words(numBits) << 3, this.parts);
    for (int i = otherParts.length - 1; i >= 0; i--) {
      this.parts[i] = otherParts[i].clone(bb[i].asLongBuffer());
    }
    this.cachedLength = numBits;
  }

  public void set(int index) {
    parts[index >> BitDocSet.BIT_SHIFT].set(index & BLOCK_BIT_MASK);
  }

  public boolean get(int index) {
    return parts[index >> BitDocSet.BIT_SHIFT].get(index & BLOCK_BIT_MASK);
  }

  public void clear(int index) {
    parts[index >> BitDocSet.BIT_SHIFT].clear(index & BLOCK_BIT_MASK);
  }

  public void set(int from, int to) {
    int firstOuterIdx = from >> BIT_SHIFT;
    int lastOuterIdx = (to - 1) >> BIT_SHIFT;
    int fromInner = from & BLOCK_BIT_MASK;
    int toInner = ((to - 1) & BLOCK_BIT_MASK) + 1;
    if (firstOuterIdx == lastOuterIdx) {
      parts[firstOuterIdx].set(fromInner, toInner);
      return;
    }
    parts[firstOuterIdx].set(fromInner, MAX_BLOCK_BITS);
    for (int i = firstOuterIdx + 1; i < lastOuterIdx; i++) {
      parts[i].set(0, MAX_BLOCK_BITS);
    }
    parts[lastOuterIdx].set(0, toInner);
  }

  public int length() {
    if (cachedLength == -1) {
      return cachedLength =
          Math.toIntExact(Arrays.stream(parts).mapToLong(FixedBitSet::length).sum());
    } else {
      assert cachedLength
          == Math.toIntExact(Arrays.stream(parts).mapToLong(FixedBitSet::length).sum());
      return cachedLength;
    }
  }

  public int cardinality() {
    return Math.toIntExact(Arrays.stream(parts).mapToLong(FixedBitSet::cardinality).sum());
  }

  public void and(FixedBitSets other) {
    FixedBitSet[] otherParts = other.parts;
    int i = 0;
    for (int lim = Math.min(parts.length, otherParts.length); i < lim; i++) {
      parts[i].and(otherParts[i]);
    }
    for (int lim = parts.length; i < lim; i++) {
      parts[i].clear();
    }
  }

  public int andCount(FixedBitSets other) {
    FixedBitSet[] otherParts = other.parts;
    long ret = 0;
    for (int i = 0, lim = Math.min(parts.length, otherParts.length); i < lim; i++) {
      ret += FixedBitSet.intersectionCount(parts[i], otherParts[i]);
    }
    return Math.toIntExact(ret);
  }

  public boolean intersects(FixedBitSets other) {
    FixedBitSet[] otherBits = other.parts;
    for (int i = 0, lim = Math.min(parts.length, otherBits.length); i < lim; i++) {
      if (parts[i].intersects(otherBits[i])) {
        return true;
      }
    }
    return false;
  }

  public void or(FixedBitSets other) {
    FixedBitSet[] otherParts = other.parts;
    for (int i = 0, lim = Math.min(parts.length, otherParts.length); i < lim; i++) {
      parts[i].or(otherParts[i]);
    }
  }

  public int unionCount(FixedBitSets other) {
    FixedBitSet[] otherParts = other.parts;
    long ret = 0;
    int i = 0;
    for (int lim = Math.min(parts.length, otherParts.length); i < lim; i++) {
      ret += FixedBitSet.unionCount(parts[i], otherParts[i]);
    }
    FixedBitSet[] remainder;
    if (i < (remainder = parts).length || i < (remainder = otherParts).length) {
      for (int lim = remainder.length; i < lim; i++) {
        ret += remainder[i].cardinality();
      }
    }
    return Math.toIntExact(ret);
  }

  public void andNot(FixedBitSets other) {
    FixedBitSet[] otherParts = other.parts;
    for (int i = 0, lim = Math.min(parts.length, otherParts.length); i < lim; i++) {
      parts[i].andNot(otherParts[i]);
    }
  }

  public int andNotCount(FixedBitSets other) {
    FixedBitSet[] otherParts = other.parts;
    long ret = 0;
    int i = 0;
    for (int lim = Math.min(parts.length, otherParts.length); i < lim; i++) {
      ret += FixedBitSet.andNotCount(parts[i], otherParts[i]);
    }
    for (int lim = parts.length; i < lim; i++) {
      ret += parts[i].cardinality();
    }
    return Math.toIntExact(ret);
  }

  @Override
  public FixedBitSets clone() {
    return new FixedBitSets(this);
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(parts);
  }

  public int prevSetBit(int from) {
    for (int i = from >> BIT_SHIFT, fromLocal = from & BLOCK_BIT_MASK; i >= 0; i--) {
      int nextLocal = parts[i].prevSetBit(fromLocal);
      if (nextLocal == -1) {
        fromLocal = BLOCK_BIT_MASK;
      } else {
        return (i << BIT_SHIFT) + nextLocal;
      }
    }
    return -1;
  }

  public int nextSetBit(int from) {
    for (int i = from >> BIT_SHIFT, fromLocal = from & BLOCK_BIT_MASK; i < parts.length; i++) {
      int nextLocal = parts[i].nextSetBit(fromLocal);
      if (nextLocal == DocIdSetIterator.NO_MORE_DOCS) {
        fromLocal = 0;
      } else {
        return (i << BIT_SHIFT) + nextLocal;
      }
    }
    return DocIdSetIterator.NO_MORE_DOCS;
  }

  public void flip(int from, int to) {
    int firstOuterIdx = from >> BIT_SHIFT;
    int lastOuterIdx = (to - 1) >> BIT_SHIFT;
    int fromInner = from & BLOCK_BIT_MASK;
    int toInner = ((to - 1) & BLOCK_BIT_MASK) + 1;
    if (firstOuterIdx == lastOuterIdx) {
      parts[firstOuterIdx].flip(fromInner, toInner);
      return;
    }
    parts[firstOuterIdx].flip(fromInner, MAX_BLOCK_BITS);
    for (int i = firstOuterIdx + 1; i < lastOuterIdx; i++) {
      parts[i].flip(0, MAX_BLOCK_BITS);
    }
    parts[lastOuterIdx].flip(0, toInner);
  }

  public Bits asReadOnlyBits() {
    return new Bits() {
      @Override
      public boolean get(int index) {
        return FixedBitSets.this.get(index);
      }

      @Override
      public int length() {
        return FixedBitSets.this.length();
      }
    };
  }

  private static class ResettableDocIdSetIterator extends DocIdSetIterator {

    private final DocIdSetIterator delegate;
    private int lim;
    private int adjust = 0;
    private int nextDoc;
    private int startDoc;

    private ResettableDocIdSetIterator(DocIdSetIterator delegate) throws IOException {
      this.delegate = delegate;
      this.lim = MAX_BLOCK_BITS;
      int nextDoc = delegate.nextDoc();
      this.nextDoc = nextDoc;
      this.startDoc = nextDoc;
    }

    private boolean reset() {
      if (nextDoc == NO_MORE_DOCS) {
        return false;
      }
      startDoc = nextDoc;
      adjust = lim;
      lim += MAX_BLOCK_BITS;
      return true;
    }

    @Override
    public int docID() {
      if (startDoc == nextDoc) {
        return -1;
      } else {
        throw new IllegalStateException();
      }
    }

    @Override
    public int nextDoc() throws IOException {
      int ret = nextDoc;
      if (ret >= lim) {
        return NO_MORE_DOCS;
      }
      nextDoc = delegate.nextDoc();
      return ret - adjust;
    }

    @Override
    public int advance(int target) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      throw new UnsupportedOperationException();
    }
  }

  public void or(DocIdSetIterator iter) throws IOException {
    ResettableDocIdSetIterator partIter = new ResettableDocIdSetIterator(iter);
    for (FixedBitSet part : parts) {
      part.or(partIter);
      if (!partIter.reset()) {
        break;
      }
    }
  }
}
