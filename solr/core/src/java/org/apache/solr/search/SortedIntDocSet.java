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

import com.carrotsearch.hppc.IntHashSet;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.IntBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet.ByteBufferStruct;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.search.HeapCacheFbsModifier.SentinelPacket;

/** A simple sorted int[] array implementation of {@link DocSet}, good for small sets. */
public class SortedIntDocSet extends DocSet {
  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(SortedIntDocSet.class)
          + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;

  private final Closeable[] close;
  private final HeapCacheFbsModifier.State closed;
  protected final IntBuffer[] docs;
  final int capacity;

  private void check() {
    closed.check();
  }

  /**
   * @param parts Sorted list of ids
   */
  public SortedIntDocSet(Parts parts) {
    super(parts.arr.length > 0);
    this.close = parts.close;
    this.closed = parts.closed;
    this.docs = parts.arr;
    this.capacity = getCapacity(docs);
  }

  @Override
  protected void doClose() throws IOException {
    check();
    Closeable c;
    if (close != null && (c = close[0]) != null) {
      c.close();
    }
  }

  static int getCapacity(IntBuffer[] docs) {
    int lastOuterIdx = docs.length - 1;
    if (lastOuterIdx == -1) {
      return 0;
    } else {
      return docs[lastOuterIdx].capacity() + (lastOuterIdx * MAX_ARR_SIZE);
    }
  }

  /**
   * @param parts Sorted list of ids
   * @param len Number of ids in the list
   */
  public SortedIntDocSet(Parts parts, int len) {
    this(shrinkAndClose(parts, len));
  }

  private static Parts shrinkAndClose(Parts parts, int len) {
    try (Closeable c = parts.close[0]) {
      return shrink(parts, len);
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  public static Parts grow(IntBuffer[] buffer, int limit, int newSize) {
    Parts newParts = allocate(newSize);
    if (limit <= 0) return newParts;
    IntBuffer[] ret = newParts.arr;
    int lastIdx = limit - 1;
    int i = lastIdx >> SortedIntDocSet.WORDS_SHIFT;
    buffercopy(buffer[i], 0, ret[i], 0, (lastIdx & SortedIntDocSet.ARR_MASK) + 1);
    while (--i >= 0) {
      buffercopy(buffer[i], 0, ret[i], 0, SortedIntDocSet.MAX_ARR_SIZE);
    }
    return newParts;
  }

  public IntBuffer[] getDocs() {
    return docs;
  }

  @Override
  public int size() {
    return capacity;
  }

  private static final IntBuffer[] zeroInts = new IntBuffer[0];
  private static final Parts zeroIntsParts =
      new Parts(
          zeroInts, new Closeable[] {DocSet.NOOP_CLOSEABLE}, new HeapCacheFbsModifier.State());
  private static final SortedIntDocSet zero = new SortedIntDocSet(zeroIntsParts);

  // -5 b/c there are 32 bits per int
  static final int WORDS_SHIFT = BitDocSet.BIT_SHIFT - 5;
  static final int MAX_ARR_SIZE = 1 << WORDS_SHIFT;
  static final int ARR_MASK = MAX_ARR_SIZE - 1;

  public static final class Parts {
    public final IntBuffer[] arr;
    public final Closeable[] close;
    public final HeapCacheFbsModifier.State closed;

    public Parts(IntBuffer[] arr, Closeable[] close, HeapCacheFbsModifier.State closed) {
      this.arr = arr;
      this.close = close;
      this.closed = closed;
    }
  }

  public static Parts allocate(int size) {
    if (size <= 0) return zeroIntsParts;
    int outerSize = ((size - 1) >> WORDS_SHIFT) + 1;
    IntBuffer[] ret = new IntBuffer[outerSize];
    Closeable[] close = new Closeable[1];
    HeapCacheFbsModifier.State closed = new HeapCacheFbsModifier.State();
    int i = outerSize - 1;
    ByteBufferStruct[] bb =
        FixedBitSets.MODIFIER.allocateBytesArr(size << 2, new SentinelPacket(close, closed), false);
    ret[i] = bb[i].buf.asIntBuffer();
    while (--i >= 0) {
      ret[i] = bb[i].buf.asIntBuffer();
    }
    return new Parts(ret, close, closed);
  }

  public static Parts shrink(Parts parts, int newSize) {
    if (newSize == 0) {
      try (Closeable c = parts.close[0]) {
        return zeroIntsParts;
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    Parts newParts = allocate(newSize);
    IntBuffer[] ret = newParts.arr;
    IntBuffer[] arr = parts.arr;
    int i = ret.length - 1;
    int lastIdxSize = ((newSize - 1) & ARR_MASK) + 1;
    buffercopy(arr[i], 0, ret[i], 0, lastIdxSize);
    while (--i >= 0) {
      // don't share content
      buffercopy(arr[i], 0, ret[i], 0, SortedIntDocSet.MAX_ARR_SIZE);
    }
    return newParts;
  }

  public static int intersectionSize(
      SortedIntDocSet smallerSortedList, SortedIntDocSet biggerSortedList) {
    final IntBuffer[] a = smallerSortedList.docs;
    final IntBuffer[] b = biggerSortedList.docs;

    // The next doc we are looking for will be much closer to the last position we tried
    // than it will be to the midpoint between last and high... so probe ahead using
    // a function of the ratio of the sizes of the sets.
    int step = (biggerSortedList.capacity / smallerSortedList.capacity) + 1;

    // Since the majority of probes should be misses, we'll already be above the last probe
    // and shouldn't need to move larger than the step size on average to step over our target (and
    // thus lower the high upper bound a lot.)... but if we don't go over our target, it's a big
    // miss... so double it.
    step = step + step;

    // FUTURE: come up with a density such that target * density == likely position?
    // then check step on one side or the other?
    // (density could be cached in the DocSet)... length/maxDoc

    // FUTURE: try partitioning like a sort algorithm.  Pick the midpoint of the big
    // array, find where that should be in the small array, and then recurse with
    // the top and bottom half of both arrays until they are small enough to use
    // a fallback intersection method.
    // NOTE: I tried this and it worked, but it was actually slower than this current
    // highly optimized approach.

    int icount = 0;
    int low = 0;
    int max = biggerSortedList.capacity - 1;

    for (int i = 0; i < smallerSortedList.capacity; i++) {
      int doca = a[i >> WORDS_SHIFT].get(i & ARR_MASK);

      int high = max;

      int probe = low + step; // 40% improvement!

      // short linear probe to see if we can drop the high pointer in one big jump.
      if (probe < high) {
        if (b[probe >> WORDS_SHIFT].get(probe & ARR_MASK) >= doca) {
          // success!  we cut down the upper bound by a lot in one step!
          high = probe;
        } else {
          // relative failure... we get to move the low pointer, but not my much
          low = probe + 1;

          // reprobe worth it? it appears so!
          probe = low + step;
          if (probe < high) {
            if (b[probe >> WORDS_SHIFT].get(probe & ARR_MASK) >= doca) {
              high = probe;
            } else {
              low = probe + 1;
            }
          }
        }
      }

      // binary search the rest of the way
      while (low <= high) {
        int mid = (low + high) >>> 1;
        int docb = b[mid >> WORDS_SHIFT].get(mid & ARR_MASK);

        if (docb < doca) {
          low = mid + 1;
        } else if (docb > doca) {
          high = mid - 1;
        } else {
          icount++;
          low = mid + 1; // found it, so start at next element
          break;
        }
      }
      // Didn't find it... low is now positioned on the insertion point,
      // which is higher than what we were looking for, so continue using
      // the same low point.
    }

    return icount;
  }

  public static boolean intersects(
      SortedIntDocSet smallerSortedList, SortedIntDocSet biggerSortedList) {
    // see intersectionSize for more in-depth comments of this algorithm

    final IntBuffer[] a = smallerSortedList.docs;
    final IntBuffer[] b = biggerSortedList.docs;

    int step = (biggerSortedList.capacity / smallerSortedList.capacity) + 1;

    step = step + step;

    int low = 0;
    int max = biggerSortedList.capacity - 1;

    for (int i = 0; i < smallerSortedList.capacity; i++) {
      int doca = a[i >> WORDS_SHIFT].get(i & ARR_MASK);
      int high = max;
      int probe = low + step;
      if (probe < high) {
        if (b[probe >> WORDS_SHIFT].get(probe & ARR_MASK) >= doca) {
          high = probe;
        } else {
          low = probe + 1;
          probe = low + step;
          if (probe < high) {
            if (b[probe >> WORDS_SHIFT].get(probe & ARR_MASK) >= doca) {
              high = probe;
            } else {
              low = probe + 1;
            }
          }
        }
      }

      while (low <= high) {
        int mid = (low + high) >>> 1;
        int docb = b[mid >> WORDS_SHIFT].get(mid & ARR_MASK);

        if (docb < doca) {
          low = mid + 1;
        } else if (docb > doca) {
          high = mid - 1;
        } else {
          return true;
        }
      }
    }

    return false;
  }

  @Override
  public int intersectionSize(DocSet other) {
    check();
    if (!(other instanceof SortedIntDocSet)) {
      // BitDocSet is  better at random access than we are
      int icount = 0;
      for (int i = 0; i < docs.length; i++) {
        IntBuffer sub = docs[i];
        for (int j = 0, lim = sub.capacity(); j < lim; j++) {
          if (other.exists(sub.get(j))) icount++;
        }
      }
      return icount;
    }

    // make "a" the smaller set.
    SortedIntDocSet otherSet = (SortedIntDocSet) other;
    otherSet.check();
    final SortedIntDocSet a = capacity < otherSet.capacity ? this : otherSet;
    final SortedIntDocSet b = capacity < otherSet.capacity ? otherSet : this;

    if (a.capacity == 0) return 0;

    // if b is 8 times bigger than a, use the modified binary search.
    if ((b.capacity >> 3) >= a.capacity) {
      return intersectionSize(a, b);
    }

    // if they are close in size, just do a linear walk of both.
    int icount = 0;
    int i = 0, j = 0;
    IntBuffer[] aDocs = a.docs;
    IntBuffer[] bDocs = b.docs;
    int doca = aDocs[i >> WORDS_SHIFT].get(i & ARR_MASK),
        docb = bDocs[j >> WORDS_SHIFT].get(j & ARR_MASK);
    for (; ; ) {
      // switch on the sign bit somehow? Hopefully JVM is smart enough to just test once.

      // Since set a is less dense then set b, doca is likely to be greater than docb so
      // check that case first.  This resulted in a 13% speedup.
      if (doca > docb) {
        if (++j >= b.capacity) break;
        docb = bDocs[j >> WORDS_SHIFT].get(j & ARR_MASK);
      } else if (doca < docb) {
        if (++i >= a.capacity) break;
        doca = aDocs[i >> WORDS_SHIFT].get(i & ARR_MASK);
      } else {
        icount++;
        if (++i >= a.capacity) break;
        doca = aDocs[i >> WORDS_SHIFT].get(i & ARR_MASK);
        if (++j >= b.capacity) break;
        docb = bDocs[j >> WORDS_SHIFT].get(j & ARR_MASK);
      }
    }
    return icount;
  }

  @Override
  public boolean intersects(DocSet other) {
    check();
    if (!(other instanceof SortedIntDocSet)) {
      // assume BitDocSet is better at random access than we are
      for (IntBuffer sub : docs) {
        for (int i = 0, lim = sub.capacity(); i < lim; i++) {
          if (other.exists(sub.get(i))) return true;
        }
      }
      return false;
    }

    // make "a" the smaller set.
    SortedIntDocSet otherSet = (SortedIntDocSet) other;
    otherSet.check();
    final SortedIntDocSet a = capacity < otherSet.capacity ? this : otherSet;
    final SortedIntDocSet b = capacity < otherSet.capacity ? otherSet : this;

    if (a.capacity == 0) return false;

    // if b is 8 times bigger than a, use the modified binary search.
    if ((b.capacity >> 3) >= a.capacity) {
      return intersects(a, b);
    }

    // if they are close in size, just do a linear walk of both.
    int i = 0, j = 0;
    IntBuffer[] aDocs = a.docs;
    IntBuffer[] bDocs = b.docs;
    int doca = aDocs[i >> WORDS_SHIFT].get(i & ARR_MASK),
        docb = bDocs[j >> WORDS_SHIFT].get(j & ARR_MASK);
    for (; ; ) {
      // switch on the sign bit somehow?  Hopefull JVM is smart enough to just test once.

      // Since set a is less dense then set b, doca is likely to be greater than docb so
      // check that case first.  This resulted in a 13% speedup.
      if (doca > docb) {
        if (++j >= b.capacity) break;
        docb = bDocs[j >> WORDS_SHIFT].get(j & ARR_MASK);
      } else if (doca < docb) {
        if (++i >= a.capacity) break;
        doca = aDocs[i >> WORDS_SHIFT].get(i & ARR_MASK);
      } else {
        return true;
      }
    }
    return false;
  }

  /** puts the intersection of a and b into the target array and returns the size */
  public static int intersection(
      IntBuffer[] a, int lena, IntBuffer[] b, int lenb, IntBuffer[] target) {
    if (lena > lenb) {
      int ti = lena;
      lena = lenb;
      lenb = ti;
      IntBuffer[] ta = a;
      a = b;
      b = ta;
    }

    if (lena == 0) return 0;

    // if b is 8 times bigger than a, use the modified binary search.
    if ((lenb >> 3) >= lena) {
      return intersectionBinarySearch(a, lena, b, lenb, target);
    }

    int icount = 0;
    int i = 0, j = 0;
    int doca = a[i >> WORDS_SHIFT].get(i & ARR_MASK), docb = b[j >> WORDS_SHIFT].get(j & ARR_MASK);
    for (; ; ) {
      if (doca > docb) {
        if (++j >= lenb) break;
        docb = b[j >> WORDS_SHIFT].get(j & ARR_MASK);
      } else if (doca < docb) {
        if (++i >= lena) break;
        doca = a[i >> WORDS_SHIFT].get(i & ARR_MASK);
      } else {
        target[icount >> WORDS_SHIFT].put(icount++ & ARR_MASK, doca);
        if (++i >= lena) break;
        doca = a[i >> WORDS_SHIFT].get(i & ARR_MASK);
        if (++j >= lenb) break;
        docb = b[j >> WORDS_SHIFT].get(j & ARR_MASK);
      }
    }
    return icount;
  }

  /**
   * Puts the intersection of a and b into the target array and returns the size. lena should be
   * smaller than lenb
   */
  protected static int intersectionBinarySearch(
      IntBuffer[] a, int lena, IntBuffer[] b, int lenb, IntBuffer[] target) {
    int step = (lenb / lena) + 1;
    step = step + step;

    int icount = 0;
    int low = 0;
    int max = lenb - 1;

    for (int i = 0; i < lena; i++) {
      int doca = a[i >> WORDS_SHIFT].get(i & ARR_MASK);

      int high = max;

      int probe = low + step; // 40% improvement!

      // short linear probe to see if we can drop the high pointer in one big jump.
      if (probe < high) {
        if (b[probe >> WORDS_SHIFT].get(probe & ARR_MASK) >= doca) {
          // success!  we cut down the upper bound by a lot in one step!
          high = probe;
        } else {
          // relative failure... we get to move the low pointer, but not my much
          low = probe + 1;

          // reprobe worth it? it appears so!
          probe = low + step;
          if (probe < high) {
            if (b[probe >> WORDS_SHIFT].get(probe & ARR_MASK) >= doca) {
              high = probe;
            } else {
              low = probe + 1;
            }
          }
        }
      }

      // binary search
      while (low <= high) {
        int mid = (low + high) >>> 1;
        int docb = b[mid >> WORDS_SHIFT].get(mid & ARR_MASK);

        if (docb < doca) {
          low = mid + 1;
        } else if (docb > doca) {
          high = mid - 1;
        } else {
          target[icount >> WORDS_SHIFT].put(icount++ & ARR_MASK, doca);
          low = mid + 1; // found it, so start at next element
          break;
        }
      }
      // Didn't find it... low is now positioned on the insertion point,
      // which is higher than what we were looking for, so continue using
      // the same low point.
    }

    return icount;
  }

  @Override
  public DocSet intersection(DocSet other) {
    check();
    if (!(other instanceof SortedIntDocSet)) {
      int icount = 0;
      Parts newParts = allocate(capacity);
      IntBuffer[] arr = newParts.arr;
      for (int i = 0; i < capacity; i++) {
        int doc = docs[i >> WORDS_SHIFT].get(i & ARR_MASK);
        if (other.exists(doc)) {
          arr[icount >> WORDS_SHIFT].put(icount & ARR_MASK, doc);
          icount++;
        }
      }
      if (icount == capacity) {
        return this; // no change
      }
      return new SortedIntDocSet(newParts, icount);
    }

    SortedIntDocSet otherSet = (SortedIntDocSet) other;
    otherSet.check();
    int maxsz = Math.min(capacity, otherSet.capacity);
    Parts newParts = allocate(maxsz);
    IntBuffer[] arr = newParts.arr;
    int sz = intersection(docs, capacity, otherSet.docs, otherSet.capacity, arr);
    if (sz == capacity) {
      try (Closeable c = newParts.close[0]) {
        return this; // no change
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return new SortedIntDocSet(newParts, sz);
  }

  protected static int andNotBinarySearch(
      IntBuffer[] a, int lena, IntBuffer[] b, int lenb, IntBuffer[] target) {
    int step = (lenb / lena) + 1;
    step = step + step;

    int count = 0;
    int low = 0;
    int max = lenb - 1;

    outer:
    for (int i = 0; i < lena; i++) {
      int doca = a[i >> WORDS_SHIFT].get(i & ARR_MASK);

      int high = max;

      int probe = low + step; // 40% improvement!

      // short linear probe to see if we can drop the high pointer in one big jump.
      if (probe < high) {
        if (b[probe >> WORDS_SHIFT].get(probe & ARR_MASK) >= doca) {
          // success!  we cut down the upper bound by a lot in one step!
          high = probe;
        } else {
          // relative failure... we get to move the low pointer, but not my much
          low = probe + 1;

          // reprobe worth it? it appears so!
          probe = low + step;
          if (probe < high) {
            if (b[probe >> WORDS_SHIFT].get(probe & ARR_MASK) >= doca) {
              high = probe;
            } else {
              low = probe + 1;
            }
          }
        }
      }

      // binary search
      while (low <= high) {
        int mid = (low + high) >>> 1;
        int docb = b[mid >> WORDS_SHIFT].get(mid & ARR_MASK);

        if (docb < doca) {
          low = mid + 1;
        } else if (docb > doca) {
          high = mid - 1;
        } else {
          low = mid + 1; // found it, so start at next element
          continue outer;
        }
      }
      // Didn't find it... low is now positioned on the insertion point,
      // which is higher than what we were looking for, so continue using
      // the same low point.
      target[count >> WORDS_SHIFT].put(count++ & ARR_MASK, doca);
    }

    return count;
  }

  /** puts the intersection of a and not b into the target array and returns the size */
  public static int andNot(IntBuffer[] a, int lena, IntBuffer[] b, int lenb, IntBuffer[] target) {
    if (lena == 0) return 0;
    if (lenb == 0) {
      int i = lena >> WORDS_SHIFT;
      buffercopy(a[i], 0, target[i], 0, lena & ARR_MASK);
      while (--i >= 0) {
        buffercopy(a[i], 0, target[i], 0, MAX_ARR_SIZE);
      }
      return lena;
    }

    // if b is 8 times bigger than a, use the modified binary search.
    if ((lenb >> 3) >= lena) {
      return andNotBinarySearch(a, lena, b, lenb, target);
    }

    int count = 0;
    int i = 0, j = 0;
    int doca = a[i >> WORDS_SHIFT].get(i & ARR_MASK), docb = b[j >> WORDS_SHIFT].get(j & ARR_MASK);
    for (; ; ) {
      if (doca > docb) {
        if (++j >= lenb) break;
        docb = b[j >> WORDS_SHIFT].get(j & ARR_MASK);
      } else if (doca < docb) {
        target[count >> WORDS_SHIFT].put(count++ & ARR_MASK, doca);
        if (++i >= lena) break;
        doca = a[i >> WORDS_SHIFT].get(i & ARR_MASK);
      } else {
        if (++i >= lena) break;
        doca = a[i >> WORDS_SHIFT].get(i & ARR_MASK);
        if (++j >= lenb) break;
        docb = b[j >> WORDS_SHIFT].get(j & ARR_MASK);
      }
    }

    int leftover = lena - i;

    if (leftover > 0) {
      arraycopy(a, i, target, count, leftover);
      count += leftover;
    }

    return count;
  }

  public static void arraycopy(
      IntBuffer[] src, int srcIdx, IntBuffer[] dest, int destIdx, int len) {
    if (len == 0) return;
    int srcOuterOffset = srcIdx >> WORDS_SHIFT;
    final int destOuterOffset = destIdx >> WORDS_SHIFT;
    int srcInnerOffset = srcIdx & ARR_MASK;
    int destInnerOffset = destIdx & ARR_MASK;
    final int len1;
    final int len2;
    IntBuffer srcArr1;
    IntBuffer srcArr2;

    // the array offset of the word for the last "bit" element.
    final int destOuterLimit = (destIdx + len - 1) >> WORDS_SHIFT;

    if (srcInnerOffset <= destInnerOffset) {
      len1 = destInnerOffset - srcInnerOffset;
      len2 = MAX_ARR_SIZE - len1;
      srcArr1 = null;
      srcArr2 = src[srcOuterOffset];
    } else {
      len2 = srcInnerOffset - destInnerOffset;
      len1 = MAX_ARR_SIZE - len2;
      srcArr1 = src[srcOuterOffset]; // clear out-of-scope bits
      srcArr2 = ++srcOuterOffset < src.length ? src[srcOuterOffset] : null;
    }
    // special handling for the first word, which may be partial
    IntBuffer destArr = dest[destOuterOffset];
    if (srcArr1 == null) {
      buffercopy(
          srcArr2,
          srcInnerOffset,
          destArr,
          destInnerOffset,
          Math.min(len, MAX_ARR_SIZE - destInnerOffset));
    } else if (srcArr2 == null) {
      buffercopy(
          srcArr1,
          srcInnerOffset,
          destArr,
          destInnerOffset,
          Math.min(len, MAX_ARR_SIZE - srcInnerOffset));
    } else {
      int initialLen = MAX_ARR_SIZE - srcInnerOffset;
      if (len <= initialLen) {
        buffercopy(srcArr1, srcInnerOffset, destArr, destInnerOffset, len);
      } else {
        buffercopy(srcArr1, srcInnerOffset, destArr, destInnerOffset, initialLen);
        buffercopy(
            srcArr2, 0, destArr, destInnerOffset + initialLen, Math.min(len2, len - initialLen));
      }
    }
    if (destOuterOffset == destOuterLimit) return;

    for (int i = destOuterOffset + 1; i < destOuterLimit; i++) {
      // inner words are guaranteed to not be partial, so this can be very simple
      srcArr1 = srcArr2;
      srcArr2 = src[++srcOuterOffset];
      destArr = dest[i];
      buffercopy(srcArr1, len2, destArr, 0, len1);
      buffercopy(srcArr2, 0, destArr, len1, len2);
    }
    srcArr1 = srcArr2;
    srcArr2 = ++srcOuterOffset < src.length ? src[srcOuterOffset] : null;

    // special handling for the last word, which may be partial
    int remainder = ((destIdx + len - 1) & ARR_MASK) + 1;
    destArr = dest[destOuterLimit];
    if (srcArr2 == null || remainder <= len1) {
      buffercopy(srcArr1, len2, destArr, 0, remainder);
    } else {
      buffercopy(srcArr1, len2, destArr, 0, len1);
      buffercopy(srcArr2, 0, destArr, len1, remainder - len1);
    }
  }

  static void buffercopy(IntBuffer src, int srcOff, IntBuffer dest, int destOff, int len) {
    dest.slice().position(destOff).put(src.slice(srcOff, len));
  }

  @Override
  public DocSet andNot(DocSet other) {
    check();
    if (other.size() == 0) return this;

    if (!(other instanceof SortedIntDocSet)) {
      int count = 0;
      Parts newParts = allocate(capacity);
      IntBuffer[] arr = newParts.arr;
      for (int i = 0; i < capacity; i++) {
        int doc = docs[i >> WORDS_SHIFT].get(i & ARR_MASK);
        if (!other.exists(doc)) arr[count >> WORDS_SHIFT].put(count++ & ARR_MASK, doc);
      }
      if (count == capacity) {
        try (Closeable c = newParts.close[0]) {
          return this; // no change
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
      return new SortedIntDocSet(newParts, count);
    }

    SortedIntDocSet otherSet = (SortedIntDocSet) other;
    otherSet.check();
    Parts newParts = allocate(capacity);
    IntBuffer[] arr = newParts.arr;
    int sz = andNot(docs, capacity, otherSet.docs, otherSet.capacity, arr);
    if (sz == capacity) {
      try (Closeable c = newParts.close[0]) {
        return this; // no change
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return new SortedIntDocSet(newParts, sz);
  }

  @Override
  public void addAllTo(FixedBitSets target) {
    check();
    for (IntBuffer sub : docs) {
      for (int i = 0, lim = sub.capacity(); i < lim; i++) {
        target.set(sub.get(i));
      }
      assert closed.notClosed() : "docset already closed";
    }
  }

  @Override
  public boolean exists(int doc) {
    assert closed.notClosed() : "docset already closed";
    // this could be faster by estimating where in the list the doc is likely to appear,
    // but we should get away from using exists() anyway.
    int low = 0;
    int high = capacity - 1;
    // binary search
    while (low <= high) {
      int mid = (low + high) >>> 1;
      int docb = docs[mid >> WORDS_SHIFT].get(mid & ARR_MASK);

      if (docb < doc) {
        low = mid + 1;
      } else if (docb > doc) {
        high = mid - 1;
      } else {
        return true;
      }
    }
    return false;
  }

  @Override
  public DocIterator iterator() {
    check();
    return new DocIterator() {
      int pos = 0;

      @Override
      public boolean hasNext() {
        return pos < capacity;
      }

      @Override
      public Integer next() {
        return nextDoc();
      }

      /** The remove operation is not supported by this Iterator. */
      @Override
      public void remove() {
        throw new UnsupportedOperationException(
            "The remove  operation is not supported by this Iterator.");
      }

      @Override
      public int nextDoc() {
        assert closed.notClosed() : "docset already closed";
        return docs[pos >> WORDS_SHIFT].get(pos++ & ARR_MASK);
      }

      @Override
      public float score() {
        return 0.0f;
      }
    };
  }

  @Override
  public Bits getBits() {
    check();
    IntHashSet hashSet = new IntHashSet(capacity);
    for (IntBuffer sub : docs) {
      for (int i = 0, lim = sub.capacity(); i < lim; i++) {
        hashSet.add(sub.get(i));
      }
      assert closed.notClosed() : "docset already closed";
    }

    return new Bits() {
      @Override
      public boolean get(int index) {
        return hashSet.contains(index);
      }

      @Override
      public int length() {
        return getLength();
      }
    };
  }

  /** the {@link Bits#length()} or maxdoc (1 greater than largest possible doc number) */
  private int getLength() {
    int size = size();
    if (size == 0) {
      return 0;
    } else {
      int idx = size - 1;
      return getDocs()[idx >> WORDS_SHIFT].get(idx & ARR_MASK) + 1;
    }
  }

  @Override
  protected FixedBitSets getFixedBitSet() {
    return getFixedBitSetClone();
  }

  @Override
  protected FixedBitSets getFixedBitSetClone() {
    FixedBitSets ret = new FixedBitSets(getLength());
    addAllTo(ret);
    return ret;
  }

  @Override
  public DocSet union(DocSet other) {
    check();
    // TODO could be more efficient if both are SortedIntDocSet
    FixedBitSets otherBits = other.getFixedBitSet();
    FixedBitSets newbits = BitDocSet.ensureCapacity(getFixedBitSetClone(), otherBits.length());
    newbits.or(otherBits);
    return new BitDocSet(newbits);
  }

  private volatile int[] cachedOrdIdxMap; // idx of first doc _beyond_ the corresponding seg

  private int[] getOrdIdxMap(LeafReaderContext ctx) {
    final int[] cached = cachedOrdIdxMap;
    if (cached != null) {
      return cached;
    } else {
      List<LeafReaderContext> leaves = ReaderUtil.getTopLevelContext(ctx).leaves();
      final int[] ret = new int[leaves.size()];
      int lastLimit = 0;
      int lastLimitDoc = docs[0].get(0); // capacity != 0
      for (LeafReaderContext lrc : leaves) {
        // sanity check that initial `lastLimit*` values are valid (and consequently that our
        // initial setting of `startIdx` for context.ord==0 won't inadvertently include invalid
        // docs).
        assert lrc.ord != 0 || lastLimitDoc >= lrc.docBase;
        final int max =
            lrc.docBase + lrc.reader().maxDoc(); // one past the max doc in this segment.
        if (lastLimitDoc >= max) {
          ret[lrc.ord] = lastLimit;
          continue;
        }
        assert lastLimitDoc >= lrc.docBase;
        final int nextLimit = binarySearch(docs, lastLimit + 1, capacity, max);
        lastLimit = nextLimit < 0 ? ~nextLimit : nextLimit;
        lastLimitDoc =
            lastLimit < capacity
                ? docs[lastLimit >> WORDS_SHIFT].get(lastLimit & ARR_MASK)
                : DocIdSetIterator.NO_MORE_DOCS;
        ret[lrc.ord] = lastLimit;
      }
      return cachedOrdIdxMap = ret; // set/replace atomically after building
    }
  }

  @Override
  public DocIdSetIterator iterator(LeafReaderContext context) {
    check();

    if (capacity == 0 || context.reader().maxDoc() < 1) {
      // empty docset or entirely empty segment (verified that the latter actually happens)
      // NOTE: wrt the "empty docset" case, this is not just an optimization; this shortcircuits
      // also to prevent the static DocSet.EmptyLazyHolder.INSTANCE from having cachedOrdIdxMap
      // initiated across different IndexReaders.
      return null;
    }

    final int startIdx;
    final int limitIdx;
    if (context.isTopLevel) {
      startIdx = 0;
      limitIdx = capacity;
    } else {
      int[] ordIdxMap = getOrdIdxMap(context);
      startIdx = context.ord == 0 ? 0 : ordIdxMap[context.ord - 1];
      limitIdx = ordIdxMap[context.ord];

      if (startIdx >= limitIdx) {
        return null; // verified this does happen
      }
    }
    final int base = context.docBase;

    return new DocIdSetIterator() {
      int idx = startIdx - 1;
      int adjustedDoc = -1;

      @Override
      public int docID() {
        return adjustedDoc;
      }

      @Override
      public int nextDoc() {
        return adjustedDoc =
            (++idx >= limitIdx)
                ? NO_MORE_DOCS
                : (docs[idx >> WORDS_SHIFT].get(idx & ARR_MASK) - base);
      }

      @Override
      public int advance(int target) {
        if (++idx >= limitIdx || target == NO_MORE_DOCS) return adjustedDoc = NO_MORE_DOCS;
        target += base;

        // probe next
        int rawDoc = docs[idx >> WORDS_SHIFT].get(idx & ARR_MASK);
        if (rawDoc >= target) return adjustedDoc = rawDoc - base;

        // TODO: probe more before resorting to binary search?

        final int findIdx = binarySearch(docs, idx + 1, limitIdx, target);
        idx = findIdx < 0 ? ~findIdx : findIdx;
        return adjustedDoc =
            idx < limitIdx ? docs[idx >> WORDS_SHIFT].get(idx & ARR_MASK) - base : NO_MORE_DOCS;
      }

      @Override
      public long cost() {
        return (long) limitIdx - startIdx;
      }
    };
  }

  public static int binarySearch(IntBuffer[] a, int fromIndex, int toIndex, int key) {
    int low = fromIndex;
    int high = toIndex - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      int midVal = a[mid >> WORDS_SHIFT].get(mid & ARR_MASK);

      if (midVal < key) low = mid + 1;
      else if (midVal > key) high = mid - 1;
      else return mid; // key found
    }
    return -(low + 1); // key not found.
  }

  @Override
  public DocSetQuery makeQuery() {
    return new DocSetQuery(this);
  }

  @Override
  public SortedIntDocSet clone() {
    check();
    Closeable[] close = new Closeable[1];
    HeapCacheFbsModifier.State closed = new HeapCacheFbsModifier.State();
    IntBuffer[] newDocs = new IntBuffer[docs.length];
    ByteBufferStruct[] bb =
        FixedBitSets.MODIFIER.allocateBytesArr(
            capacity << 2, new SentinelPacket(close, closed), false);
    for (int i = docs.length - 1; i >= 0; i--) {
      newDocs[i] = bb[i].buf.asIntBuffer().put(docs[i].slice()).clear();
    }
    return new SortedIntDocSet(new Parts(newDocs, close, closed));
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + ((long) capacity << 2);
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }

  @Override
  public String toString() {
    return "SortedIntDocSet{"
        + "size="
        + size()
        + ","
        + "ramUsed="
        + RamUsageEstimator.humanReadableUnits(ramBytesUsed())
        + '}';
  }
}
