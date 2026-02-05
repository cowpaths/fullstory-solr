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

import static org.apache.solr.search.SortedIntDocSet.ARR_MASK;
import static org.apache.solr.search.SortedIntDocSet.WORDS_SHIFT;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.IntBuffer;

/** Copied from {@link org.apache.lucene.util.LSBRadixSorter} */
public final class LSBRadixSorter2D implements Closeable {

  private static final int INSERTION_SORT_THRESHOLD = 30;
  private static final int HISTOGRAM_SIZE = 256;

  private final SortedIntDocSet.Parts histogram = SortedIntDocSet.allocate(HISTOGRAM_SIZE);
  private SortedIntDocSet.Parts buffer = SortedIntDocSet.allocate(0);
  private int bufferCapacity = 0;

  private static void buildHistogram(IntBuffer[] array, int len, IntBuffer[] histogram, int shift) {
    for (int i = 0; i < len; ++i) {
      final int b = (array[i >> WORDS_SHIFT].get(i & ARR_MASK) >>> shift) & 0xFF;
      IntBuffer buf = histogram[b >> WORDS_SHIFT];
      int innerIdx = b & ARR_MASK;
      buf.put(innerIdx, buf.get(innerIdx) + 1); // TODO: vectorize?
    }
  }

  private static void sumHistogram(IntBuffer[] histogram) {
    int accum = 0;
    for (int i = 0; i < HISTOGRAM_SIZE; ++i) {
      final int count = histogram[i >> WORDS_SHIFT].get(i & ARR_MASK);
      histogram[i >> WORDS_SHIFT].put(i & ARR_MASK, accum);
      accum += count;
    }
  }

  private static void reorder(
      IntBuffer[] array, int len, IntBuffer[] histogram, int shift, IntBuffer[] dest) {
    for (int i = 0; i < len; ++i) {
      final int v = array[i >> WORDS_SHIFT].get(i & ARR_MASK);
      final int b = (v >>> shift) & 0xFF;
      IntBuffer buf = histogram[b >> WORDS_SHIFT];
      int innerIdx = b & ARR_MASK;
      int destIdx = buf.get(innerIdx);
      buf.put(innerIdx, destIdx + 1);
      dest[destIdx >> WORDS_SHIFT].put(destIdx & ARR_MASK, v);
    }
  }

  private static boolean sort(
      IntBuffer[] array, int len, IntBuffer[] histogram, int shift, IntBuffer[] dest) {
    for (IntBuffer sub : histogram) {
      for (int i = 0, lim = sub.capacity(); i < lim; i++) {
        sub.put(i, 0); // TODO: vector broadcast?
      }
    }
    buildHistogram(array, len, histogram, shift);
    if (histogram[0].get(0) == len) {
      return false;
    }
    sumHistogram(histogram);
    reorder(array, len, histogram, shift, dest);
    return true;
  }

  private static void insertionSort(IntBuffer[] array, int off, int len) {
    for (int i = off + 1, end = off + len; i < end; ++i) {
      for (int j = i; j > off; --j) {
        int prevIdx = j - 1;
        int tmp;
        int candidate;
        if ((tmp = array[prevIdx >> WORDS_SHIFT].get(prevIdx & ARR_MASK))
            > (candidate = array[j >> WORDS_SHIFT].get(j & ARR_MASK))) {
          array[prevIdx >> WORDS_SHIFT].put(prevIdx & ARR_MASK, candidate);
          array[j >> WORDS_SHIFT].put(j & ARR_MASK, tmp);
        } else {
          break;
        }
      }
    }
  }

  /**
   * Sort {@code array[0:len]} in place.
   *
   * @param numBits how many bits are required to store any of the values in {@code array[0:len]}.
   *     Pass {@code 32} if unknown.
   */
  public void sort(int numBits, final IntBuffer[] array, int len) {
    if (len < INSERTION_SORT_THRESHOLD) {
      insertionSort(array, 0, len);
      return;
    }

    try (Closeable c = buffer.close[0]) {
      buffer = SortedIntDocSet.grow(buffer.arr, bufferCapacity, len);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    bufferCapacity = len;

    IntBuffer[] arr = array;

    IntBuffer[] buf = buffer.arr;

    for (int shift = 0; shift < numBits; shift += 8) {
      if (sort(arr, len, histogram.arr, shift, buf)) {
        // swap arrays
        IntBuffer[] tmp = arr;
        arr = buf;
        buf = tmp;
      }
    }

    if (array == buf) {
      SortedIntDocSet.arraycopy(arr, 0, array, 0, len);
    }
  }

  @Override
  public void close() throws IOException {
    try (Closeable c1 = histogram.close[0];
        Closeable c2 = buffer.close[0]) {
      // close both
    }
  }
}
