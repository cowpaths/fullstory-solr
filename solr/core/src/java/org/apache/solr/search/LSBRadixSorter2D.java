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
import static org.apache.solr.search.SortedIntDocSet.DocIdList;

import java.util.Arrays;

/** Copied from {@link org.apache.lucene.util.LSBRadixSorter} */
public final class LSBRadixSorter2D {

  private static final int INSERTION_SORT_THRESHOLD = 30;
  private static final int HISTOGRAM_SIZE = 256;

  private final DocIdList histogram = SortedIntDocSet.allocate(HISTOGRAM_SIZE);
  private DocIdList buffer = SortedIntDocSet.zeroDocSet().getDocs();
  private int bufferCapacity = 0;

  private static void buildHistogram(DocIdList array, int len, DocIdList histogram, int shift) {
    for (int i = 0; i < len; ++i) {
      final int b = (array.get(i) >>> shift) & 0xFF;
      histogram.set(b, histogram.get(b) + 1);
    }
  }

  private static void sumHistogram(DocIdList histogram) {
    int accum = 0;
    for (int i = 0; i < HISTOGRAM_SIZE; ++i) {
      final int count = histogram.get(i);
      histogram.set(i, accum);
      accum += count;
    }
  }

  private static void reorder(DocIdList array, int len, DocIdList histogram, int shift, DocIdList dest) {
    for (int i = 0; i < len; ++i) {
      final int v = array.get(i);
      final int b = (v >>> shift) & 0xFF;
      int destIdx = histogram.get(b);
      histogram.set(b, destIdx + 1);
      dest.set(destIdx, v);
    }
  }

  private static boolean sort(DocIdList array, int len, DocIdList histogram, int shift, DocIdList dest) {
    histogram.setZero();
    buildHistogram(array, len, histogram, shift);
    if (histogram.get(0) == len) {
      return false;
    }
    sumHistogram(histogram);
    reorder(array, len, histogram, shift, dest);
    return true;
  }

  private static void insertionSort(SortedIntDocSet.DocIdList array, int off, int len) {
    for (int i = off + 1, end = off + len; i < end; ++i) {
      for (int j = i; j > off; --j) {
        int prevIdx = j - 1;
        int tmp;
        int candidate;
        if ((tmp = array.get(prevIdx))
            > (candidate = array.get(j))) {
          array.set(prevIdx, candidate);
          array.set(j, tmp);
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
  public void sort(int numBits, final SortedIntDocSet.DocIdList array, int len) {
    if (len < INSERTION_SORT_THRESHOLD) {
      insertionSort(array, 0, len);
      return;
    }

    buffer = buffer.grow(bufferCapacity, len);
    bufferCapacity = len;

    SortedIntDocSet.DocIdList arr = array;

    SortedIntDocSet.DocIdList buf = buffer;

    for (int shift = 0; shift < numBits; shift += 8) {
      if (sort(arr, len, histogram, shift, buf)) {
        // swap arrays
        DocIdList tmp = arr;
        arr = buf;
        buf = tmp;
      }
    }

    if (array == buf) {
      arr.copyTo(0, array, 0, len);
    }
  }
}
