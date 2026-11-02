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
package org.apache.solr.storage;

import java.lang.reflect.Method;
import org.apache.solr.SolrTestCaseJ4;

/**
 * Verifies that {@link Cache2}'s randomized initial LRU chain-link order (see its constructor)
 * always produces a single, fully-connected, bidirectionally-consistent cycle through every real
 * slot {@code 1..capacity} -- exactly once each -- regardless of the randomly chosen stride. A
 * stride not coprime to {@code capacity} would silently produce multiple disjoint cycles (orphaning
 * some slots as permanently unevictable while double-linking others), with no exception to catch
 * it; this test is the structural-integrity gate for that failure mode.
 */
public class TestCache2ChainIntegrity extends SolrTestCaseJ4 {

  private static final int ITERATIONS_PER_CAPACITY = 200;

  public void testChainVisitsEverySlotExactlyOnce() {
    int[] capacities = {1, 2, 3, 4, 5, 7, 11, 97, 100, 101, 511, 512, 720, 1009, 2310};
    for (int capacity : capacities) {
      for (int iter = 0; iter < ITERATIONS_PER_CAPACITY; iter++) {
        checkChainIntegrity(newCache(capacity), capacity);
      }
    }
  }

  /**
   * Large-capacity check (fewer iterations -- each allocates off-heap backing stores, so iteration
   * count and the largest capacity tested are both kept modest to bound total off-heap churn):
   * exercises the stride-selection logic at larger capacities, including the realistic upper end of
   * single-partition capacity (2,621,440) exactly once. {@code choosePermutationStride}'s floor and
   * continued-fraction thresholds are pure ratios of {@code capacity} (no absolute cutoff that only
   * engages at extreme scale), so smaller capacities already exercise the same relative behavior --
   * the single large-capacity iteration exists to confirm the real production ceiling specifically,
   * not because the algorithm behaves differently there.
   */
  public void testChainVisitsEverySlotExactlyOnceAtLargeCapacity() {
    int[] capacities = {40_960, 163_840};
    for (int capacity : capacities) {
      for (int iter = 0; iter < 5; iter++) {
        checkChainIntegrity(newCache(capacity), capacity);
      }
    }
    checkChainIntegrity(newCache(2_621_440), 2_621_440);
  }

  /**
   * Verifies the actual locality guarantee {@code choosePermutationStride} is meant to provide: for
   * every lag up to a realistic concurrent-burst size, chain entries that many steps apart in
   * acquisition order must not land on numerically-adjacent (and therefore physically cache-line-
   * adjacent) slots. This is the property the continued-fraction partial-quotient check exists to
   * guarantee for all lags simultaneously, not just a brute-force-checked prefix -- this test
   * spot-checks that guarantee at a lag count (10,000) far larger than would be practical to verify
   * via brute-force rejection sampling at construction time.
   */
  public void testChainMaintainsLocalityAcrossBurstLags() {
    // capacity is far smaller than the realistic production ceiling (2,621,440) to bound off-heap
    // allocation churn across iterations; choosePermutationStride's thresholds are pure ratios of
    // capacity, so this still exercises the same relative behavior (confirmed empirically that the
    // minAcceptableGap guarantee below still holds at this capacity/maxLag ratio).
    int capacity = 200_000;
    int maxLag = 10_000;
    int minAcceptableGap = 2; // matches the empirically-observed worst case for this threshold

    for (int iter = 0; iter < 10; iter++) {
      Cache2.DualQueueCache cache = newCache(capacity);
      int[] chainOrder = new int[capacity];
      int cur = cache.getNextVolatile(cache.COLD_HEAD);
      for (int i = 0; i < capacity; i++) {
        chainOrder[i] = cur;
        cur = cache.getNextVolatile(cur);
      }

      // Check a sample of starting positions (checking all ~2.6M would be too slow) at every lag
      // up to maxLag: slot numbers at positions i and i+lag must differ by at least
      // minAcceptableGap.
      int sampleCount = 50;
      for (int s = 0; s < sampleCount; s++) {
        int start = s * (capacity / sampleCount);
        for (int lag = 1; lag <= maxLag; lag *= 2) { // geometric sampling of lags, not exhaustive
          int other = (start + lag) % capacity;
          int diff = Math.abs(chainOrder[start] - chainOrder[other]);
          int gap = Math.min(diff, capacity - diff);
          assertTrue(
              "iter="
                  + iter
                  + " start="
                  + start
                  + " lag="
                  + lag
                  + ": slots "
                  + chainOrder[start]
                  + " and "
                  + chainOrder[other]
                  + " too close (gap="
                  + gap
                  + ")",
              gap >= minAcceptableGap);
        }
      }
    }
  }

  /**
   * Regression test for a bug found via external review: an earlier version of {@code
   * maxPartialQuotient} excluded the continued fraction's first quotient and was not evaluated
   * symmetrically for {@code stride} vs. {@code capacity-stride}, together allowing pathologically
   * small/large strides (e.g. {@code stride=7} at this capacity) to slip through acceptance despite
   * producing severe near-sequential chaining. Verifies directly, via reflection into the
   * package-private helpers, that such strides are now correctly and symmetrically rejected, and
   * that a known-good stride is still accepted.
   */
  public void testMaxPartialQuotientRejectsPathologicalStridesSymmetrically() throws Exception {
    Method maxPartialQuotient =
        Cache2.class.getDeclaredMethod("maxPartialQuotient", int.class, int.class);
    maxPartialQuotient.setAccessible(true);

    int capacity = 2_621_440;
    int maxPq = 20; // matches Cache2.MAX_PARTIAL_QUOTIENT

    for (int badStride : new int[] {7, 10, 100}) {
      int sym = Math.min(badStride, capacity - badStride);
      int pq = (int) maxPartialQuotient.invoke(null, capacity, sym);
      assertTrue(
          "stride="
              + badStride
              + " should be rejected (pathologically small, severe k=1 false sharing) but"
              + " maxPartialQuotient="
              + pq,
          pq > maxPq);

      int mirrored = capacity - badStride;
      int symMirrored = Math.min(mirrored, capacity - mirrored);
      int pqMirrored = (int) maxPartialQuotient.invoke(null, capacity, symMirrored);
      assertEquals(
          "stride="
              + badStride
              + " and its mirror capacity-stride="
              + mirrored
              + " must be treated identically (same physical cycle, traversed in opposite"
              + " direction)",
          pq,
          pqMirrored);
    }

    // A stride known (from prior investigation) to pass the check should still be accepted.
    int goodStride = 939196;
    int goodSym = Math.min(goodStride, capacity - goodStride);
    int goodPq = (int) maxPartialQuotient.invoke(null, capacity, goodSym);
    assertTrue(
        "known-good stride=" + goodStride + " should be accepted but maxPartialQuotient=" + goodPq,
        goodPq <= maxPq);
  }

  private static Cache2.DualQueueCache newCache(int capacity) {
    return new Cache2.DualQueueCache(capacity);
  }

  /**
   * Single-pass structural-integrity check, using primitive arrays throughout (no boxed
   * collections) to keep this test tractable at realistic large capacities: walks forward via
   * {@code next} from {@code COLD_HEAD}, recording each slot and asserting no slot is visited twice
   * and every slot {@code 1..capacity} is eventually visited; then walks backward via {@code prev}
   * from {@code COLD_TAIL}, asserting it retraces the exact same sequence in reverse (i.e. {@code
   * next}/{@code prev} are true inverses, not just that {@code next} alone forms a valid cycle).
   */
  private void checkChainIntegrity(Cache2.DualQueueCache cache, int capacity) {
    int[] forwardOrder = new int[capacity];
    boolean[] seen = new boolean[capacity + 1];
    int cur = cache.getNextVolatile(cache.COLD_HEAD);
    for (int i = 0; i < capacity; i++) {
      assertTrue(
          "capacity=" + capacity + ": slot " + cur + " out of real-slot range",
          cur >= 1 && cur <= capacity);
      assertTrue(
          "capacity=" + capacity + ": slot " + cur + " visited twice in forward walk", !seen[cur]);
      seen[cur] = true;
      forwardOrder[i] = cur;
      cur = cache.getNextVolatile(cur);
    }
    assertEquals(
        "capacity=" + capacity + ": forward walk did not reach COLD_TAIL after capacity steps",
        cache.COLD_TAIL,
        cur);

    cur = cache.getPrevVolatile(cache.COLD_TAIL);
    for (int i = capacity - 1; i >= 0; i--) {
      assertEquals(
          "capacity=" + capacity + ": backward walk diverged from forward walk at position " + i,
          forwardOrder[i],
          cur);
      cur = cache.getPrevVolatile(cur);
    }
    assertEquals(
        "capacity=" + capacity + ": backward walk did not reach COLD_HEAD after capacity steps",
        cache.COLD_HEAD,
        cur);
  }
}
