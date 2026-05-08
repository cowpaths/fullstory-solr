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

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/** Thread-safe aggregate timing for one Lucene segment (one leaf per search). */
public final class SegmentSearchStats {

  /** Recency-biased reservoir for percentile estimates (Dropwizard exponentially decaying). */
  private final Histogram latencyHistogram =
      new Histogram(new ExponentiallyDecayingReservoir());

  private final LongAdder leafVisits = new LongAdder();
  private final LongAdder totalNanos = new LongAdder();
  private final AtomicLong minNanos = new AtomicLong(Long.MAX_VALUE);
  private final AtomicLong maxNanos = new AtomicLong(Long.MIN_VALUE);

  /** Records wall-clock time spent on one leaf (scorer setup + collection + finish). */
  public void recordLeafNanos(long nanos) {
    if (nanos < 0) {
      nanos = 0;
    }
    leafVisits.increment();
    totalNanos.add(nanos);
    minNanos.accumulateAndGet(nanos, Math::min);
    maxNanos.accumulateAndGet(nanos, Math::max);
    latencyHistogram.update(nanos);
  }

  public Snapshot snapshot() {
    long visits = leafVisits.sum();
    long total = totalNanos.sum();
    long min = minNanos.get();
    long max = maxNanos.get();
    if (visits == 0) {
      return Snapshot.empty();
    }
    if (min == Long.MAX_VALUE) {
      min = 0;
    }
    if (max == Long.MIN_VALUE) {
      max = 0;
    }
    double avgMs = visits > 0 ? (total / 1_000_000.0) / visits : 0;

    com.codahale.metrics.Snapshot hs = latencyHistogram.getSnapshot();
    int histogramSampleSize = hs.size();
    double p50Ms = nanoToMs(hs.getMedian());
    double p90Ms = nanoToMs(hs.getValue(0.90));
    double p95Ms = nanoToMs(hs.get95thPercentile());
    double p99Ms = nanoToMs(hs.get99thPercentile());

    return new Snapshot(
        visits,
        total / 1_000_000.0,
        avgMs,
        min / 1_000_000.0,
        max / 1_000_000.0,
        histogramSampleSize,
        latencyHistogram.getCount(),
        p50Ms,
        p90Ms,
        p95Ms,
        p99Ms);
  }

  private static double nanoToMs(double nanos) {
    return nanos / 1_000_000.0;
  }

  /** Immutable view for metrics / admin API. */
  public static final class Snapshot {
    public static final String DECAYING_HISTOGRAM_TYPE = "exponentiallyDecaying";

    public final long leafVisits;
    public final double totalTimeMs;
    public final double avgTimeMs;
    public final double minTimeMs;
    public final double maxTimeMs;
    /** Values currently in the percentile reservoir (may be less than {@link #histogramUpdateCount}). */
    public final int histogramSampleSize;
    /** Total leaf timings recorded into the histogram. */
    public final long histogramUpdateCount;
    public final double p50TimeMs;
    public final double p90TimeMs;
    public final double p95TimeMs;
    public final double p99TimeMs;

    private Snapshot(
        long leafVisits,
        double totalTimeMs,
        double avgTimeMs,
        double minTimeMs,
        double maxTimeMs,
        int histogramSampleSize,
        long histogramUpdateCount,
        double p50TimeMs,
        double p90TimeMs,
        double p95TimeMs,
        double p99TimeMs) {
      this.leafVisits = leafVisits;
      this.totalTimeMs = totalTimeMs;
      this.avgTimeMs = avgTimeMs;
      this.minTimeMs = minTimeMs;
      this.maxTimeMs = maxTimeMs;
      this.histogramSampleSize = histogramSampleSize;
      this.histogramUpdateCount = histogramUpdateCount;
      this.p50TimeMs = p50TimeMs;
      this.p90TimeMs = p90TimeMs;
      this.p95TimeMs = p95TimeMs;
      this.p99TimeMs = p99TimeMs;
    }

    /** All-zero snapshot when no leaf visits have been recorded. */
    public static Snapshot empty() {
      return new Snapshot(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }
  }
}
