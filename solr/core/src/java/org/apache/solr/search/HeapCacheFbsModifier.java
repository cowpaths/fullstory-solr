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

import com.codahale.metrics.Gauge;
import com.sun.management.OperatingSystemMXBean;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.FixedBitSet.ByteBufferStruct;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.ThreadInterruptedException;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Pools buffers backed by heap byte[] of largest possible size */
public class HeapCacheFbsModifier
    implements FixedBitSet.Modifier, AutoCloseable, SolrMetricProducer {

  public static HeapCacheFbsModifier getInstance() {
    try {
      return FixedBitSets.registerModifier(
          () -> {
            if (TPS > 0 && TPS_OFFHEAP > 0) {
              HeapCacheFbsModifier offheap =
                  new HeapCacheFbsModifier(false, TPS_OFFHEAP, true, null);
              return new HeapCacheFbsModifier(true, TPS, false, offheap);
            } else if (TPS > 0) {
              return new HeapCacheFbsModifier(true, TPS, false, null);
            } else if (TPS_OFFHEAP > 0) {
              return new HeapCacheFbsModifier(true, TPS_OFFHEAP, true, null);
            } else {
              throw new IllegalStateException();
            }
          });
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final int BLOCK_SIZE_BYTES = SortedIntDocSet.MAX_ARR_SIZE << 2;
  private static final int MAX_BLOCKS_PER_PARTITION = Integer.MAX_VALUE / BLOCK_SIZE_BYTES;
  private final int nBlocks;
  private final boolean offheap;

  private static final int MIN_BLOCK_SIZE = 4096; // 4k

  private static final int ALIGN_SIZE = 1 << 21; // 2m
  private static final int ALIGN_OVERHEAD = ALIGN_SIZE - 1; // 2m - 1
  private static final int ALIGN_ALLOC_MINSIZE = ALIGN_SIZE + ALIGN_OVERHEAD;

  private final boolean unregister;
  private final ByteBufferStruct[] pool;

  private final AtomicInteger top;
  private final BlockingQueue<ByteBufferStruct[]> releaseQueue =
      SINGLE_THREADED_REF_HANDLER ? null : new ArrayBlockingQueue<>(1024, false);

  public static final class State {
    // NOTE: could be volatile, but that's not really the failure mode we care about here.
    // so for performance we'll leave this non-volatile.
    private boolean isClosed = false;

    boolean notClosed() {
      return !isClosed;
    }

    void check() {
      if (isClosed) {
        throw new IllegalStateException("already closed " + System.identityHashCode(this));
      }
    }
  }

  private static final class RefHandlerPacket {
    private final ByteBufferStruct[] buf;
    private final State closed;

    private RefHandlerPacket(ByteBufferStruct[] buf, State closed) {
      this.buf = buf;
      this.closed = closed;
    }
  }

  private final ReferenceHandler<RefHandlerPacket> refHandler;

  public static final String POOL_BULK_FAULT_IN_PROPNAME = "solr.fbspool.bulkFaultIn";
  public static final String POOL_SYNCHRONOUS_FAULT_IN_PROPNAME = "solr.fbspool.synchronousFaultIn";
  public static final String POOL_BACKING_FILE_PROPNAME = "solr.fbspool.file";
  public static final String POOL_FORCE_TRANSPARENT_HUGEPAGE = "solr.fbspool.offheap.thp";
  public static final String POOL_OFFHEAP_TARGET_MB_PROPNAME = "solr.fbspool.offheap.targetMB";
  public static final String POOL_ONHEAP_TARGET_MB_PROPNAME = "solr.fbspool.onheap.targetMB";
  public static final String POOL_PERCENT_FOR_POOLED_PROPNAME = "solr.fbspool.percentForPooled";
  public static final String POOL_DUMP_STATS_ON_TEST_PROPNAME = "solr.fbspool.dumpStatsOnTest";
  public static final String POOL_ALLOW_EXPLICIT_CLOSE_PROPNAME = "solr.fbspool.allowExplicitClose";

  /** 0 -> disable thp, 1 -> enable thp, -1 -> ergonomic default */
  private static final int OFFHEAP_THP =
      EnvUtils.getPropertyAsInteger(POOL_FORCE_TRANSPARENT_HUGEPAGE, -1);

  /** TODO: temporarily package-visible */
  static final boolean ALLOW_EXPLICIT_CLOSE =
      EnvUtils.getPropertyAsBool(POOL_ALLOW_EXPLICIT_CLOSE_PROPNAME, true);

  private static final String POOL_BACKING_FILE =
      EnvUtils.getProperty(POOL_BACKING_FILE_PROPNAME, "");

  private static final boolean BULK_FAULT_IN =
      EnvUtils.getPropertyAsBool(POOL_BULK_FAULT_IN_PROPNAME, false);

  private static final boolean SYNCHRONOUS_FAULT_IN =
      EnvUtils.getPropertyAsBool(POOL_SYNCHRONOUS_FAULT_IN_PROPNAME, true);

  private static final long TPS;
  private static final long TPS_OFFHEAP;

  private static final boolean STATIC_HUGEPAGES;

  public static boolean isEnabled() {
    return TPS > 0 || TPS_OFFHEAP > 0;
  }

  private static final Path HUGEPAGES_SYSFS_PATH =
      Path.of("/sys/kernel/mm/hugepages/hugepages-2048kB/");

  private static long getTargetPoolSize(String propName, long maxMemory, int divisor) {
    long defaultTargetPoolSize = maxMemory / divisor; // default to 1/<divisor> of heap
    int targetPoolSizeMB =
        EnvUtils.getPropertyAsInteger(propName, Math.toIntExact(defaultTargetPoolSize >> 20));
    if (targetPoolSizeMB == -1) {
      return defaultTargetPoolSize;
    } else {
      return ((long) targetPoolSizeMB) << 20;
    }
  }

  private final HeapCacheFbsModifier fallback;

  HeapCacheFbsModifier(boolean offheap) {
    this(
        false,
        TPS,
        offheap,
        TPS_OFFHEAP == 0 ? null : new HeapCacheFbsModifier(false, TPS_OFFHEAP, !offheap, null));
  }

  private static final boolean SINGLE_THREADED_REF_HANDLER =
      ReferenceHandler.PARALLEL_HEAD_FACTOR == 1 && !ALLOW_EXPLICIT_CLOSE;

  private HeapCacheFbsModifier(
      boolean unregister, long targetPoolSize, boolean offheap, HeapCacheFbsModifier fallback) {
    this.fallback = fallback;
    this.offheap = offheap;
    this.unregister = unregister;
    if (EnvUtils.getProperty("tests.seed") == null) {
      nBlocks = Math.toIntExact(targetPoolSize / BLOCK_SIZE_BYTES);
    } else {
      // grossly undersize, to ensure re-use in test context (hacky, ignores "MB")
      nBlocks = Math.max(1, Math.toIntExact(targetPoolSize >> 20));
    }
    if (log.isInfoEnabled()) {
      log.info(
          "offheap={}, n_blocks={}, pool_size_bytes={}",
          offheap,
          nBlocks,
          RamUsageEstimator.humanReadableUnits((long) nBlocks * BLOCK_SIZE_BYTES));
    }
    int numPartitions = ((nBlocks - 1) / MAX_BLOCKS_PER_PARTITION) + 1;
    pool = new ByteBufferStruct[nBlocks];
    int blockIdx = 0;
    if (offheap) {
      poolOffheap(nBlocks, numPartitions, blockIdx, pool);
    } else if (POOL_BACKING_FILE.isEmpty()) {
      poolOnheap(nBlocks, numPartitions, blockIdx, pool);
    } else {
      try {
        poolFileBacked(nBlocks, numPartitions, blockIdx, pool, this);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    top = new AtomicInteger(nBlocks);
    refHandler =
        new ReferenceHandler<>(
            ALLOW_EXPLICIT_CLOSE,
            (toRelease) -> {
              try {
                toRelease.closed.isClosed = true;
                totalClosedBatches.increment();
                if (SINGLE_THREADED_REF_HANDLER) {
                  release(toRelease.buf);
                } else {
                  releaseQueue.put(toRelease.buf);
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ThreadInterruptedException(e);
              }
            },
            this::postCollect,
            SINGLE_THREADED_REF_HANDLER
                ? null
                : () -> {
                  try {
                    releaseLoop();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new ThreadInterruptedException(e);
                  }
                });
  }

  private static final double SOFT_THROTTLE_THRESHOLD =
      (double) EnvUtils.getPropertyAsInteger("solr.fbspool.softThrottleThreshold", 75) / 100;
  private static final double THROTTLE_THRESHOLD =
      (double) EnvUtils.getPropertyAsInteger("solr.fbspool.throttleThreshold", 90) / 100;
  private static final double THROTTLE_THRESHOLD_DIFF = 1.0 - THROTTLE_THRESHOLD;
  private static final double SOFT_THROTTLE_THRESHOLD_DIFF =
      THROTTLE_THRESHOLD - SOFT_THROTTLE_THRESHOLD;
  private static final long SOFT_MIN_THROTTLE_MILLIS = 5;
  private static final long MIN_THROTTLE_MILLIS = 500;
  private static final long MAX_THROTTLE_MILLIS = 2000;
  private static final long MAX_VARIABLE_THROTTLE = MAX_THROTTLE_MILLIS - MIN_THROTTLE_MILLIS;
  private static final long MAX_VARIABLE_SOFT_THROTTLE =
      MIN_THROTTLE_MILLIS - SOFT_MIN_THROTTLE_MILLIS;

  private static final long CHECK_FREQUENCY_NANOS = TimeUnit.MILLISECONDS.toNanos(250);
  private static final AtomicLong LAST_CHECKED_CPU_LOAD = new AtomicLong();
  private static volatile double cachedCpuLoad;

  private void postCollect() throws InterruptedException {
    if (STATIC_HUGEPAGES) return;
    double cpuLoad = getCpuLoad();
    if (throttleIfNecessary(cpuLoad) > 0) {
      throttleCount.increment();
    } else if (SINGLE_THREADED_REF_HANDLER && softThrottleIfNecessary(cpuLoad) > 0) {
      softThrottleCount.increment();
    }
  }

  private static long softThrottleIfNecessary(double cpuLoad) throws InterruptedException {
    if (cpuLoad > SOFT_THROTTLE_THRESHOLD) {
      long sleepMillis =
          Math.min(
              MIN_THROTTLE_MILLIS,
              SOFT_MIN_THROTTLE_MILLIS
                  + (long)
                      (((cpuLoad - SOFT_THROTTLE_THRESHOLD) / SOFT_THROTTLE_THRESHOLD_DIFF)
                          * MAX_VARIABLE_SOFT_THROTTLE));
      Thread.sleep(sleepMillis);
      return sleepMillis;
    }
    return 0;
  }

  private static long throttleIfNecessary(double cpuLoad) throws InterruptedException {
    if (cpuLoad > THROTTLE_THRESHOLD) {
      long sleepMillis =
          Math.min(
              MAX_THROTTLE_MILLIS,
              MIN_THROTTLE_MILLIS
                  + (long)
                      (((cpuLoad - THROTTLE_THRESHOLD) / THROTTLE_THRESHOLD_DIFF)
                          * MAX_VARIABLE_THROTTLE));
      Thread.sleep(sleepMillis);
      return sleepMillis;
    }
    return 0;
  }

  private static double getCpuLoad() {
    long now = System.nanoTime();
    long lastChecked = LAST_CHECKED_CPU_LOAD.get();
    double cpuLoad;
    if (now - lastChecked < CHECK_FREQUENCY_NANOS) {
      cpuLoad = cachedCpuLoad;
    } else if (LAST_CHECKED_CPU_LOAD.compareAndSet(lastChecked, now)) {
      cpuLoad = OS_BEAN.getCpuLoad();
      if (!Double.isNaN(cpuLoad)) {
        cachedCpuLoad = cpuLoad;
      }
    } else {
      cpuLoad = cachedCpuLoad; // best-effort; just continue to use cached value
    }
    return cpuLoad;
  }

  private static final OperatingSystemMXBean OS_BEAN =
      (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

  private static void poolOnheap(
      int nBlocks, int numPartitions, int blockIdx, ByteBufferStruct[] pool) {
    for (int i = numPartitions - 1,
            partitionNumBlocks = ((nBlocks - 1) % MAX_BLOCKS_PER_PARTITION) + 1;
        i >= 0;
        i--) {
      int partitionSize = partitionNumBlocks * BLOCK_SIZE_BYTES;
      ByteBuffer partition = ByteBuffer.allocate(partitionSize).order(FixedBitSet.BYTE_ORDER);
      for (int j = 0; j < partitionNumBlocks; j++) {
        pool[blockIdx++] =
            new ByteBufferStruct(partition.slice(j * BLOCK_SIZE_BYTES, BLOCK_SIZE_BYTES));
      }
      partitionNumBlocks = MAX_BLOCKS_PER_PARTITION;
    }
  }

  private static Path poolFileBacked(
      int nBlocks, int numPartitions, int blockIdx, ByteBufferStruct[] pool, Object caller)
      throws IOException {
    long blockSizeBytesL = BLOCK_SIZE_BYTES;

    // truncate to 2M blocks
    long partitionMaxBytes = ((MAX_BLOCKS_PER_PARTITION * blockSizeBytesL) >> 21) << 21;

    int effectiveMaxBlocksPerPartition = Math.toIntExact(partitionMaxBytes / blockSizeBytesL);
    Path backingFile;
    String seed = EnvUtils.getProperty("tests.seed");
    if (seed == null) {
      // not in a test context
      backingFile = Path.of(POOL_BACKING_FILE);
    } else {
      // test context; resolve relative to tmpdir, and scoped to this object in order to
      // avoid conflicts (note: we'll ignore the possibility of hash collisions here)
      backingFile =
          Path.of(EnvUtils.getProperty("java.io.tmpdir"))
              .resolve(
                  POOL_BACKING_FILE
                      + "_"
                      + seed
                      + "_"
                      + Integer.toUnsignedString(System.identityHashCode(caller), 16));
    }
    try (FileChannel fc =
        FileChannel.open(
            backingFile,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE_NEW)) {
      fc.truncate(nBlocks * (long) BLOCK_SIZE_BYTES);
      for (int i = numPartitions - 1,
              partitionNumBlocks = ((nBlocks - 1) % effectiveMaxBlocksPerPartition) + 1;
          i >= 0;
          i--) {
        // NOTE: this is designed to be a single file per JVM, lasting the entire JVM lifetime,
        // so we don't need to worry about unmapping (as we do for MMapDirectory).
        ByteBuffer partition =
            fc.map(
                    FileChannel.MapMode.READ_WRITE,
                    i * partitionMaxBytes,
                    partitionNumBlocks * blockSizeBytesL)
                .order(FixedBitSet.BYTE_ORDER);
        for (int j = 0; j < partitionNumBlocks; j++) {
          pool[blockIdx++] =
              new ByteBufferStruct(partition.slice(j * BLOCK_SIZE_BYTES, BLOCK_SIZE_BYTES));
        }
        partitionNumBlocks = effectiveMaxBlocksPerPartition;
      }
    } finally {
      Files.delete(backingFile); // delete, but mapping will stay open
    }
    return backingFile;
  }

  private static final SolrMetricsContext DISABLED = new SolrMetricsContext(null, null, null);

  private SolrMetricsContext solrMetricsContext;

  /**
   * This object is inherently scoped to the JVM, but metrics are managed by the CoreContainer. In
   * most real-world use cases this is fine, but if there's more than one CoreContainer per JVM,
   * metrics can be initialized multiple times. In such cases, CoreContainer metric lifecycle may be
   * out of sync with {@link HeapCacheFbsModifier} life cycle, so we just bail and disable metrics
   * entirely if they are registered multiple times.
   */
  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    // sync on some arbitrary private object
    synchronized (top) {
      if (solrMetricsContext != null) {
        if (solrMetricsContext != DISABLED) {
          if (log.isInfoEnabled()) {
            log.info("double-init metrics; disabling", new RuntimeException("stack trace"));
          }
          try {
            SolrMetricProducer.super.close();
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          } finally {
            solrMetricsContext = DISABLED;
          }
        }
        return;
      }
      log.info("init metrics");
      solrMetricsContext = parentContext.getChildContext(this);
      Gauge<?> cacheMap =
          new MetricsMap(
              map -> {
                writeStats(this, map);
                if (fallback != null) {
                  map.put("fallback", (MapWriter) fmap -> writeStats(fallback, fmap));
                  assert fallback.fallback == null;
                }
              });
      getSolrMetricsContext().gauge(cacheMap, true, scope, "DOCSET");
    }
  }

  private static void writeStats(HeapCacheFbsModifier h, MapWriter.EntryWriter map)
      throws IOException {
    long totalClosedBatches = h.totalClosedBatches.sum();
    long explicitBatchCloseCount = h.refHandler.explicitCloseCount();
    long allocated = h.allocated.sum();
    long exhausted = h.exhausted.sum();
    int extant = h.top.get();
    int avail = extant < 0 ? ~extant : extant;
    map.put("offheap", h.offheap);
    map.put("outstandingRefCount", h.refHandler.getOutstandingSize());
    map.put("activeRefProcessingThreads", h.refHandler.activeThreadCount());
    map.put("allocatedBytes", allocated * BLOCK_SIZE_BYTES);
    map.put("exhaustedBytes", exhausted * BLOCK_SIZE_BYTES);
    map.put("allocatedRatio", (double) allocated / (allocated + exhausted));
    map.put("availableBytes", avail * BLOCK_SIZE_BYTES);
    map.put("availableBlockRatio", (double) avail / h.nBlocks);
    map.put(
        "totalBlockSize",
        RamUsageEstimator.humanReadableUnits((long) h.nBlocks * BLOCK_SIZE_BYTES));
    map.put("explicitBatchCloseCount", explicitBatchCloseCount);
    map.put("totalBatchCloseCount", totalClosedBatches);
    map.put("explicitBatchCloseRatio", (double) explicitBatchCloseCount / totalClosedBatches);
    map.put("throttleCount", h.throttleCount.sum());
    map.put("softThrottleCount", h.softThrottleCount.sum());
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    SolrMetricsContext ctx = solrMetricsContext;
    return ctx == DISABLED ? null : ctx;
  }

  /**
   * In order to verify that this codepath is active throughout tests, we provide this option to
   * dump pool stats to disk (temp dir) upon close. Update default value to {@code true} below, or
   * add this propname to {@code /gradle/testing/randomization.gradle} and set it to {@code true}
   * there.
   */
  private static final boolean DUMP_STATS_ON_TEST =
      EnvUtils.getPropertyAsBool(POOL_DUMP_STATS_ON_TEST_PROPNAME, false);

  @Override
  @SuppressWarnings("try")
  public void close() {
    if (unregister) {
      try {
        if (FixedBitSets.unregisterModifier(this, refHandler)) {
          try (Closeable c = SolrMetricProducer.super::close;
              fallback) {
            SolrMetricsContext ctx;
            String seed;
            if (DUMP_STATS_ON_TEST
                && (ctx = getSolrMetricsContext()) != null
                && (seed = EnvUtils.getProperty("tests.seed")) != null) {
              // test context, dump stats
              dumpStats(ctx, seed);
            }
          } finally {
            Arrays.fill(pool, null);
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    } else {
      try (refHandler;
          fallback) {
        SolrMetricProducer.super.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      } finally {
        Arrays.fill(pool, null);
      }
    }
  }

  private void dumpStats(SolrMetricsContext ctx, String seed) throws IOException {
    Map<String, Object> snapshot = ctx.getMetricsSnapshot();
    Object ct;
    if (((ct = snapshot.get("DOCSET.docsetCache.exhaustedCount")) != null
            && !Long.valueOf(0).equals(ct))
        || ((ct = snapshot.get("DOCSET.docsetCache.allocatedCount")) != null
            && !Long.valueOf(0).equals(ct))) {
      Path hcfmDumpDir = Path.of(EnvUtils.getProperty("java.io.tmpdir")).resolve("hcfm_" + seed);
      FileUtils.createDirectories(hcfmDumpDir);
      Path dumpfile =
          hcfmDumpDir.resolve(Integer.toUnsignedString(System.identityHashCode(this), 16));
      try (PrintStream out =
          new PrintStream(
              Channels.newOutputStream(
                  FileChannel.open(
                      dumpfile, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)))) {
        snapshot.forEach(
            (k, v) -> {
              out.println(k + ": " + v + " (" + (v == null ? null : v.getClass()) + ")");
            });
      }
    }
  }

  private final LongAdder allocated = new LongAdder();
  private final LongAdder exhausted = new LongAdder();
  private final LongAdder waste = new LongAdder();
  private final LongAdder totalClosedBatches = new LongAdder();
  private final LongAdder throttleCount = new LongAdder();
  private final LongAdder softThrottleCount = new LongAdder();

  @Override
  public ByteBufferStruct allocateBytes(int size, boolean withMemorySegment) {
    throw new UnsupportedOperationException();
  }

  private static final ByteBufferStruct[] EMPTY = new ByteBufferStruct[0];

  public int available() {
    int extant = this.top.get();
    return extant < 0 ? ~extant : extant;
  }

  public int favailable() {
    int extant = fallback == null ? 0 : fallback.top.get();
    return extant < 0 ? ~extant : extant;
  }

  @Override
  public ByteBufferStruct[] allocateBytesArr(
      int numBytes, Object sentinel, boolean withMemorySegment) {
    if (numBytes == 0) {
      return EMPTY;
    }
    int totalPartitionCount = ((numBytes - 1) >> BYTE_SHIFT) + 1;
    int adjustedPartitionCount;
    int adjust;
    int lastLen = ((numBytes - 1) & BYTE_MASK) + 1;
    if (lastLen < MIN_BYTES_FOR_POOLED) {
      adjustedPartitionCount = totalPartitionCount - 1;
      adjust = 1;
    } else {
      adjustedPartitionCount = totalPartitionCount;
      adjust = 0;
    }
    if (adjustedPartitionCount == 0) {
      return new ByteBufferStruct[] {
        new ByteBufferStruct(
            ByteBuffer.allocate(numBytes).order(FixedBitSet.BYTE_ORDER), withMemorySegment)
      };
    }
    for (int avail = this.top.get(); ; ) {
      if (avail <= 0) {
        // fallback to main heap allocation

        // either exhausted (`avail == 0`), or in the process of being updated with
        // reclaimed blocks. In the former case, we obviously want fallback allocation;
        // but we also fallback in the latter case, because we are strictly opportunistic
        // in the hot/allocation path. We _never_ want to block here (which would potentially
        // wait for the thread scheduler to arbitrarily wake up the reclaim thread, etc...)
        if (fallback == null) {
          return allocateBytesArr(-1, 0, numBytes, null, withMemorySegment, adjust);
        } else {
          return fallback.allocateBytesArr(numBytes, sentinel, withMemorySegment);
        }
      }
      int tryReserve = Math.min(avail, adjustedPartitionCount);
      int witness = this.top.compareAndExchange(avail, avail - tryReserve);
      if (witness == avail) {
        // we have a batch reservation; now actually allocate
        return allocateBytesArr(
            avail - 1, tryReserve, numBytes, sentinel, withMemorySegment, adjust);
      } else {
        avail = witness;
      }
    }
  }

  private final LongAdder collected = new LongAdder();

  private static final VarHandle H = MethodHandles.arrayElementVarHandle(ByteBufferStruct[].class);

  private void releaseLoop() throws InterruptedException {
    for (; ; ) {
      ByteBufferStruct[] toRelease = releaseQueue.take();
      release(toRelease);
      if (!STATIC_HUGEPAGES) {
        // NOTE: don't increment `throttleCount` here, it would double-count with RefQueue
        // processing threads, which keeps count consistent.
        double cpuLoad = getCpuLoad();
        if (throttleIfNecessary(cpuLoad) == 0 && softThrottleIfNecessary(cpuLoad) > 0) {
          softThrottleCount.increment();
        }
      }
    }
  }

  private void release(ByteBufferStruct[] toRelease) {
    for (ByteBufferStruct bb : toRelease) {
      int bufSize = bb.buf.remaining();
      if (bufSize < MAX_BYTES) {
        waste.add(bufSize - MAX_BYTES);
      }
      if (offheap) {
        // NOTE: for off-heap we only need to zero out the buffer if bulk-faultin is
        // disabled OR doesn't call MADV_POPULATE_WRITE (which would zero out the buffer
        // upon acquire).
        if (!BULK_FAULT_IN || MADV_BULK_FAULTIN != MADV_POPULATE_WRITE) {
          clear(bb);
        } else {
          bb.buf.clear();
        }
        madviseRelease(bb);
      } else if (STATIC_HUGEPAGES) {
        bb.buf.clear();
        if (!madvise(bb, MADV_DONTNEED) || !madvise(bb, MADV_POPULATE_WRITE)) {
          // if DONTNEED failed, pages still hold dirty data and must be explicitly zeroed;
          // if POPULATE_WRITE failed (e.g., hugetlb pool temporarily exhausted by external
          // process), fall back to explicit zeroing to keep fault cost off the user thread
          clear(bb);
        }
      } else {
        // on-heap buffers never get zeroed out at the OS level, so we have to do it ourselves.
        clear(bb);
      }
    }
    int newTop;
    for (int top = this.top.get(); ; ) {
      newTop = top + toRelease.length;
      int witness = this.top.compareAndExchange(top, ~newTop);
      if (witness == top) {
        break;
      }
      top = witness;
    }
    int idx = newTop;
    for (ByteBufferStruct bb : toRelease) {
      // pool[destOff++ & POOL_SIZE_MASK] = bb;
      idx--;
      while (!H.compareAndSet(pool, idx, null, bb)) {
        // wait for consumer thread(s) to catch up
        Thread.yield();
      }
    }
    if (!this.top.compareAndSet(~newTop, newTop)) {
      // single-threaded producer, so this should literally never happen.
      throw new IllegalStateException();
    }
    collected.add(toRelease.length);
  }

  public static void clear(ByteBufferStruct bb) {
    // auto-vectorized; this is fast.
    ByteBuffer buf = bb.buf;
    for (int i = 0, lim = buf.remaining(); i < lim; i++) {
      buf.put(i, (byte) 0);
    }
    buf.clear();
  }

  @Override
  public FixedBitSet.Modifier partitioned(int bitShift) {
    assert bitShift == BitDocSet.BIT_SHIFT;
    return this;
  }

  private static final int BYTE_SHIFT = BitDocSet.BIT_SHIFT - 3;
  private static final int MAX_BYTES = 1 << BYTE_SHIFT;
  private static final int BYTE_MASK = MAX_BYTES - 1;

  private static final int MIN_BYTES_FOR_POOLED =
      Math.toIntExact(
          ((long) BLOCK_SIZE_BYTES
                  * EnvUtils.getPropertyAsInteger(POOL_PERCENT_FOR_POOLED_PROPNAME, 100))
              / 100);

  private ByteBufferStruct[] allocateBytesArr(
      int top,
      int pooledReserved,
      int numBytes,
      Object sentinel,
      boolean withMemorySegment,
      int adjust) {
    int lastIdx = (numBytes - 1) >> BYTE_SHIFT;
    ByteBufferStruct[] ret = new ByteBufferStruct[lastIdx + 1];
    int i = 0;
    int lastLen = ((numBytes - 1) & BYTE_MASK) + 1;
    if (i < pooledReserved) {
      for (int fullPooledLim = Math.min(pooledReserved - 1, lastIdx); i < fullPooledLim; i++) {
        ret[i] = initBuf(top--);
      }
      ByteBufferStruct lastPooled = initBuf(top);
      ret[i] = lastPooled;
      if (i++ == lastIdx && lastLen < MAX_BYTES) {
        lastPooled.buf.limit(lastLen);
        waste.add(MAX_BYTES - lastLen);
      }
      ByteBufferStruct[] pooled;
      if (i == ret.length) {
        pooled = ret;
      } else {
        // if any are pooled, we register the pooled ones for tracking
        pooled = new ByteBufferStruct[pooledReserved];
        System.arraycopy(ret, 0, pooled, 0, pooledReserved);
        exhausted.add(ret.length - pooledReserved - adjust);
      }
      State closed;
      if (sentinel instanceof SentinelPacket) {
        SentinelPacket sp = (SentinelPacket) sentinel;
        closed = sp.closed;
        sentinel = sp.sentinel;
      } else {
        closed = new State(); // dummy
      }
      Closeable ref = refHandler.add(sentinel, new RefHandlerPacket(pooled, closed));
      if (sentinel instanceof Closeable[]) {
        ((Closeable[]) sentinel)[0] = ref;
      }
      allocated.add(pooledReserved);
    } else {
      exhausted.add(ret.length - adjust);
    }
    for (; i < lastIdx; i++) {
      // full unpooled
      ret[i] =
          new ByteBufferStruct(
              ByteBuffer.allocate(MAX_BYTES).order(FixedBitSet.BYTE_ORDER), withMemorySegment);
    }
    if (i == lastIdx) {
      // last idx is unpooled
      ret[i] =
          new ByteBufferStruct(
              ByteBuffer.allocate(lastLen).order(FixedBitSet.BYTE_ORDER), withMemorySegment);
    }
    return ret;
  }

  public static final class SentinelPacket {
    private final Object sentinel;
    private final State closed;

    public SentinelPacket(Object sentinel, State closed) {
      this.sentinel = sentinel;
      this.closed = closed;
    }
  }

  private ByteBufferStruct initBuf(int idx) {
    // ByteBuffer ret = pool[head & POOL_SIZE_MASK].clear().limit(size);
    ByteBufferStruct ret = (ByteBufferStruct) H.getAndSetAcquire(pool, idx, null);
    if (BULK_FAULT_IN && offheap) {
      madvise(ret, MADV_BULK_FAULTIN); // bulk fault in if necessary
    }
    return ret;
  }

  public long exhaustedCount() {
    return exhausted.sum();
  }

  public long allocatedCount() {
    return allocated.sum();
  }

  public long collectedCount() {
    return collected.sum();
  }

  public long outstandingCount() {
    return refHandler.getOutstandingSize();
  }

  public int activeThreadCount() {
    return refHandler.activeThreadCount();
  }

  public long fexhaustedCount() {
    return fallback == null ? 0 : fallback.exhausted.sum();
  }

  public long fallocatedCount() {
    return fallback == null ? 0 : fallback.allocated.sum();
  }

  public long fcollectedCount() {
    return fallback == null ? 0 : fallback.collected.sum();
  }

  public long foutstandingCount() {
    return fallback == null ? 0 : fallback.refHandler.getOutstandingSize();
  }

  public int factiveThreadCount() {
    return fallback == null ? 0 : fallback.refHandler.activeThreadCount();
  }

  private static final int MADV_FREE = 8;
  private static final int MADV_DONTNEED = 4;
  private static final int MADV_WILLNEED = 3;
  private static final int MADV_POPULATE_WRITE = 23;

  private static final boolean SUPPORT_MADV;
  private static final boolean SUPPORT_MADV_POPULATE_WRITE;

  static {
    ByteBuffer bb =
        ByteBuffer.allocateDirect((MIN_BLOCK_SIZE << 1) - 1).alignedSlice(MIN_BLOCK_SIZE);
    ByteBufferStruct bbs = new ByteBufferStruct(bb);
    boolean support;
    try {
      support = FixedBitSet.madvise(bbs.m, MADV_DONTNEED); // try a standard advice
    } catch (Throwable e) {
      log.warn("disabling madvise", e);
      support = false;
    }
    SUPPORT_MADV = support;
    log.info("support for madvise(): {}", SUPPORT_MADV);
    if (SUPPORT_MADV) {
      try {
        SUPPORT_MADV_POPULATE_WRITE = FixedBitSet.madvise(bbs.m, MADV_POPULATE_WRITE);
      } catch (Throwable e) {
        throw new AssertionError(e);
      }
      if (SUPPORT_MADV_POPULATE_WRITE) {
        log.info("enabled support for MADV_POPULATE_WRITE");
      } else {
        log.warn("disabling support for MADV_POPULATE_WRITE");
      }
    } else {
      SUPPORT_MADV_POPULATE_WRITE = false;
    }
  }

  private static final int MADV_BULK_FAULTIN =
      (SUPPORT_MADV_POPULATE_WRITE && SYNCHRONOUS_FAULT_IN) ? MADV_POPULATE_WRITE : MADV_WILLNEED;

  static {
    long maxMemory = Runtime.getRuntime().maxMemory();

    // default to 1/16 of heap
    long onHeapTarget = getTargetPoolSize(POOL_ONHEAP_TARGET_MB_PROPNAME, maxMemory, 16);
    if (POOL_BACKING_FILE.isEmpty() || EnvUtils.getProperty("tests.seed") != null) {
      TPS = onHeapTarget;
      STATIC_HUGEPAGES = false;
    } else {
      try {
        if (!"hugetlbfs"
            .equals(Files.getFileStore(Path.of(POOL_BACKING_FILE).getParent()).type())) {
          TPS = onHeapTarget;
          STATIC_HUGEPAGES = false;
        } else {
          // if hugetlbfs, then just use all the remaining allocated hugepages
          Path freePath = HUGEPAGES_SYSFS_PATH.resolve("free_hugepages");
          Path resvPath = HUGEPAGES_SYSFS_PATH.resolve("resv_hugepages");
          long free = Long.parseLong(Files.readString(freePath).trim());
          long resv = Long.parseLong(Files.readString(resvPath).trim());
          long availableHugePages = free - resv;
          TPS = availableHugePages << 21; // 2M per hugepage
          STATIC_HUGEPAGES = BLOCK_SIZE_BYTES == ALIGN_SIZE && SUPPORT_MADV_POPULATE_WRITE;
          log.info("STATIC_HUGEPAGES={}", STATIC_HUGEPAGES);
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    // default to 1/4 of heap
    TPS_OFFHEAP = getTargetPoolSize(POOL_OFFHEAP_TARGET_MB_PROPNAME, maxMemory, 4);

    if (log.isInfoEnabled()) {
      log.info("block_size={}", RamUsageEstimator.humanReadableUnits(BLOCK_SIZE_BYTES));
    }
  }

  private static final int MADV_NOHUGEPAGE = 15;
  private static final int MADV_HUGEPAGE = 14;

  private static void madviseRelease(ByteBufferStruct bb) {
    madvise(bb, MADV_FREE);
  }

  @SuppressWarnings("preview")
  private static boolean madvise(ByteBufferStruct bb, int advice) {
    if (SUPPORT_MADV && bb.m != null) {
      try {
        // 2. Execute the call directly on the memory segment
        if (!FixedBitSet.madvise(bb.m, advice)) {
          log.warn("madvise {} failed", advice);
          return false;
        }
      } catch (Throwable t) {
        log.error("exception calling madvise {}", advice, t);
        return false;
      }
    }
    return true;
  }

  private static void poolOffheap(
      int nBlocks, int numPartitions, int blockIdx, ByteBufferStruct[] pool) {
    // truncate to 2M blocks
    int partitionMaxBytes = ((MAX_BLOCKS_PER_PARTITION * BLOCK_SIZE_BYTES) >> 21) << 21;

    int effectiveMaxBlocksPerPartition = partitionMaxBytes / BLOCK_SIZE_BYTES;

    final boolean doTHP;
    switch (OFFHEAP_THP) {
      case 0:
        doTHP = false;
        break;
      case 1:
        doTHP = true;
        break;
      case -1:
        doTHP = BLOCK_SIZE_BYTES == ALIGN_SIZE;
        break;
      default:
        throw new IllegalArgumentException(
            POOL_FORCE_TRANSPARENT_HUGEPAGE + "must be one of [-1, 0, 1]; found " + OFFHEAP_THP);
    }

    for (int i = numPartitions - 1,
            partitionNumBlocks = ((nBlocks - 1) % effectiveMaxBlocksPerPartition) + 1;
        i >= 0;
        i--) {
      int partitionSize = partitionNumBlocks * BLOCK_SIZE_BYTES;
      ByteBuffer partition =
          ByteBuffer.allocateDirect(Math.max(ALIGN_ALLOC_MINSIZE, partitionSize + ALIGN_OVERHEAD))
              .alignedSlice(ALIGN_SIZE)
              .order(FixedBitSet.BYTE_ORDER);

      if (doTHP) {
        madvise(new ByteBufferStruct(partition), MADV_HUGEPAGE);
      } else {
        madvise(new ByteBufferStruct(partition), MADV_NOHUGEPAGE);
      }

      // NOTE: below, `MADV_POPULATE_WRITE` and `MADV_DONTNEED` in smaller chunks to avoid blowing
      // out memory (which would happen if we did this in larger chunks). It's important to
      // pre-allocate the pagetable entries because we don't want synchronous callers to incur that
      // hit a little at a time as the pool "warms up".
      for (int j = 0; j < partitionNumBlocks; j++) {
        ByteBuffer block = partition.slice(j * BLOCK_SIZE_BYTES, BLOCK_SIZE_BYTES);
        ByteBufferStruct bbs = new ByteBufferStruct(block);
        if (SUPPORT_MADV_POPULATE_WRITE) {
          madvise(bbs, MADV_POPULATE_WRITE); // pre-build pagetable entries
        } else {
          // touch every block manually
          madvise(bbs, MADV_WILLNEED);
          for (int k = 0, lim = block.remaining(); k < lim; k += MIN_BLOCK_SIZE) {
            block.get(k);
          }
        }
        madvise(bbs, MADV_DONTNEED); // force release of physical memory
        pool[blockIdx++] = bbs;
      }

      partitionNumBlocks = effectiveMaxBlocksPerPartition;
    }
  }
}
