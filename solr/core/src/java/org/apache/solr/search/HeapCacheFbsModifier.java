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
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
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
import java.util.concurrent.atomic.AtomicInteger;
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

  private static final int BLOCK_SIZE_BYTES = SortedIntDocSet.MAX_ARR_SIZE << 2;
  private static final int MAX_BLOCKS_PER_PARTITION = Integer.MAX_VALUE / BLOCK_SIZE_BYTES;
  private final int nBlocks;
  private final boolean offheap;

  private static final int MIN_BLOCK_SIZE = 4096; // 4k

  private static final int ALIGN_SIZE = 1 << 21; // 2m
  private static final int ALIGN_OVERHEAD = ALIGN_SIZE - 1; // 2m - 1
  private static final int ALIGN_ALLOC_MINSIZE = ALIGN_SIZE + ALIGN_OVERHEAD;

  // dummy, for efficiently clearing buffers
  private static final ByteBuffer FRESH =
      ByteBuffer.allocateDirect(Math.max(ALIGN_ALLOC_MINSIZE, BLOCK_SIZE_BYTES + ALIGN_OVERHEAD))
          .alignedSlice(ALIGN_SIZE)
          .order(FixedBitSet.BYTE_ORDER);

  private final boolean unregister;
  private final ByteBufferStruct[] pool;

  private final AtomicInteger top;
  private final BlockingQueue<ByteBufferStruct[]> releaseQueue =
      new ArrayBlockingQueue<>(1024, false);

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
  public static final String POOL_ALWAYS_ONE_UNPOOLED_PROPNAME = "solr.fbspool.alwaysOneUnpooled";
  public static final String POOL_DUMP_STATS_ON_TEST_PROPNAME = "solr.fbspool.dumpStatsOnTest";
  public static final String POOL_ALLOW_EXPLICIT_CLOSE_PROPNAME = "solr.fbspool.allowExplicitClose";

  /** 0 -> disable thp, 1 -> enable thp, -1 -> ergonomic default */
  private static final int OFFHEAP_THP =
      EnvUtils.getPropertyAsInteger(POOL_FORCE_TRANSPARENT_HUGEPAGE, -1);

  private static final boolean ALLOW_EXPLICIT_CLOSE =
      EnvUtils.getPropertyAsBool(POOL_ALLOW_EXPLICIT_CLOSE_PROPNAME, true);

  private static final String POOL_BACKING_FILE =
      EnvUtils.getProperty(POOL_BACKING_FILE_PROPNAME, "");

  private static final boolean BULK_FAULT_IN =
      EnvUtils.getPropertyAsBool(POOL_BULK_FAULT_IN_PROPNAME, false);

  private static final boolean SYNCHRONOUS_FAULT_IN =
      EnvUtils.getPropertyAsBool(POOL_SYNCHRONOUS_FAULT_IN_PROPNAME, true);

  private static final long TPS;
  private static final long TPS_OFFHEAP;

  public static boolean isEnabled() {
    return TPS > 0 || TPS_OFFHEAP > 0;
  }

  private static final Path HUGEPAGES_SYSFS_PATH =
      Path.of("/sys/kernel/mm/hugepages/hugepages-2048kB/");

  static {
    long maxMemory = Runtime.getRuntime().maxMemory();

    // default to 1/16 of heap
    long onHeapTarget = getTargetPoolSize(POOL_ONHEAP_TARGET_MB_PROPNAME, maxMemory, 16);
    if (POOL_BACKING_FILE.isEmpty() || EnvUtils.getProperty("tests.seed") != null) {
      TPS = onHeapTarget;
    } else {
      try {
        if (!"hugetlbfs"
            .equals(Files.getFileStore(Path.of(POOL_BACKING_FILE).getParent()).type())) {
          TPS = onHeapTarget;
        } else {
          // if hugetlbfs, then just use all the remaining allocated hugepages
          Path freePath = HUGEPAGES_SYSFS_PATH.resolve("free_hugepages");
          Path resvPath = HUGEPAGES_SYSFS_PATH.resolve("resv_hugepages");
          long free = Long.parseLong(Files.readString(freePath).trim());
          long resv = Long.parseLong(Files.readString(resvPath).trim());
          long availableHugePages = free - resv;
          TPS = availableHugePages << 21; // 2M per hugepage
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
    this(false, TPS, offheap, new HeapCacheFbsModifier(false, TPS_OFFHEAP, !offheap, null));
  }

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
          "n_blocks={}, pool_size_bytes={}",
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
                releaseQueue.put(toRelease.buf);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ThreadInterruptedException(e);
              }
            },
            () -> {
              try {
                releaseLoop();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ThreadInterruptedException(e);
              }
            });
  }

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
    map.put("allocatedCount", allocated);
    map.put("exhaustedCount", exhausted);
    map.put("allocatedRatio", (double) allocated / (allocated + exhausted));
    map.put("availableBlockCount", avail);
    map.put("availableBlockRatio", (double) avail / h.nBlocks);
    map.put(
        "totalBlockSize",
        RamUsageEstimator.humanReadableUnits((long) h.nBlocks * BLOCK_SIZE_BYTES));
    map.put("explicitBatchCloseCount", explicitBatchCloseCount);
    map.put("totalBatchCloseCount", totalClosedBatches);
    map.put("explicitBatchCloseRatio", (double) explicitBatchCloseCount / totalClosedBatches);
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
  private final LongAdder totalClosedBatches = new LongAdder();

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
    int adjustedPartitionCount = ((numBytes - 1) >> BYTE_SHIFT) + 1 - ADJUST;
    if (adjustedPartitionCount == 0) {
      return new ByteBufferStruct[] {
        new ByteBufferStruct(ByteBuffer.allocate(numBytes).order(FixedBitSet.BYTE_ORDER))
      };
    }
    for (int avail = this.top.get(); ; ) {
      if (avail == 0) {
        // exhausted; fallback to main heap allocation
        if (fallback == null) {
          return allocateBytesArr(-1, 0, numBytes, null, withMemorySegment);
        } else {
          return fallback.allocateBytesArr(numBytes, sentinel, withMemorySegment);
        }
      } else if (avail < 0) {
        Thread.yield(); // let producer thread complete
        avail = this.top.get();
        continue;
      }
      int tryReserve = Math.min(avail, adjustedPartitionCount);
      int witness = this.top.compareAndExchange(avail, avail - tryReserve);
      if (witness == avail) {
        // we have a batch reservation; now actually allocate
        return allocateBytesArr(avail - 1, tryReserve, numBytes, sentinel, withMemorySegment);
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

        if (offheap) {
          // NOTE: for off-heap we only need to zero out the buffer if bulk-faultin is
          // disabled OR doesn't call MADV_POPULATE_WRITE (which would zero out the buffer
          // upon acquire).
          if (!BULK_FAULT_IN || MADV_BULK_FAULTIN != MADV_POPULATE_WRITE) {
            bb.buf.put(FRESH.slice(0, bb.buf.remaining())).clear();
          }
          madviseRelease(bb.buf);
        } else {
          // on-heap buffers never get zeroed out at the OS level, so we have to do it ourselves.
          bb.buf.put(FRESH.slice(0, bb.buf.remaining())).clear();
        }
        while (!H.compareAndSet(pool, idx, null, bb)) {
          // wait for consumer thread(s) to catch up
          Thread.yield();
        }
      }
      collected.add(toRelease.length);
      if (!this.top.compareAndSet(~newTop, newTop)) {
        // single-threaded producer, so this should literally never happen.
        throw new IllegalStateException();
      }
      Thread.yield(); // background; try not to monopolize CPU
    }
  }

  @Override
  public FixedBitSet.Modifier partitioned(int bitShift) {
    assert bitShift == BitDocSet.BIT_SHIFT;
    return this;
  }

  private static final int BYTE_SHIFT = BitDocSet.BIT_SHIFT - 3;
  private static final int MAX_BYTES = 1 << BYTE_SHIFT;
  private static final int BYTE_MASK = MAX_BYTES - 1;

  /**
   * In order to provide some heap pressure, when this field is set to {@code 1}, every non-empty
   * allocation allocates at least one heap {@link ByteBuffer}. This ensures that there is no wasted
   * space (every pooled {@link ByteBuffer} that's used will be full), there is nominal heap
   * pressure to drive GC (and pool entry reclamation), and that there's a consistent dimorphism to
   * avoid JIT over-fitting.
   */
  private static final int ADJUST =
      EnvUtils.getPropertyAsBool(POOL_ALWAYS_ONE_UNPOOLED_PROPNAME, true) ? 1 : 0;

  private ByteBufferStruct[] allocateBytesArr(
      int top, int pooledReserved, int numBytes, Object sentinel, boolean withMemorySegment) {
    int lastIdx = (numBytes - 1) >> BYTE_SHIFT;
    ByteBufferStruct[] ret = new ByteBufferStruct[lastIdx + 1];
    int i = 0;
    for (int fullPooledLim = Math.min(pooledReserved - 1, lastIdx); i < fullPooledLim; i++) {
      ret[i] = initBuf(top--, MAX_BYTES);
    }
    int lastLen = ((numBytes - 1) & BYTE_MASK) + 1;
    if (i < pooledReserved) {
      ret[i] = initBuf(top, i++ == lastIdx ? lastLen : MAX_BYTES);
      ByteBufferStruct[] pooled;
      if (i == ret.length) {
        pooled = ret;
      } else {
        // if any are pooled, we register the pooled ones for tracking
        pooled = new ByteBufferStruct[pooledReserved];
        System.arraycopy(ret, 0, pooled, 0, pooledReserved);
        exhausted.add(ret.length - pooledReserved - ADJUST);
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
      exhausted.add(ret.length - ADJUST);
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

  private ByteBufferStruct initBuf(int idx, int size) {
    // ByteBuffer ret = pool[head & POOL_SIZE_MASK].clear().limit(size);
    ByteBufferStruct ret = (ByteBufferStruct) H.getAndSetAcquire(pool, idx, null);
    ret.buf.clear().limit(size);
    if (BULK_FAULT_IN && offheap) {
      madvise(ret.buf, MADV_BULK_FAULTIN); // bulk fault in if necessary
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

  private static final MethodHandle MADVISE_HANDLE = getMadviseHandle();

  private static final boolean SUPPORT_MADV_POPULATE_WRITE;

  static {
    ByteBuffer bb = ByteBuffer.allocateDirect(MIN_BLOCK_SIZE);
    SUPPORT_MADV_POPULATE_WRITE = madvise(bb, MADV_POPULATE_WRITE);
    log.warn("disabling support for MADV_POPULATE_WRITE");
  }

  private static final int MADV_BULK_FAULTIN =
      (SUPPORT_MADV_POPULATE_WRITE && SYNCHRONOUS_FAULT_IN) ? MADV_POPULATE_WRITE : MADV_WILLNEED;

  private static final int MADV_NOHUGEPAGE = 15;
  private static final int MADV_HUGEPAGE = 14;

  @SuppressWarnings("preview")
  private static MethodHandle getMadviseHandle() {
    Linker linker = Linker.nativeLinker();
    // Look up the symbol once during class loading
    SymbolLookup stdlib = linker.defaultLookup();

    MemorySegment madviseAddress =
        stdlib.find("madvise").orElseThrow(() -> new RuntimeException("madvise not found"));

    // Function signature: int madvise(void *addr, size_t length, int advice)
    FunctionDescriptor descriptor =
        FunctionDescriptor.of(
            ValueLayout.JAVA_INT, // Return type
            ValueLayout.ADDRESS, // addr
            ValueLayout.JAVA_LONG, // length (size_t)
            ValueLayout.JAVA_INT // advice
            );

    return linker.downcallHandle(madviseAddress, descriptor);
  }

  private static void madviseRelease(ByteBuffer bb) {
    madvise(bb, MADV_FREE);
  }

  @SuppressWarnings("preview")
  private static boolean madvise(ByteBuffer bb, int advice) {
    try {
      MemorySegment segment = MemorySegment.ofBuffer(bb);
      // 2. Execute the call directly on the memory segment
      int result = (int) MADVISE_HANDLE.invokeExact(segment, segment.byteSize(), advice);
      if (result == 0) {
        return true;
      } else {
        log.warn("madvise {} failed", advice);
      }
    } catch (Throwable t) {
      log.error("exception calling madvise {}", advice, t);
    }
    return false;
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
        madvise(partition, MADV_HUGEPAGE);
      } else {
        madvise(partition, MADV_NOHUGEPAGE);
      }

      // NOTE: below, `MADV_POPULATE_WRITE` and `MADV_DONTNEED` in smaller chunks to avoid blowing
      // out memory (which would happen if we did this in larger chunks). It's important to
      // pre-allocate the pagetable entries because we don't want synchronous callers to incur that
      // hit a little at a time as the pool "warms up".
      for (int j = 0; j < partitionNumBlocks; j++) {
        ByteBuffer block = partition.slice(j * BLOCK_SIZE_BYTES, BLOCK_SIZE_BYTES);
        if (SUPPORT_MADV_POPULATE_WRITE) {
          madvise(block, MADV_POPULATE_WRITE); // pre-build pagetable entries
        } else {
          // touch every block manually
          madvise(block, MADV_WILLNEED);
          for (int k = 0, lim = block.remaining(); k < lim; k += MIN_BLOCK_SIZE) {
            block.get(k);
          }
        }
        madvise(block, MADV_DONTNEED); // force release of physical memory
        pool[blockIdx++] = new ByteBufferStruct(block);
      }

      partitionNumBlocks = effectiveMaxBlocksPerPartition;
    }
  }
}
