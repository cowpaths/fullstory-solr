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
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
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
      return FixedBitSets.registerModifier(HeapCacheFbsModifier::new);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int BLOCK_SIZE_BYTES = SortedIntDocSet.MAX_ARR_SIZE << 2;
  private static final int MAX_BLOCKS_PER_PARTITION = Integer.MAX_VALUE / BLOCK_SIZE_BYTES;
  private final int nBlocks;
  private static final long TAIL_MASK = -1L >>> Integer.SIZE;

  private static final int ALIGN_SIZE = 4096; // 4k
  private static final int ALIGN_OVERHEAD = ALIGN_SIZE - 1; // 4k - 1
  private static final int ALIGN_ALLOC_MINSIZE = ALIGN_SIZE + ALIGN_OVERHEAD; // 4k - 1

  // dummy, for efficiently clearing buffers
  private static final ByteBuffer FRESH =
      ByteBuffer.allocateDirect(Math.max(ALIGN_ALLOC_MINSIZE, BLOCK_SIZE_BYTES + ALIGN_OVERHEAD))
          .alignedSlice(ALIGN_SIZE)
          .order(FixedBitSet.BYTE_ORDER);

  private final boolean unregister;
  private final ByteBuffer[] pool;

  private final AtomicInteger top;
  private final BlockingQueue<ByteBuffer[]> releaseQueue = new ArrayBlockingQueue<>(1024, false);

  private final ReferenceHandler<ByteBuffer[]> refHandler;

  public static final String POOL_OFFHEAP_TARGET_MB_PROPNAME = "solr.fbspool.offheap.targetMB";
  public static final String POOL_ONHEAP_TARGET_MB_PROPNAME = "solr.fbspool.onheap.targetMB";
  public static final String POOL_ALWAYS_ONE_UNPOOLED_PROPNAME = "solr.fbspool.alwaysOneUnpooled";
  public static final String POOL_DUMP_STATS_ON_TEST_PROPNAME = "solr.fbspool.dumpStatsOnTest";

  private static final long TPS;
  private static final long TPS_OFFHEAP;

  static {
    long maxMemory = Runtime.getRuntime().maxMemory();
    long defaultTargetPoolSize = maxMemory / 16; // default to 1/16 of heap
    long defaultOffheapTargetPoolSize = maxMemory / 4; // default to 1/4 of heap

    int targetPoolSizeMB =
        EnvUtils.getPropertyAsInteger(
            POOL_ONHEAP_TARGET_MB_PROPNAME, Math.toIntExact(defaultTargetPoolSize >> 20));
    int targetOffheapPoolSizeMB =
        EnvUtils.getPropertyAsInteger(
            POOL_OFFHEAP_TARGET_MB_PROPNAME, Math.toIntExact(defaultOffheapTargetPoolSize >> 20));
    if (targetPoolSizeMB == -1) {
      TPS = defaultTargetPoolSize;
    } else {
      TPS = ((long) targetPoolSizeMB) << 20;
    }
    if (targetPoolSizeMB == -1) {
      TPS_OFFHEAP = defaultOffheapTargetPoolSize;
    } else {
      TPS_OFFHEAP = ((long) targetOffheapPoolSizeMB) << 20;
    }
    if (log.isInfoEnabled()) {
      log.info("block_size={}", RamUsageEstimator.humanReadableUnits(BLOCK_SIZE_BYTES));
    }
  }

  private final HeapCacheFbsModifier fallback;

  private HeapCacheFbsModifier() {
    this(true, TPS, false, new HeapCacheFbsModifier(false, TPS_OFFHEAP, true, null));
  }

  HeapCacheFbsModifier(boolean offheap) {
    this(false, TPS, offheap, new HeapCacheFbsModifier(false, TPS_OFFHEAP, !offheap, null));
  }

  HeapCacheFbsModifier(
      boolean unregister, long targetPoolSize, boolean offheap, HeapCacheFbsModifier fallback) {
    this.fallback = fallback;
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
    pool = new ByteBuffer[nBlocks];
    int blockIdx = 0;
    if (offheap) {
      poolOffheap(nBlocks, numPartitions, blockIdx, pool);
    } else {
      poolOnheap(nBlocks, numPartitions, blockIdx, pool);
    }
    top = new AtomicInteger(nBlocks);
    refHandler =
        new ReferenceHandler<>(
            (toRelease) -> {
              try {
                releaseQueue.put(toRelease);
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            },
            () -> {
              try {
                releaseLoop();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
  }

  private static void poolOnheap(int nBlocks, int numPartitions, int blockIdx, ByteBuffer[] pool) {
    for (int i = numPartitions - 1, partitionNumBlocks = ((nBlocks - 1) / numPartitions) + 1;
        i >= 0;
        i--) {
      int partitionSize = partitionNumBlocks * BLOCK_SIZE_BYTES;
      ByteBuffer partition =
          ByteBuffer.allocate(partitionSize + ALIGN_OVERHEAD).order(FixedBitSet.BYTE_ORDER);
      for (int j = 0; j < partitionNumBlocks; j++) {
        pool[blockIdx++] = partition.slice(j * BLOCK_SIZE_BYTES, BLOCK_SIZE_BYTES);
      }
      partitionNumBlocks = MAX_BLOCKS_PER_PARTITION;
    }
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
                long allocated = this.allocated.sum();
                long exhausted = this.exhausted.sum();
                int extant = this.top.get();
                int avail = extant < 0 ? ~extant : extant;
                map.put("outstandingRefCount", refHandler.getOutstandingSize());
                map.put("activeRefProcessingThreads", refHandler.activeThreadCount());
                map.put("allocatedCount", allocated);
                map.put("exhaustedCount", exhausted);
                map.put("allocatedRatio", (double) allocated / (allocated + exhausted));
                map.put("availableBlockCount", avail);
                map.put("availableBlockRatio", (double) avail / nBlocks);
              });
      getSolrMetricsContext().gauge(cacheMap, true, scope, "DOCSET");
    }
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

  @Override
  public ByteBuffer allocateBytes(int size) {
    throw new UnsupportedOperationException();
  }

  private static final ByteBuffer[] EMPTY = new ByteBuffer[0];

  public int available() {
    int extant = this.top.get();
    return extant < 0 ? ~extant : extant;
  }

  @Override
  public ByteBuffer[] allocateBytesArr(int numBytes, Object sentinel) {
    if (numBytes == 0) {
      return EMPTY;
    }
    int adjustedPartitionCount = ((numBytes - 1) >> BYTE_SHIFT) + 1 - ADJUST;
    if (adjustedPartitionCount == 0) {
      return new ByteBuffer[] {ByteBuffer.allocate(numBytes).order(FixedBitSet.BYTE_ORDER)};
    }
    for (int avail = this.top.get(); ; ) {
      if (avail == 0) {
        // exhausted; fallback to main heap allocation
        if (fallback == null) {
          return allocateBytesArr(-1, 0, numBytes, null);
        } else {
          return fallback.allocateBytesArr(numBytes, sentinel);
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
        return allocateBytesArr(avail - 1, tryReserve, numBytes, sentinel);
      } else {
        avail = witness;
      }
    }
  }

  private final LongAdder collected = new LongAdder();

  private static final VarHandle H = MethodHandles.arrayElementVarHandle(ByteBuffer[].class);

  private void releaseLoop() throws InterruptedException {
    for (; ; ) {
      ByteBuffer[] toRelease = releaseQueue.take();
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
      for (ByteBuffer bb : toRelease) {
        // pool[destOff++ & POOL_SIZE_MASK] = bb;
        idx--;
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

  private ByteBuffer[] allocateBytesArr(
      int top, int pooledReserved, int numBytes, Object sentinel) {
    int lastIdx = (numBytes - 1) >> BYTE_SHIFT;
    ByteBuffer[] ret = new ByteBuffer[lastIdx + 1];
    int i = 0;
    for (int fullPooledLim = Math.min(pooledReserved - 1, lastIdx); i < fullPooledLim; i++) {
      ret[i] = initBuf(top--, MAX_BYTES);
    }
    int lastLen = ((numBytes - 1) & BYTE_MASK) + 1;
    if (i < pooledReserved) {
      ret[i] = initBuf(top, i++ == lastIdx ? lastLen : MAX_BYTES);
      ByteBuffer[] pooled;
      if (i == ret.length) {
        pooled = ret;
      } else {
        // if any are pooled, we register the pooled ones for tracking
        pooled = new ByteBuffer[pooledReserved];
        System.arraycopy(ret, 0, pooled, 0, pooledReserved);
        exhausted.add(ret.length - pooledReserved - ADJUST);
      }
      Closeable ref = refHandler.add(sentinel, pooled);
      if (sentinel instanceof Closeable[]) {
        ((Closeable[]) sentinel)[0] = ref;
      }
      allocated.add(pooledReserved);
    } else {
      exhausted.add(ret.length - ADJUST);
    }
    for (; i < lastIdx; i++) {
      // full unpooled
      ret[i] = ByteBuffer.allocate(MAX_BYTES).order(FixedBitSet.BYTE_ORDER);
    }
    if (i == lastIdx) {
      // last idx is unpooled
      ret[i] = ByteBuffer.allocate(lastLen).order(FixedBitSet.BYTE_ORDER);
    }
    return ret;
  }

  private ByteBuffer initBuf(int idx, int size) {
    // ByteBuffer ret = pool[head & POOL_SIZE_MASK].clear().limit(size);
    ByteBuffer ret = (ByteBuffer) H.getAndSetAcquire(pool, idx, null);
    ret.clear().limit(size);
    // zero it out
    return ret.put(FRESH.slice(0, size)).flip();
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

  private static final int MADV_PAGEOUT = 21;

  @SuppressWarnings("preview")
  private static void poolOffheap(int nBlocks, int numPartitions, int blockIdx, ByteBuffer[] pool) {
    // 1. Find the C 'madvise' function
    Linker linker = Linker.nativeLinker();
    SymbolLookup libc = linker.defaultLookup();

    MethodHandle madvise =
        linker.downcallHandle(
            libc.find("madvise").get(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_INT));

    for (int i = numPartitions - 1, partitionNumBlocks = ((nBlocks - 1) / numPartitions) + 1;
        i >= 0;
        i--) {
      int partitionSize = partitionNumBlocks * BLOCK_SIZE_BYTES;
      ByteBuffer partition =
          ByteBuffer.allocateDirect(Math.max(ALIGN_ALLOC_MINSIZE, partitionSize + ALIGN_OVERHEAD))
              .alignedSlice(ALIGN_SIZE)
              .order(FixedBitSet.BYTE_ORDER);
      for (int j = 0; j < partitionNumBlocks; j++) {
        pool[blockIdx++] = partition.slice(j * BLOCK_SIZE_BYTES, BLOCK_SIZE_BYTES);
      }
      try {
        MemorySegment segment = MemorySegment.ofBuffer(partition);
        // 2. Execute the call directly on the memory segment
        int result = (int) madvise.invokeExact(segment, segment.byteSize(), MADV_PAGEOUT);
        if (result != 0) log.warn("madvise failed");
      } catch (Throwable t) {
        log.error("error forcing to swap", t);
      }
      partitionNumBlocks = MAX_BLOCKS_PER_PARTITION;
    }
  }
}
