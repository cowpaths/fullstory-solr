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
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
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
  private static final int MAX_BLOCKS_PER_PARTITION;
  static final int N_BLOCKS;
  private static final long TAIL_MASK = -1L >>> Integer.SIZE;
  private static final long HEAD_MASK = ~TAIL_MASK;
  private static final int POOL_ARR_SIZE;
  private static final int POOL_SIZE_MASK;

  // dummy, for efficiently clearing buffers
  private static final ByteBuffer FRESH =
      ByteBuffer.allocate(BLOCK_SIZE_BYTES).order(FixedBitSet.BYTE_ORDER);

  private final boolean unregister;
  private final ByteBuffer[] pool;
  private final Closeable cleanup;

  private final AtomicLong headAndTail;
  private final BlockingQueue<ByteBuffer[]> releaseQueue = new ArrayBlockingQueue<>(1024, false);

  private final ReferenceHandler<ByteBuffer[]> refHandler;

  public static final String POOL_OFFHEAP_PROPNAME = "solr.fbspool.offheap";
  public static final String POOL_TARGET_MB_PROPNAME = "solr.fbspool.targetMB";
  public static final String POOL_BACKING_FILE_PROPNAME = "solr.fbspool.file";
  public static final String POOL_ALWAYS_ONE_UNPOOLED_PROPNAME = "solr.fbspool.alwaysOneUnpooled";
  public static final String POOL_DUMP_STATS_ON_TEST_PROPNAME = "solr.fbspool.dumpStatsOnTest";

  private static final boolean POOL_OFFHEAP =
      EnvUtils.getPropertyAsBool(POOL_OFFHEAP_PROPNAME, true);

  private static final String POOL_BACKING_FILE =
      EnvUtils.getProperty(POOL_BACKING_FILE_PROPNAME, "");

  static {
    long maxMemory = Runtime.getRuntime().maxMemory();
    long defaultTargetPoolSize = maxMemory / 16; // default to 1/16 of heap

    // max of 1/2 of heap (off-heap), 1/4 of heap (on-heap), unlimited (off-heap file-backed)
    long maxPoolSize =
        POOL_BACKING_FILE.isEmpty() ? (maxMemory / (POOL_OFFHEAP ? 2 : 4)) : Long.MAX_VALUE;

    int targetPoolSizeMB =
        EnvUtils.getPropertyAsInteger(
            POOL_TARGET_MB_PROPNAME, Math.toIntExact(defaultTargetPoolSize >> 20));
    long targetPoolSizeSpec;
    if (targetPoolSizeMB == -1) {
      targetPoolSizeSpec = defaultTargetPoolSize;
    } else {
      targetPoolSizeSpec = ((long) targetPoolSizeMB) << 20;
    }
    long targetPoolSize = Math.min(maxPoolSize, targetPoolSizeSpec);
    if (EnvUtils.getProperty("tests.seed") == null) {
      N_BLOCKS = Math.toIntExact(targetPoolSize / BLOCK_SIZE_BYTES);
    } else {
      // grossly undersize, to ensure re-use in test context (hacky, ignores "MB")
      N_BLOCKS = Math.max(1, Math.toIntExact(targetPoolSizeSpec >> 20));
    }
    MAX_BLOCKS_PER_PARTITION = Integer.MAX_VALUE / BLOCK_SIZE_BYTES;
    // NOTE: we must oversize by _at least_ 2x, to avoid concurrency issues
    POOL_ARR_SIZE = Integer.highestOneBit(N_BLOCKS - 1) << 2;
    POOL_SIZE_MASK = POOL_ARR_SIZE - 1;
    if (log.isInfoEnabled()) {
      log.info(
          "block_size={}, n_blocks={}, pool_size_bytes={}",
          RamUsageEstimator.humanReadableUnits(BLOCK_SIZE_BYTES),
          N_BLOCKS,
          RamUsageEstimator.humanReadableUnits((long) N_BLOCKS * BLOCK_SIZE_BYTES));
    }
  }

  private HeapCacheFbsModifier() {
    this(true);
  }

  HeapCacheFbsModifier(boolean unregister) {
    this.unregister = unregister;
    int numPartitions = ((N_BLOCKS - 1) / MAX_BLOCKS_PER_PARTITION) + 1;
    pool = new ByteBuffer[POOL_ARR_SIZE];
    int blockIdx = 0;
    if (POOL_BACKING_FILE.isEmpty()) {
      poolAnonymous(numPartitions, blockIdx, pool);
      cleanup = null;
    } else {
      try {
        Path backing = poolFileBacked(numPartitions, blockIdx, pool, this);
        cleanup =
            () -> {
              IOUtils.deleteFilesIfExist(backing);
            };
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    headAndTail = new AtomicLong(N_BLOCKS);
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

  private static void poolAnonymous(int numPartitions, int blockIdx, ByteBuffer[] pool) {
    for (int i = numPartitions - 1, partitionNumBlocks = ((N_BLOCKS - 1) / numPartitions) + 1;
        i >= 0;
        i--) {
      int partitionSize = partitionNumBlocks * BLOCK_SIZE_BYTES;
      ByteBuffer partition =
          (POOL_OFFHEAP
                  ? ByteBuffer.allocateDirect(partitionSize)
                  : ByteBuffer.allocate(partitionSize))
              .order(FixedBitSet.BYTE_ORDER);
      for (int j = 0; j < partitionNumBlocks; j++) {
        pool[blockIdx++] = partition.slice(j * BLOCK_SIZE_BYTES, BLOCK_SIZE_BYTES);
      }
      partitionNumBlocks = MAX_BLOCKS_PER_PARTITION;
    }
  }

  private static Path poolFileBacked(
      int numPartitions, int blockIdx, ByteBuffer[] pool, Object caller) throws IOException {
    long blockSizeBytesL = BLOCK_SIZE_BYTES;
    long partitionMaxBytes = MAX_BLOCKS_PER_PARTITION * blockSizeBytesL;
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
            StandardOpenOption.CREATE)) {
      for (int i = numPartitions - 1, partitionNumBlocks = ((N_BLOCKS - 1) / numPartitions) + 1;
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
          pool[blockIdx++] = partition.slice(j * BLOCK_SIZE_BYTES, BLOCK_SIZE_BYTES);
        }
        partitionNumBlocks = MAX_BLOCKS_PER_PARTITION;
      }
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
    synchronized (headAndTail) {
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
                long extant = this.headAndTail.get();
                int head = (int) (extant >>> Integer.SIZE);
                final int tail = (int) extant;
                int avail = tail - head;
                map.put("outstandingRefCount", refHandler.getOutstandingSize());
                map.put("activeRefProcessingThreads", refHandler.activeThreadCount());
                map.put("allocatedCount", allocated);
                map.put("exhaustedCount", exhausted);
                map.put("allocatedRatio", (double) allocated / (allocated + exhausted));
                map.put("availableBlockCount", avail);
                map.put("availableBlockRatio", (double) avail / N_BLOCKS);
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
              cleanup) {
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
      try (refHandler) {
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
    long extant = this.headAndTail.get();
    int head = (int) (extant >>> Integer.SIZE);
    final int tail = (int) extant;
    return tail - head;
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
    for (long extant = this.headAndTail.get(); ; ) {
      int head = (int) (extant >>> Integer.SIZE);
      final int tail = (int) extant;
      int avail = tail - head;
      if (avail == 0) {
        // exhausted; fallback to main heap allocation
        return allocateBytesArr(-1, 0, numBytes, null);
      } else if (avail < 0) {
        throw new IllegalStateException();
      }
      int tryReserve = Math.min(avail, adjustedPartitionCount);
      long witness =
          this.headAndTail.compareAndExchange(
              extant, ((((long) head + tryReserve) << Integer.SIZE)) | (tail & TAIL_MASK));
      if (witness == extant) {
        // we have a batch reservation; now actually allocate
        return allocateBytesArr(head, tryReserve, numBytes, sentinel);
      } else {
        extant = witness;
      }
    }
  }

  private final LongAdder collected = new LongAdder();

  private void releaseLoop() throws InterruptedException {
    int destOff = N_BLOCKS;
    for (; ; ) {
      ByteBuffer[] toRelease = releaseQueue.take();
      for (ByteBuffer bb : toRelease) {
        pool[destOff++ & POOL_SIZE_MASK] = bb;
      }
      collected.add(toRelease.length);
      for (long extant = headAndTail.get(); ; ) {
        long witness =
            headAndTail.compareAndExchange(extant, (extant & HEAD_MASK) | (destOff & TAIL_MASK));
        if (witness == extant) {
          break;
        } else {
          extant = witness;
        }
      }
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
      int head, int pooledReserved, int numBytes, Object sentinel) {
    int lastIdx = (numBytes - 1) >> BYTE_SHIFT;
    ByteBuffer[] ret = new ByteBuffer[lastIdx + 1];
    int i = 0;
    for (int fullPooledLim = Math.min(pooledReserved - 1, lastIdx); i < fullPooledLim; i++) {
      ret[i] = initBuf(head++, MAX_BYTES);
    }
    int lastLen = ((numBytes - 1) & BYTE_MASK) + 1;
    if (i < pooledReserved) {
      ret[i] = initBuf(head, i++ == lastIdx ? lastLen : MAX_BYTES);
      ByteBuffer[] pooled;
      if (i == ret.length) {
        pooled = ret;
      } else {
        // if any are pooled, we register the pooled ones for tracking
        pooled = new ByteBuffer[pooledReserved];
        System.arraycopy(ret, 0, pooled, 0, pooledReserved);
        exhausted.add(ret.length - pooledReserved - ADJUST);
      }
      refHandler.add(sentinel, pooled);
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

  private ByteBuffer initBuf(int head, int size) {
    ByteBuffer ret = pool[head & POOL_SIZE_MASK].clear().limit(size);
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
}
