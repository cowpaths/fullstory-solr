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

import static org.apache.solr.storage.CompressingDirectory.COMPRESSION_BLOCK_SIZE;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiFunction;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.ThreadInterruptedException;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.storage.CachedCompressedIndexInput.NodeRefStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fixed-size block cache backed by a memory-mapped file. Manages an LRU queue of decompressed
 * blocks, evicting the least-recently-used evictable block when the pool is exhausted.
 *
 * <p>If eviction finds all blocks pinned, {@link #acquireNode()} returns {@code null} and the
 * caller is expected to decompress the block into a temporary heap buffer and serve the read
 * uncached.
 *
 * <p>Pin/unpin semantics and the LRU list protocol are inherited from {@link Cache2}.
 *
 * <p>The pool is split across N independent {@link Cache2.DualQueueCache} instances (one per CPU,
 * rounded to the next power of two). Acquire routes to a randomly chosen partition via {@link
 * ThreadLocalRandom}; pin and unpin route to the owning partition decoded from the handle.
 */
public class BlockCache implements Closeable, SolrMetricProducer {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int MAX_BLOCKS_PER_PARTITION =
      Integer.highestOneBit(Integer.MAX_VALUE / COMPRESSION_BLOCK_SIZE);
  private static final int POOL_SHIFT = Integer.numberOfTrailingZeros(MAX_BLOCKS_PER_PARTITION);
  private static final int POOL_MASK = MAX_BLOCKS_PER_PARTITION - 1;

  // ---------------------------------------------------------------------------
  // Handle encoding: (generation << 32) | (partitionIndex << PART_SHIFT) | localSlot
  //
  // Cache2.acquireNode() returns an opaque long encoding (generation, slot); BlockCache ORs in
  // the partition index at bits 20-31 to form the full handle stored in accessMapped.
  // ---------------------------------------------------------------------------

  /** Number of bits reserved for the local slot within a partition (supports up to 1M slots). */
  static final int PART_SHIFT = 20;

  /** Sentinel handle meaning "no cached block". */
  static final long NULL_HANDLE = -1L;

  static long encodeHandle(int partitionIdx, long cache2Handle) {
    return cache2Handle | ((long) partitionIdx << PART_SHIFT);
  }

  private int partOf(long handle) {
    return ((int) handle) >>> PART_SHIFT;
  }

  // ---------------------------------------------------------------------------
  // Node
  // ---------------------------------------------------------------------------

  private static final ByteBuffer EXCEPTION_SENTINEL = ByteBuffer.allocate(0);

  private static final class StrongRef extends Cache.Val {

    private final NodeRefStruct nrs; // reachability only

    StrongRef(NodeRefStruct nrs) {
      super(1);
      this.nrs = nrs;
    }
  }

  private final Cache<StrongRef>[] holdRefs;

  private final ReferenceQueue<? super IndexInput> collected = new ReferenceQueue<>();

  private final ExecutorService drainExec =
      ExecutorUtil.newMDCAwareSingleThreadExecutor(new SolrNamedThreadFactory("blockCacheDrainer"));

  private final Future<?> drainTask;

  private void drain() {
    try {
      while (true) {
        NodeRefStruct ref = (NodeRefStruct) collected.remove();
        ref.closeFor(this);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static Cache<StrongRef>[] initHoldRefs(int length) {
    @SuppressWarnings({"unchecked", "rawtypes"})
    Cache<StrongRef>[] ret = new Cache[length];
    for (int i = length - 1; i >= 0; i--) {
      ret[i] = new Cache<>(List.of());
    }
    return ret;
  }

  private StrongRef createStrongRef(IndexInput in, Cache.Node<StrongRef> n) {
    return new StrongRef(new NodeRefStruct(in, collected, outstandingRefs, n));
  }

  private final BiFunction<IndexInput, Cache.Node<StrongRef>, StrongRef> createStrongRef =
      this::createStrongRef;

  /**
   * Registers a {@link WeakReference} to the specified {@link IndexInput}. The {@link
   * WeakReference} carries a strong reference to the associated specified {@link NodeRefStruct},
   * which is used to unpin any pinned cache nodes upon GC of the {@link IndexInput}.
   *
   * <p>Callers may use the returned {@link StrongRef} to explicitly unpin any node referenced by
   * the specified {@link NodeRefStruct} and remove the strong ref to the {@link WeakReference},
   * thereby pruning pointless references.
   */
  NodeRefStruct register(IndexInput in) {
    Cache.Node<StrongRef> n = new Cache.Node<>(createStrongRef, in);
    holdRefs[tlrIndex()].unpin(n, false);
    outstandingRefs.increment();
    refsCreated.increment();
    return n.getPayload().nrs;
  }

  /**
   * A cache entry: wraps a decompressed block buffer and carries a reference count for safe
   * concurrent eviction.
   *
   * <p>Lifecycle:
   *
   * <ol>
   *   <li>Returned by {@link BlockCache#acquireNode()} pinned (refCount=1), <em>not</em> in the LRU
   *       list.
   *   <li>Caller populates `getValue()` and publishes the node (e.g. via an {@code AtomicReference}
   *       slot). The node is still pinned.
   *   <li>Subsequent callers call {@link Cache#pin(Cache.Node)}, which either re-pins
   *       (refCount&gt;0 → increment only) or first-pins (refCount=0 → remove from list +
   *       increment).
   *   <li>Each caller eventually calls {@link Cache#unpin(Cache.Node, boolean)}. The last unpin
   *       (refCount→0) inserts the node at the LRU head (most-recently-used, lowest eviction
   *       priority).
   *   <li>When evicted by {@link BlockCache#acquireNode()}, refCount is set to -1 permanently. Any
   *       reader that encounters the node via a stale slot sees the negative count, fails {@link
   *       Cache#pin(Cache.Node)}, and falls back to loading.
   * </ol>
   */
  public static final class Val extends Cache2.TsVal {

    /**
     * Completion signal: false = pending, true = done. Written under {@code synchronized(this)}
     * with {@code notifyAll()}; fast-path readers check this volatile field before entering the
     * monitor.
     */
    private volatile boolean populated;

    private volatile boolean waiting;

    private final int cacheBlockOrd;

    private volatile ByteBuffer cached;

    private Val(ByteBuffer prepopulated) {
      super(Integer.MAX_VALUE >> 1);
      this.cached = prepopulated;
      this.cacheBlockOrd = -1;
    }

    Val(int cacheBlockOrd, int initialRefCount) {
      super(initialRefCount);
      this.cacheBlockOrd = cacheBlockOrd;
    }

    ByteBuffer populate(byte[] arr, int off, int len, BlockCache c) {
      assert cacheBlockOrd >= 0;
      ByteBuffer ret = c.slice(cacheBlockOrd);
      // NOTE: for consistency between `populate()` and on-demand restore
      // in `join()`, we call `clear()` here, _not_ `flip()`.
      // It is the responsibility of the buffer lease recipient to duplicate
      // and set proper limit and byte order.
      ret = ret.put(arr, off, len).clear().asReadOnlyBuffer();
      cached = ret;
      assert !populated;
      populated = true;
      if (waiting) {
        synchronized (this) {
          notifyAll();
        }
      }
      return ret;
    }

    /**
     * Waits for this node's buffer to be populated, blocking if necessary.
     *
     * @throws CompletionException if population failed
     */
    @SuppressWarnings("ReferenceEquality")
    public ByteBuffer join(BlockCache c) {
      if (cacheBlockOrd == -1) {
        // tail buffer, always populated, already read-only
        return cached;
      }
      if (!populated) {
        waiting = true;
        synchronized (this) {
          while (!populated) {
            try {
              wait();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new ThreadInterruptedException(e);
            }
          }
        }
      }
      ByteBuffer ret = cached;
      if (ret == null) {
        ret = c.slice(cacheBlockOrd).asReadOnlyBuffer();
        cached = ret;
      } else if (ret == EXCEPTION_SENTINEL) {
        throw new CompletionException("other thread exception in buffer population", null);
      }
      return ret;
    }

    /** Marks this node as failed, unblocking any threads waiting in {@link #join}. */
    public void completeExceptionally(Throwable t) {
      assert cacheBlockOrd >= 0;
      cached = EXCEPTION_SENTINEL;
      assert !populated;
      populated = true;
      if (waiting) {
        synchronized (this) {
          notifyAll();
        }
      }
    }

    private static final VarHandle CACHED;

    static {
      try {
        CACHED = MethodHandles.lookup().findVarHandle(Val.class, "cached", ByteBuffer.class);
      } catch (ReflectiveOperationException e) {
        throw new Error(e);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Partition
  // ---------------------------------------------------------------------------

  private static final class Partition extends Cache2.DualQueueCache<Val> {
    Partition(int capacity, Iterable<BlockCache.Val> pool) {
      super(capacity, pool);
    }

    @Override
    protected void resetPayload(int slot, int newRefCount) {
      BlockCache.Val v = payload[slot];
      v.refCount = newRefCount;
      v.lastUnpinNanos = 0;
      // fromHot is intentionally NOT reset: acquireTail() sets it before resetPayload() is called,
      // and BlockCache.acquireNode() reads it immediately after to update hotAcquisitions.
      v.populated = false;
      v.waiting = false;
      v.cached = null;
    }
  }

  // ---------------------------------------------------------------------------
  // Construction
  // ---------------------------------------------------------------------------

  private final ByteBuffer[] pool;

  private final int nPartitions;
  private final Partition[] partitions;

  private final long totalBytes;
  private final LongAdder acquisitions = new LongAdder();
  private final LongAdder poolExhausted = new LongAdder();
  private final LongAdder pinnedCount = new LongAdder();
  private final LongAdder closedCount = new LongAdder();
  // close(node, false) skipped because the node was already dead (payload null or refCount=-1,
  // meaning it was already evicted by acquireTail before we could explicitly close it).
  private final LongAdder closeSkippedDead = new LongAdder();
  // close(node, false) skipped because the node was pinned (refCount>0 or in UNPIN_SENTINEL
  // transition). The node will be recycled naturally once all readers unpin it.
  private final LongAdder closeSkippedPinned = new LongAdder();
  private final LongAdder hits = new LongAdder();
  private final LongAdder hotAcquisitions = new LongAdder();
  private final LongAdder hotUnpinned = new LongAdder();
  // Synchronous (demand) decompressions: supply() called on the read path in cacheMiss(), meaning
  // the reader had to wait for the block to be fetched and decompressed.
  private final LongAdder blocksDecompressedDemand = new LongAdder();
  // Asynchronous (readahead) decompressions: supply() called from BlockPreloader on the ioExec
  // thread pool, ahead of any reader request.
  private final LongAdder blocksDecompressedReadahead = new LongAdder();
  // acquireNode() returned a node but the CAS to claim the accessMapped slot was lost to another
  // thread; the node was returned unused. No decompression occurred. Temporary diagnostic counter
  // to verify: misses == blocksDecompressedDemand + blocksDecompressedReadahead + casRaceLoss.
  private final LongAdder casRaceLoss = new LongAdder();

  private final LongAdder failedPin = new LongAdder();
  private final LongAdder prepopulated = new LongAdder();

  private final LongAdder outstandingRefs = new LongAdder();
  private final LongAdder refsCreated = new LongAdder();

  private volatile SolrMetricsContext solrMetricsContext;

  /**
   * Creates a new block cache backed by a freshly-created temp file. The file is deleted
   * immediately after mapping so it does not outlive the JVM.
   */
  public BlockCache(long targetBytes, Path backingFile) throws IOException {
    int nBlocks = Math.toIntExact(targetBytes / COMPRESSION_BLOCK_SIZE);
    this.pool = initPool(nBlocks, backingFile, true);
    this.partitions = distribute(nBlocks);
    this.nPartitions = partitions.length;
    this.holdRefs = initHoldRefs(nPartitions);
    this.totalBytes = (long) nBlocks * COMPRESSION_BLOCK_SIZE;
    this.drainTask = drainExec.submit(this::drain);
    log.info(
        "BlockCache initialized: nBlocks={}, targetBytes={}, nPartitions={}",
        pool.length,
        targetBytes,
        partitions.length);
  }

  /**
   * Creates a block cache backed by an existing file. The file is mmapped as-is; its size (rounded
   * down to a block boundary) determines the cache capacity. The file is not deleted.
   */
  public BlockCache(Path existingBackingFile) throws IOException {
    this(
        existingBackingFile,
        Files.size(existingBackingFile) / COMPRESSION_BLOCK_SIZE * COMPRESSION_BLOCK_SIZE);
  }

  private BlockCache(Path existingBackingFile, long targetBytes) throws IOException {
    int nBlocks = Math.toIntExact(targetBytes / COMPRESSION_BLOCK_SIZE);
    this.pool = initPool(nBlocks, existingBackingFile, false);
    this.partitions = distribute(nBlocks);
    this.nPartitions = partitions.length;
    this.holdRefs = initHoldRefs(nPartitions);
    this.totalBytes = (long) nBlocks * COMPRESSION_BLOCK_SIZE;
    this.drainTask = drainExec.submit(this::drain);
    log.info(
        "BlockCache initialized from existing file {}: nBlocks={}, targetBytes={}, nPartitions={}",
        existingBackingFile,
        pool.length,
        targetBytes,
        partitions.length);
  }

  private static Partition[] distribute(int nBlocks) {
    int nPartitions = computeNPartitions(nBlocks);
    Partition[] parts = new Partition[nPartitions];
    int base = nBlocks / nPartitions;
    int remainder = nBlocks % nPartitions;
    int offset = 0;
    for (int i = 0; i < nPartitions; i++) {
      int count = base + (i < remainder ? 1 : 0);
      int from = offset;
      int to = offset + count;
      Iterable<Val> poolList =
          () ->
              new Iterator<Val>() {
                int i = from;

                @Override
                public boolean hasNext() {
                  return i < to;
                }

                @Override
                public Val next() {
                  return new Val(i++, 0);
                }
              };
      parts[i] = new Partition(count, poolList);
      offset += count;
    }
    return parts;
  }

  private static int computeNPartitions(int nBlocks) {
    int cpus = Runtime.getRuntime().availableProcessors();
    int n = Integer.highestOneBit(Math.max(1, cpus));
    if (n > nBlocks / n) {
      // small number of blocks relative to processors; fallback to single partition
      return 1;
    }
    return n;
  }

  // ---------------------------------------------------------------------------
  // API
  // ---------------------------------------------------------------------------

  /**
   * Pins the slot identified by {@code handle}. Routes to the owning partition decoded from the
   * handle. Returns {@code false} if the slot is permanently dead.
   */
  boolean pin(long handle) {
    int p = partOf(handle);
    int rc = partitions[p].pin(handle);
    if (rc > 0) {
      pinnedCount.increment();
      if (partitions[p].getPayload(handle).fromHot()) hotUnpinned.decrement();
    } else if (rc < 0) {
      return false;
    }
    hits.increment();
    return true;
  }

  /** Releases a pin on the slot identified by {@code handle}. */
  void unpin(long handle) {
    unpin(handle, true);
  }

  @SuppressWarnings({"ReferenceEquality", "fallthrough"})
  void unpin(long handle, boolean recordAccess) {
    int p = partOf(handle);
    switch (partitions[p].unpin(handle, recordAccess)) {
      case 1:
        hotUnpinned.increment();
        // fallthrough
      case 0:
        pinnedCount.decrement();
        // on last unpin, null out the cached ByteBuffer. recreating is cheap.
        Val v = partitions[p].getPayload(handle);
        if (v != null) {
          ByteBuffer cur = v.cached;
          if (cur != null && cur != EXCEPTION_SENTINEL) {
            // CAS ensures we never clobber EXCEPTION_SENTINEL; if it loses, cached is already
            // null or sentinel, both fine. Worst case of a benign loss: another slice() call.
            Val.CACHED.compareAndSet(v, cur, null);
          }
        }
    }
  }

  /**
   * Acquires a pinned slot from a randomly chosen partition. Returns {@link #NULL_HANDLE} if the
   * partition is exhausted.
   */
  long acquireNode() {
    int p = tlrIndex();
    long ar = partitions[p].acquireNode();
    if (ar != -1L) {
      acquisitions.increment();
      pinnedCount.increment();
      if (partitions[p].getPayload(ar).fromHot()) {
        hotAcquisitions.increment();
        hotUnpinned.decrement();
      }
      return encodeHandle(p, ar);
    } else {
      poolExhausted.increment();
      return NULL_HANDLE;
    }
  }

  Val getPayload(long handle) {
    return partitions[partOf(handle)].getPayload(handle);
  }

  public void writeMetrics(MapWriter.EntryWriter ew) throws IOException {
    long prep = prepopulated.sum();
    long pinnedBytes = pinnedCount.sum() * COMPRESSION_BLOCK_SIZE;
    long h = hits.sum();
    long acq = acquisitions.sum();
    long misses = acq - prep;
    long hotUnpinnedBytes = hotUnpinned.sum() * COMPRESSION_BLOCK_SIZE;
    ew.put("outstandingRefs", outstandingRefs.sum());
    ew.put("refsCreated", refsCreated.sum());
    ew.put("totalBytes", totalBytes);
    ew.put("closedCount", closedCount.sum());
    ew.put("closeSkippedDead", closeSkippedDead.sum());
    ew.put("closeSkippedPinned", closeSkippedPinned.sum());
    ew.put("acquisitions", acq);
    ew.put("hotAcquisitions", hotAcquisitions.sum());
    ew.put("poolExhausted", poolExhausted.sum());
    ew.put("pinnedBytes", pinnedBytes);
    ew.put("unpinnedBytes", totalBytes - pinnedBytes);
    ew.put("hotUnpinnedBytes", hotUnpinnedBytes);
    ew.put("prepopulated", prep);
    ew.put("blocksDecompressedDemand", blocksDecompressedDemand.sum());
    ew.put("blocksDecompressedReadahead", blocksDecompressedReadahead.sum());
    ew.put("casRaceLoss", casRaceLoss.sum());
    ew.put("failedPin", failedPin.sum());
    ew.put("hits", h);
    ew.put("hitRate", h + misses == 0 ? 1.0 : (double) h / (h + misses));
    ew.put(
        "usage",
        RamUsageEstimator.humanReadableUnits(pinnedBytes)
            + " / "
            + RamUsageEstimator.humanReadableUnits(hotUnpinnedBytes)
            + " / "
            + RamUsageEstimator.humanReadableUnits(totalBytes));
  }

  /**
   * Records one synchronous (demand) block decompression: the reader stalled waiting for the fetch.
   */
  void recordDecompressionDemand() {
    blocksDecompressedDemand.increment();
  }

  /**
   * Records one asynchronous (readahead) block decompression: fetched speculatively by
   * BlockPreloader.
   */
  void recordDecompressionReadahead() {
    blocksDecompressedReadahead.increment();
  }

  /**
   * Records one CAS race loss: acquireNode() succeeded but compareAndSet lost to another thread.
   */
  void recordCasRaceLoss() {
    casRaceLoss.increment();
  }

  void recordFailedPin() {
    failedPin.increment();
  }

  /**
   * Records one block written through to the cache by an IndexOutput (write-time prepopulation).
   */
  public void recordPrepopulated() {
    prepopulated.increment();
  }

  @Override
  public synchronized void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    if (solrMetricsContext != null) return;
    solrMetricsContext = parentContext.getChildContext(this);
    MetricsMap mm = new MetricsMap(this::writeMetrics);
    solrMetricsContext.gauge(mm, true, scope, SolrInfoBean.Category.DIRECTORY.toString());
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }

  /**
   * Recycles the slot identified by {@code handle} back to the eviction-tail of its owning
   * partition (making it a high-priority reuse candidate). Used when a slot was acquired but
   * ultimately not needed (e.g. lost CAS race).
   */
  boolean close(long handle) {
    return close(handle, false);
  }

  boolean close(long handle, boolean unconditional) {
    int p = partOf(handle);
    Val v;
    if (unconditional) {
      v = null;
      pinnedCount.decrement();
    } else {
      // Read fromHot() before the close. Only relevant for the non-unconditional path where the
      // slot is in the evictable list (refCount=0); unconditional slots are pinned and not in
      // any queue.
      v = partitions[p].getPayload(handle);
    }
    boolean closed;
    if (unconditional) {
      partitions[p].closeUnconditional(handle);
      closed = true;
    } else {
      closed = partitions[p].close(handle);
    }
    if (closed) {
      closedCount.increment();
      if (v != null && v.fromHot()) hotUnpinned.decrement();
      return true;
    } else {
      // Categorize the skip to help diagnose phantom-block accumulation.
      // NOTE: racy read of refCount; accurate enough for diagnostics.
      Val vv = partitions[p].getPayload(handle);
      if (vv == null || vv.refCount() < 0) {
        closeSkippedDead.increment();
      } else {
        closeSkippedPinned.increment();
      }
      return false;
    }
  }

  private int tlrIndex() {
    return ThreadLocalRandom.current().nextInt(nPartitions);
  }

  /**
   * Builds a node-level {@link BlockCache} from system properties:
   *
   * <ul>
   *   <li>{@code solr.blockCache.path} — path to an existing file to use as the backing store; if
   *       the file already exists its current size determines the capacity; otherwise created
   *       fresh. If absent, a temporary file is created and immediately deleted (does not outlive
   *       the JVM).
   *   <li>{@code solr.blockCache.kilobytes} — capacity in KiB when no path is given (default 1
   *       GiB).
   * </ul>
   *
   * <p>Must be set as JVM system properties before startup.
   */
  public static BlockCache buildFromProperties() throws IOException {
    String pathProp = System.getProperty("solr.blockCache.path", "");
    long kilobytes = Long.getLong("solr.blockCache.kilobytes", 1L << 20);
    if (!pathProp.isEmpty()) {
      Path backingFile = Path.of(pathProp);
      return Files.exists(backingFile)
          ? new BlockCache(backingFile)
          : new BlockCache(kilobytes * 1024L, backingFile);
    }
    Path tmpFile =
        Path.of(System.getProperty("java.io.tmpdir"))
            .resolve("solr-block-cache-" + java.util.UUID.randomUUID() + ".tmp");
    return new BlockCache(kilobytes * 1024L, tmpFile);
  }

  @Override
  @SuppressWarnings("try")
  public void close() throws IOException {
    try (Closeable c = SolrMetricProducer.super::close) {
      drainTask.cancel(true);
      ExecutorUtil.shutdownAndAwaitTermination(drainExec);
    }
    // MappedByteBuffers are not explicitly unmapped here; the JVM will release them on exit.
  }

  // ---------------------------------------------------------------------------
  // Pool initialization
  // ---------------------------------------------------------------------------

  /**
   * Allocates the pool as slices of a file-backed memory-mapped region (adapted from {@code
   * HeapCacheFbsModifier.poolFileBacked}). If {@code createAndDelete} is true, the file is created
   * fresh, sized to {@code targetBytes}, and deleted immediately after mapping so that it does not
   * outlive the JVM. If false, the file must already exist and is mmapped without truncation or
   * deletion.
   */
  private static ByteBuffer[] initPool(final int nBlocks, Path backingFile, boolean createAndDelete)
      throws IOException {
    final long blockSizeL = COMPRESSION_BLOCK_SIZE;
    // Round partition size down to a 2 MiB boundary (matches HeapCacheFbsModifier convention).
    final long partitionMaxBytes = ((MAX_BLOCKS_PER_PARTITION * blockSizeL) >> 21) << 21;
    final int effectiveMaxBlocksPerPartition = Math.toIntExact(partitionMaxBytes / blockSizeL);
    final int numPartitions = ((nBlocks - 1) / effectiveMaxBlocksPerPartition) + 1;
    final ByteBuffer[] pool = new ByteBuffer[numPartitions];

    Set<StandardOpenOption> openOpts =
        EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE);
    if (createAndDelete) {
      openOpts.add(StandardOpenOption.CREATE_NEW);
    }
    try (FileChannel fc = FileChannel.open(backingFile, openOpts)) {
      if (createAndDelete) {
        fc.truncate(nBlocks * blockSizeL);
      }

      // Iterate partitions from high to low so that the remainder partition (which may be
      // smaller than effectiveMaxBlocksPerPartition) is handled first.
      for (int i = numPartitions - 1,
              partitionNumBlocks = ((nBlocks - 1) % effectiveMaxBlocksPerPartition) + 1;
          i >= 0;
          i--) {
        ByteBuffer partition =
            fc.map(
                FileChannel.MapMode.READ_WRITE,
                (long) i * partitionMaxBytes,
                partitionNumBlocks * blockSizeL);
        pool[i] = partition;
        partitionNumBlocks = effectiveMaxBlocksPerPartition;
      }
    } finally {
      if (createAndDelete) {
        Files.delete(backingFile);
      }
    }
    return pool;
  }

  private ByteBuffer slice(int cacheBlockOrd) {
    return pool[cacheBlockOrd >> POOL_SHIFT].slice(
        (cacheBlockOrd & POOL_MASK) * COMPRESSION_BLOCK_SIZE, COMPRESSION_BLOCK_SIZE);
  }
}
