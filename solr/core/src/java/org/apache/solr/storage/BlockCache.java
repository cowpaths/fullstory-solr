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
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.StampedLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.lucene.internal.hppc.BitMixer;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.BlockCacheMapping;
import org.apache.lucene.store.BlockCacheMmapProvider;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.ThreadInterruptedException;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.ObjectCache;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.storage.CachedCompressedIndexInput.NodeRefStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fixed-size block cache backed by a memory-mapped file. Manages an LRU queue of decompressed
 * blocks, evicting the least-recently-used evictable block when the pool is exhausted.
 *
 * <p>If eviction finds all blocks pinned, {@link #acquireNode(long[])} returns {@code null} and the
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

  private static final boolean ENABLE_CACHE_PERSISTENCE =
      EnvUtils.getPropertyAsBool("solr.blockCache.persistence", true);

  private static final boolean PROSPECTIVE_READAHEAD =
      EnvUtils.getPropertyAsBool("solr.blockCache.prospectiveReadahead", false);

  // ---------------------------------------------------------------------------
  // Handle encoding: (generation << 32) | (partitionIndex << PART_SHIFT) | localSlot
  //
  // Cache2.acquireNode() returns an opaque long encoding (generation, slot); BlockCache ORs in
  // the partition index at bits 20-31 to form the full handle stored in accessMapped.
  // ---------------------------------------------------------------------------

  /** Number of bits reserved for the local slot within a partition (supports up to 1M slots). */
  static final int PART_SHIFT = 20;

  /**
   * Sentinel handle meaning "no cached block". Zero is safe because {@link Cache2} never allocates
   * slot 0 (reserved), so the slot field of any real handle is always &ge;1, making 0 impossible as
   * a valid handle. This also matches the default element value of {@link AtomicLongArray}.
   */
  static final long NULL_HANDLE = 0L;

  private static long encodeHandle(int partitionIdx, long cache2Handle) {
    return cache2Handle | ((long) partitionIdx << PART_SHIFT);
  }

  private static final ConcurrentHashMap<
          Map.Entry<CoreContainer, String>, CompletableFuture<Object>>
      ENFORCE_UNIQUE = new ConcurrentHashMap<>();

  public static <T> T coreContainerSingleton(
      CoreContainer cc, String key, Class<T> clazz, Function<String, ? extends T> mappingFunction) {
    ObjectCache c = cc.getObjectCache();
    T extant = c.get(key, clazz);
    if (extant != null) {
      return extant;
    }
    Map.Entry<CoreContainer, String> uniquenessKey =
        new AbstractMap.SimpleImmutableEntry<>(cc, key);
    CompletableFuture<T> weCompute = new CompletableFuture<>();
    @SuppressWarnings("unchecked")
    CompletableFuture<T> otherComputes =
        (CompletableFuture<T>)
            ENFORCE_UNIQUE.putIfAbsent(uniquenessKey, (CompletableFuture<Object>) weCompute);
    if (otherComputes != null) {
      try {
        return otherComputes.get(1, TimeUnit.MINUTES);
      } catch (TimeoutException e) {
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ThreadInterruptedException(e);
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof RuntimeException) {
          throw (RuntimeException) cause;
        } else if (cause instanceof Error) {
          throw (Error) cause;
        } else {
          throw new RuntimeException(e);
        }
      }
    }
    try {
      T ret = c.get(key, clazz);
      if (ret == null) {
        ret = mappingFunction.apply(key);
        if (c.put(key, ret) != null) {
          throw new IllegalStateException();
        }
      }
      weCompute.complete(ret);
      return ret;
    } catch (Throwable t) {
      weCompute.completeExceptionally(t);
      throw t;
    } finally {
      if (!ENFORCE_UNIQUE.remove(uniquenessKey, weCompute)) {
        log.error("unable to remove entry for {}", uniquenessKey);
      }
    }
  }

  private int partOf(long handle) {
    return ((int) handle) >>> PART_SHIFT;
  }

  // ---------------------------------------------------------------------------
  // Node
  // ---------------------------------------------------------------------------

  private static final ByteBuffer EXCEPTION_SENTINEL = ByteBuffer.allocate(0);

  public void decrementOutstandingRefs() {
    outstandingRefs.decrement();
  }

  /**
   * Returns a UUID whose 128 bits are the raw MD5 digest of {@code key} (encoded as UTF-8), with no
   * version/variant bit masking. Prefer this over {@link UUID#nameUUIDFromBytes} when the full
   * 128-bit space matters (e.g. content-keyed cache identifiers).
   */
  static UUID rawMd5UUID(String key) {
    ByteBuffer hash = rawMd5(key);
    return new UUID(hash.getLong(), hash.getLong());
  }

  static ByteBuffer rawMd5(String val) {
    try {
      return ByteBuffer.wrap(
          MessageDigest.getInstance("MD5").digest(val.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e); // MD5 is mandated by the JVM spec
    }
  }

  /**
   * Derives a stable, replica-unique refId from the nearest {@code core.properties} file found by
   * walking up from {@code localPath}. Returns {@code null} if no {@code core.properties} is found
   * or if it contains neither a {@code name} nor a {@code coreNodeName} property.
   */
  static UUID refIdFromCoreProperties(Path coreRootDirectory, Path localPath) throws IOException {
    if (coreRootDirectory == null) return null;
    Path root = coreRootDirectory.toAbsolutePath().normalize();
    Path normalized = localPath.toAbsolutePath().normalize();
    if (!normalized.startsWith(root)) return null;
    // Walk up from localPath, stopping before coreRootDirectory itself (core.properties lives
    // in a direct subdirectory of it, not in coreRootDirectory itself).
    for (Path dir = normalized; !dir.equals(root); dir = dir.getParent()) {
      Path corePropsPath = dir.resolve("core.properties");
      if (Files.isRegularFile(corePropsPath)) {
        Properties props = new Properties();
        try (InputStream in = Files.newInputStream(corePropsPath)) {
          props.load(in);
        }
        String name = props.getProperty("name", "");
        String coreNodeName = props.getProperty("coreNodeName", "");
        if (name.isEmpty() && coreNodeName.isEmpty()) {
          return null;
        }
        String relPath = dir.relativize(normalized).toString();
        String key = name + "\n" + coreNodeName + "\n" + relPath;
        return rawMd5UUID(key);
      }
    }
    return null;
  }

  /**
   * Cache3.Val subclass for the primary hold-ref pool. Holds the NodeRefStruct for reachability.
   */
  static final class HoldRef extends Cache3.Val {

    private WeakReference<Object> ref; // cleared by reset() when slot is released

    HoldRef() {
      super(0);
    }

    @Override
    void reset() {
      ref = null;
    }
  }

  // Default total pool size for outstanding hold-refs. Covers typical peak workloads (~1M refs).
  // Override via -Dsolr.blockCache.holdRefPoolSize=<n>.
  private static final int HOLD_REF_POOL_SIZE =
      Integer.getInteger("solr.blockCache.holdRefPoolSize", 1 << 20);

  private final Cache3<HoldRef>[] holdRefs3; // primary: fixed-size pool; no per-registration alloc
  private final Cache<WeakReference<Object>>[] holdRefs; // fallback: when pool is exhausted

  private final ReferenceQueue<Object> collected = new ReferenceQueue<>();

  private final ExecutorService drainExec =
      ExecutorUtil.newMDCAwareSingleThreadExecutor(new SolrNamedThreadFactory("blockCacheDrainer"));

  private final Future<?> drainTask;

  private void drain() {
    try {
      while (true) {
        ((RetainedRef<?>) collected.remove()).closeFor(this);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Cache3<HoldRef>[] initHoldRefs3(int nPartitions, int perPartitionCapacity) {
    Cache3<HoldRef>[] ret = new Cache3[nPartitions];
    for (int i = 0; i < nPartitions; i++) {
      List<HoldRef> vals = new ArrayList<>(perPartitionCapacity);
      for (int j = 0; j < perPartitionCapacity; j++) vals.add(new HoldRef());
      ret[i] = new Cache3<>(perPartitionCapacity, vals);
    }
    return ret;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Cache<WeakReference<Object>>[] initHoldRefs(int length) {
    Cache<WeakReference<Object>>[] ret = new Cache[length];
    for (int i = length - 1; i >= 0; i--) {
      ret[i] = new Cache<>();
    }
    return ret;
  }

  private NodeRefStruct createNrs(
      CachedCompressedIndexInput in, Cache.Node<WeakReference<Object>> n) {
    return new NodeRefStruct(in, collected, n, -1L);
  }

  private final BiFunction<
          CachedCompressedIndexInput, Cache.Node<WeakReference<Object>>, WeakReference<Object>>
      createNrs = this::createNrs;

  private Batch createBatch(Object referrent, Cache.Node<WeakReference<Object>> n) {
    return new Batch(referrent, collected, n, -1L);
  }

  private final BiFunction<Object, Cache.Node<WeakReference<Object>>, WeakReference<Object>>
      createBatch = this::createBatch;

  private final Object referentKey = new Object();

  abstract static class RetainedRef<V> extends WeakReference<V> {

    private static final VarHandle CACHE3_HANDLE;

    static {
      try {
        CACHE3_HANDLE =
            MethodHandles.lookup().findVarHandle(RetainedRef.class, "cache3Handle", long.class);
      } catch (ReflectiveOperationException e) {
        throw new Error(e);
      }
    }

    // for cleanup upon GC
    private final Cache.Node<?> remove; // non-null on Cache fallback path
    // Cache3 primary path: (partIdx << 32 | slot); -1L if using Cache fallback.
    private volatile long cache3Handle;

    protected RetainedRef() {
      super(null, null);
      this.remove = null;
      this.cache3Handle = -3L;
    }

    protected RetainedRef(
        V referent, ReferenceQueue<? super V> q, Cache.Node<?> remove, long cache3Handle) {
      super(referent, q);
      this.remove = remove;
      this.cache3Handle = cache3Handle;
    }

    /** Unpins the current block on {@link CachedCompressedIndexInput#close()}. */
    final void closeFor(BlockCache cache) {
      boolean claimed;
      int snapshot = (int) cache3Handle;
      switch (snapshot) {
        case -3:
          claimed = CACHE3_HANDLE.compareAndSet(this, -3L, -2L);
          break;
        case -2:
          return; // UNINITIALIZED: never registered, nothing to do
        case -1:
          claimed = Cache.tryRemove(remove);
          break;
        default:
          claimed = cache.releaseHoldRef(cache3Handle);
      }
      if (claimed) {
        doCloseFor(cache);
        if (snapshot != -3) {
          cache.outstandingHoldRefs.decrement();
        }
      }
    }

    abstract void doCloseFor(BlockCache cache);
  }

  private static final class Batch extends RetainedRef<Object> {

    private final List<NodeRefStruct> toClose = new ArrayList<>();

    private Batch(
        Object referrent,
        ReferenceQueue<? super Object> q,
        Cache.Node<?> remove,
        long cache3Handle) {
      super(referrent, q, remove, cache3Handle);
    }

    @Override
    void doCloseFor(BlockCache cache) {
      for (NodeRefStruct nrs : toClose) {
        nrs.closeFor(cache);
      }
    }
  }

  private final Function<Object, Map.Entry<Object, Batch>> batchInitFunction =
      this::batchInitFunction;

  private Map.Entry<Object, Batch> batchInitFunction(Object k) {
    Object referent = new Object();
    int partIdx = tlrIndex();
    Cache3<HoldRef> p = holdRefs3[partIdx];
    int slot = p.acquire();
    Batch ret;
    if (slot != Cache3.NULL_SLOT) {
      ret = new Batch(referent, collected, null, (long) partIdx << 32 | slot);
      p.getPayload(slot).ref = ret;
    } else {
      // Pool exhausted: fall back to heap-allocated Cache.Node.
      Cache.Node<WeakReference<Object>> n = new Cache.Node<>(createBatch, referent);
      holdRefs[partIdx].add(n);
      ret = (Batch) n.getPayload();
    }
    outstandingHoldRefs.increment();
    return new AbstractMap.SimpleImmutableEntry<>(referent, ret);
  }

  private static final long CACHE_VALIDATION_MAGIC =
      rawMd5("org.apache.solr.storage.BlockCache#CACHE_VALIDATION_MAGIC").getLong();

  /**
   * Metadata bytes per cache block in the persistent backing file: 16-byte UUID + 4-byte blockIdx.
   */
  private static final int META_BYTES_PER_BLOCK = 20;

  /** Validation trailer appended after block metadata: 8-byte randomId + 8-byte signature. */
  private static final int TRAILER_BYTES = 16;

  /**
   * Written to the first 8 bytes of the trailer on startup to "claim" the file. The second 8 bytes
   * hold {@code randomId ^ CACHE_VALIDATION_MAGIC} and are written on clean shutdown. On the next
   * startup, if {@code storedId ^ storedSig == CACHE_VALIDATION_MAGIC} the cache is valid.
   */
  private final long randomId =
      rawMd5(
              Long.toString(
                  BitMixer.mixPhi(System.currentTimeMillis()) ^ BitMixer.mix(System.nanoTime())))
          .getLong();

  private final StampedLock closeLock = new StampedLock();

  private final BlockCacheMapping mapping;

  /**
   * Maps the metadata+trailer region of the backing file, or {@code null} for ephemeral caches.
   * Layout: {@code [nBlocks × META_BYTES_PER_BLOCK bytes][TRAILER_BYTES]}.
   */
  private final MappedByteBuffer metaBuf;

  /**
   * Sorted triplets {@code [uuidMsb, uuidLsb, (blockIdx << 32 | handleLow32)]} for binary-search
   * lookup of pre-existing valid cache entries on startup. {@code handleLow32} encodes {@code
   * (partIdx << PART_SHIFT) | localSlot} and can be passed directly to {@link #pin} with
   * generation=0 in the upper 32 bits. Empty for ephemeral or fresh caches.
   */
  private final long[] extantMap;

  /**
   * Registers a {@link WeakReference} to the specified {@link IndexInput}. The {@link
   * WeakReference} carries a strong reference to the associated specified {@link NodeRefStruct},
   * which is used to unpin any pinned cache nodes upon GC of the {@link IndexInput}.
   *
   * <p>Callers may use the returned {@link NodeRefStruct} to explicitly unpin any node referenced
   * by it and remove the strong ref to the {@link WeakReference}, thereby pruning pointless
   * references.
   */
  @SuppressWarnings("unchecked")
  NodeRefStruct register(CachedCompressedIndexInput in) {
    NodeRefStruct nrs;
    SolrRequestInfo sri;
    SolrQueryRequest req;
    if ((sri = SolrRequestInfo.getRequestInfo()) != null && (req = sri.getReq()) != null) {
      // dummy referent for batching cleanup.
      Map<Object, Object> ctx = req.getContext();
      Map.Entry<Object, Batch> b;
      synchronized (ctx) {
        b = (Map.Entry<Object, Batch>) ctx.computeIfAbsent(referentKey, batchInitFunction);
      }
      in.setBatchReferrent(b.getKey());
      nrs = new NodeRefStruct();
      List<NodeRefStruct> toClose = b.getValue().toClose;
      synchronized (toClose) {
        toClose.add(nrs);
      }
    } else {
      int partIdx = tlrIndex();
      Cache3<HoldRef> p = holdRefs3[partIdx];
      int slot = p.acquire();
      if (slot != Cache3.NULL_SLOT) {
        nrs = new NodeRefStruct(in, collected, null, (long) partIdx << 32 | slot);
        p.getPayload(slot).ref = nrs;
      } else {
        // Pool exhausted: fall back to heap-allocated Cache.Node.
        Cache.Node<WeakReference<Object>> n = new Cache.Node<>(createNrs, in);
        holdRefs[partIdx].add(n);
        nrs = (NodeRefStruct) n.getPayload();
      }
      outstandingHoldRefs.increment();
    }
    outstandingRefs.increment();
    refsCreated.increment();
    return nrs;
  }

  /**
   * Atomically claims and releases the hold-ref slot encoded in {@code handle}. Returns {@code
   * true} if this thread won the claim (preventing double-cleanup between explicit close and GC
   * drain). {@code handle} must be a value previously returned by {@link #register} on the Cache3
   * primary path (i.e. not {@code -1L}).
   */
  boolean releaseHoldRef(long handle) {
    int partIdx = (int) (handle >>> 32);
    int slot = (int) handle;
    return holdRefs3[partIdx].tryRelease(slot);
  }

  /**
   * A cache entry: wraps a decompressed block buffer and carries a reference count for safe
   * concurrent eviction.
   *
   * <p>Lifecycle:
   *
   * <ol>
   *   <li>Returned by {@link BlockCache#acquireNode(long[])} pinned (refCount=1), <em>not</em> in
   *       the LRU list.
   *   <li>Caller populates the buffer and publishes the node (e.g. via an {@link
   *       java.util.concurrent.atomic.AtomicLongArray} slot). The node is still pinned.
   *   <li>Subsequent callers call {@link BlockCache#pin(long)}, which either re-pins (refCount&gt;0
   *       → increment only) or first-pins (refCount=0 → remove from list + increment).
   *   <li>Each caller eventually calls {@link BlockCache#unpin(long)}. The last unpin (refCount→0)
   *       inserts the node at the LRU head (most-recently-used, lowest eviction priority).
   *   <li>When evicted by {@link BlockCache#acquireNode(long[])}, refCount is set to -1
   *       permanently. Any reader that encounters the node via a stale slot sees the negative
   *       count, fails {@link BlockCache#pin(long)}, and falls back to loading.
   * </ol>
   */
  public static final class Val extends Cache2.TsVal {

    /**
     * Completion signal: false = pending, true = done. Written under {@code synchronized(this)}
     * with {@code notifyAll()}; fast-path readers check this volatile field before entering the
     * monitor.
     */
    private volatile boolean populated;

    boolean isPopulated() {
      return populated;
    }

    private volatile boolean waiting;

    private final int cacheBlockOrd;

    private volatile ByteBuffer cached;

    private volatile long lastLoadHintNanos;

    Val(int cacheBlockOrd, int initialRefCount) {
      super(initialRefCount);
      this.cacheBlockOrd = cacheBlockOrd;
    }

    @Override
    void reset() {
      // fromHot is intentionally NOT reset: acquireTail() sets it before resetPayload() is called,
      // and BlockCache.acquireNode() reads it immediately after to update hotAcquisitions.
      populated = false;
      waiting = false;
      cached = null;
      lastLoadHintNanos = 0;
      super.reset();
    }

    ByteBuffer populate(byte[] arr, int off, int len, UUID blobUUID, int blockIdx, BlockCache c) {
      assert cacheBlockOrd >= 0;
      ByteBuffer ret = c.slice(cacheBlockOrd);
      StampedLock l = c.closeLock;
      long stamp = l.tryReadLock();
      if (stamp == 0) {
        throw new AlreadyClosedException("blockcache shutting down");
      }
      try {
        ByteBuffer meta = c.metaBuf;
        int metaBase = meta == null ? -1 : cacheBlockOrd * META_BYTES_PER_BLOCK;
        if (metaBase >= 0) {
          // Sentinel blockIdx=-1 written before msb for strict safety: even under adversarial
          // write reordering, no entry can be committed with a garbage blockIdx.
          // Any crash before the commit writes (msb + lsb + real blockIdx) leaves this slot with
          // blockIdx=-1; buildExtantMap skips it.
          meta.putInt(metaBase + 16, -1);
        }
        // NOTE: for consistency between `populate()` and on-demand restore
        // in `join()`, we call `clear()` here, _not_ `flip()`.
        // It is the responsibility of the buffer lease recipient to duplicate
        // and set proper limit and byte order.
        ret = ret.put(arr, off, len).clear().asReadOnlyBuffer();
        if (metaBase >= 0) {
          meta.putLong(metaBase, blobUUID.getMostSignificantBits());
          meta.putLong(metaBase + 8, blobUUID.getLeastSignificantBits());
          // Commit: blockIdx != -1 acts as the "entry is valid" signal.
          meta.putInt(metaBase + 16, blockIdx);
        }
      } finally {
        l.unlock(stamp);
      }
      cached = ret;
      lastLoadHintNanos = System.nanoTime();
      assert !populated;
      populated = true;
      if (waiting) {
        synchronized (this) {
          notifyAll();
        }
      }
      return ret;
    }

    private static final long JOIN_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(30);

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
        long startNanos = System.nanoTime();
        synchronized (this) {
          while (!populated) {
            long elapsedNanos = System.nanoTime() - startNanos;
            if (elapsedNanos >= JOIN_TIMEOUT_NANOS) {
              throw new IllegalStateException(
                  "join() timed out: Val was never populated (possible populate() caller crash)");
            }
            long remainingNanos = JOIN_TIMEOUT_NANOS - elapsedNanos;
            try {
              wait(remainingNanos / 1_000_000, (int) (remainingNanos % 1_000_000));
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
    private static final VarHandle LAST_LOAD_HINT_NANOS;

    static {
      try {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        CACHED = lookup.findVarHandle(Val.class, "cached", ByteBuffer.class);
        LAST_LOAD_HINT_NANOS = lookup.findVarHandle(Val.class, "lastLoadHintNanos", long.class);
      } catch (ReflectiveOperationException e) {
        throw new Error(e);
      }
    }

    private static final long LOAD_HINT_THROTTLE_NANOS = TimeUnit.SECONDS.toNanos(5);

    private void maybeLoadHint(BlockCache c) {
      long now = System.nanoTime();
      long last = lastLoadHintNanos;
      if (now - last < LOAD_HINT_THROTTLE_NANOS) return;
      if (LAST_LOAD_HINT_NANOS.compareAndSet(this, last, now)) {
        c.mapping.loadHint(cacheBlockOrd);
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
  }

  // ---------------------------------------------------------------------------
  // Construction
  // ---------------------------------------------------------------------------

  private final ByteBuffer[] pool;

  private final int nPartitions;
  private final Partition[] partitions;

  private final long totalBytes;
  private final boolean alwaysPrepareWrite;
  private volatile boolean firstAcquireLogged;
  private final AtomicLong prepareWriteFailures = new AtomicLong();
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
  // Warm-start hits: acquireNode(uuid, blockIdx) found a pre-existing slot in the extant map and
  // successfully pinned it (generation still 0). The caller skips fetch+decompress entirely.
  private final LongAdder warmStartHits = new LongAdder();

  private final LongAdder outstandingHoldRefs = new LongAdder();
  private final LongAdder outstandingRefs = new LongAdder();
  private final LongAdder refsCreated = new LongAdder();

  private volatile SolrMetricsContext solrMetricsContext;

  /** Returns the total backing-file size in bytes required to hold {@code nBlocks} blocks. */
  static long backingFileBytes(int nBlocks) {
    return (long) nBlocks * (COMPRESSION_BLOCK_SIZE + META_BYTES_PER_BLOCK) + TRAILER_BYTES;
  }

  /**
   * Creates a new ephemeral block cache backed by a freshly-created file. The file is deleted
   * immediately after mapping and does not outlive the JVM. No metadata or trailer is maintained.
   */
  public BlockCache(long targetBytes, Path backingFile) throws IOException {
    int nBlocks = Math.toIntExact(targetBytes / COMPRESSION_BLOCK_SIZE);
    long dataSize = (long) nBlocks * COMPRESSION_BLOCK_SIZE;
    try (FileChannel fc =
        FileChannel.open(
            backingFile,
            EnumSet.of(
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE))) {
      fc.truncate(dataSize);
    }
    BlockCacheMapping m;
    try {
      m = BlockCacheMmapProvider.getDefault().open(backingFile, COMPRESSION_BLOCK_SIZE, nBlocks);
    } finally {
      Files.delete(backingFile);
    }
    this.mapping = m;
    log.info("BlockCache ephemeral mapping: {}", m);
    this.pool = m.dataPool();
    this.metaBuf = null;
    this.alwaysPrepareWrite = false;
    this.extantMap = new long[0];
    this.partitions = distribute(nBlocks);
    this.nPartitions = partitions.length;
    this.holdRefs3 =
        initHoldRefs3(
            nPartitions, Math.max(1, Math.min(HOLD_REF_POOL_SIZE, nBlocks << 6) / nPartitions));
    this.holdRefs = initHoldRefs(nPartitions);
    this.totalBytes = (long) nBlocks * COMPRESSION_BLOCK_SIZE;
    this.drainTask = drainExec.submit(this::drain);
    log.info(
        "BlockCache initialized (ephemeral): nBlocks={}, targetBytes={}, nPartitions={}",
        nBlocks,
        targetBytes,
        nPartitions);
  }

  /**
   * Opens a block cache backed by an existing persistent file. The file size determines the cache
   * capacity. The trailer is validated; if invalid the extant map is empty (cold start) but the
   * data region is still used.
   */
  public BlockCache(Path existingBackingFile) throws IOException {
    long fileSize = Files.size(existingBackingFile);
    long dataPerBlock = (long) COMPRESSION_BLOCK_SIZE + META_BYTES_PER_BLOCK;
    int nBlocks = Math.toIntExact((fileSize - TRAILER_BYTES) / dataPerBlock);
    if (nBlocks <= 0) {
      throw new IOException(
          "BlockCache backing file too small (" + fileSize + " bytes): " + existingBackingFile);
    }
    long dataSize = (long) nBlocks * COMPRESSION_BLOCK_SIZE;
    long metaSize = (long) nBlocks * META_BYTES_PER_BLOCK + TRAILER_BYTES;
    MappedByteBuffer mb;
    try (FileChannel fc =
        FileChannel.open(existingBackingFile, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
      mb = fc.map(MapMode.READ_WRITE, dataSize, metaSize);
    }
    mb.order(ByteOrder.LITTLE_ENDIAN);
    BlockCacheMapping m =
        BlockCacheMmapProvider.getDefault()
            .open(existingBackingFile, COMPRESSION_BLOCK_SIZE, nBlocks);
    this.mapping = m;
    log.info("BlockCache persistent mapping: {}", m);
    this.pool = m.dataPool();
    this.metaBuf = mb;
    int trailerOffset = nBlocks * META_BYTES_PER_BLOCK;
    long storedId = mb.getLong(trailerOffset);
    long storedSig = mb.getLong(trailerOffset + 8);
    boolean valid = (storedId ^ storedSig) == CACHE_VALIDATION_MAGIC;
    this.partitions = distribute(nBlocks);
    this.nPartitions = partitions.length;
    if (ENABLE_CACHE_PERSISTENCE && valid) {
      this.alwaysPrepareWrite = true;
      this.extantMap = buildExtantMap(nBlocks, mb, partitions);
    } else {
      log.warn(
          "BlockCache backing file {} failed validation (storedId={} sig={}); cold start",
          existingBackingFile,
          Long.toHexString(storedId),
          Long.toHexString(storedSig));
      this.alwaysPrepareWrite = !m.invalidateAll();
      this.extantMap = new long[0];
    }
    // Claim the file: write randomId, clear signature (prevents stale warm-start on crash).
    mb.putLong(trailerOffset, randomId);
    mb.putLong(trailerOffset + 8, 0L);
    mb.force();
    this.holdRefs3 =
        initHoldRefs3(
            nPartitions, Math.max(1, Math.min(HOLD_REF_POOL_SIZE, nBlocks << 6) / nPartitions));
    this.holdRefs = initHoldRefs(nPartitions);
    this.totalBytes = (long) nBlocks * COMPRESSION_BLOCK_SIZE;
    this.drainTask = drainExec.submit(this::drain);
    log.info(
        "BlockCache restored from {}: nBlocks={}, targetBytes={}, nPartitions={}, extantEntries={}",
        existingBackingFile,
        nBlocks,
        totalBytes,
        nPartitions,
        extantMap.length / 3);
  }

  /**
   * Reads the metadata region and returns a sorted extant map of pre-existing valid cache entries.
   * Each entry is a triplet {@code [uuidMsb, uuidLsb, (blockIdx << 32 | handleLow32)]} sorted
   * ascending by {@code (uuidMsb, uuidLsb)} for binary-search access in {@code acquireNode}. {@code
   * handleLow32} encodes {@code (partIdx << PART_SHIFT) | localSlot} for the slot that holds global
   * block {@code i}, so the warm-start path can call {@link #pin} directly with generation=0 in the
   * upper 32 bits.
   *
   * <p>For a 340 GiB (usable) cache the on-disk metadata is ~27 MiB; the in-memory extant map is
   * ~32 MiB. Even if never trimmed, this is acceptable steady-state overhead.
   */
  private static long[] buildExtantMap(int nBlocks, ByteBuffer metaBuf, Partition[] partitions) {
    long[] ret = new long[nBlocks * 3];
    int j = 0;
    int curPart = 0;
    int curPartEnd = partitions[0].capacity; // exclusive; i == curPartEnd triggers advance
    for (int i = 0; i < nBlocks; i++) {
      if (i == curPartEnd) {
        curPartEnd += partitions[++curPart].capacity;
      }
      int base = i * META_BYTES_PER_BLOCK;
      long uuidMsb = metaBuf.getLong(base);
      long uuidLsb = metaBuf.getLong(base + 8);
      if (uuidMsb == 0 && uuidLsb == 0) continue; // uninitialized entry
      int blockIdx = metaBuf.getInt(base + 16);
      if (blockIdx == -1) continue; // in-progress write (populate() did not commit)

      // 1-indexed; matches reverse-order Val insertion in distribute()
      int localSlot = curPartEnd - i;

      int handleLow32 = (curPart << PART_SHIFT) | localSlot;
      ret[j++] = uuidMsb;
      ret[j++] = uuidLsb;
      ret[j++] = ((long) blockIdx << 32) | (handleLow32 & 0xFFFFFFFFL);
    }
    long[] valid = j == ret.length ? ret : Arrays.copyOf(ret, j);
    sortExtantMap(valid, j / 3);
    return valid;
  }

  private static final int SORT_RADIX_BITS = 16;
  private static final int SORT_BUCKETS = 1 << SORT_RADIX_BITS;
  private static final int SORT_MASK = SORT_BUCKETS - 1;

  /**
   * Sorts the first {@code nEntries} triplets in {@code map} by {@code (uuidMsb, uuidLsb,
   * blockIdx)} using LSD radix sort with a 16-bit radix (10 passes over the 160-bit key). The count
   * array (65536 ints = 256 KiB) fits in L2 cache. Ping-pong between {@code map} and a scratch
   * buffer; 10 passes (even) guarantees the result lands back in {@code map} without a final copy.
   */
  private static void sortExtantMap(long[] map, int nEntries) {
    if (nEntries < 2) return;
    int[] count = new int[SORT_BUCKETS];
    final int n3 = map.length;
    long[] src = map, dst = new long[n3];
    for (int pass = 0; pass < 10; pass++) {
      // Passes 0-1: blockIdx = upper 32 bits of index 2 (least significant; done first).
      // Passes 2-5: uuidLsb (index 1).
      // Passes 6-9: uuidMsb (index 0; most significant; done last, determines final order).
      int keyIdx = pass < 2 ? 2 : pass < 6 ? 1 : 0;
      int shift;
      switch (pass) {
        case 2:
        case 6:
          shift = 0;
          break;
        case 3:
        case 7:
          shift = 16;
          break;
        case 0:
        case 4:
        case 8:
          shift = 32;
          break;
        case 1:
        case 5:
        case 9:
          shift = 48;
          break;
        default:
          throw new AssertionError("unexpected pass: " + pass);
      }
      Arrays.fill(count, 0);
      for (int i = keyIdx; i < n3; i += 3) {
        count[(int) (src[i] >>> shift) & SORT_MASK]++;
      }
      // Inclusive prefix sums, pre-multiplied by 3 so each count[b] is a direct array index.
      int sum = 0;
      for (int b = 0; b < SORT_BUCKETS; b++) {
        sum += count[b];
        count[b] = sum * 3;
      }
      for (int i = n3 - 3; i >= 0; i -= 3) { // reverse scatter preserves stability
        int dest = count[(int) (src[i + keyIdx] >>> shift) & SORT_MASK] -= 3;
        dst[dest] = src[i];
        dst[dest + 1] = src[i + 1];
        dst[dest + 2] = src[i + 2];
      }
      long[] tmp = src;
      src = dst;
      dst = tmp;
    }
    // After 10 passes (even) the result is in `map` (verified by ping-pong trace).
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
                int i = to - 1;

                @Override
                public boolean hasNext() {
                  return i >= from;
                }

                @Override
                public Val next() {
                  return new Val(i--, 0);
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
   * handle. Returns {@code null} if the slot is permanently dead, otherwise the pinned {@link Val}.
   */
  Val pin(long handle) {
    Partition p = partitions[partOf(handle)];
    int rc = p.pin(handle);
    if (rc < 0) {
      return null;
    }
    Val v = p.getPayload(handle);
    v.maybeLoadHint(this);
    if (rc > 0) {
      pinnedCount.increment();
      if (v.fromHot()) hotUnpinned.decrement();
    }
    hits.increment();
    return v;
  }

  /**
   * Non-mutating optimistic check: returns {@code true} if the handle appears live and pinnable.
   */
  boolean pinnable(long handle) {
    Val v = (Val) partitions[partOf(handle)].pinnable(handle);
    if (v == null) {
      return false;
    } else {
      if (PROSPECTIVE_READAHEAD) v.maybeLoadHint(this);
      return true;
    }
  }

  /** Releases a pin on the slot identified by {@code handle}. */
  void unpin(long handle) {
    unpin(handle, true);
  }

  @SuppressWarnings({"ReferenceEquality", "fallthrough"})
  void unpin(long handle, boolean recordAccess) {
    Partition p = partitions[partOf(handle)];
    switch (p.unpin(handle, recordAccess)) {
      case 1:
        hotUnpinned.increment();
        // fallthrough
      case 0:
        pinnedCount.decrement();
        // on last unpin, null out the cached ByteBuffer. recreating is cheap.
        Val v = p.getPayload(handle);
        ByteBuffer cur = v.cached;
        if (cur != null && cur != EXCEPTION_SENTINEL) {
          // CAS ensures we never clobber EXCEPTION_SENTINEL; if it loses, cached is already
          // null or sentinel, both fine. Worst case of a benign loss: another slice() call.
          Val.CACHED.compareAndSet(v, cur, null);
        }
    }
  }

  /**
   * Acquires a pinned slot from a randomly chosen partition. Returns {@code null} if the partition
   * is exhausted (outHandle[0] remains {@link #NULL_HANDLE}); otherwise returns the {@link Val} and
   * sets outHandle[0] to the encoded handle.
   */
  Val acquireNode(long[] outHandle) {
    if (!firstAcquireLogged) {
      firstAcquireLogged = true;
      log.info("acquireNode first call, mapping={}", mapping);
    }
    int i = tlrIndex();
    Partition p = partitions[i];
    Val v = p.acquireNode(outHandle);
    if (v != null) {
      acquisitions.increment();
      pinnedCount.increment();
      if (v.fromHot()) {
        hotAcquisitions.increment();
        hotUnpinned.decrement();
      }
      if (alwaysPrepareWrite || (outHandle[0] >>> 32) > 1) {
        int ret = mapping.prepareWrite(v.cacheBlockOrd);
        if (ret != 0) {
          long n = prepareWriteFailures.incrementAndGet();
          if (n == 1 || n % 1000 == 0) {
            log.warn("prepareWrite failure #{}: ret={} blockIdx={}", n, ret, v.cacheBlockOrd);
          }
        }
      }
      outHandle[0] = encodeHandle(i, outHandle[0]);
      return v;
    } else {
      poolExhausted.increment();
      return null;
    }
  }

  /**
   * Variant of {@link #acquireNode(long[])} that first checks the extant map for a pre-existing
   * warm cache entry matching {@code (blobUUID, blockIdx)}. On a hit, the returned {@link Val}
   * already contains valid decompressed data, so the caller can skip the fetch+decompress step.
   *
   * <p>Falls through to the normal eviction-based acquire if the extant map has no entry or the
   * backing file was not present at startup (ephemeral cache).
   */
  Val acquireNode(long[] outHandle, UUID blobUUID, int blockIdx) {
    int handleLow = extantMapLookup(blobUUID, blockIdx);
    if (handleLow > 0) {
      // Attempt to pin the pre-existing slot at generation=0 (upper 32 bits zero). If the slot
      // was evicted and recycled since startup, its generation will have advanced past 0 and
      // pin() returns null, in which case we fall through to the normal eviction-based acquire.
      // Generation wraps back to 0 after 2^32 evictions of the same slot (~4 billion), which
      // is not a realistic scenario within a single process lifetime.
      long handle = Integer.toUnsignedLong(handleLow);
      Val v = pin(handle);
      if (v != null) {
        // Signal to callers that the pool buffer already holds valid data from a previous run;
        // they can skip populate(). Set before publishing the handle, so any thread that obtains
        // this Val sees populated=true immediately and join() fast-paths to the slice without
        // ever entering the synchronized notify machinery (which exists only for the async cold
        // path where one thread fetches while others wait). Only written from within BlockCache.
        v.populated = true;
        outHandle[0] = handle;
        return v;
      }
    }
    return acquireNode(outHandle);
  }

  /**
   * Binary-searches {@link #extantMap} for {@code (blobUUID, blockIdx)}.
   *
   * @return the handle's low 32 bits ({@code (partIdx << PART_SHIFT) | localSlot}) if found, or
   *     {@code -1}
   */
  private int extantMapLookup(UUID blobUUID, int blockIdx) {
    long searchMsb = blobUUID.getMostSignificantBits();
    long searchLsb = blobUUID.getLeastSignificantBits();
    int lo = 0, hi = extantMap.length / 3 - 1;
    while (lo <= hi) {
      int mid = (lo + hi) >>> 1;
      int base = mid * 3;
      int cmp = Long.compareUnsigned(extantMap[base], searchMsb);
      if (cmp == 0) cmp = Long.compareUnsigned(extantMap[base + 1], searchLsb);
      if (cmp == 0) cmp = Integer.compareUnsigned((int) (extantMap[base + 2] >>> 32), blockIdx);
      if (cmp < 0) {
        lo = mid + 1;
      } else if (cmp > 0) {
        hi = mid - 1;
      } else {
        return (int) extantMap[base + 2]; // handleLow32 in low 32 bits
      }
    }
    return -1;
  }

  public void writeMetrics(MapWriter.EntryWriter ew) throws IOException {
    long prep = prepopulated.sum();
    long pinnedBytes = pinnedCount.sum() * COMPRESSION_BLOCK_SIZE;
    long h = hits.sum();
    long acq = acquisitions.sum();
    long misses = acq - prep;
    long hotUnpinnedBytes = hotUnpinned.sum() * COMPRESSION_BLOCK_SIZE;
    ew.put("outstandingHoldRefs", outstandingHoldRefs.sum());
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
    ew.put("warmStartHits", warmStartHits.sum());
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

  void releaseHint(Val v) {
    mapping.release(v.cacheBlockOrd);
  }

  void recordWarmStartHit() {
    warmStartHits.increment();
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
    return close(handle, null);
  }

  boolean close(long handle, BlockCache.Val lostCAS) {
    Partition p = partitions[partOf(handle)];
    if (lostCAS == null) {
      // Read fromHot() before the close. Only relevant for the non-unconditional path where the
      // slot is in the evictable list (refCount=0); unconditional slots are pinned and not in
      // any queue.
      Val v = p.getPayload(handle);
      boolean fromHot = v.fromHot();
      switch (p.close(handle)) {
        case 0:
          if (fromHot) hotUnpinned.decrement();
          break;
        case -1:
          closeSkippedDead.increment();
          return false;
        default:
          closeSkippedPinned.increment();
          return false;
      }
    } else if (lostCAS.isPopulated()) {
      p.unpin(handle, false);
      closeSkippedPinned.increment();
      return false;
    } else {
      pinnedCount.decrement();
      p.closeUnconditional(handle);
    }
    closedCount.increment();
    return true;
  }

  private int tlrIndex() {
    return ThreadLocalRandom.current().nextInt(nPartitions);
  }

  /**
   * Builds a node-level {@link BlockCache} from system properties:
   *
   * <ul>
   *   <li>{@code solr.blockCache.path} — path to a pre-existing persistent backing file; if the
   *       file exists its size determines the capacity and the cache is warmed from it. If absent
   *       or the file does not exist, an ephemeral temporary file is used instead.
   *   <li>{@code solr.blockCache.kilobytes} — capacity in KiB for the ephemeral fallback (default 1
   *       GiB); ignored when an existing persistent file is found.
   * </ul>
   *
   * <p>Must be set as JVM system properties before startup.
   */
  public static BlockCache buildFromProperties() throws IOException {
    String pathProp = System.getProperty("solr.blockCache.path", "");
    long kilobytes = Long.getLong("solr.blockCache.kilobytes", 1L << 20);
    if (!pathProp.isEmpty()) {
      Path backingFile = Path.of(pathProp);
      if (Files.exists(backingFile)) {
        return new BlockCache(backingFile);
      }
    }
    Path tmpFile =
        Path.of(System.getProperty("java.io.tmpdir"))
            .resolve("solr-block-cache-" + java.util.UUID.randomUUID() + ".tmp");
    return new BlockCache(kilobytes * 1024L, tmpFile); // ephemeral
  }

  @Override
  @SuppressWarnings("try")
  public void close() throws IOException {
    try (Closeable c = SolrMetricProducer.super::close) {
      try {
        // acquire `closeLock` and never release, to prevent further writes
        if (metaBuf != null) {
          if (closeLock.tryWriteLock(10, TimeUnit.SECONDS) == 0) {
            log.warn("could not acquire lock to write cache validation; entries not persistent");
          } else {
            mapping.force(); // sync all data
            int nBlocks = (int) (totalBytes / COMPRESSION_BLOCK_SIZE);
            int trailerOffset = nBlocks * META_BYTES_PER_BLOCK;
            metaBuf.force(); // sync all metadata
            metaBuf.putLong(trailerOffset + 8, randomId ^ CACHE_VALIDATION_MAGIC);
            metaBuf.force(); // record validation
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        drainTask.cancel(true);
        ExecutorUtil.shutdownAndAwaitTermination(drainExec);
        try {
          mapping.close();
        } catch (IOException e) {
          log.warn("Failed to close cache mapping", e);
        }
      }
    }
  }

  private ByteBuffer slice(int cacheBlockOrd) {
    return pool[cacheBlockOrd >> POOL_SHIFT].slice(
        (cacheBlockOrd & POOL_MASK) * COMPRESSION_BLOCK_SIZE, COMPRESSION_BLOCK_SIZE);
  }
}
