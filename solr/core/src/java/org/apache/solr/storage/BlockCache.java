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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadLocalRandom;
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
 * <p>Pin/unpin semantics and the LRU list protocol are inherited from {@link Cache}.
 *
 * <p>The pool is split across N independent {@link Cache.DualQueueCache} instances (one per CPU,
 * rounded to the next power of two). Each pin/unpin/acquire routes to a randomly chosen partition
 * via {@link ThreadLocalRandom}, distributing list-operation contention across partitions without
 * requiring any node-to-partition affinity.
 */
public class BlockCache implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int MAX_BLOCKS_PER_PARTITION = Integer.MAX_VALUE / COMPRESSION_BLOCK_SIZE;

  // ---------------------------------------------------------------------------
  // Node
  // ---------------------------------------------------------------------------

  /**
   * A cache entry: wraps a decompressed block buffer and carries a reference count for safe
   * concurrent eviction.
   *
   * <p>Lifecycle:
   *
   * <ol>
   *   <li>Returned by {@link BlockCache#acquireNode()} pinned (refCount=1), <em>not</em> in the LRU
   *       list.
   *   <li>Caller populates {@link #getValue()} and publishes the node (e.g. via an {@code
   *       AtomicReference} slot). The node is still pinned.
   *   <li>Subsequent callers call {@link Cache#pin(Cache.Node)}, which either re-pins
   *       (refCount&gt;0 → increment only) or first-pins (refCount=0 → remove from list +
   *       increment).
   *   <li>Each caller eventually calls {@link Cache#unpin(Cache.Node)}. The last unpin (refCount→0)
   *       inserts the node at the LRU head (most-recently-used, lowest eviction priority).
   *   <li>When evicted by {@link BlockCache#acquireNode()}, refCount is set to -1 permanently. Any
   *       reader that encounters the node via a stale slot sees the negative count, fails {@link
   *       Cache#pin(Cache.Node)}, and falls back to loading.
   * </ol>
   */
  public static final class Node extends Cache.Node<ByteBuffer> {

    /**
     * Completion signal: fulfilled with {@code value} by {@link #populate} on the winning thread;
     * other threads joining this node wait here until the buffer is ready (or failed).
     */
    private final CompletableFuture<ByteBuffer> future = new CompletableFuture<>();

    private Node(ByteBuffer prepopulated) {
      super(null, null, Integer.MAX_VALUE >> 1);
      future.complete(prepopulated);
    }

    Node(ByteBuffer buf, Cache.Node<ByteBuffer> prev, int initialRefCount) {
      super(buf, prev, initialRefCount);
    }

    ByteBuffer populate(byte[] arr, int off, int len) {
      ByteBuffer value = getValue();
      assert value != null;
      value.clear().put(arr, off, len);
      future.complete(value);
      return value;
    }

    /**
     * Waits for this node's buffer to be populated, blocking if necessary.
     *
     * @throws CompletionException if population failed
     */
    public ByteBuffer join() {
      return future.join();
    }

    /** Marks this node as failed, unblocking any threads waiting in {@link #join()}. */
    public boolean completeExceptionally(Throwable t) {
      return future.completeExceptionally(t);
    }
  }

  // ---------------------------------------------------------------------------
  // Partition
  // ---------------------------------------------------------------------------

  private static final class Partition extends Cache.DualQueueCache<ByteBuffer, Node> {
    Partition(List<ByteBuffer> pool) {
      super(pool, true);
    }

    @Override
    protected BlockCache.Node createNode(
        ByteBuffer value, Cache.Node<ByteBuffer> prev, int initialRefCount) {
      return new BlockCache.Node(value, prev, initialRefCount);
    }
  }

  /** Creates a synthetic node not backed by a pool slot (e.g. an always-pinned tail block). */
  Node createTailNode(ByteBuffer tailBuf) {
    return new Node(tailBuf);
  }

  // ---------------------------------------------------------------------------
  // Construction
  // ---------------------------------------------------------------------------

  private final Partition[] partitions;

  /**
   * Creates a new block cache backed by a freshly-created temp file. The file is deleted
   * immediately after mapping so it does not outlive the JVM.
   */
  public BlockCache(long targetBytes, Path backingFile) throws IOException {
    ByteBuffer[] pool = initPool(targetBytes, backingFile, true);
    this.partitions = distribute(pool);
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
    ByteBuffer[] pool = initPool(targetBytes, existingBackingFile, false);
    this.partitions = distribute(pool);
    log.info(
        "BlockCache initialized from existing file {}: nBlocks={}, targetBytes={}, nPartitions={}",
        existingBackingFile,
        pool.length,
        targetBytes,
        partitions.length);
  }

  private static Partition[] distribute(ByteBuffer[] pool) {
    int nBlocks = pool.length;
    int nPartitions = computeNPartitions(nBlocks);
    Partition[] parts = new Partition[nPartitions];
    int base = nBlocks / nPartitions;
    int remainder = nBlocks % nPartitions;
    int offset = 0;
    List<ByteBuffer> poolList = Arrays.asList(pool);

    for (int i = 0; i < nPartitions; i++) {
      int count = base + (i < remainder ? 1 : 0);
      parts[i] = new Partition(poolList.subList(offset, offset + count));
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
   * Pins {@code node}. Partition-agnostic: delegates to partition 0 because {@link Cache#pin}
   * operates solely on the node's refCount and list pointers, with no partition-local state.
   *
   * <p>{@link Cache#pin(Cache.Node)} is effectively a static method, so it doesn't matter which
   * queue we call it on. TODO: make it <i>actually</i> static, for clarity?
   */
  boolean pin(Node node) {
    return partitions[0].pin(node);
  }

  /**
   * Releases a pin, routing the node to a randomly chosen partition's LRU head. No node-to-
   * partition affinity is required: the ByteBuffer value is valid in any partition's pool.
   */
  void unpin(Node node) {
    unpin(node, true);
  }

  void unpin(Node node, boolean recordAccess) {
    partitions[tlrIndex()].unpin(node, recordAccess);
  }

  /**
   * Acquires a pinned node from a randomly chosen partition, falling back to other partitions if
   * the first is fully pinned. Returns {@code null} only if all partitions are exhausted.
   */
  Node acquireNode() {
    return partitions[tlrIndex()].acquireNode();
  }

  /**
   * Recycles a node back to the pool at the eviction-tail of a randomly chosen partition (making it
   * a high-priority reuse candidate). Used when a node was acquired but ultimately not needed (e.g.
   * lost CAS race).
   */
  boolean close(Node node) {
    return partitions[tlrIndex()].close(node);
  }

  private int tlrIndex() {
    return ThreadLocalRandom.current().nextInt(partitions.length);
  }

  @Override
  public void close() {
    // MappedByteBuffers are not explicitly unmapped here; the JVM will release them on exit.
    // TODO: add explicit unmap via ByteBufferGuard / unmapHack if needed.
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
  private static ByteBuffer[] initPool(long targetBytes, Path backingFile, boolean createAndDelete)
      throws IOException {
    final int nBlocks = Math.toIntExact(targetBytes / COMPRESSION_BLOCK_SIZE);
    final ByteBuffer[] pool = new ByteBuffer[nBlocks];
    final long blockSizeL = COMPRESSION_BLOCK_SIZE;
    // Round partition size down to a 2 MiB boundary (matches HeapCacheFbsModifier convention).
    final long partitionMaxBytes = ((long) MAX_BLOCKS_PER_PARTITION * blockSizeL >> 21) << 21;
    final int effectiveMaxBlocksPerPartition = Math.toIntExact(partitionMaxBytes / blockSizeL);
    final int numPartitions = ((nBlocks - 1) / effectiveMaxBlocksPerPartition) + 1;

    Set<StandardOpenOption> openOpts =
        EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE);
    if (createAndDelete) {
      openOpts.add(StandardOpenOption.CREATE_NEW);
    }
    try (FileChannel fc = FileChannel.open(backingFile, openOpts)) {
      if (createAndDelete) {
        fc.truncate(nBlocks * blockSizeL);
      }

      int blockIdx = 0;
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
        partition.order(ByteOrder.LITTLE_ENDIAN);
        for (int j = 0; j < partitionNumBlocks; j++) {
          pool[blockIdx++] =
              partition
                  .slice(j * COMPRESSION_BLOCK_SIZE, COMPRESSION_BLOCK_SIZE)
                  .order(ByteOrder.LITTLE_ENDIAN);
        }
        partitionNumBlocks = effectiveMaxBlocksPerPartition;
      }
    } finally {
      if (createAndDelete) {
        Files.delete(backingFile);
      }
    }
    return pool;
  }
}
