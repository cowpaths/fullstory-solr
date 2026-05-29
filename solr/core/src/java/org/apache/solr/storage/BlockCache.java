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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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
 */
public class BlockCache extends Cache implements Closeable {

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
   *   <li>Caller populates {@link #buf} and publishes the node (e.g. via an {@code AtomicReference}
   *       slot). The node is still pinned.
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
  public static final class Node extends Cache.Node {

    /** Decompressed block content. */
    final ByteBuffer buf;

    /**
     * Completion signal: fulfilled with {@link #buf} by {@link #populate} on the winning thread;
     * other threads joining this node wait here until the buffer is ready (or failed).
     */
    private final CompletableFuture<ByteBuffer> future = new CompletableFuture<>();

    private Node(ByteBuffer buf, Cache.Node prev, int initialRefCount) {
      super(prev, initialRefCount);
      this.buf = buf;
    }

    ByteBuffer populate(byte[] arr, int off, int len) {
      buf.clear().put(arr, off, len);
      future.complete(buf);
      return buf;
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
  // Construction
  // ---------------------------------------------------------------------------

  public BlockCache(long targetBytes, Path backingFile) throws IOException {
    int nBlocks = Math.toIntExact(targetBytes / COMPRESSION_BLOCK_SIZE);

    // Wire all pool buffers directly into the LRU list in a single pass. No CAS needed here —
    // init is single-threaded. Each node starts evictable (refCount=0) and in the list.
    ByteBuffer[] pool = initPool(nBlocks, backingFile);
    Cache.Node prev = lruHead;
    for (ByteBuffer buf : pool) {
      Node node = new Node(buf, prev, 0);
      prev.next.set(node);
      prev = node;
    }
    prev.next.set(lruTail);
    lruTail.prev = prev;

    log.info("BlockCache initialized: nBlocks={}, targetBytes={}", nBlocks, targetBytes);
  }

  /**
   * Allocates the pool as slices of a file-backed memory-mapped region (adapted from {@code
   * HeapCacheFbsModifier.poolFileBacked}). The backing file is deleted immediately after mapping so
   * that it does not outlive the JVM.
   */
  private static ByteBuffer[] initPool(int nBlocks, Path backingFile) throws IOException {
    final ByteBuffer[] pool = new ByteBuffer[nBlocks];
    final long blockSizeL = COMPRESSION_BLOCK_SIZE;
    // Round partition size down to a 2 MiB boundary (matches HeapCacheFbsModifier convention).
    final long partitionMaxBytes = ((long) MAX_BLOCKS_PER_PARTITION * blockSizeL >> 21) << 21;
    final int effectiveMaxBlocksPerPartition = Math.toIntExact(partitionMaxBytes / blockSizeL);
    final int numPartitions = ((nBlocks - 1) / effectiveMaxBlocksPerPartition) + 1;

    try (FileChannel fc =
        FileChannel.open(
            backingFile,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE_NEW)) {
      fc.truncate(nBlocks * blockSizeL);

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
      Files.delete(backingFile);
    }
    return pool;
  }

  // ---------------------------------------------------------------------------
  // API
  // ---------------------------------------------------------------------------

  /**
   * Acquires a pinned {@link Node} whose buffer is ready to be populated with decompressed block
   * content. The caller must eventually call {@link #unpin(Cache.Node)}.
   *
   * <p>By the LRU invariant all nodes in the list have refCount==0, so the tail node is always
   * evictable. The returned node is <em>not</em> in the list; it will be re-inserted at the head on
   * the final {@link #unpin(Cache.Node)}.
   *
   * <p>Returns {@code null} if the list is empty (all blocks are pinned). In that case the caller
   * should decompress into a temporary heap buffer, serve the read directly, and discard it.
   */
  public Node acquireNode() {
    for (Cache.Node candidateBase; (candidateBase = lruTail.prev) != lruHead; ) {
      if (candidateBase.refCount.compareAndSet(0, -1)) {
        if (!removeFromList(candidateBase)) {
          throw new IllegalStateException();
        }
        // Safe cast: all non-sentinel nodes in this cache's list are BlockCache.Node instances.
        Node candidate = (Node) candidateBase;
        // Return a new Node wrapping the same buffer, pinned (refCount=1), not in list.
        return new Node(candidate.buf, null, 1);
      }
      // CAS failed: a concurrent pin() or acquireNode() just claimed this node and is in the
      // middle of removeFromList(). Spin briefly; lruTail.prev will change momentarily.
    }
    return null; // list empty — all blocks pinned
  }

  /**
   * Explicitly releases a node when its owning {@code IndexInput} is closed. If the node is still
   * evictable (refCount==0 and in the list), marks it dead and recycles the buffer back into the
   * pool at the tail, making it the highest-priority eviction candidate. If the node has already
   * been evicted or is currently pinned, this is a no-op.
   */
  void close(Node node) {
    if (!node.refCount.compareAndSet(0, -1)) {
      // pinned or already dead — best effort, bail
      return;
    }
    if (removeFromList(node)) {
      insertAtTail(node.buf);
    } else {
      throw new IllegalStateException();
    }
  }

  private void insertAtTail(ByteBuffer buf) {
    for (; ; ) {
      Cache.Node pred = lruTail.prev;
      Cache.Node oldNext = reserve(pred, RESERVED);
      if (oldNext != lruTail) {
        if (oldNext == REMOVED) {
          continue; // pred was concurrently removed; retry
        }
        // release reservation; pred is no longer tail's predecessor
        if (!pred.next.compareAndSet(RESERVED, oldNext)) {
          throw new IllegalStateException();
        }
        continue;
      }
      Node node = new Node(buf, pred, 0);
      node.next.set(lruTail);
      lruTail.prev = node;
      if (!pred.next.compareAndSet(RESERVED, node)) {
        throw new IllegalStateException("unexpected concurrent modification during tail insertion");
      }
      return;
    }
  }

  @Override
  public void close() {
    // MappedByteBuffers are not explicitly unmapped here; the JVM will release them on exit.
    // TODO: add explicit unmap via ByteBufferGuard / unmapHack if needed.
  }
}
