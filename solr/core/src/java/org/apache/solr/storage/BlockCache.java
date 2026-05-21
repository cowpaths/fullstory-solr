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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fixed-size block cache backed by a memory-mapped file. Manages an LRU queue of decompressed
 * blocks, evicting the least-recently-used evictable block when the pool is exhausted.
 *
 * <p>If eviction finds all blocks pinned, {@link #acquireNode()} returns {@code null} and the
 * caller is expected to decompress the block into a temporary heap buffer and serve the read
 * uncached.
 */
public class BlockCache implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int MAX_BLOCKS_PER_PARTITION = Integer.MAX_VALUE / COMPRESSION_BLOCK_SIZE;

  // ---------------------------------------------------------------------------
  // LRU node (adapted from ReferenceHandler.Ref)
  // ---------------------------------------------------------------------------

  /**
   * A cache entry: wraps a decompressed block buffer, carries a reference count for safe concurrent
   * eviction, and the doubly-linked list pointers used for LRU ordering.
   *
   * <p>Each {@code Node} instance is a one-off wrapper for a single acquire/reclaim cycle of its
   * buffer. Once evicted, {@code refCount} is set to -1 permanently; any reader that subsequently
   * encounters the node via a stale {@code accessMapped} slot will see the negative count, fail
   * {@link #pin(Node)}, and fall back to loading.
   *
   * <p>Lifecycle:
   *
   * <ol>
   *   <li>Returned by {@link BlockCache#acquireNode()} pinned (refCount=1), not yet in LRU
   *       (next==null).
   *   <li>Caller populates {@link #buf}, calls {@link BlockCache#unpin(Node)}. First time refCount
   *       reaches 0 (next==null), the node inserts itself at the LRU head and becomes evictable.
   *   <li>Caller CAS-sets the {@code accessMapped} slot. Unpin must precede this publication so
   *       that any node reachable by readers is already in the LRU.
   *   <li>Subsequent readers call {@link BlockCache#pin(Node)}, which re-pins and moves the node to the LRU head.
   *   <li>When evicted, refCount is permanently set to -1; stale slot references self-detect.
   * </ol>
   *
   * <p>Head of the list = most recently used; tail = least recently used / eviction candidate.
   */
  public static final class Node {

    /** Decompressed block content. {@code null} for sentinel nodes only. */
    public final ByteBuffer buf;

    /**
     * Reference count.
     *
     * <ul>
     *   <li>1 — freshly acquired, pinned
     *   <li>&gt;1 — pinned by multiple readers
     *   <li>0 — evictable
     *   <li>-1 — permanently dead (evicted); node must not be used
     * </ul>
     */
    private final AtomicInteger refCount;

    /** LRU list link toward the head (most-recently-used end). */
    private final AtomicReference<Node> next = new AtomicReference<>();

    /** LRU list link toward the tail (least-recently-used end). */
    private volatile Node prev;

    private Node(ByteBuffer buf, Node prev, int initialRefCount) {
      this.buf = buf;
      this.prev = prev;
      this.refCount = new AtomicInteger(initialRefCount);
    }

    /** Sentinel constructor (head / tail / protocol sentinels). */
    private Node() {
      this.buf = null;
      this.refCount = null;
    }

  }

  // Sentinels for the lock-free linked-list protocol, mirroring ReferenceHandler.
  // RESERVED: this node's `next` link is currently being modified by exactly one thread.
  // REMOVED:  this node has been spliced out of the list.
  private static final Node RESERVED = new Node();
  private static final Node REMOVED = new Node();

  /** Most-recently-used sentinel. Real nodes are inserted immediately after this. */
  private final Node lruHead = new Node();

  /** Least-recently-used sentinel. Eviction candidates are immediately before this. */
  private final Node lruTail = new Node();

  /** Count of nodes with refCount==0 (evictable). Consulted only when eviction fails. */
  private final LongAdder evictableCount = new LongAdder();

  // ---------------------------------------------------------------------------
  // Construction
  // ---------------------------------------------------------------------------

  public BlockCache(long targetBytes, Path backingFile) throws IOException {
    int nBlocks = Math.toIntExact(targetBytes / COMPRESSION_BLOCK_SIZE);

    // Wire up the sentinel doubly-linked list: head <-> tail.
    lruHead.next.set(lruTail);
    lruTail.prev = lruHead;

    // Wire all pool buffers directly into the LRU list in a single pass. No CAS needed here —
    // init is single-threaded. Each node starts evictable (refCount=0).
    ByteBuffer[] pool = initPool(nBlocks, backingFile);
    Node prev = lruHead;
    for (ByteBuffer buf : pool) {
      Node node = new Node(buf, prev, 0);
      prev.next.set(node);
      prev = node;
    }
    prev.next.set(lruTail);
    lruTail.prev = prev;
    evictableCount.add(nBlocks);

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
  // LRU list operations (adapted from ReferenceHandler)
  // ---------------------------------------------------------------------------

  /**
   * Atomically acquires the exclusive right to modify {@code ref.next}. Spins while the link is
   * held by another thread (RESERVED), then CAS-swaps it to {@code reservation} and returns the
   * prior value. Returns REMOVED immediately if the node has already been spliced out.
   */
  private static Node reserve(Node ref, Node reservation) {
    Node next = ref.next.get();
    for (; ; ) {
      while (next == RESERVED) {
        if (reservation == REMOVED) {
          Thread.yield();
        }
        next = ref.next.get();
      }
      if (next == REMOVED) {
        return next;
      }
      Node extant = ref.next.compareAndExchange(next, reservation);
      if (extant == next) {
        return next;
      }
      next = extant;
    }
  }

  private void insertAtHead(Node node) {
    Node oldNext = reserve(lruHead, RESERVED);
    assert oldNext != REMOVED : "lruHead sentinel should never be removed";
    node.next.set(oldNext);
    oldNext.prev = node;
    if (!lruHead.next.compareAndSet(RESERVED, node)) {
      throw new IllegalStateException("unexpected concurrent modification of lruHead.next");
    }
  }

  private void insertAtTail(ByteBuffer buf) {
    for (; ; ) {
      Node pred = lruTail.prev;
      Node oldNext = reserve(pred, RESERVED);
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

  private boolean removeFromList(Node node) {
    Node next = reserve(node, REMOVED);
    if (next == REMOVED) {
      return false;
    }
    Node prev;
    for (; ; ) {
      prev = node.prev;
      if (prev.next.compareAndSet(node, RESERVED)) {
        break;
      }
      Thread.yield();
    }
    next.prev = prev;
    if (!prev.next.compareAndSet(RESERVED, next)) {
      throw new IllegalStateException("unexpected concurrent modification during list removal");
    }
    return true;
  }

  // ---------------------------------------------------------------------------
  // API
  // ---------------------------------------------------------------------------

  /**
   * Pins {@code node} for the duration of a read, preventing eviction. If the node is already in
   * the LRU, also moves it to the head (touch). Returns {@code false} if the node is permanently
   * dead (evicted); the caller must then fall back to loading.
   */
  boolean pin(Node node) {
    int rc;
    do {
      rc = node.refCount.get();
      if (rc < 0) {
        return false;
      }
    } while (!node.refCount.compareAndSet(rc, rc + 1));
    if (rc == 0) {
      evictableCount.decrement();
    }
    touch(node);
    return true;
  }

  private void touch(Node node) {
    if (removeFromList(node)) {
      node.prev = lruHead;
      insertAtHead(node);
    }
  }

  /** Releases a read pin. */
  void unpin(Node node) {
    if (node.refCount.decrementAndGet() == 0) {
      evictableCount.increment();
    }
  }

  /**
   * Explicitly releases a node when its owning {@code IndexInput} is closed. If the node is still
   * evictable (refCount==0), marks it dead and recycles the buffer back into the pool at the tail,
   * making it the highest-priority eviction candidate. If the node has already been evicted
   * concurrently, this is a no-op.
   */
  void close(Node node) {
    if (!node.refCount.compareAndSet(0, -1)) {
      // still referenced somehow? best effort, just bail
      return;
    }
    if (!removeFromList(node)) {
      throw new IllegalStateException();
    }
    insertAtTail(node.buf);
  }

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /**
   * Acquires a pinned {@link Node} whose buffer is ready to be populated with decompressed block
   * content. The caller must eventually call {@link #unpin(Node)}.
   *
   * <p>Returns {@code null} if all blocks are pinned. In that case the caller should decompress
   * into a temporary heap buffer, serve the read directly, and discard it — the block will not be
   * cached for this access.
   */
  public Node acquireNode() {
    for (Node candidate; (candidate = lruTail.prev) != lruHead; ) {
      if (candidate.refCount.compareAndSet(0, -1)) {
        evictableCount.decrement();
        if (!removeFromList(candidate)) {
          throw new IllegalStateException();
        }
        Node node = new Node(candidate.buf, lruHead, 1);
        insertAtHead(node);
        return node;
      }
      if (evictableCount.sum() == 0) {
        return null;
      }
    }
    return null; // all nodes pinned
  }

  @Override
  public void close() {
    // MappedByteBuffers are not explicitly unmapped here; the JVM will release them on exit.
    // TODO: add explicit unmap via ByteBufferGuard / unmapHack if needed.
  }
}
