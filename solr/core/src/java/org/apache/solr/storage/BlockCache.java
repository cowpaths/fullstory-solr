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
   * eviction, a back-reference to the {@code accessMapped} slot that holds it, and the doubly-linked
   * list pointers used for LRU ordering.
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
     *   <li>0 — evictable
     *   <li>&gt;0 — pinned by one or more active readers
     *   <li>-1 — claimed by the eviction path; prevents concurrent pinning
     * </ul>
     */
    final AtomicInteger refCount = new AtomicInteger(0);

    /**
     * Back-reference to the {@code accessMapped} slot that holds this node. The eviction path
     * clears this slot (via CAS) before removing the node from the LRU list, so readers that race
     * with eviction will see {@code null} and fall back to loading.
     */
    volatile AtomicReference<Node> slot;

    /** LRU list link toward the head (most-recently-used end). */
    final AtomicReference<Node> next = new AtomicReference<>();

    /** LRU list link toward the tail (least-recently-used end). */
    volatile Node prev;

    Node(ByteBuffer buf, Node prev) {
      this.buf = buf;
      this.prev = prev;
    }

    /** Sentinel constructor (head / tail / protocol sentinels). */
    private Node() {
      this.buf = null;
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

  // ---------------------------------------------------------------------------
  // Pool
  // ---------------------------------------------------------------------------

  private final ByteBuffer[] pool;
  private final int nBlocks;

  /**
   * Stack pointer into {@code pool}. {@code pool[top-1]} is the next never-yet-used buffer.
   * When this reaches zero all buffers are in the LRU and eviction is needed.
   */
  private final AtomicInteger top;

  // ---------------------------------------------------------------------------
  // Construction
  // ---------------------------------------------------------------------------

  public BlockCache(long targetBytes, Path backingFile) throws IOException {
    this.nBlocks = Math.toIntExact(targetBytes / COMPRESSION_BLOCK_SIZE);
    this.pool = new ByteBuffer[nBlocks];

    // Wire up the sentinel doubly-linked list: head <-> tail.
    lruHead.next.set(lruTail);
    lruTail.prev = lruHead;

    initPool(backingFile);
    this.top = new AtomicInteger(nBlocks);

    log.info(
        "BlockCache initialized: nBlocks={}, targetBytes={}",
        nBlocks,
        targetBytes);
  }

  /**
   * Allocates the pool as slices of a file-backed memory-mapped region (adapted from
   * {@code HeapCacheFbsModifier.poolFileBacked}). The backing file is deleted immediately after
   * mapping so that it does not outlive the JVM.
   */
  private void initPool(Path backingFile) throws IOException {
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

  /** Inserts {@code node} immediately after the LRU head (most-recently-used position). */
  private void insertAtHead(Node node) {
    node.prev = lruHead;
    Node oldNext = reserve(lruHead, RESERVED);
    assert oldNext != REMOVED : "lruHead sentinel should never be removed";
    oldNext.prev = node;
    node.next.set(oldNext);
    if (!lruHead.next.compareAndSet(RESERVED, node)) {
      throw new IllegalStateException("unexpected concurrent modification of lruHead.next");
    }
  }

  /**
   * Removes {@code node} from the LRU list. Returns {@code false} if the node was already removed
   * (e.g., concurrent eviction).
   */
  private boolean removeFromList(Node node) {
    Node next = reserve(node, REMOVED);
    if (next == REMOVED) {
      return false;
    }
    // Lock the predecessor's next link so we can splice around `node`.
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
  // Public API
  // ---------------------------------------------------------------------------

  /**
   * Acquires a node whose buffer is ready to be populated with decompressed block content.
   *
   * <p>Returns {@code null} if the pool is exhausted <em>and</em> no evictable block can be found
   * (all blocks pinned). In that case the caller should decompress into a temporary heap buffer,
   * serve the read directly, and discard the buffer — the block will not be cached for this access.
   */
  public Node acquireNode() {
    for (int t = top.get(); ; ) {
      if (t > 0) {
        if (top.compareAndSet(t, t - 1)) {
          return new Node(pool[t - 1], null);
        }
        t = top.get();
      } else {
        return evictOne();
      }
    }
  }

  /**
   * Registers {@code node} in the LRU list and stores its back-reference slot. Must be called
   * after the caller has successfully CAS-set the corresponding {@code accessMapped} slot to
   * {@code node}, so that the slot reference is always valid when seen by the eviction path.
   */
  public void registerNode(Node node, AtomicReference<Node> slot) {
    node.slot = slot;
    insertAtHead(node);
  }

  /**
   * Moves {@code node} to the head of the LRU queue to record a recent access. The caller must
   * hold a pin on {@code node} (i.e., have called {@link #pin} successfully) before calling this.
   */
  public void touchNode(Node node) {
    removeFromList(node);
    insertAtHead(node);
  }

  /**
   * Pins {@code node} for the duration of a read, preventing eviction. Returns {@code false} if
   * the node is concurrently being evicted; the caller must then fall back to loading.
   */
  public boolean pin(Node node) {
    int rc;
    do {
      rc = node.refCount.get();
      if (rc < 0) {
        return false; // eviction in progress
      }
    } while (!node.refCount.compareAndSet(rc, rc + 1));
    return true;
  }

  /** Releases a read pin on {@code node}, making it eligible for eviction again once count → 0. */
  public void unpin(Node node) {
    node.refCount.decrementAndGet();
  }

  /**
   * Walks from the LRU tail toward the head to find the oldest evictable (refCount == 0) node.
   * Atomically claims it by setting refCount to -1, clears its {@code accessMapped} slot, removes
   * it from the list, and resets its state for reuse.
   *
   * @return the evicted node, ready for repopulation, or {@code null} if all nodes are pinned.
   */
  private Node evictOne() {
    Node candidate = lruTail.prev;
    while (candidate != lruHead) {
      // Skip nodes that are already being spliced out by another thread.
      if (candidate.next.get() != REMOVED && candidate.refCount.compareAndSet(0, -1)) {
        // We own this node for eviction.
        AtomicReference<Node> slot = candidate.slot;
        if (slot != null) {
          slot.compareAndSet(candidate, null);
          candidate.slot = null;
        }
        removeFromList(candidate);
        candidate.refCount.set(0); // reset for reuse
        return candidate;
      }
      candidate = candidate.prev;
    }
    return null; // all nodes pinned
  }

  @Override
  public void close() {
    // MappedByteBuffers are not explicitly unmapped here; the JVM will release them on exit.
    // TODO: add explicit unmap via ByteBufferGuard / unmapHack if needed.
  }
}
