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
import java.util.EnumSet;
import java.util.Set;
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
public class BlockCache extends Cache.DualQueueCache<ByteBuffer, BlockCache.Node>
    implements Closeable {

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

    private Node(ByteBuffer buf, Cache.Node<ByteBuffer> prev, int initialRefCount) {
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

    /**
     * Completes this node with an externally-owned buffer (e.g. an mmap slice) without copying.
     * Unlike {@link #populate}, the node's {@link #getValue()} is not used; {@code buf} is stored
     * directly as the future's result. Intended for synthetic always-pinned nodes (such as the
     * local tail block) that are not backed by a pool slot.
     *
     * <p>TODO: revisit — there may be a cleaner way to model this without a separate method.
     */
    ByteBuffer populateDirect(ByteBuffer buf) {
      future.complete(buf);
      return buf;
    }

    /** Marks this node as failed, unblocking any threads waiting in {@link #join()}. */
    public boolean completeExceptionally(Throwable t) {
      return future.completeExceptionally(t);
    }
  }

  // ---------------------------------------------------------------------------
  // Construction
  // ---------------------------------------------------------------------------

  /**
   * Creates a new block cache backed by a freshly-created temp file. The file is deleted
   * immediately after mapping so it does not outlive the JVM.
   */
  public BlockCache(long targetBytes, Path backingFile) throws IOException {
    super(initPool(targetBytes, backingFile, true), true);
    log.info(
        "BlockCache initialized: nBlocks={}, targetBytes={}",
        targetBytes / COMPRESSION_BLOCK_SIZE,
        targetBytes);
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
    super(initPool(targetBytes, existingBackingFile, false), true);
    log.info(
        "BlockCache initialized from existing file {}: nBlocks={}, targetBytes={}",
        existingBackingFile,
        targetBytes / COMPRESSION_BLOCK_SIZE,
        targetBytes);
  }

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

  // ---------------------------------------------------------------------------
  // API
  // ---------------------------------------------------------------------------

  /**
   * Creates a {@link BlockCache.Node} carrying the given buffer. Overrides the base factory so that
   * all nodes inserted into this cache's list are of type {@link BlockCache.Node} and can be safely
   * cast on acquisition.
   */
  @Override
  protected Node createNode(ByteBuffer value, Cache.Node<ByteBuffer> prev, int initialRefCount) {
    return new Node(value, prev, initialRefCount);
  }

  @Override
  public void close() {
    // MappedByteBuffers are not explicitly unmapped here; the JVM will release them on exit.
    // TODO: add explicit unmap via ByteBufferGuard / unmapHack if needed.
  }
}
