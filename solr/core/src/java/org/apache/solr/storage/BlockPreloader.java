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

import static org.apache.solr.storage.CompressingDirectory.COMPRESSION_BLOCK_SHIFT;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.lucene.codecs.CodecUtil;
import java.util.function.IntUnaryOperator;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.internal.hppc.IntCursor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ThreadInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared preload/readahead utilities for {@link BlockCache}-backed index directories.
 *
 * <p>Provides the core {@link #ensureLoaded} / {@link #ensureLoadedSerial} / {@link #populateBuf} / {@link #parseCfeBlockIndexes}
 * operations, plus the segment-chaining skeleton {@link #readAheadSegs} / {@link
 * #finishReadAheadSeg}, used by both {@link GCSDirectory} and {@link AccessDirectory2}.
 * Backend-specific concerns (supplier, permit acquisition, per-segment work) are abstracted via
 * {@link BlockSupplier}, {@link Permits}, and {@link SegmentPreloadTask}.
 */
class BlockPreloader {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Supplies decompressed bytes for a single compressed block. Implementations are responsible for
   * reading and decompressing the block from the storage backend (e.g., GCS, local filesystem).
   */
  @FunctionalInterface
  interface BlockSupplier {
    /**
     * @param blockOffset byte offset of the compressed block within the backend storage
     * @param compressedLen number of compressed bytes to read
     * @param decompressedLen expected decompressed size in bytes
     * @return ByteBuffer with exactly {@code decompressedLen} decompressed bytes remaining
     */
    byte[] supply(long blockOffset, int compressedLen, int decompressedLen) throws IOException;
  }

  /**
   * Controls how many concurrent preload tasks may be in flight. Implementations handle backend-
   * specific resource constraints (e.g., semaphore + open-channel limit for GCS, plain semaphore
   * for local I/O).
   */
  interface Permits {
    /**
     * Attempt to acquire a permit. Returns {@code true} if acquired, {@code false} if the timeout
     * elapsed or a throttle limit was reached. Must not block longer than {@code timeoutMillis}
     * milliseconds.
     */
    boolean tryAcquire(int timeoutMillis);

    /** Release a previously acquired permit. */
    void release();
  }

  /** Returns a {@link Permits} instance backed by a plain {@link Semaphore}. */
  static Permits ofSemaphore(Semaphore s) {
    return new Permits() {
      @Override
      public boolean tryAcquire(int timeoutMillis) {
        try {
          return s.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new ThreadInterruptedException(e);
        }
      }

      @Override
      public void release() {
        s.release();
      }
    };
  }

  /**
   * Ensures block {@code idx} is asynchronously loaded into {@code accessMapped}. Returns {@code
   * true} if the block is already present/pinnable or an async load was scheduled; {@code false} if
   * the cache or permit pool is exhausted (the caller should stop issuing further read-ahead).
   *
   * <p>The {@code decompressedLen} must be computed correctly by the caller: {@link
   * CompressingDirectory#COMPRESSION_BLOCK_SIZE} for all blocks except the last, where it is the
   * remaining byte count.
   */
  static boolean ensureLoaded(
      AtomicReferenceArray<Cache.Node<BlockCache.Val>> accessMapped,
      long[] blockOffsets,
      int idx,
      int decompressedLen,
      BlockCache cache,
      ExecutorService ioExec,
      Permits permits,
      int timeoutMillis,
      BlockSupplier supplier) {
    Cache.Node<BlockCache.Val> extant = accessMapped.get(idx);
    if ((extant == null || !extant.pinnable()) && permits.tryAcquire(timeoutMillis)) {
      Cache.Node<BlockCache.Val> toPopulate = cache.acquireNode();
      if (toPopulate == null) {
        permits.release();
        return false;
      }
      try {
        ioExec.submit(
            () -> {
              try {
                if (accessMapped.compareAndSet(idx, extant, toPopulate)) {
                  long blockOffset = blockOffsets[idx];
                  int compressedLen = (int) (blockOffsets[idx + 1] - blockOffset);
                  populateBuf(
                      blockOffset,
                      compressedLen,
                      idx,
                      decompressedLen,
                      toPopulate,
                      accessMapped,
                      cache,
                      supplier);
                  // NOTE: don't unpin in `finally`! populateBuf already unpins on the error path.
                  cache.unpin(toPopulate, false);
                } else {
                  cache.close(toPopulate, true);
                }
              } finally {
                permits.release();
              }
              return null;
            });
      } catch (Throwable t) {
        permits.release();
        throw t;
      }
    }
    return true;
  }

  /**
   * Like {@link #ensureLoaded}, but loads a sequence of blocks serially within a single {@code
   * ioExec} task (one permit covers all blocks). Appropriate for spinning-disk backends where
   * sequential I/O is preferred over parallel I/O. Returns {@code true} if the task was submitted
   * (or all blocks were already loaded); {@code false} if the permit could not be acquired.
   *
   * <p>{@code decompressedLen} is a function from block index to decompressed byte count; callers
   * should return {@link CompressingDirectory#COMPRESSION_BLOCK_SIZE} for all blocks except the
   * last, where it is the remaining byte count.
   */
  static boolean ensureLoadedSerial(
      AtomicReferenceArray<Cache.Node<BlockCache.Val>> accessMapped,
      long[] blockOffsets,
      Iterator<IntCursor> blockIdxIter,
      IntUnaryOperator decompressedLen,
      BlockCache cache,
      ExecutorService ioExec,
      Permits permits,
      int timeoutMillis,
      BlockSupplier supplier) {
    // TODO: pre-inspect at least some of the blocks (analogous to the extant.pinnable() check in
    // ensureLoaded) to avoid acquiring a permit when all requested blocks are already cached.
    if (!permits.tryAcquire(timeoutMillis)) return false;
    try {
      ioExec.submit(
          () -> {
            try {
              while (blockIdxIter.hasNext()) {
                int idx = blockIdxIter.next().value;
                Cache.Node<BlockCache.Val> extant = accessMapped.get(idx);
                if (extant != null && extant.pinnable()) continue;
                Cache.Node<BlockCache.Val> toPopulate = cache.acquireNode();
                if (toPopulate == null) return null; // cache full — stop
                long blockOffset = blockOffsets[idx];
                int compressedLen = (int) (blockOffsets[idx + 1] - blockOffset);
                if (accessMapped.compareAndSet(idx, extant, toPopulate)) {
                  populateBuf(
                      blockOffset,
                      compressedLen,
                      idx,
                      decompressedLen.applyAsInt(idx),
                      toPopulate,
                      accessMapped,
                      cache,
                      supplier);
                  cache.unpin(toPopulate, false);
                } else {
                  cache.close(toPopulate, true);
                }
              }
            } finally {
              permits.release();
            }
            return null;
          });
    } catch (Throwable t) {
      permits.release();
      throw t;
    }
    return true;
  }

  /**
   * Populates {@code node} with decompressed bytes from {@code supplier}, then marks it complete.
   * On failure, marks the node exceptionally, removes it from {@code accessMapped}, and unpins and
   * closes it.
   *
   * <p>Callers <em>must not</em> unpin {@code node} in a {@code finally} block surrounding this
   * call: this method already unpins on the error path, and the caller must call {@code
   * cache.unpin(node, false)} only on success.
   */
  static ByteBuffer populateBuf(
      long blockOffset,
      int compressedLen,
      int blockIdx,
      int decompressedLen,
      Cache.Node<BlockCache.Val> node,
      AtomicReferenceArray<Cache.Node<BlockCache.Val>> accessMapped,
      BlockCache cache,
      BlockSupplier supplier)
      throws IOException {
    ByteBuffer buf;
    try {
      byte[] heapBuf = supplier.supply(blockOffset, compressedLen, decompressedLen);
      buf =
          node.getPayload()
              .populate(
                  heapBuf,
                  0,
                  decompressedLen,
                  cache);
    } catch (Throwable t) {
      node.getPayload().completeExceptionally(t);
      accessMapped.compareAndSet(blockIdx, node, null);
      cache.unpin(node);
      cache.close(node);
      throw CachedCompressedIndexInput.unwrapException(t);
    }
    return buf;
  }

  /**
   * Parses a Lucene {@code .cfe} compound-entry file and returns the block index (within the
   * corresponding {@code .cfs} blob) of the first compressed block of each logical sub-file. Block
   * indices are computed as {@code subFileByteOffset >> COMPRESSION_BLOCK_SHIFT}. Duplicate and
   * out-of-order indices are suppressed.
   *
   * @param dir the {@link Directory} to read the {@code .cfe} from (read access only)
   * @param cfeName the name of the {@code .cfe} file
   * @return sorted, deduplicated list of block indices; empty list on parse failure
   */
  static IntArrayList parseCfeBlockIndexes(Directory dir, String cfeName) {
    try (IndexInput cfeIn = dir.openInput(cfeName, IOContext.READONCE)) {
      return parseCfeBlockIndexes(cfeIn);
    } catch (IOException e) {
      log.debug("parseCfeBlockIndexes: failed to parse {}", cfeName, e);
    }
    return new IntArrayList();
  }

  static IntArrayList parseCfeBlockIndexes(IndexInput cfeIn) {
    IntArrayList blockIndexes = new IntArrayList(16);
    try {
      if (CodecUtil.readBEInt(cfeIn) != CodecUtil.CODEC_MAGIC) return blockIndexes;
      cfeIn.readString(); // codec name — discard
      CodecUtil.readBEInt(cfeIn); // version — discard
      cfeIn.skipBytes(16); // segment ID (StringHelper.ID_LENGTH)
      cfeIn.skipBytes(cfeIn.readByte() & 0xFF); // suffix bytes (empty for Lucene90, but generic)
      int n = cfeIn.readVInt();
      int last = -1;
      for (int i = 0; i < n; i++) {
        cfeIn.readString(); // sub-file name — discard
        long offset = cfeIn.readLong(); // byte offset of this sub-file within the .cfs data
        cfeIn.readLong(); // length — discard
        int offsetBlock = (int) (offset >> COMPRESSION_BLOCK_SHIFT);
        if (offsetBlock > last) {
          blockIndexes.add(offsetBlock);
          last = offsetBlock;
        }
      }
    } catch (IOException e) {
      log.debug("parseCfeBlockIndexes: failed to parse {}", cfeIn, e);
    }
    return blockIndexes;
  }

  /**
   * Performs per-segment preload work. Implementations capture backend-specific state and issue
   * {@link #ensureLoaded} calls for the relevant blocks. Non-priority work (e.g., CFS sub-files
   * beyond the first batch) may be deferred by appending {@link Runnable}s to {@code followup};
   * these are executed synchronously on the last segment's thread by {@link #finishReadAheadSeg}.
   *
   * <p>{@code followup} is {@code null} when the caller does not want deferred work (e.g., the
   * single-segment {@code onFirstBlockMiss} path).
   */
  @FunctionalInterface
  interface SegmentPreloadTask {
    void preload(List<Runnable> followup) throws Exception;
  }

  /**
   * Submits segment preload tasks one at a time via {@code ioExec}, chaining through the iterator.
   * Each task is dispatched only after the previous one completes, preventing semaphore exhaustion.
   * When all tasks are exhausted, delegates to {@link #finishReadAheadSeg}. Must be called only
   * when the iterator has at least one element.
   */
  // TODO: potential for lock contention here, esp. w/ SynchronousQueue
  static void readAheadSegs(
      Iterator<? extends SegmentPreloadTask> tasks,
      ExecutorService ioExec,
      List<Runnable> followup,
      Runnable onComplete) {
    ioExec.submit(
        () -> {
          tasks.next().preload(followup);
          if (tasks.hasNext()) {
            readAheadSegs(tasks, ioExec, followup, onComplete);
          } else {
            finishReadAheadSeg(followup, onComplete);
          }
          return null;
        });
  }

  /**
   * Runs all {@code followup} tasks inline on the calling thread, then invokes {@code onComplete}
   * in a {@code finally} block. Either or both arguments may be {@code null}.
   */
  static void finishReadAheadSeg(List<Runnable> followup, Runnable onComplete) {
    try {
      if (followup != null) {
        for (Runnable r : followup) r.run();
      }
    } finally {
      if (onComplete != null) onComplete.run();
    }
  }
}
