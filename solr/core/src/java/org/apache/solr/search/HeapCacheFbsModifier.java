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

import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.common.util.EnvUtils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Pools buffers backed by heap byte[] of largest possible size
 */
public class HeapCacheFbsModifier implements FixedBitSet.Modifier, AutoCloseable {
  private static final int BLOCK_SIZE_BYTES = SortedIntDocSet.MAX_ARR_SIZE << 2;
  private static final int MAX_BLOCKS_PER_PARTITION;
  private static final int N_BLOCKS;
  private static final long TAIL_MASK = -1L >>> Integer.SIZE;
  private static final long HEAD_MASK = ~TAIL_MASK;
  private static final int POOL_ARR_SIZE;
  private static final int POOL_SIZE_MASK;

  // dummy, for efficiently clearing buffers
  private static final ByteBuffer FRESH = ByteBuffer.allocate(BLOCK_SIZE_BYTES);

  private final ByteBuffer[] pool;

  private final AtomicLong headAndTail;
  private int releaseTail;
  private final BlockingQueue<List<ByteBuffer>> releaseQueue = new ArrayBlockingQueue<>(2048, false);

  private final ReferenceHandler<List<ByteBuffer>> refHandler;

  public static final String POOL_TARGET_MB_PROPNAME = "solr.fbspool.targetMB";

  static {
    long maxMemory = Runtime.getRuntime().maxMemory();
    long defaultTargetPoolSize = Math.toIntExact(maxMemory / 16); // default to 1/16 of heap
    long maxPoolSize = Math.toIntExact(maxMemory / 2); // max of 1/2 of heap
    int targetPoolSizeMB = EnvUtils.getPropertyAsInteger(POOL_TARGET_MB_PROPNAME, Math.toIntExact(defaultTargetPoolSize >> 20));
    long targetPoolSizeSpec;
    if (targetPoolSizeMB == -1) {
      targetPoolSizeSpec = defaultTargetPoolSize;
    } else {
      targetPoolSizeSpec = ((long) targetPoolSizeMB) << 20;
    }
    long targetPoolSize = Math.min(maxPoolSize, targetPoolSizeSpec);
    N_BLOCKS = Math.toIntExact(targetPoolSize / BLOCK_SIZE_BYTES);
    MAX_BLOCKS_PER_PARTITION = Integer.MAX_VALUE / BLOCK_SIZE_BYTES;
    POOL_ARR_SIZE = Integer.highestOneBit(N_BLOCKS) << 1;
    POOL_SIZE_MASK = POOL_ARR_SIZE - 1;
  }

  public HeapCacheFbsModifier() {
    int numPartitions = ((N_BLOCKS - 1) / MAX_BLOCKS_PER_PARTITION) + 1;
    pool = new ByteBuffer[POOL_ARR_SIZE];
    int blockIdx = 0;
    for (int i = numPartitions - 1, partitionNumBlocks = ((N_BLOCKS - 1) / numPartitions) + 1; i >= 0; i--) {
      ByteBuffer partition = ByteBuffer.allocate(partitionNumBlocks * BLOCK_SIZE_BYTES);
      for (int j = 0; j < partitionNumBlocks; j++) {
        pool[blockIdx++] = partition.slice(j * BLOCK_SIZE_BYTES, BLOCK_SIZE_BYTES);
      }
      partitionNumBlocks = MAX_BLOCKS_PER_PARTITION;
    }
    headAndTail = new AtomicLong(N_BLOCKS);
    releaseTail = N_BLOCKS;
    refHandler = new ReferenceHandler<>((toRelease) -> {
      try {
        releaseQueue.put(toRelease);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }, () -> {
      try {
        releaseLoop();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    FixedBitSets.MODIFIER = this;
  }

  @Override
  public void close() {
    try (refHandler) {
      FixedBitSets.MODIFIER = FixedBitSet.DEFAULT_MODIFIER;
    }
  }

  private final LongAdder allocated = new LongAdder();
  private final LongAdder exhausted = new LongAdder();

  @Override
  public ByteBuffer allocateBytes(int size) {
    ByteBuffer ret = allocateBytes0(size);
    return ret == null ? ByteBuffer.allocate(size) : ret;
  }

  private ByteBuffer allocateBytes0(int size) {
    for (long extant = this.headAndTail.get(); ; ) {
      int head = (int) (extant >> Integer.SIZE);
      long tail = extant & TAIL_MASK;
      int avail = (int) tail - head;
      if (avail == 0) {
        // exhausted; fallback to main heap allocation
        exhausted.increment();
        return null;
      } else if (avail < 0) {
        throw new IllegalStateException("avail="+avail+", "+tail+", "+((int) tail)+", "+head+" !!! "+allocated.sum()+" ~= "+ collected.sum());
      }
      long witness = this.headAndTail.compareAndExchange(extant, ((long) (head + 1) << Integer.SIZE) | tail);
      if (witness == extant) {
        allocated.increment();
        ByteBuffer ret = pool[head & POOL_SIZE_MASK].clear().limit(size);
        // zero it out
        return ret.put(FRESH.slice(0, size)).flip();
      } else {
        extant = witness;
      }
    }
  }

  private final LongAdder collected = new LongAdder();

  private void releaseLoop() throws InterruptedException {
    for (; ; ) {
      List<ByteBuffer> toRelease = releaseQueue.take();
      int destOff = releaseTail;
      for (ByteBuffer bb : toRelease) {
        pool[destOff++ & POOL_SIZE_MASK] = bb;
      }
      collected.add(toRelease.size());
      releaseTail = destOff;
      for (long extant = headAndTail.get(); ; ) {
        long witness = headAndTail.compareAndExchange(extant, (extant & HEAD_MASK) | destOff);
        if (witness == extant) {
          break;
        } else {
          extant = witness;
        }
      }
    }
  }

  @Override
  public FixedBitSet.Modifier getBatchModifier(Object sentinel, int batchSize) {
    List<ByteBuffer> blocks = new ArrayList<>(batchSize);
    return new FixedBitSet.Modifier() {
      @Override
      public ByteBuffer allocateBytes(int size) {
        ByteBuffer ret = HeapCacheFbsModifier.this.allocateBytes0(size);
        if (ret == null) {
          return ByteBuffer.allocate(size);
        } else {
          blocks.add(ret);
          return ret;
        }
      }

      @Override
      public void close() {
        try {
          if (!blocks.isEmpty()) {
            refHandler.add(sentinel, blocks);
          }
        } finally {
          FixedBitSet.Modifier.super.close();
        }
      }
    };
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
