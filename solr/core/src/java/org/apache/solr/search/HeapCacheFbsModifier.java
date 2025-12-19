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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Pools buffers backed by heap byte[] of largest possible size
 */
public class HeapCacheFbsModifier implements FixedBitSet.Modifier {
  private static final int BLOCK_SIZE_BYTES = SortedIntDocSet.MAX_ARR_SIZE << 2;
  private static final int MAX_BLOCKS_PER_PARTITION;
  private static final int N_BLOCKS;
  private static final long TAIL_MASK = -1L >>> Integer.SIZE;
  private static final int POOL_ARR_SIZE;
  private static final int POOL_SIZE_MASK;

  // dummy, for efficiently clearing buffers
  private static final ByteBuffer FRESH = ByteBuffer.allocate(BLOCK_SIZE_BYTES);

  private final ByteBuffer[] pool;

  private final AtomicLong headAndTail;

  static {
    long maxMemory = Runtime.getRuntime().maxMemory();
    long defaultTargetPoolSize = Math.toIntExact(maxMemory / 4); // default to 1/4 of heap
    long maxPoolSize = Math.toIntExact(maxMemory / 2); // max of 1/2 of heap
    long targetPoolSizeSpec = EnvUtils.getPropertyAsLong("solr.fbspool.targetsize", defaultTargetPoolSize);
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
      byte[] partition = new byte[partitionNumBlocks * BLOCK_SIZE_BYTES];
      for (int j = 0; j < partitionNumBlocks; j++) {
        pool[blockIdx++] = ByteBuffer.wrap(partition, j * BLOCK_SIZE_BYTES, BLOCK_SIZE_BYTES);
      }
      partitionNumBlocks = MAX_BLOCKS_PER_PARTITION;
    }
    headAndTail = new AtomicLong(N_BLOCKS);
  }

  @Override
  public ByteBuffer allocateBytes(int size, Object sentinel) {
    for (long extant = this.headAndTail.get(); ; ) {
      int head = (int) (extant >> Integer.SIZE);
      long tail = extant & TAIL_MASK;
      int avail = (int) tail - head;
      if (avail > 0) {
        head++;
      } else if (avail == 0) {
        // exhausted; fallback to main heap allocation
        return ByteBuffer.allocate(size);
      } else {
        throw new IllegalStateException();
      }
      long witness = this.headAndTail.compareAndExchange(extant, ((long) head << Integer.SIZE) | tail);
      if (witness == extant) {
        ByteBuffer ret = pool[head & POOL_SIZE_MASK].clear().slice(0, size);
        // zero it out
        return ret.put(FRESH.slice(0, size)).clear();
      } else {
        extant = witness;
      }
    }
  }
}
