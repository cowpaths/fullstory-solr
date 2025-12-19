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

/**
 * Pools buffers backed by heap byte[] of largest possible size
 */
public class HeapCacheFbsModifier implements FixedBitSet.Modifier {
  private static final int BLOCK_SIZE_BYTES = SortedIntDocSet.MAX_ARR_SIZE << 2;
  private static final int MAX_BLOCKS_PER_PARTITION;
  private static final int N_BLOCKS;

  private static final ByteBuffer[] POOL;

  static {
    long maxMemory = Runtime.getRuntime().maxMemory();
    long defaultTargetPoolSize = Math.toIntExact(maxMemory / 4); // default to 1/4 of heap
    long maxPoolSize = Math.toIntExact(maxMemory / 2); // max of 1/2 of heap
    long targetPoolSizeSpec = EnvUtils.getPropertyAsLong("solr.fbspool.targetsize", defaultTargetPoolSize);
    long targetPoolSize = Math.min(maxPoolSize, targetPoolSizeSpec);
    N_BLOCKS = Math.toIntExact(targetPoolSize / BLOCK_SIZE_BYTES);
    MAX_BLOCKS_PER_PARTITION = Integer.MAX_VALUE / BLOCK_SIZE_BYTES;
    int numPartitions = ((N_BLOCKS - 1) / MAX_BLOCKS_PER_PARTITION) + 1;
    POOL = new ByteBuffer[N_BLOCKS];
    int blockIdx = 0;
    for (int i = numPartitions - 1, partitionNumBlocks = ((N_BLOCKS - 1) / numPartitions) + 1; i >= 0; i--) {
      byte[] partition = new byte[partitionNumBlocks * BLOCK_SIZE_BYTES];
      for (int j = 0; j < partitionNumBlocks; j++) {
        POOL[blockIdx++] = ByteBuffer.wrap(partition, j * BLOCK_SIZE_BYTES, BLOCK_SIZE_BYTES);
      }
      partitionNumBlocks = MAX_BLOCKS_PER_PARTITION;
    }
  }

  @Override
  public ByteBuffer allocateBytes(int size, Object sentinel) {
    return null;
  }
}
