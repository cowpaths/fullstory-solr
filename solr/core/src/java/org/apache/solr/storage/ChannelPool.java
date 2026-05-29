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

import com.google.cloud.ReadChannel;

/**
 * Concurrent LRU pool that caps the number of concurrently open {@link ReadChannel}s across all
 * {@link GCSDirectory} instances sharing this pool.
 *
 * <p>Each open channel is represented by a permit {@link Node}. The pool is pre-populated with
 * {@code maxChannels} permit slots. Acquiring a permit either claims a free slot or evicts the
 * least-recently-used unpinned permit, signalling the owning {@link GCSDirectory} {@code
 * GCSIndexInput} to close its channel lazily on next access. Permits are pinned for the duration of
 * each read and recycled via {@link #close(Cache.Node)} when the owning {@code IndexInput} is
 * closed.
 */
class ChannelPool extends Cache<ReadChannel, ChannelPool.Node> {

  /** A node representing one open {@link ReadChannel} slot. */
  static final class Node extends Cache.Node<ReadChannel> {
    Node(ReadChannel value, Cache.Node<ReadChannel> prev, int initialRefCount) {
      super(value, prev, initialRefCount);
    }
  }

  ChannelPool(int maxChannels) {
    super(new ReadChannel[maxChannels], true);
  }

  @Override
  protected Node createNode(ReadChannel value, Cache.Node<ReadChannel> prev, int initialRefCount) {
    return new Node(value, prev, initialRefCount);
  }

  @Override
  Node acquireNode() {
    throw new UnsupportedOperationException(
        "call acquireNode(Function<ReadChannel, ReadChannel>) instead");
  }
}
