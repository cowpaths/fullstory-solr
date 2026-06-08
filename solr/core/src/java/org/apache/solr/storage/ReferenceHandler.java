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

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.lucene.util.IOConsumer;

public class ReferenceHandler<K, V> implements Closeable {

  static class Ref<K, V> extends WeakReference<K> {
    private final Cache.Node<Ref<K, V>> node;
    private final V target;

    public Ref(K referent, V target, ReferenceQueue<? super K> q) {
      super(referent, q);
      this.target = target;
      this.node = new Cache.Node<>(this, null, 1);
    }
  }

  @SuppressWarnings("unchecked")
  private final Cache<ReferenceHandler.Ref<K, V>, Cache.Node<Ref<K, V>>> list =
      new Cache<ReferenceHandler.Ref<K, V>, Cache.Node<Ref<K, V>>>(new Ref[0], false);

  private final ReferenceQueue<K> refQueue = new ReferenceQueue<>();
  private final Future<?> drainRefQueue;
  private volatile boolean alive = true;

  /**
   * Creates a new block cache backed by a freshly-created temp file. The file is deleted
   * immediately after mapping so it does not outlive the JVM.
   */
  @SuppressWarnings("unchecked")
  public ReferenceHandler(ExecutorService exec, IOConsumer<V> onCollection) throws IOException {
    this.drainRefQueue =
        exec.submit(
            () -> {
              while (alive) {
                Ref<K, V> remove;
                try {
                  remove = (Ref<K, V>) refQueue.remove();
                } catch (InterruptedException ex) {
                  if (alive) {
                    throw ex;
                  } else {
                    // normal exit mechanism
                    Thread.currentThread().interrupt();
                    return null;
                  }
                }
                try {
                  onCollection.accept(remove.target);
                } finally {
                  list.pin(remove.node); // remove from list (discard)
                }
              }
              return null;
            });
  }

  public void register(K val, V target) {
    Ref<K, V> ref = new Ref<>(val, target, refQueue);
    list.unpin(ref.node); // put it in the list
  }

  @Override
  public void close() {
    alive = false;
    drainRefQueue.cancel(true);
    try {
      drainRefQueue.get(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      throw new RuntimeException(e);
    }
  }
}
