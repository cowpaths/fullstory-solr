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
package org.apache.solr.search.facet;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntFunction;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrCache;

public class CachingSlotAcc extends SlotAcc {

  private final SlotAcc backing;
  private final Object key;
  private final SolrCache<Object, CacheFuture<SimpleOrderedMap<Object>>> cache;
  private CacheFuture<SimpleOrderedMap<Object>> cacheFuture;

  @SuppressWarnings("unchecked")
  public CachingSlotAcc(SlotAcc backing, Object key, SolrCache<Object, ?> cache) {
    super(backing.fcontext);
    this.backing = backing;
    this.key = key;
    this.cache = (SolrCache<Object, CacheFuture<SimpleOrderedMap<Object>>>) cache;
  }

  @Override
  public String toString() {
    return backing.toString();
  }

  @Override
  public void setNextReader(LeafReaderContext readerContext) throws IOException {
    backing.setNextReader(readerContext);
  }

  @Override
  public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
    backing.collect(doc, slot, slotContext);
  }

  @Override
  public int collect(DocSet docs, int slot, IntFunction<SlotContext> slotContext)
      throws IOException {
    cacheFuture =
        cache.computeIfAbsent(
            key,
            (k) -> {
              int ret = backing.collect(docs, slot, slotContext);
              SimpleOrderedMap<Object> vals = new SimpleOrderedMap<>();
              backing.setValues(vals, 0);
              return new CacheFuture<>(ret, vals, backing);
            });
    return cacheFuture.ret;
  }

  private static final class CacheFuture<V> {
    private final int ret;
    private final WeakReference<SlotAcc> backing;
    private final CompletableFuture<V> vals = new CompletableFuture<>();

    private CacheFuture(int ret, SlotAcc backing) {
      this.ret = ret;
      this.backing = new WeakReference<>(backing);
    }

    private CacheFuture(int ret, V vals, SlotAcc backing) {
      this.ret = ret;
      this.backing = new WeakReference<>(backing);
      this.vals.complete(vals);
    }
  }

  @Override
  public int compare(int slotA, int slotB) {
    return backing.compare(slotA, slotB);
  }

  @Override
  public Object getValue(int slotNum) throws IOException {
    return backing.getValue(slotNum);
  }

  @Override
  public void setValues(SimpleOrderedMap<Object> bucket, int slotNum) throws IOException {
    SlotAcc activeSlotAcc = cacheFuture.backing.get();
    if (activeSlotAcc == backing) {
      // we set values
      TeeMap<Object> teeBucket = new TeeMap<>(bucket);
      backing.setValues(teeBucket, slotNum);
      // TODO: remove below if proactive setting works as expected
      // cacheFuture.vals.complete(teeBucket);
    } else {
      SimpleOrderedMap<Object> vals = cacheFuture.vals.getNow(null);
      if (vals == null) {
        if (activeSlotAcc != null) {
          SimpleOrderedMap<Object> directSetValues = new SimpleOrderedMap<>();
          activeSlotAcc.setValues(directSetValues, slotNum);
          vals = cacheFuture.vals.getNow(null);
          if (vals == null) {
            // TODO: if an exception is thrown in the computing thread before completing
            //  `cacheFuture.vals`, then `entries` being null here does not necessarily
            //  indicate (as it normally would) that we are safe to use `directSetValues`.
            //  Perhaps we should _always_ do `vals.get`? Really the only safe way to do
            //  this is to proactively call `setValues()` within `collect()`.
            //  Have done this (proactive) for now; assuming that works, we can simplify
            //  this method considerably.
            vals = directSetValues;
          }
        } else {
          try {
            vals = cacheFuture.vals.get(10, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
              throw (IOException) cause;
            }
            throw new RuntimeException(e);
          } catch (TimeoutException e) {
            throw new RuntimeException(e);
          }
        }
      }
      vals.forEach(bucket::add);
    }
  }

  @Override
  public void reset() throws IOException {
    backing.reset();
  }

  @Override
  public void resetIterators() throws IOException {
    backing.resetIterators();
  }

  @Override
  public void resize(Resizer resizer) {
    backing.resize(resizer);
  }

  @Override
  public void close() throws IOException {
    backing.close();
  }

  private static final class TeeMap<V> extends SimpleOrderedMap<V> {
    private final WeakReference<SimpleOrderedMap<V>> backing;

    private TeeMap(SimpleOrderedMap<V> backing) {
      this.backing = new WeakReference<>(backing);
    }

    @Override
    public void add(String name, V val) {
      SimpleOrderedMap<V> backing = this.backing.get();
      assert backing != null;
      backing.add(name, val);
      super.add(name, val);
    }
  }
}
