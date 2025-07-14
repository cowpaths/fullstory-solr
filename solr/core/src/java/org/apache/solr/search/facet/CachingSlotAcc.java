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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.IntFunction;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrCache;

public class CachingSlotAcc extends SlotAcc {

  public static final String FACET_FUNCTION_CACHE_NAME = "facetFunctionCache";

  private IntFunction<SlotContext> seenSlotContext;
  private int seenSlot = INITIAL;
  private int collectCount;

  private final SlotAcc backing;
  private final Function<Query, Object> cacheKeyFunction;
  private final SolrCache<Object, CacheFuture<SimpleOrderedMap<Object>>> cache;
  private final int countCacheDf;
  private CacheFuture<SimpleOrderedMap<Object>> cacheVal;

  @SuppressWarnings("unchecked")
  public CachingSlotAcc(
      SlotAcc backing,
      Function<Query, Object> cacheKeyFunction,
      SolrCache<?, ?> cache,
      int countCacheDf) {
    super(backing.fcontext);
    this.backing = backing;
    this.cacheKeyFunction = cacheKeyFunction;
    this.cache = (SolrCache<Object, CacheFuture<SimpleOrderedMap<Object>>>) cache;
    this.countCacheDf = countCacheDf;
  }

  @Override
  public String toString() {
    return backing.toString();
  }

  @Override
  public void setNextReader(LeafReaderContext readerContext) throws IOException {
    backing.setNextReader(readerContext);
  }

  private static final int FAILSAFE_NO_CACHE = -1;
  private static final int CACHED = -2;
  private static final int INITIAL = -3;
  private static final int BULK = -4;

  public int isCached(DocSet docs, int slot, IntFunction<SlotContext> slotContext)
      throws IOException {
    if (seenSlot != INITIAL) {
      throw new IllegalStateException();
    }
    if (docs.size() < countCacheDf) {
      seenSlot = FAILSAFE_NO_CACHE;
      return -1;
    }
    final Object cacheKey = cacheKeyFunction.apply(slotContext.apply(slot).getSlotQuery());
    boolean[] weComputed = new boolean[1];
    cacheVal =
        cache.computeIfAbsent(
            cacheKey,
            (k) -> {
              weComputed[0] = true;
              return new CacheFuture<>();
            });
    if (!weComputed[0] && valAvailable()) {
      seenSlot = CACHED;
      return cacheVal.collectCount.getNow(null);
    } else {
      seenSlot = slot;
      seenSlotContext = slotContext;
      collectCount = 0;
      return -1;
    }
  }

  @Override
  public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
    switch (seenSlot) {
      case CACHED:
        return;
      case FAILSAFE_NO_CACHE:
        break;
      case INITIAL:
        final Object cacheKey = cacheKeyFunction.apply(slotContext.apply(slot).getSlotQuery());
        boolean[] weComputed = new boolean[1];
        cacheVal =
            cache.computeIfAbsent(
                cacheKey,
                (k) -> {
                  weComputed[0] = true;
                  return new CacheFuture<>();
                });
        if (!weComputed[0] && valAvailable()) {
          seenSlot = CACHED;
        } else {
          seenSlot = slot;
          seenSlotContext = slotContext;
          collectCount = 1;
        }
        break;
      case BULK:
        throw new IllegalStateException();
      default:
        if (seenSlot != slot || seenSlotContext != slotContext) {
          seenSlot = FAILSAFE_NO_CACHE;
        }
        collectCount++;
        break;
    }
    backing.collect(doc, slot, slotContext);
  }

  @Override
  public int collect(DocSet docs, int slot, IntFunction<SlotContext> slotContext)
      throws IOException {
    switch (seenSlot) {
      case BULK:
        break;
      case INITIAL:
        seenSlot = BULK;
        break;
      default:
        throw new IllegalStateException();
    }
    // TODO: `DocSet.size()` can have considerable overhead. Evaluate whether we should do
    //  something different here.
    if (docs.size() < countCacheDf) {
      return backing.collect(docs, slot, slotContext);
    }
    final Object cacheKey = cacheKeyFunction.apply(slotContext.apply(slot).getSlotQuery());
    boolean[] weComputed = new boolean[1];
    cacheVal =
        cache.computeIfAbsent(
            cacheKey,
            (k) -> {
              int ret = backing.collect(docs, slot, slotContext);
              weComputed[0] = true;
              return new CacheFuture<>(ret);
            });
    if (!weComputed[0] && !valAvailable()) {
      // we still have to compute ourselves since we don't yet have vals cached
      int collectCount = backing.collect(docs, slot, slotContext);
      assert crosscheck(collectCount, cacheVal.collectCount);
      return collectCount;
    }
    // by this point, either _we_ did the computation (in which case result is
    // obviously ready, or cached vals are ready, which happens after collectCount
    // is ready; either way we're safe)
    return cacheVal.collectCount.getNow(null);
  }

  private boolean valAvailable() throws IOException {
    if (cacheVal.collectCount.getNow(null) == null) {
      // if we don't even have the collect count yet, we could be waiting a
      // long time, so don't bother.
      return false;
    }
    // wait a nominal amount of time to avoid doing the heavy work of `collect()`
    // just because we haven't been patient enough to wait for the values to be
    // serialized. Worst case we just have to double-collect.
    SimpleOrderedMap<Object> entries;
    try {
      entries = cacheVal.vals.get(100, TimeUnit.MILLISECONDS);
      assert entries != null;
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
      return false;
    }
    return true;
  }

  private static boolean crosscheck(int collectCount, CompletableFuture<Integer> cached) {
    Integer cachedVal = cached.getNow(null);
    return cachedVal == null || cachedVal == collectCount;
  }

  private static final class CacheFuture<V> {
    private final CompletableFuture<Integer> collectCount = new CompletableFuture<>();
    private final CompletableFuture<V> vals = new CompletableFuture<>();

    private CacheFuture() {}

    private CacheFuture(int ret) {
      this.collectCount.complete(ret);
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
    backing.key = key; // this is set directly, so we cannot propagate via override :-/
    SimpleOrderedMap<Object> cached;
    switch (seenSlot) {
      case INITIAL: // should never happen? but just bail if it does
      case FAILSAFE_NO_CACHE:
        backing.setValues(bucket, slotNum);
        return;
      case CACHED:
      case BULK:
        // nothing extra to do here
        break;
      default:
        // non-bulk collection; update collectCount
        cacheVal.collectCount.complete(collectCount);
        break;
    }

    if ((cached = cacheVal.vals.getNow(null)) == null) {
      // we must actually set vals
      try (TeeMap<Object> toCache = new TeeMap<>(bucket, key)) {
        backing.setValues(toCache, slotNum);
        cacheVal.vals.complete(toCache);
      }
    } else {
      cached.forEach((k, v) -> bucket.add(SPECIAL_KEY.equals(k) ? key : k, v));
    }
  }

  @Override
  public void reset() throws IOException {
    collectCount = 0;
    seenSlot = INITIAL;
    cacheVal = null;
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

  private static final String SPECIAL_KEY = "\0\0\0\0";

  private static final class TeeMap<V> extends SimpleOrderedMap<V> implements AutoCloseable {
    private SimpleOrderedMap<V> backing;
    private String origKey;

    private TeeMap(SimpleOrderedMap<V> backing, String origKey) {
      this.backing = backing;
      this.origKey = origKey;
    }

    @Override
    public void add(String name, V val) {
      backing.add(name, val);
      super.add(origKey.equals(name) ? SPECIAL_KEY : name, val);
    }

    /** Don't retain references (leak) any longer than necessary */
    @Override
    public void close() {
      backing = null;
      origKey = null;
    }
  }
}
