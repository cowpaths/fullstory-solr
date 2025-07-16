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
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.IntFunction;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrCache;

public class CachingSlotAcc extends SlotAcc {

  public static final String FACET_FUNCTION_CACHE_NAME = "facetFunctionCache";

  public interface SlotCacheKey extends Accountable {
    long valueRamUsageEstimate();

    ExtractCacheValueFunction extractValueFunction();
  }

  public interface ExtractCacheValueFunction {
    SlotCacheValue extract(
        SimpleOrderedMap<Object> bucket,
        SlotAcc backing,
        int slotNum,
        CompletableFuture<SlotCacheValue> f)
        throws IOException;
  }

  public interface SlotCacheValue {
    void update(SimpleOrderedMap<Object> bucket, String key);

    Comparable<?> comp();
  }

  private IntFunction<SlotContext> seenSlotContext;
  private int seenSlot = INITIAL;
  private int collectCount;

  private final SlotAcc backing;
  private final Function<Query, SlotCacheKey> cacheKeyFunction;
  private final SolrCache<SlotCacheKey, CacheFuture<SlotCacheValue>> cache;
  private final int funcCacheDf;
  private CacheFuture<SlotCacheValue>[] cacheVal;

  @SuppressWarnings({"unchecked", "rawtypes"})
  public CachingSlotAcc(
      SlotAcc backing,
      Function<Query, SlotCacheKey> cacheKeyFunction,
      SolrCache<?, ?> cache,
      int funcCacheDf) {
    super(backing.fcontext);
    this.backing = backing;
    this.cacheKeyFunction = cacheKeyFunction;
    this.cache = (SolrCache<SlotCacheKey, CacheFuture<SlotCacheValue>>) cache;
    this.funcCacheDf = funcCacheDf;
    this.cacheVal = new CacheFuture[1];
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
    if (docs.size() < funcCacheDf) {
      seenSlot = FAILSAFE_NO_CACHE;
      return -1;
    }
    final SlotCacheKey cacheKey = cacheKeyFunction.apply(slotContext.apply(slot).getSlotQuery());
    boolean[] weComputed = new boolean[1];
    cacheVal[slot] =
        cache.computeIfAbsent(
            cacheKey,
            (k) -> {
              weComputed[0] = true;
              return new CacheFuture<>(k.valueRamUsageEstimate(), k.extractValueFunction());
            });
    if (!weComputed[0] && valAvailable(cacheVal[slot])) {
      seenSlot = CACHED;
      return cacheVal[slot].collectCount.getNow(null);
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
        final SlotCacheKey cacheKey =
            cacheKeyFunction.apply(slotContext.apply(slot).getSlotQuery());
        boolean[] weComputed = new boolean[1];
        cacheVal[slot] =
            cache.computeIfAbsent(
                cacheKey,
                (k) -> {
                  weComputed[0] = true;
                  return new CacheFuture<>(k.valueRamUsageEstimate(), k.extractValueFunction());
                });
        if (!weComputed[0] && valAvailable(cacheVal[slot])) {
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
    if (docs.size() < funcCacheDf) {
      return backing.collect(docs, slot, slotContext);
    }
    final SlotCacheKey cacheKey = cacheKeyFunction.apply(slotContext.apply(slot).getSlotQuery());
    boolean[] weComputed = new boolean[1];
    cacheVal[slot] =
        cache.computeIfAbsent(
            cacheKey,
            (k) -> {
              int ret = backing.collect(docs, slot, slotContext);
              weComputed[0] = true;
              return new CacheFuture<>(ret, k.valueRamUsageEstimate(), k.extractValueFunction());
            });
    if (!weComputed[0] && !valAvailable(cacheVal[slot])) {
      // we still have to compute ourselves since we don't yet have vals cached
      int collectCount = backing.collect(docs, slot, slotContext);
      assert crosscheck(collectCount, cacheVal[slot].collectCount);
      return collectCount;
    }
    // by this point, either _we_ did the computation (in which case result is
    // obviously ready, or cached vals are ready, which happens after collectCount
    // is ready; either way we're safe)
    return cacheVal[slot].collectCount.getNow(null);
  }

  private boolean valAvailable(CacheFuture<?> cacheVal) throws IOException {
    if (cacheVal.collectCount.getNow(null) == null) {
      // if we don't even have the collect count yet, we could be waiting a
      // long time, so don't bother.
      return false;
    }
    // wait a nominal amount of time to avoid doing the heavy work of `collect()`
    // just because we haven't been patient enough to wait for the values to be
    // serialized. Worst case we just have to double-collect.
    Object entries;
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

  private static final class CacheFuture<V> implements Accountable {
    private static final long BASE_RAM_BYTES =
        RamUsageEstimator.shallowSizeOfInstance(CacheFuture.class)
            + (RamUsageEstimator.shallowSizeOfInstance(CompletableFuture.class) << 1)
            + RamUsageEstimator.shallowSizeOfInstance(Integer.class);
    private final CompletableFuture<Integer> collectCount = new CompletableFuture<>();
    private final CompletableFuture<V> vals = new CompletableFuture<>();
    private final long ramUsageEstimate;
    private final ExtractCacheValueFunction extractCacheValueFunction;

    private CacheFuture(
        long valRamUsageEstimate, ExtractCacheValueFunction extractCacheValueFunction) {
      this.ramUsageEstimate = BASE_RAM_BYTES + valRamUsageEstimate;
      this.extractCacheValueFunction = extractCacheValueFunction;
    }

    private CacheFuture(
        int ret, long ramUsageEstimate, ExtractCacheValueFunction extractCacheValueFunction) {
      this.ramUsageEstimate = ramUsageEstimate;
      this.extractCacheValueFunction = extractCacheValueFunction;
      this.collectCount.complete(ret);
    }

    @Override
    public long ramBytesUsed() {
      return ramUsageEstimate;
    }
  }

  public interface SlotComparable {
    boolean cached(int slot, int[] comps);

    boolean cached(int slot, double[] comps);

    boolean cached(int slot, long[] comps);
  }

  private final SlotComparable compFunc =
      new SlotComparable() {
        @Override
        public boolean cached(int slot, int[] comps) {
          CacheFuture<SlotCacheValue> cv = cacheVal[slot];
          SlotCacheValue tm;
          if (cv == null || (tm = cv.vals.getNow(null)) == null) {
            return false;
          } else {
            comps[slot] = ((Number) tm.comp()).intValue();
            return true;
          }
        }

        @Override
        public boolean cached(int slot, double[] comps) {
          CacheFuture<SlotCacheValue> cv = cacheVal[slot];
          SlotCacheValue tm;
          if (cv == null || (tm = cv.vals.getNow(null)) == null) {
            return false;
          } else {
            comps[slot] = ((Number) tm.comp()).doubleValue();
            return true;
          }
        }

        @Override
        public boolean cached(int slot, long[] comps) {
          CacheFuture<SlotCacheValue> cv = cacheVal[slot];
          SlotCacheValue tm;
          if (cv == null || (tm = cv.vals.getNow(null)) == null) {
            return false;
          } else {
            comps[slot] = ((Number) tm.comp()).longValue();
            return true;
          }
        }
      };

  @Override
  public int compare(int slotA, int slotB) {
    return backing.compare(slotA, slotB, compFunc);
  }

  @Override
  public Object getValue(int slotNum) throws IOException {
    return backing.getValue(slotNum);
  }

  @Override
  public void setValues(SimpleOrderedMap<Object> bucket, int slotNum) throws IOException {
    backing.key = key; // this is set directly, so we cannot propagate via override :-/
    SlotCacheValue cached;
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
        cacheVal[slotNum].collectCount.complete(collectCount);
        break;
    }

    CacheFuture<SlotCacheValue> cacheFuture = cacheVal[slotNum];
    if ((cached = cacheFuture.vals.getNow(null)) == null) {
      // we must actually set vals
      cacheFuture.extractCacheValueFunction.extract(bucket, backing, slotNum, cacheFuture.vals);
    } else {
      cached.update(bucket, key);
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
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void resize(Resizer resizer) {
    cacheVal = new CacheFuture[resizer.getNewSize()];
    backing.resize(resizer);
  }

  @Override
  public void close() throws IOException {
    backing.close();
  }

  private static final String SPECIAL_KEY = "\0\0\0\0";

  static final class TeeMap<V> extends SimpleOrderedMap<V>
      implements AutoCloseable, SlotCacheValue {
    private SimpleOrderedMap<V> backing;
    private String origKey;
    private Comparable<?> comp;

    TeeMap(SimpleOrderedMap<V> backing, String origKey, int expectMapSize) {
      super(expectMapSize);
      this.backing = backing;
      this.origKey = origKey;
    }

    @Override
    public void add(String name, V val) {
      backing.add(name, val);
      if (origKey.equals(name)) {
        name = SPECIAL_KEY;
        comp = (Comparable<?>) val;
      }
      super.add(name, val);
    }

    /** Don't retain references (leak) any longer than necessary */
    @Override
    public void close() {
      backing = null;
      origKey = null;
    }

    @Override
    public void update(SimpleOrderedMap<Object> bucket, String key) {
      forEach((k, v) -> bucket.add(SPECIAL_KEY.equals(k) ? key : k, v));
    }

    @Override
    public Comparable<?> comp() {
      return comp;
    }
  }

  private static final long TEE_MAP_BASE_RAM_BYTES =
      RamUsageEstimator.shallowSizeOfInstance(TeeMap.class)
          + RamUsageEstimator.shallowSizeOfInstance(ArrayList.class);

  public static long slotCacheEntryBaseSize(int size) {
    int listSize = size << 1; // name _and_ value
    return TEE_MAP_BASE_RAM_BYTES
        + RamUsageEstimator.alignObjectSize(
            (long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER
                + (long) RamUsageEstimator.NUM_BYTES_OBJECT_REF * listSize);
  }
}
