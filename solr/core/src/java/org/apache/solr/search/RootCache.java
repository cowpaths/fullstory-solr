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

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import java.lang.ref.SoftReference;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.util.IOFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("ReferenceEquality")
public class RootCache<K, V> implements RemovalListener<K, V>, Accountable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(RootCache.class)
          + RamUsageEstimator.shallowSizeOfInstance(CacheStats.class)
          + 2 * RamUsageEstimator.shallowSizeOfInstance(LongAdder.class);

  private static final long RAM_BYTES_PER_FUTURE =
      RamUsageEstimator.shallowSizeOfInstance(CompletableFuture.class);

  private final long initialRamBytes;

  private final String tierScope;
  private AsyncCache<RefCountingKey<K>, ValRef<V>> asyncCache;

  private final RootCache<K, V> parent;

  private final RootCache<K, V> root;

  private final long mask;

  private final RemovalListenerRegistry<RefCountingKey<K>, V> removalListeners =
      new RemovalListenerRegistry<>();

  private final IdentityHashMap<RootCache<K, V>, RemovalListener<RefCountingKey<K>, V>> children =
      new IdentityHashMap<>();

  private boolean isLeaf = true;

  private final RemovalListener<RefCountingKey<K>, V> parentRemovalListener;

  private RemovalListenerParityChecker<RefCountingKey<K>, ValRef<V>> removalListenerParityChecker;

  private final RootCache<K, V>[] pathFromRootArr;

  private final Iterable<RootCache<K, V>> pathFromRoot;

  private int maxSize;
  private long maxRamBytes;
  private int initialSize;
  private final int maxIdleTimeSec;

  private final LongAdder ramBytes = new LongAdder();

  private final RemovalListener<K, V> externalListener;

  private final RemovalListener<RefCountingKey<K>, ValRef<V>> rawEvictionListener;

  public RootCache(int maxSize, RootCache<K, V> parent, String tierScope) {
    this(maxSize, parent, tierScope, null);
  }

  public RootCache(
      int maxSize,
      RootCache<K, V> parent,
      String tierScope,
      RemovalListener<K, V> externalListener) {
    this(maxSize, Long.MAX_VALUE, 0, 0, parent, tierScope, externalListener);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public RootCache(
      int maxSize,
      long maxRamBytes,
      int initialSize,
      int maxIdleTimeSec,
      RootCache<K, V> parent,
      String tierScope,
      RemovalListener<K, V> externalListener) {
    this.externalListener = externalListener;
    this.tierScope = tierScope;
    if (parent == null) {
      rawEvictionListener =
          (key, value, cause) -> {
            V val = value.val;
            removalListeners.onRemoval(
                key, val, cause, RemovalListenerRegistry.Source.SELF, key.tierScope);
            onRemoval(key.key, value, val, cause);
          };
    } else {
      rawEvictionListener =
          (key, value, cause) -> {
            V val = value.ref.get();
            removalListeners.onRemoval(
                key, val, cause, RemovalListenerRegistry.Source.SELF, key.tierScope);
            onRemoval(key.key, value, val, cause);
          };
    }
    this.maxSize = maxSize;
    this.maxRamBytes = maxRamBytes;
    this.initialSize = initialSize;
    this.maxIdleTimeSec = maxIdleTimeSec;
    asyncCache = buildCache(null);
    this.parent = parent;
    if (parent == null) {
      root = this;
      mask = 0;
      pathFromRootArr = null;
      pathFromRoot = null;
      parentRemovalListener = null;
    } else {
      root = parent.root;
      RootCache<K, V>[] pathToParent = parent.pathFromRootArr;
      if (pathToParent == null) {
        pathFromRootArr = new RootCache[] {this};
      } else {
        pathFromRootArr = ArrayUtil.growExact(pathToParent, pathToParent.length + 1);
        pathFromRootArr[pathToParent.length] = this;
      }
      pathFromRoot = List.of(pathFromRootArr);
      this.mask = parent.addChild(this);
      parentRemovalListener =
          (key, value, cause) -> {
            assert !isLeaf || Objects.equals(tierScope, key.keyScope);
            // NOTE: assuming that `key` is annotated @Nullable in `RemovalListener` interface
            // because of the possibility of the `weakKeys()` config, we are safe treating `key`
            // as non-null.
            asyncCache
                .asMap()
                .computeIfPresent(
                    key,
                    (k, v) -> {
                      ValRef<V> valRef = v.getNow(null);
                      V val = valRef.ref.get();
                      assert isLeaf || val != null;
                      removalListeners.onRemoval(
                          k, val, cause, RemovalListenerRegistry.Source.PARENT, k.tierScope);
                      onRemoval(k.key, valRef, val, cause);
                      return null;
                    });
          };
      parent.register(
          parentRemovalListener,
          RemovalListenerRegistry.Source.SELF_AND_PARENT,
          !Objects.equals(parent.tierScope, tierScope) ? tierScope : null);
    }
    initialRamBytes = RamUsageEstimator.shallowSizeOfInstance(asyncCache.getClass());
  }

  public RootCache<K, V> getParent() {
    return parent;
  }

  Set<Map.Entry<K, String>> keySet() {
    return asyncCache.asMap().keySet().stream()
        .map((k) -> new AbstractMap.SimpleImmutableEntry<>(k.key, k.keyScope))
        .collect(Collectors.toSet());
  }

  Set<K> validate(String prefix, int[] empty, PrintStream ps) {
    int size = asyncCache.asMap().size();
    List<K> ordered =
        asyncCache.synchronous().policy().eviction().get().hottest(size).keySet().stream()
            .map((k) -> k.key)
            .collect(Collectors.toList());
    int orderedSize = ordered.size();
    if (orderedSize == 0) {
      empty[0]++;
    } else if (ps != null) {
      ps.println(
          "XXX "
              + prefix
              + orderedSize
              + "/"
              + maxSize
              + ": "
              + ordered
              + (isLeaf ? " (leaf)" : ""));
    }
    Set<K> ret = new HashSet<>(ordered);
    if (isLeaf) {
      assert children.isEmpty();
    } else {
      int[] childrenEmpty = new int[1];
      Set<K> mergedChildren = CollectionUtil.newHashSet(ordered.size());
      for (RootCache<K, V> child : children.keySet()) {
        mergedChildren.addAll(child.validate(prefix.concat("  "), childrenEmpty, ps));
      }
      if (orderedSize > 0 && childrenEmpty[0] > 0 && ps != null) {
        ps.println("XXX   " + prefix + "empty: " + childrenEmpty[0]);
      }
      if (!ret.equals(mergedChildren)) {
        throw new IllegalStateException();
      }
    }
    return ret;
  }

  // NOTE: access to assigned masks should be protected by synchronizing on `children`
  private final long[] assigned = new long[1];
  private final List<RootCache<K, V>> unscopedChildren = new ArrayList<>();
  private final Map<String, Map.Entry<List<RootCache<K, V>>, long[]>> scopedAssigned =
      new HashMap<>();

  private static long newRegisterMask(long[] assigned) {
    long prev = assigned[0];
    if (prev == 0xFFFFFFFFFFFFFFFFL) {
      throw new IllegalStateException("max register count exceeded (" + Long.SIZE + ")");
    }
    final long mask = Long.highestOneBit(~prev); // get the first unset bit
    assigned[0] = prev | mask;
    return mask;
  }

  // TODO: test/verify the thread-safety of `addChild()`, etc...
  public long addChild(RootCache<K, V> child) {
    isLeaf = false;
    final long childMask;
    synchronized (children) {
      if (Objects.equals(tierScope, child.tierScope)) {
        childMask = newRegisterMask(assigned);
        unscopedChildren.add(child);
      } else {
        childMask =
            newRegisterMask(
                scopedAssigned
                    .computeIfAbsent(
                        child.tierScope,
                        (k) -> {
                          return new AbstractMap.SimpleImmutableEntry<>(
                              new ArrayList<>(Collections.singleton(child)), new long[1]);
                        })
                    .getValue());
      }
      RemovalListener<RefCountingKey<K>, V> childRemovalListener =
          (key, value, cause) -> {
            deferRemovalQueue.add(
                new DeferredRemoval<>(
                    key.key, cause, childMask, key.knownRefCount, key.parentKey, key.keyScope));
          };
      child.register(childRemovalListener, RemovalListenerRegistry.Source.SELF_AND_CHILD, null);
      if (children.put(child, childRemovalListener) != null) {
        throw new IllegalStateException("double-added child");
      }
    }
    return childMask;
  }

  public void removeChild(RootCache<K, V> child) {
    synchronized (children) {
      RemovalListener<RefCountingKey<K>, V> childRemovalListener = children.remove(child);
      if (Objects.equals(tierScope, child.tierScope)) {
        assigned[0] &= ~child.mask;
        unscopedChildren.remove(child); // O(n); infrequent and limited size so ok.
      } else {
        scopedAssigned.computeIfPresent(
            child.tierScope,
            (k, v) -> {
              if ((v.getValue()[0] &= ~child.mask) == 0) {
                // remove entry if it's no longer tracking any assigned masks
                return null;
              } else {
                v.getKey().remove(child); // O(n); infrequent and limited size so ok.
                return v;
              }
            });
      }
      child.unregister(childRemovalListener, null);
    }
  }

  public int setMaxSize(int maxSize) {
    if (this.maxSize == maxSize) {
      return -1;
    }
    Cache<RefCountingKey<K>, ValRef<V>> sync = asyncCache.synchronous();
    Optional<Policy.Eviction<RefCountingKey<K>, ValRef<V>>> evictionOpt = sync.policy().eviction();
    if (evictionOpt.isEmpty()) {
      return -1;
    } else {
      Policy.Eviction<RefCountingKey<K>, ValRef<V>> eviction = evictionOpt.get();
      eviction.setMaximum(maxSize);
      this.maxSize = maxSize;
      initialSize = Math.min(1024, this.maxSize);
      sync.cleanUp();
      return initialSize;
    }
  }

  public boolean setMaxRamMB(long newMaxRamBytes) {
    if (newMaxRamBytes != maxRamBytes) {
      maxRamBytes = newMaxRamBytes;
      Cache<RefCountingKey<K>, ValRef<V>> sync = asyncCache.synchronous();
      Optional<Policy.Eviction<RefCountingKey<K>, ValRef<V>>> evictionOpt =
          sync.policy().eviction();
      if (evictionOpt.isPresent()) {
        Policy.Eviction<RefCountingKey<K>, ValRef<V>> eviction = evictionOpt.get();
        if (!eviction.isWeighted()) {
          // rebuild cache using weigher
          asyncCache = buildCache(asyncCache);
          return false;
        } else if (maxRamBytes == Long.MAX_VALUE) {
          // rebuild cache using maxSize
          asyncCache = buildCache(asyncCache);
          return false;
        }
        eviction.setMaximum(newMaxRamBytes);
        sync.cleanUp();
        return true;
      }
    }
    return false;
  }

  private AsyncCache<RefCountingKey<K>, ValRef<V>> buildCache(
      AsyncCache<RefCountingKey<K>, ValRef<V>> prev) {
    Caffeine<RefCountingKey<K>, ValRef<V>> builder =
        Caffeine.newBuilder()
            .initialCapacity(initialSize)
            .executor(Runnable::run)
            .evictionListener(rawEvictionListener)
            .recordStats();
    if (maxIdleTimeSec > 0) {
      builder.expireAfterAccess(Duration.ofSeconds(maxIdleTimeSec));
    }
    if (maxRamBytes != Long.MAX_VALUE) {
      builder.maximumWeight(maxRamBytes);
      builder.weigher(
          (k, v) ->
              (int) (v.recordedRamBytes != -1 ? v.recordedRamBytes : calcRamBytes(k.key, v.val)));
    } else {
      builder.maximumSize(maxSize);
    }
    removalListenerParityChecker = newRemovalInstanceParityChecker(builder);
    // TODO: we could probably make all non-root caches synchronous, but because it simplifies the
    //  code, and because we need someplace to record `mask` metadata anyway, we'll leave it
    //  all-async for now. Plan to circle back and evaluate performance implications.
    AsyncCache<RefCountingKey<K>, ValRef<V>> ret = builder.buildAsync();
    if (prev != null) {
      ret.asMap().putAll(prev.asMap());
    }
    return ret;
  }

  public V get(K rawKey) {
    // `get()` is different from other operations in that it can proceed opportunistically, and
    // return
    // any value, so we can loop from leaf to root without any locking
    RefCountingKey<K> key = new RefCountingKey<>(tierScope, rawKey);
    V ret = null;
    RootCache<K, V> c = this;
    for (; ; ) {
      // always consult at every level so that we get accurate stats and eviction policy operation
      CompletableFuture<ValRef<V>> f = c.asyncCache.getIfPresent(key);
      if ((c = c.parent) == null) {
        if (ret == null && f != null) {
          ValRef<V> valRef = f.getNow(null);
          ret = valRef == null ? null : valRef.val; // strong ref
        }
        break;
      } else if (ret == null && f != null) {
        // non-root should always return immediately, always be a SoftReference
        ret = f.getNow(null).ref.get();
      }
    }
    return ret;
  }

  public V put(K key, V value) {
    assert isLeaf;
    return parent == null
        ? rootLeafPut(key, value)
        : root.rootPut(key, value, pathFromRoot.iterator(), tierScope);
  }

  @SuppressWarnings("unchecked")
  private V rootPut(K key, V value, Iterator<RootCache<K, V>> pathToLeaf, String leafKeyScope) {
    final V[] ret = (V[]) new Object[1];
    final RootCache<K, V> child = pathToLeaf.next();
    final long childMask = child.mask;
    RefCountingKey<K> refCountingKey =
        new RefCountingKey<>(leafKeyScope, child.tierScope, key, childMask);
    asyncCache
        .asMap()
        .compute(
            refCountingKey,
            (k, v) -> {
              assert k.parentKey == null;
              long knownRefCount = k.incrementKnownRefCount(childMask);
              if (v == null) {
                assert k == refCountingKey;
                ret[0] = null;
                V fromNestedPut =
                    child.internalNestedPut(k.key, value, k, knownRefCount, pathToLeaf);

                // if we didn't have it, none of our children should have either
                assert fromNestedPut == null;

                return new MyCompletableFuture<>(
                    childMask, ValRef.strong(value, recordRamBytes(key, value)));
              } else {
                assert k != refCountingKey;
                // `getNow(null)` -- we return extant value opportunistically. If the computation is
                // still in flight, don't wait for it to finish.
                ValRef<V> extant = v.getNow(null);
                if (extant != null) {
                  ret[0] = extant.val;
                  assert ret[0] != null;
                }
                // first address any existing references
                final long extantMask = ((MyCompletableFuture<V>) v).refs;
                List<RootCache<K, V>> inScopeChildren =
                    k.tierScope == null || k.tierScope.equals(tierScope)
                        ? unscopedChildren
                        : scopedAssigned.get(k.tierScope).getKey();
                for (RootCache<K, V> c : inScopeChildren) {
                  if (c != child && (c.mask & extantMask) != 0) {
                    V fromNestedUpdate =
                        c.internalNestedUpdate(
                            k.key, value, k, k.updateKnownRefCount(knownRefCount, c.mask));

                    // if we had it, and child already did too, values should be identical
                    assert fromNestedUpdate == null || fromNestedUpdate == ret[0]
                        : fromNestedUpdate + " != " + ret[0];
                  }
                }
                V fromNestedPut =
                    child.internalNestedPut(
                        k.key, value, k, knownRefCount, pathToLeaf); // call but ignore return val

                // if we had it, and child already did too, values should be identical
                assert fromNestedPut == null || ret[0] == null || fromNestedPut == ret[0]
                    : fromNestedPut + " != " + ret[0] + "(new: " + value + ")";
                return new MyCompletableFuture<>(
                    extantMask | childMask, ValRef.strong(value, recordRamBytes(key, value)));
              }
            });
    pollDeferredRemovals();
    return ret[0];
  }

  private V rootLeafPut(K key, V value) {
    @SuppressWarnings("unchecked")
    final V[] ret = (V[]) new Object[1];
    RefCountingKey<K> refCountingKey = new RefCountingKey<>(tierScope, null, key, 0);
    asyncCache
        .asMap()
        .compute(
            refCountingKey,
            (k, v) -> {
              if (v == null) {
                assert k == refCountingKey;
              } else {
                assert k != refCountingKey;
                // `getNow(null)` -- we return extant value opportunistically. If the computation is
                // still in flight, don't wait for it to finish.
                ValRef<V> extant = v.getNow(null);
                if (extant != null) {
                  ret[0] = extant.val;
                  assert ret[0] != null;
                }
              }
              k.incrementKnownRefCount(0);
              return new MyCompletableFuture<>(0, ValRef.strong(value, recordRamBytes(key, value)));
            });
    return ret[0];
  }

  /** An interruptible alternative to {@link CompletableFuture#join()}. */
  private static <V> V get(CompletableFuture<V> f) {
    try {
      return f.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CompletionException(e);
    } catch (ExecutionException e) {
      throw new CompletionException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private V internalNestedPut(
      K key,
      V value,
      RefCountingKey<K> parentKey,
      long knownRefCount,
      Iterator<RootCache<K, V>> pathToLeaf) {
    final V[] ret = (V[]) new Object[1];
    if (isLeaf) {
      RefCountingKey<K> refCountingKey = new RefCountingKey<>(null, key, parentKey, knownRefCount);
      asyncCache
          .asMap()
          .compute(
              refCountingKey,
              (k, v) -> {
                assert !pathToLeaf.hasNext();
                if (v == null) {
                  assert k == refCountingKey;
                  ret[0] = null;
                } else {
                  assert k != refCountingKey;
                  assert parentKey == k.parentKey;
                  ret[0] =
                      get(v)
                          .ref
                          .get(); // don't care if this may be null, just return opportunistically
                  k.updateKnownRefCount(knownRefCount, 0);
                }
                // always replace with our value, in contrast to `internalNestedPutIfAbsent()`
                return new MyCompletableFuture<>(0, ValRef.soft(value, recordRamBytes(key, value)));
              });
    } else {
      final RootCache<K, V> child = pathToLeaf.next();
      final long childMask = child.mask;
      RefCountingKey<K> refCountingKey =
          new RefCountingKey<>(child.tierScope, key, parentKey, knownRefCount, childMask);
      asyncCache
          .asMap()
          .compute(
              refCountingKey,
              (k, v) -> {
                CompletableFuture<ValRef<V>> innerRet;
                if (v == null) {
                  assert k == refCountingKey;
                  assert parentKey == k.parentKey;
                  ret[0] = null;
                  V fromNestedPut =
                      child.internalNestedPut(k.key, value, k, knownRefCount, pathToLeaf);
                  assert fromNestedPut
                      == null; // if we didn't have it, none of our children should have either
                  innerRet =
                      new MyCompletableFuture<>(
                          childMask, ValRef.soft(value, recordRamBytes(key, value)));
                } else {
                  assert k != refCountingKey;
                  assert parentKey == k.parentKey;
                  ret[0] = get(v).ref.get(); // the extant value
                  // first address any existing references
                  final long extantMask = ((MyCompletableFuture<V>) v).refs;
                  List<RootCache<K, V>> inScopeChildren =
                      k.tierScope == null || k.tierScope.equals(tierScope)
                          ? unscopedChildren
                          : scopedAssigned.get(k.tierScope).getKey();
                  for (RootCache<K, V> c : inScopeChildren) {
                    if (c != child && (c.mask & extantMask) != 0) {
                      V fromNestedUpdate =
                          c.internalNestedUpdate(
                              k.key, value, k, k.updateKnownRefCount(knownRefCount, c.mask));
                      // either extant value or extant child value could be null (collected); but if
                      // non-null, they should be identical
                      assert ret[0] == null
                              || fromNestedUpdate == null
                              || ret[0] == fromNestedUpdate
                          : fromNestedUpdate + " != " + ret[0] + " (new: " + value + ")";
                    }
                  }
                  V fromNestedPut =
                      child.internalNestedPut(
                          k.key,
                          value,
                          k,
                          k.updateKnownRefCount(knownRefCount, childMask),
                          pathToLeaf);
                  // either extant value or extant child value could be null (collected); but if
                  // non-null, they should be identical
                  assert ret[0] == null || fromNestedPut == null || ret[0] == fromNestedPut
                      : fromNestedPut + " != " + ret[0] + " (new: " + value + ")";
                  innerRet =
                      new MyCompletableFuture<>(
                          ((MyCompletableFuture<V>) v).refs | childMask,
                          ValRef.soft(value, recordRamBytes(key, value)));
                  ;
                }
                return innerRet;
              });
      pollDeferredRemovals();
    }
    return ret[0];
  }

  @SuppressWarnings("unchecked")
  private V internalNestedUpdate(K key, V value, RefCountingKey<K> parentKey, long knownRefCount) {
    final V[] ret = (V[]) new Object[1];
    RefCountingKey<K> refCountingKey = new RefCountingKey<>(null, key, parentKey, knownRefCount);
    if (isLeaf) {
      asyncCache
          .asMap()
          .computeIfPresent(
              refCountingKey,
              (k, v) -> {
                assert k != refCountingKey;
                assert parentKey == k.parentKey;
                ret[0] =
                    get(v)
                        .ref
                        .get(); // don't care if this may be null, just return opportunistically
                k.updateKnownRefCount(knownRefCount, 0);
                // always replace with our value, in contrast to `internalNestedPutIfAbsent()`
                return new MyCompletableFuture<>(0, ValRef.soft(value, recordRamBytes(key, value)));
              });
    } else {
      asyncCache
          .asMap()
          .computeIfPresent(
              refCountingKey,
              (k, v) -> {
                assert k != refCountingKey;
                assert parentKey == k.parentKey;
                ret[0] = get(v).ref.get(); // the extant value
                // address any existing references
                final long extantMask = ((MyCompletableFuture<V>) v).refs;
                List<RootCache<K, V>> inScopeChildren =
                    k.tierScope == null || k.tierScope.equals(tierScope)
                        ? unscopedChildren
                        : scopedAssigned.get(k.tierScope).getKey();
                for (RootCache<K, V> c : inScopeChildren) {
                  if ((c.mask & extantMask) != 0) {
                    V fromNestedUpdate =
                        c.internalNestedUpdate(
                            k.key, value, k, k.updateKnownRefCount(knownRefCount, c.mask));
                    // either extant value or extant child value could be null (collected); but if
                    // non-null, they should be identical
                    assert ret[0] == null || fromNestedUpdate == null || ret[0] == fromNestedUpdate
                        : fromNestedUpdate + " != " + ret[0] + " (new: " + value + ")";
                  }
                }
                return new MyCompletableFuture<>(
                    ((MyCompletableFuture<V>) v).refs,
                    ValRef.soft(value, recordRamBytes(key, value)));
              });
      pollDeferredRemovals();
    }
    return ret[0];
  }

  public V remove(K rawKey) {
    return root.rootRemove(new RefCountingKey<>(tierScope, rawKey));
  }

  private V rootRemove(RefCountingKey<K> key) {
    @SuppressWarnings("unchecked")
    V[] ret = (V[]) new Object[1];
    asyncCache
        .asMap()
        .computeIfPresent(
            key,
            (k, v) -> {
              ValRef<V> valRef = v.getNow(null);
              if (valRef != null) {
                ret[0] = valRef.val;
                rawEvictionListener.onRemoval(k, valRef, RemovalCause.EXPLICIT);
              }
              return null;
            });
    return ret[0];
  }

  public long asyncHits() {
    return asyncHits.sum();
  }

  public long asyncLookups() {
    return asyncLookups.sum();
  }

  public long inserts() {
    return inserts.sum();
  }

  private final LongAdder asyncHits = new LongAdder();
  private final LongAdder asyncLookups = new LongAdder();
  private final LongAdder inserts = new LongAdder();
  private CacheStats discountStats;

  public void resetStats() {
    asyncHits.reset();
    asyncLookups.reset();
    inserts.reset();
    discountStats = asyncCache.synchronous().stats();
  }

  public CacheStats stats() {
    CacheStats raw = asyncCache.synchronous().stats();
    return discountStats == null ? raw : raw.minus(discountStats);
  }

  public long size() {
    return asyncCache.synchronous().estimatedSize();
  }

  public V computeIfAbsent(K key, IOFunction<? super K, ? extends V> mappingFunction)
      throws IOException {
    if (!isLeaf) {
      throw new IllegalStateException("may only call computeIfAbsent() on leaf caches!");
    }
    // NOTE: we support using this as a singleton cache (i.e., leaf with no parent)
    return root.rootComputeIfAbsent(
        key, mappingFunction, root == this ? null : pathFromRoot.iterator(), tierScope);
  }

  @SuppressWarnings("unchecked")
  private V internalNestedPutIfAbsent(
      K key,
      V value,
      RefCountingKey<K> parentKey,
      long knownRefCount,
      Iterator<RootCache<K, V>> pathToLeaf) {
    final V[] ret = (V[]) new Object[1];
    if (isLeaf) {
      RefCountingKey<K> refCountingKey = new RefCountingKey<>(null, key, parentKey, knownRefCount);
      asyncCache
          .asMap()
          .compute(
              refCountingKey,
              (k, v) -> {
                assert !pathToLeaf.hasNext();
                if (v == null) {
                  assert k == refCountingKey;
                  ret[0] = value;
                  return new MyCompletableFuture<>(
                      0, ValRef.soft(value, recordRamBytes(key, value)));
                } else {
                  assert k != refCountingKey;
                  assert parentKey == k.parentKey : parentKey + " != " + k.parentKey;
                  V extant = get(v).ref.get();
                  if (extant == null) {
                    // this should actually be possible at the leaf.
                    // TODO: figure out how to handle the existing entry
                    ret[0] = value;
                  } else {
                    ret[0] = extant;
                  }
                  k.updateKnownRefCount(knownRefCount, 0);
                  return v;
                }
              });
    } else {
      final RootCache<K, V> child = pathToLeaf.next();
      final long childMask = child.mask;
      RefCountingKey<K> refCountingKey =
          new RefCountingKey<>(child.tierScope, key, parentKey, knownRefCount, childMask);
      asyncCache
          .asMap()
          .compute(
              refCountingKey,
              (k, v) -> {
                CompletableFuture<ValRef<V>> innerRet;
                if (v == null) {
                  assert k == refCountingKey;
                  assert parentKey == k.parentKey;
                  ret[0] = value;
                  V fromNestedPut =
                      child.internalNestedPutIfAbsent(k.key, value, k, knownRefCount, pathToLeaf);
                  assert fromNestedPut
                      == value; // if we didn't have it, none of our children should have either
                  innerRet =
                      new MyCompletableFuture<>(
                          childMask, ValRef.soft(value, recordRamBytes(key, value)));
                } else {
                  assert k != refCountingKey;
                  assert parentKey == k.parentKey : parentKey + " != " + k.parentKey;
                  ((MyCompletableFuture<V>) v).registerChildMask(childMask);
                  V fromNestedPut =
                      child.internalNestedPutIfAbsent(
                          k.key,
                          value,
                          k,
                          k.updateKnownRefCount(knownRefCount, childMask),
                          pathToLeaf); // call but ignore return val
                  V extant;
                  assert value == (extant = get(v).ref.get()) : value + " != " + extant;
                  ret[0] = value;

                  // if we had it, and our children already did too, values should be identical
                  assert fromNestedPut == value;

                  innerRet = v;
                }
                return innerRet;
              });
      pollDeferredRemovals();
    }
    return ret[0];
  }

  private long recordRamBytes(K key, V value) {
    long recorded = calcRamBytes(key, value);
    ramBytes.add(recorded);
    return recorded;
  }

  private static <K, V> long calcRamBytes(K key, V value) {
    assert value != null;
    return RamUsageEstimator.sizeOfObject(key, RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED)
        + RamUsageEstimator.sizeOfObject(value, RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED)
        + RamUsageEstimator.LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY
        + RAM_BYTES_PER_FUTURE;
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + initialRamBytes + ramBytes.sum();
  }

  public Map.Entry<K, Exception> forEachTopEntry(
      int maxEntries, IOFunction<Map.Entry<K, V>, Boolean> regenerate) {
    Map<RefCountingKey<K>, ValRef<V>> hottest =
        asyncCache.synchronous().policy().eviction().map(p -> p.hottest(maxEntries)).orElse(null);
    if (hottest != null) {
      for (Map.Entry<RefCountingKey<K>, ValRef<V>> e : hottest.entrySet()) {
        K key = e.getKey().key;
        try {
          if (!regenerate.apply(new AbstractMap.SimpleImmutableEntry<>(key, e.getValue().val))) {
            break;
          }
        } catch (Exception ex) {
          return new AbstractMap.SimpleImmutableEntry<>(key, ex);
        }
      }
    }
    return null;
  }

  public void cleanup() {
    pollDeferredRemovals();
    asyncCache.synchronous().cleanUp();
  }

  private static class ValRef<V> {
    private final V val;
    private final SoftReference<V> ref;
    private long recordedRamBytes;

    private static <V> ValRef<V> strong(V val, long ramBytes) {
      return new ValRef<>(val, null, ramBytes);
    }

    private static <V> ValRef<V> soft(V val, long ramBytes) {
      return new ValRef<>(null, new SoftReference<>(val), ramBytes);
    }

    private ValRef(V val, SoftReference<V> ref, long recordedRamBytes) {
      this.val = val;
      this.ref = ref;
      this.recordedRamBytes = recordedRamBytes;
    }
  }

  @SuppressWarnings("unchecked")
  private V rootComputeIfAbsent(
      K key,
      IOFunction<? super K, ? extends V> mappingFunction,
      Iterator<RootCache<K, V>> pathToLeaf,
      String leafKeyScope)
      throws IOException {
    boolean[] weCompute = new boolean[1];
    final RootCache<K, V> child;
    final long childMask;
    final String childTierScope;
    if (isLeaf) {
      child = null;
      childMask = 0;
      childTierScope = null;
    } else {
      child = pathToLeaf.next();
      childMask = child.mask;
      childTierScope = child.tierScope;
    }
    RefCountingKey<K> refCountingKey =
        new RefCountingKey<>(leafKeyScope, childTierScope, key, childMask);
    @SuppressWarnings({"unchecked", "rawtypes"})
    RefCountingKey<K>[] rootKey = new RefCountingKey[] {refCountingKey};
    CompletableFuture<ValRef<V>> f =
        asyncCache
            .asMap()
            .compute(
                refCountingKey,
                (k, v) -> {
                  if (v == null) {
                    assert refCountingKey == k;
                    weCompute[0] = true;
                    return new MyCompletableFuture<>(0);
                  }
                  assert refCountingKey != k;
                  rootKey[0] = k;
                  return v;
                });
    V ret;
    if (weCompute[0]) {
      try {
        ret = mappingFunction.apply(key);
        f.complete(ValRef.strong(ret, -1));
      } catch (Throwable t) {
        f.completeExceptionally(t);
        throw t;
      }
    } else {
      QueryLimits queryLimits = QueryLimits.getCurrentLimits();
      TimeAllowedLimit timeLimit;
      if (queryLimits != null
          && (timeLimit =
                  (TimeAllowedLimit)
                      queryLimits.currentLimitValueFor(TimeAllowedLimit.class).orElse(null))
              != null) {
        try {
          ret = f.get(timeLimit.nanosRemaining(), TimeUnit.NANOSECONDS).val;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new CompletionException(e);
        } catch (TimeoutException e) {
          throw new CompletionException(e);
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();
          if (cause instanceof IOException) {
            // Computation had an IOException, likely index problems, so fail this result too
            throw (IOException) cause;
          }
          if (cause instanceof CancellableCollector.QueryCancelledException) {
            // The reserved slot that we were waiting for got cancelled, so we will compute directly
            // If we go back to waiting for a new cache result then that can lead to thread
            // starvation. Should we record a cache miss here?
            return mappingFunction.apply(key);
          }
          throw new CompletionException(e);
        }
      } else {
        ret = get(f).val;
      }
    }
    if (child != null) {
      asyncCache
          .asMap()
          .computeIfPresent(
              refCountingKey,
              (k, v) -> {
                final V useValue;
                if (k != rootKey[0]) {
                  assert v != f;
                  ValRef<V> valRef = v.getNow(null);
                  if (valRef == null) {
                    // a `join()` here would deadlock. This case should be incredibly rare, and
                    // should only occur under absurdly heavy load. So although we could cycle
                    // back and do v.join() outside of computeIfPresent(), there's no reason to
                    // think that the corresponding entry would still be present by the time we
                    // come back to propagate it to children, so we're better off just leaving
                    // the current entry in place, skipping adding to children, and returning
                    // the evicted entry's value (which should be perfectly valid) to the caller.
                    return v;
                  }
                  useValue = valRef.val;
                } else if (v != f) {
                  // the only way this happens (same key, different value) is as the result of an
                  // unconditional "put()", so we know the value will be populated (let NPE be
                  // thrown as an implicit assertion)
                  useValue = v.getNow(null).val;
                } else {
                  assert v == f;
                  assert ret == v.getNow(null).val; // value should be populated
                  if (weCompute[0]) {
                    // only `recordRamBytes()` once, and only when we're certain that the concrete
                    // value has been added to the map.
                    ValRef<V> valRef = get(v); // `join()` here is safe, should return immediately
                    assert valRef.recordedRamBytes == -1 : "should only be set once";
                    assert ret == valRef.val;
                    valRef.recordedRamBytes = recordRamBytes(key, ret);
                  }
                  useValue = ret;
                }
                final long knownRefCount = k.incrementKnownRefCount(childMask);
                ((MyCompletableFuture<V>) v).registerChildMask(childMask);
                assert knownRefCount > 0;
                V fromNestedPut =
                    child.internalNestedPutIfAbsent(key, useValue, k, knownRefCount, pathToLeaf);
                assert fromNestedPut == useValue : fromNestedPut + " != " + useValue;
                return v;
              });
      pollDeferredRemovals();
    }
    return ret;
  }

  @SuppressWarnings("unchecked")
  private void pollDeferredRemovals() {
    DeferredRemoval<K> toRemove;
    while ((toRemove = deferRemovalQueue.poll()) != null) {
      // TODO: consider limiting the number of times any given thread will loop here?
      DeferredRemoval<K> e = toRemove;
      asyncCache
          .asMap()
          .computeIfPresent(
              new RefCountingKey<>(e.keyScope, e.key),
              (k, f) -> {
                if (e.parentKey != k) {
                  // One way this could happen: pk's associated entries are evicted simultaneously
                  // from both children. before the deferred evictions are processed on the parent,
                  // the parent evicts inherently, but the same key is inserted again under a new
                  // `RefCountingKey` instance before processing the evictions from children that
                  // are associated with the old key. The right thing to do is ignore these deferred
                  // evictions because they are guaranteed to be stale.
                  return f;
                }
                if (k.knownRefCount > e.knownRefCount
                    && k.getKnownRefCount(e.mask) > e.knownRefCount) {
                  // there have been cache insertions for this key to the child that generated this
                  // eviction event, so the eviction event is stale and should be ignored.
                  return f;
                }
                if (((MyCompletableFuture<V>) f).unregisterChildMask(e.mask) != 0) {
                  // there are still references from children other than the one that generated this
                  // eviction event, so the entry should not yet be removed locally
                  return f;
                } else {
                  // there are no more child references to this entry, so it should be removed
                  // locally
                  ValRef<V> valRef = f.getNow(null);
                  V val = parent == null ? valRef.val : valRef.ref.get();
                  assert val != null;
                  RemovalCause cause = e.cause;
                  removalListeners.onRemoval(
                      k, val, cause, RemovalListenerRegistry.Source.CHILD, k.tierScope);
                  onRemoval(k.key, valRef, val, cause);
                  return null;
                }
              });
    }
  }

  private static final class MyCompletableFuture<V> extends CompletableFuture<V> {
    private long refs;

    private MyCompletableFuture(long initialMask) {
      refs = initialMask;
    }

    private MyCompletableFuture(long initialMask, V value) {
      refs = initialMask;
      complete(value);
    }

    private long registerChildMask(long childMask) {
      long ret = refs;
      refs |= childMask;
      return ret;
    }

    private long unregisterChildMask(long childMask) {
      return refs &= ~childMask;
    }
  }

  private static final class RefCountingKey<K> {
    private final String keyScope;
    private final String tierScope;
    private final K key;
    private final RefCountingKey<K> parentKey;
    private long knownRefCount;
    private final long[] knownRefCounts;

    private RefCountingKey(String keyScope, K key) {
      this.keyScope = keyScope;
      this.key = key;
      this.tierScope = null;
      this.parentKey = null;
      this.knownRefCounts = null;
    }

    private RefCountingKey(String keyScope, String tierScope, K key, long childMask) {
      this.keyScope = keyScope;
      this.tierScope = tierScope;
      this.key = key;
      this.parentKey = null;
      if (childMask == 0) {
        knownRefCounts = null;
      } else {
        knownRefCounts = new long[Long.SIZE];
      }
    }

    private RefCountingKey(
        String tierScope, K key, RefCountingKey<K> parentKey, long initialKnownRefCount) {
      this.keyScope = parentKey.keyScope;
      this.tierScope = tierScope;
      this.key = key;
      this.parentKey = parentKey;
      knownRefCount = initialKnownRefCount;
      knownRefCounts = null;
    }

    private RefCountingKey(
        String tierScope,
        K key,
        RefCountingKey<K> parentKey,
        long initialKnownRefCount,
        long childMask) {
      this.keyScope = parentKey.keyScope;
      this.tierScope = tierScope;
      this.key = key;
      this.parentKey = parentKey;
      knownRefCount = initialKnownRefCount;
      knownRefCounts = new long[Long.SIZE];
      knownRefCounts[Long.numberOfLeadingZeros(childMask)] = initialKnownRefCount;
    }

    private long updateKnownRefCount(long update, long childMask) {
      assert update >= knownRefCount;
      knownRefCount = update;
      if (childMask == 0) {
        return update;
      }
      int idx = Long.numberOfLeadingZeros(childMask);
      assert update >= knownRefCounts[idx];
      knownRefCounts[idx] = update;
      return update;
    }

    private long incrementKnownRefCount(long childMask) {
      long ret = ++knownRefCount;
      if (childMask == 0) {
        return ret;
      }
      int idx = Long.numberOfLeadingZeros(childMask);
      assert ret >= knownRefCounts[idx];
      knownRefCounts[idx] = ret;
      return ret;
    }

    private long getKnownRefCount(long childMask) {
      return knownRefCounts[Long.numberOfLeadingZeros(childMask)];
    }

    @Override
    public int hashCode() {
      return keyScope == null ? key.hashCode() : (key.hashCode() ^ keyScope.hashCode());
    }

    @Override
    @SuppressWarnings("EqualsUnsafeCast")
    public boolean equals(Object obj) {
      RefCountingKey<?> other = (RefCountingKey<?>) obj;
      return key.equals(other.key) && Objects.equals(keyScope, other.keyScope);
    }

    @Override
    public String toString() {
      return getClass().getName() + "@" + Integer.toHexString(System.identityHashCode(this));
    }
  }

  private static final class DeferredRemoval<K> {
    private final String keyScope;
    private final K key;
    private final RemovalCause cause;
    private final long mask;
    private final long knownRefCount;
    private final RefCountingKey<K> parentKey;

    private DeferredRemoval(
        K key,
        RemovalCause cause,
        long mask,
        long knownRefCount,
        RefCountingKey<K> parentKey,
        String keyScope) {
      this.key = key;
      this.cause = cause;
      this.mask = mask;
      this.knownRefCount = knownRefCount;
      this.parentKey = parentKey;
      this.keyScope = keyScope;
    }
  }

  private final Queue<DeferredRemoval<K>> deferRemovalQueue = new ConcurrentLinkedQueue<>();

  int deferredRemaining() {
    return deferRemovalQueue.size();
  }

  public boolean register(
      RemovalListener<RefCountingKey<K>, V> listener,
      RemovalListenerRegistry.Source source,
      String tierScope) {
    return removalListeners.register(listener, source, tierScope);
  }

  public boolean unregister(RemovalListener<RefCountingKey<K>, V> listener, String tierScope) {
    return removalListeners.unregister(listener, tierScope);
  }

  @SuppressWarnings("rawtypes")
  public void close() {
    if (parent != null) {
      parent.unregister(
          parentRemovalListener, !Objects.equals(parent.tierScope, tierScope) ? tierScope : null);
      parent.removeChild(this);
    }
    List<RootCache<?, ?>> childrenToClose;
    synchronized (children) {
      if (children.isEmpty()) {
        return;
      }
      // TODO: this is unusual, and perhaps we should just strictly enforce that children should
      //  always be closed explicitly before parents are closed?
      //  children cannot exist without parents, so if we are closing all our children must
      //  close also.
      childrenToClose = List.of(children.keySet().toArray(new RootCache[0]));
    }
    for (RootCache<?, ?> child : childrenToClose) {
      child.close();
    }
  }

  public void clear() {
    // NOTE: here we follow the pattern in `solr.CaffeineCache`, but perhaps we could instead, e.g.,
    // do: `asyncCache.asMap().clear()`?
    asyncCache.synchronous().invalidateAll();
    ramBytes.reset();
  }

  private void onRemoval(K key, ValRef<V> valRef, V val, RemovalCause cause) {
    if (valRef.recordedRamBytes >= 0) {
      ramBytes.add(-valRef.recordedRamBytes);
    }
    onRemoval(key, val, cause);
  }

  @Override
  public void onRemoval(K key, V value, RemovalCause cause) {
    assert removalListenerParityChecker.increment();
    if (externalListener != null) {
      externalListener.onRemoval(key, value, cause);
    }
  }

  private static <K, V> RemovalListenerParityChecker<K, ValRef<V>> newRemovalInstanceParityChecker(
      Caffeine<K, ValRef<V>> builder) {
    boolean[] assertionsEnabled = new boolean[1];
    assert assertionsEnabled[0] = true;
    return assertionsEnabled[0] ? new RemovalListenerParityChecker<>(builder) : null;
  }

  boolean verifyRemovalCounts() {
    return removalListenerParityChecker.verifyRemovalCounts();
  }

  static final class RemovalListenerParityChecker<K, V> {
    private final LongAdder stockRemovalEventCount = new LongAdder();
    private final LongAdder manualRemovalEventCount = new LongAdder();

    RemovalListenerParityChecker(Caffeine<K, V> builder) {
      builder.removalListener(
          (k, v, c) -> {
            stockRemovalEventCount.increment();
          });
    }

    /**
     * Verifies that we are reconstructing removal events that accurately correspond to removal
     * events generated
     *
     * <p>TODO: make this private and call it from within an `assert` in yet-to-be-implemented
     * `close()` method
     */
    boolean verifyRemovalCounts() {
      long one = stockRemovalEventCount.sum();
      long two = manualRemovalEventCount.sum();
      log.info("verify! {} ?= {}", one, two);
      return one == two;
    }

    boolean increment() {
      manualRemovalEventCount.increment();
      return true;
    }
  }
}
