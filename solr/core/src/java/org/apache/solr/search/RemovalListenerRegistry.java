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

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import java.lang.invoke.MethodHandles;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A listener registry that protects against resource leaks
 *
 * @param <K> key type
 * @param <V> value type
 */
public class RemovalListenerRegistry<K, V> {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public enum Source {
    PARENT(0b100),
    SELF(0b010),
    CHILD(0b001),
    SELF_AND_CHILD(0b011),
    SELF_AND_PARENT(0b110),
    CHILD_AND_PARENT(0b101),
    ALL(0b111);

    private final int contains;

    Source(int contains) {
      this.contains = contains;
    }

    private boolean includes(Source source) {
      return this == source || (this.contains & source.contains) == source.contains;
    }
  }

  private final boolean autoExpunge;
  private final Set<Ref<RemovalListener<K, V>>> listeners = ConcurrentHashMap.newKeySet();
  private final Map<String, Set<Ref<RemovalListener<K, V>>>> tierScopedListeners =
      new ConcurrentHashMap<>();

  public RemovalListenerRegistry() {
    this(true);
  }

  public RemovalListenerRegistry(boolean autoExpunge) {
    this.autoExpunge = autoExpunge;
  }

  /**
   * NOTE: initially we are extremely defensive about the possibility of a memory leak here, so we
   * handle weak references ourselves. This is not the normal "use WeakReference so you don't have
   * to be careful with your code" -- rather, it's a (possibly temporary) insurance that we will be
   * proactively aware of any issues that do arise while this is still "in beta".
   */
  private final ReferenceQueue<RemovalListener<K, V>> refQueue = new ReferenceQueue<>();

  private static final class Ref<T> extends WeakReference<T> {
    private final String scope;
    private final int hash;
    private final Source source;

    public Ref(T referent, ReferenceQueue<? super T> q, Source source, String scope) {
      super(referent, q);
      this.hash = System.identityHashCode(referent);
      this.source = source;
      this.scope = scope;
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        // these will only be cleanup from the reference queue.
        return true;
      }
      T ours = get();
      return ours != null && ours.equals(((Ref<?>) obj).get());
    }
  }

  public boolean register(RemovalListener<K, V> listener, Source source, String tierScope) {
    if (autoExpunge) {
      expunge(true);
    }
    boolean ret;
    if (tierScope == null) {
      ret = listeners.add(new Ref<>(listener, refQueue, source, null));
    } else {
      ret =
          tierScopedListeners
              .computeIfAbsent(tierScope, (k) -> ConcurrentHashMap.newKeySet())
              .add(new Ref<>(listener, refQueue, source, tierScope));
    }
    if (!ret) {
      log.warn("already added listener: {}", listener);
    }
    return ret;
  }

  public boolean unregister(RemovalListener<K, V> listener, String tierScope) {
    final boolean ret;
    if (tierScope == null) {
      ret = listeners.remove(new Ref<>(listener, null, null, null));
    } else {
      boolean[] removed = new boolean[1];
      tierScopedListeners.computeIfPresent(
          tierScope,
          (k, v) -> {
            removed[0] = v.remove(new Ref<>(listener, null, null, null));
            return v.isEmpty() ? null : v;
          });
      ret = removed[0];
    }
    if (!ret) {
      log.warn("attempted to remove absent listener of scope \"{}\": {}", tierScope, listener);
    }
    return ret;
  }

  @SuppressWarnings("unchecked")
  public int expunge(boolean suppressLogWarnings) {
    int ret = 0;
    Ref<RemovalListener<K, V>> collected;
    while ((collected = (Ref<RemovalListener<K, V>>) refQueue.poll()) != null) {
      final boolean removed;
      if (collected.scope == null) {
        removed = listeners.remove(collected);
      } else {
        boolean[] didRemove = new boolean[1];
        final Ref<RemovalListener<K, V>> collectedF = collected;
        tierScopedListeners.computeIfPresent(
            collected.scope,
            (k, v) -> {
              didRemove[0] = v.remove(collectedF);
              return v.isEmpty() ? null : v;
            });
        removed = didRemove[0];
      }
      if (removed) {
        ret++;
        if (!suppressLogWarnings) {
          log.warn("Resource leak! (failed to manually unregister listener)");
        }
      }
    }
    return ret;
  }

  public boolean isEmpty() {
    return listeners.isEmpty();
  }

  public void onRemoval(K key, V value, RemovalCause cause, Source source, String tierScope) {
    boolean missingRef = notifyListeners(listeners, key, value, cause, source);
    Set<Ref<RemovalListener<K, V>>> inScopeListeners;
    if (tierScope != null && (inScopeListeners = tierScopedListeners.get(tierScope)) != null) {
      missingRef |= notifyListeners(inScopeListeners, key, value, cause, source);
    }
    if (missingRef) {
      expunge(false);
    }
  }

  private boolean notifyListeners(
      Set<Ref<RemovalListener<K, V>>> listeners,
      K key,
      V value,
      RemovalCause cause,
      Source source) {
    boolean missingRef = false;
    for (Ref<RemovalListener<K, V>> listenerRef : listeners) {
      if (!listenerRef.source.includes(source)) {
        continue;
      }
      RemovalListener<K, V> listener = listenerRef.get();
      if (listener == null) {
        missingRef = true;
      } else {
        listener.onRemoval(key, value, cause);
      }
    }
    return missingRef;
  }
}
