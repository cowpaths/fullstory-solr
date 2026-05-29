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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Generic concurrent LRU cache with pin/unpin support.
 *
 * <p>Subclasses supply the concrete {@link Node} subtype (which carries the payload) and override
 * {@link #createNode} to construct it. This class manages the doubly-linked LRU list, the pin/unpin
 * reference-count protocol, and the acquire/release/recycle lifecycle.
 *
 * <p><b>LRU invariant:</b> a node is in the doubly-linked list if and only if its {@code refCount}
 * is 0 (evictable). Pinned nodes (refCount &gt; 0) are spliced out of the list.
 *
 * <ul>
 *   <li>{@link #pin}: first pin (CAS 0&rarr;1) removes the node from the list; re-pins (CAS
 *       N&rarr;N+1, N&gt;0) are pure refcount increments.
 *   <li>{@link #unpin}: last unpin (result=0) inserts the node at the list head (most-recently
 *       used); higher unpins are pure refcount decrements.
 *   <li>{@link #acquireNode}: evicts the tail node and returns a fresh pinned node carrying the
 *       same value (the subclass controls the concrete node type via {@link #createNode}).
 * </ul>
 *
 * @param <V> the value type carried by each node
 */
class Cache<V, N extends Cache.Node<V>> {

  // ---------------------------------------------------------------------------
  // Node
  // ---------------------------------------------------------------------------

  /**
   * A linked-list node carrying a reference count for safe concurrent eviction.
   *
   * <p>The {@code value} field holds the payload managed by the cache (e.g. a {@link
   * java.nio.ByteBuffer} for {@link BlockCache}); it is {@code null} for sentinel nodes. Subclasses
   * may also add typed convenience fields that alias this value.
   *
   * <ul>
   *   <li>&ge;1 — pinned (not in LRU list)
   *   <li>0 — evictable (in LRU list)
   *   <li>-1 — permanently dead (evicted); node must not be used
   * </ul>
   *
   * <p>Head of the list = most recently used; tail = least recently used / eviction candidate.
   */
  static class Node<V> {

    /** The payload managed by this cache entry. {@code null} for sentinel nodes. */
    final V value;

    /**
     * Reference count.
     *
     * <ul>
     *   <li>&ge;1 — pinned (not in LRU list)
     *   <li>0 — evictable (in LRU list)
     *   <li>-1 — permanently dead (evicted); node must not be used
     * </ul>
     */
    final AtomicInteger refCount;

    /** LRU list link toward the head (most-recently-used end). */
    final AtomicReference<Node<V>> next = new AtomicReference<>();

    /** LRU list link toward the tail (least-recently-used end). */
    volatile Node<V> prev;

    Node(V value, Node<V> prev, int initialRefCount) {
      this.value = value;
      this.prev = prev;
      this.refCount = new AtomicInteger(initialRefCount);
    }

    /** Sentinel constructor (head / tail / protocol sentinels). */
    Node() {
      this.value = null;
      this.refCount = null;
    }
  }

  // ---------------------------------------------------------------------------
  // Sentinels
  // ---------------------------------------------------------------------------

  // Static prototypes typed as Node<?> to avoid the type-parameter restriction on static fields.
  // Each Cache<V> instance casts these once to Node<V> final fields (see constructor below);
  // all subsequent uses are fully typed. The cast is safe because sentinels are only ever
  // identity-compared (==) and their value/refCount fields are never accessed.
  private static final Node<?> RESERVED_PROTO = new Node<>();
  private static final Node<?> REMOVED_PROTO = new Node<>();

  // Per-instance typed views of the sentinels.
  // RESERVED: this node's `next` link is currently being modified by exactly one thread.
  // REMOVED:  this node has been spliced out of the list.
  final Node<V> RESERVED;
  final Node<V> REMOVED;

  /** Most-recently-used sentinel. Real nodes are inserted immediately after this. */
  private final Node<V> lruHead = new Node<>();

  /** Least-recently-used sentinel. Eviction candidates are immediately before this. */
  private final Node<V> lruTail = new Node<>();

  @SuppressWarnings("unchecked")
  private Cache() {
    RESERVED = (Node<V>) RESERVED_PROTO;
    REMOVED = (Node<V>) REMOVED_PROTO;
    lruHead.next.set(lruTail);
    lruTail.prev = lruHead;
  }

  /**
   * Initializing constructor: wires {@code initialValues} into the LRU list in order (first value
   * at the head, last at the tail) using {@link #createNode}. Equivalent to calling the no-arg
   * constructor followed by {@link #insertAtTail} for each value, but faster because no CAS is
   * needed during single-threaded initialization.
   */
  Cache(V[] initialValues) {
    this();
    Node<V> prev = lruHead;
    for (V value : initialValues) {
      N node = createNode(value, prev, 0);
      prev.next.set(node);
      prev = node;
    }
    prev.next.set(lruTail);
    lruTail.prev = prev;
  }

  // ---------------------------------------------------------------------------
  // Node factory
  // ---------------------------------------------------------------------------

  /**
   * Creates a new node carrying {@code value}. Subclasses override to return a concrete subtype
   * (e.g. {@link BlockCache.Node}), ensuring that all nodes in the list are of the expected type.
   */
  @SuppressWarnings("unchecked")
  protected N createNode(V value, Node<V> prev, int initialRefCount) {
    return (N) new Node<>(value, prev, initialRefCount);
  }

  // ---------------------------------------------------------------------------
  // LRU list operations
  // ---------------------------------------------------------------------------

  /**
   * Atomically acquires the exclusive right to modify {@code ref.next}. Spins while the link is
   * held by another thread (RESERVED), then CAS-swaps it to {@code reservation} and returns the
   * prior value. Returns REMOVED immediately if the node has already been spliced out.
   */
  Node<V> reserve(Node<V> ref, Node<V> reservation) {
    Node<V> next = ref.next.get();
    for (; ; ) {
      while (next == RESERVED_PROTO) {
        if (reservation == REMOVED_PROTO) {
          Thread.yield();
        }
        next = ref.next.get();
      }
      if (next == REMOVED_PROTO) {
        return next;
      }
      Node<V> extant = ref.next.compareAndExchange(next, reservation);
      if (extant == next) {
        return next;
      }
      next = extant;
    }
  }

  void insertAtHead(Node<V> node) {
    Node<V> oldNext = reserve(lruHead, RESERVED);
    assert oldNext != REMOVED_PROTO : "lruHead sentinel should never be removed";
    node.next.set(oldNext);
    oldNext.prev = node;
    if (!lruHead.next.compareAndSet(RESERVED, node)) {
      throw new IllegalStateException("unexpected concurrent modification of lruHead.next");
    }
  }

  boolean removeFromList(Node<V> node) {
    Node<V> next = reserve(node, REMOVED);
    if (next == REMOVED_PROTO) {
      return false;
    }
    Node<V> prev;
    for (; ; ) {
      prev = node.prev;
      if (prev.next.compareAndSet(node, RESERVED)) {
        break;
      }
      Thread.yield();
    }
    next.prev = prev;
    if (!prev.next.compareAndSet(RESERVED, next)) {
      throw new IllegalStateException("unexpected concurrent modification during list removal");
    }
    return true;
  }

  void insertAtTail(V value) {
    for (; ; ) {
      Node<V> pred = lruTail.prev;
      Node<V> oldNext = reserve(pred, RESERVED);
      if (oldNext != lruTail) {
        if (oldNext == REMOVED_PROTO) {
          continue; // pred was concurrently removed; retry
        }
        // release reservation; pred is no longer tail's predecessor
        if (!pred.next.compareAndSet(RESERVED, oldNext)) {
          throw new IllegalStateException();
        }
        continue;
      }
      Node<V> node = createNode(value, pred, 0);
      node.next.set(lruTail);
      lruTail.prev = node;
      if (!pred.next.compareAndSet(RESERVED, node)) {
        throw new IllegalStateException("unexpected concurrent modification during tail insertion");
      }
      return;
    }
  }

  // ---------------------------------------------------------------------------
  // Pin / unpin
  // ---------------------------------------------------------------------------

  static final int UNPIN_SENTINEL = Integer.MIN_VALUE >> 1;

  /**
   * Pins {@code node} for the duration of a read, preventing eviction. Returns {@code false} if the
   * node is permanently dead (evicted); the caller must then fall back to loading.
   *
   * <p>If this is the <em>first</em> pin (refCount transitions 0&rarr;1), the node is removed from
   * the LRU list (it is no longer evictable). If the node is already pinned (refCount&gt;0), the
   * refcount is simply incremented with no list operation.
   */
  boolean pin(Node<V> node) {
    int rc = node.refCount.get();
    for (; ; ) {
      switch (rc) {
        case -1:
          return false;
        case UNPIN_SENTINEL:
          rc = node.refCount.get();
          continue;
      }
      assert rc >= 0;
      int witness = node.refCount.compareAndExchange(rc, rc + 1);
      if (witness == rc) {
        break;
      } else {
        rc = witness;
      }
    }
    if (rc == 0) {
      // First pin: remove from the evictable list.
      removeFromList(node);
    }
    return true;
  }

  /**
   * Releases a read pin. If this is the last pin (refCount transitions 1&rarr;0), the node is
   * inserted at the LRU head (most-recently-used position) and becomes evictable.
   */
  void unpin(Node<V> node) {
    int rc = node.refCount.get();
    for (; ; ) {
      if (rc == 1) {
        int witness = node.refCount.compareAndExchange(1, UNPIN_SENTINEL);
        if (witness == 1) {
          node.prev = lruHead;
          insertAtHead(node);
          if (!node.refCount.compareAndSet(UNPIN_SENTINEL, 0)) {
            throw new IllegalStateException();
          }
          return;
        } else {
          rc = witness;
        }
      } else {
        assert rc > 1;
        int witness = node.refCount.compareAndExchange(rc, rc - 1);
        if (witness == rc) {
          return;
        } else {
          rc = witness;
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Acquire / release
  // ---------------------------------------------------------------------------

  /**
   * Acquires a pinned node whose value is ready to be used. Evicts the least-recently-used
   * evictable node from the tail of the list, then returns a fresh node (via {@link #createNode})
   * carrying the same value, pinned (refCount=1) and not yet in the list.
   *
   * <p>Returns {@code null} if the list is empty (all nodes are pinned).
   */
  N acquireNode() {
    for (Node<V> candidate; (candidate = lruTail.prev) != lruHead; ) {
      if (candidate.refCount.compareAndSet(0, -1)) {
        if (!removeFromList(candidate)) {
          throw new IllegalStateException();
        }
        return createNode(candidate.value, null, 1);
      }
      // CAS failed: a concurrent pin() or acquireNode() just claimed this node and is in the
      // middle of removeFromList(). Spin briefly; lruTail.prev will change momentarily.
    }
    return null; // list empty — all nodes pinned
  }

  /**
   * Explicitly releases a node when its owning resource is closed. If the node is still evictable
   * (refCount==0 and in the list), marks it dead and recycles its value back to the pool via {@link
   * #insertAtTail}, making it the highest-priority eviction candidate for reuse. Returns {@code
   * true} if the node was successfully released, {@code false} if the node was already evicted or
   * pinned (best-effort; caller need not retry).
   */
  boolean close(Node<V> node) {
    if (!node.refCount.compareAndSet(0, -1)) {
      // pinned or already dead — best effort, bail
      return false;
    }
    if (removeFromList(node)) {
      insertAtTail(node.value);
    } else {
      throw new IllegalStateException();
    }
    return true;
  }
}
