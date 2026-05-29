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
 * <p>Subclasses supply the concrete {@link Node} subtype (which carries the payload) and implement
 * resource-specific acquisition and eviction logic. This class manages only the doubly-linked LRU
 * list and the pin/unpin reference-count protocol.
 *
 * <p><b>LRU invariant:</b> a node is in the doubly-linked list if and only if its {@code refCount}
 * is 0 (evictable). Pinned nodes (refCount &gt; 0) are spliced out of the list.
 *
 * <ul>
 *   <li>{@link #pin}: first pin (CAS 0&rarr;1) removes the node from the list; re-pins (CAS
 *       N&rarr;N+1, N&gt;0) are pure refcount increments.
 *   <li>{@link #unpin}: last unpin (result=0) inserts the node at the list head (most-recently
 *       used); higher unpins are pure refcount decrements.
 * </ul>
 */
class Cache {

  // ---------------------------------------------------------------------------
  // Node
  // ---------------------------------------------------------------------------

  /**
   * A linked-list node carrying a reference count for safe concurrent eviction.
   *
   * <p>Subclasses add the payload field(s) relevant to the concrete cache type.
   *
   * <ul>
   *   <li>&ge;1 — pinned (not in LRU list)
   *   <li>0 — evictable (in LRU list)
   *   <li>-1 — permanently dead (evicted); node must not be used
   * </ul>
   *
   * <p>Head of the list = most recently used; tail = least recently used / eviction candidate.
   */
  static class Node {

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
    final AtomicReference<Node> next = new AtomicReference<>();

    /** LRU list link toward the tail (least-recently-used end). */
    volatile Node prev;

    Node(Node prev, int initialRefCount) {
      this.prev = prev;
      this.refCount = new AtomicInteger(initialRefCount);
    }

    /** Sentinel constructor (head / tail / protocol sentinels). */
    Node() {
      this.refCount = null;
    }
  }

  // Sentinels for the lock-free linked-list protocol.
  // RESERVED: this node's `next` link is currently being modified by exactly one thread.
  // REMOVED:  this node has been spliced out of the list.
  static final Node RESERVED = new Node();
  static final Node REMOVED = new Node();

  /** Most-recently-used sentinel. Real nodes are inserted immediately after this. */
  final Node lruHead = new Node();

  /** Least-recently-used sentinel. Eviction candidates are immediately before this. */
  final Node lruTail = new Node();

  Cache() {
    lruHead.next.set(lruTail);
    lruTail.prev = lruHead;
  }

  // ---------------------------------------------------------------------------
  // LRU list operations
  // ---------------------------------------------------------------------------

  /**
   * Atomically acquires the exclusive right to modify {@code ref.next}. Spins while the link is
   * held by another thread (RESERVED), then CAS-swaps it to {@code reservation} and returns the
   * prior value. Returns REMOVED immediately if the node has already been spliced out.
   */
  static Node reserve(Node ref, Node reservation) {
    Node next = ref.next.get();
    for (; ; ) {
      while (next == RESERVED) {
        if (reservation == REMOVED) {
          Thread.yield();
        }
        next = ref.next.get();
      }
      if (next == REMOVED) {
        return next;
      }
      Node extant = ref.next.compareAndExchange(next, reservation);
      if (extant == next) {
        return next;
      }
      next = extant;
    }
  }

  void insertAtHead(Node node) {
    Node oldNext = reserve(lruHead, RESERVED);
    assert oldNext != REMOVED : "lruHead sentinel should never be removed";
    node.next.set(oldNext);
    oldNext.prev = node;
    if (!lruHead.next.compareAndSet(RESERVED, node)) {
      throw new IllegalStateException("unexpected concurrent modification of lruHead.next");
    }
  }

  boolean removeFromList(Node node) {
    Node next = reserve(node, REMOVED);
    if (next == REMOVED) {
      return false;
    }
    Node prev;
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
  boolean pin(Node node) {
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
  void unpin(Node node) {
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
}
