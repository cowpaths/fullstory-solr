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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.function.BiFunction;

/**
 * Concurrent intrusive doubly-linked list for tracking live resources. Each node is either <em>in
 * the list</em> (state=0, claimable) or <em>claimed/removed</em> (state=-1).
 *
 * <p>The two operations are:
 *
 * <ul>
 *   <li>{@link #add}: inserts a node at the head of the list.
 *   <li>{@link #tryRemove}: atomically claims and splices out a node; exactly one concurrent caller
 *       wins the CAS and returns {@code true}.
 * </ul>
 *
 * <p>Designed for the {@link BlockCache} hold-ref overflow path: nodes are inserted on {@link
 * BlockCache#register} and claimed on {@link CachedCompressedIndexInput.NodeRefStruct#closeFor}.
 */
class Cache<V extends Cache.Val> {

  // ---------------------------------------------------------------------------
  // Val
  // ---------------------------------------------------------------------------

  static class Val {
    /** 0 = in list (claimable); -1 = claimed/removed. */
    volatile int state;
  }

  // ---------------------------------------------------------------------------
  // Node
  // ---------------------------------------------------------------------------

  static final class Node<V extends Cache.Val> {

    private V payload;

    /** Link toward the head. Accessed atomically via {@link Cache#NEXT}. */
    private volatile Node<V> next;

    /** Link toward the tail. Written under {@link Cache#NEXT} reservation of predecessor. */
    private volatile Node<V> prev;

    /** Sentinel constructor. */
    private Node() {}

    /**
     * Payload-factory constructor. Passes {@code this} to the factory so the payload can back-ref
     * its own node (e.g. for the drain-time {@link #tryRemove} call site).
     */
    <K> Node(BiFunction<K, Node<V>, V> payloadFunc, K key) {
      this.payload = payloadFunc.apply(key, this);
    }

    V getPayload() {
      return payload;
    }
  }

  // ---------------------------------------------------------------------------
  // VarHandles
  // ---------------------------------------------------------------------------

  private static final VarHandle NEXT;
  private static final VarHandle STATE;

  static {
    try {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      NEXT = lookup.findVarHandle(Node.class, "next", Node.class);
      STATE = lookup.findVarHandle(Val.class, "state", int.class);
    } catch (ReflectiveOperationException e) {
      throw new Error(e);
    }
  }

  // ---------------------------------------------------------------------------
  // Sentinels
  // ---------------------------------------------------------------------------

  private static final Node<?> RESERVED = new Node<>();
  private static final Node<?> REMOVED = new Node<>();

  private final Node<V> head = new Node<>();

  // ---------------------------------------------------------------------------
  // Constructor
  // ---------------------------------------------------------------------------

  Cache() {
    Node<V> tail = new Node<>();
    head.next = tail;
    tail.prev = head;
  }

  // ---------------------------------------------------------------------------
  // List operations
  // ---------------------------------------------------------------------------

  @SuppressWarnings("unchecked")
  private static <V extends Val> Node<V> reserve(Node<V> ref, Node<?> reservation) {
    Node<V> next = ref.next;
    for (; ; ) {
      while (next == RESERVED) {
        if (reservation == REMOVED) {
          Thread.yield();
        }
        next = ref.next;
      }
      if (next == REMOVED) {
        return next;
      }
      Node<V> witness = (Node<V>) NEXT.compareAndExchange(ref, next, reservation);
      if (witness == next) return next;
      next = witness;
    }
  }

  private static <V extends Val> void removeFromList(Node<V> node) {
    Node<V> next = reserve(node, REMOVED);
    if (next == REMOVED) return; // already removed; nothing to do
    Node<V> prev;
    for (; ; ) {
      prev = node.prev;
      if (NEXT.compareAndSet(prev, node, RESERVED)) {
        break;
      }
      Thread.yield();
    }
    next.prev = prev;
    if (!NEXT.compareAndSet(prev, RESERVED, next)) {
      throw new IllegalStateException("unexpected concurrent modification during list removal");
    }
  }

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /** Inserts {@code node} at the head of the list (state must be 0 on entry). */
  void add(Node<V> node) {
    node.prev = head;
    Node<V> oldNext = reserve(head, RESERVED);
    assert oldNext != REMOVED : "head sentinel should never be removed";
    node.next = oldNext;
    oldNext.prev = node;
    if (!NEXT.compareAndSet(head, RESERVED, node)) {
      throw new IllegalStateException("unexpected concurrent modification of head.next");
    }
  }

  /**
   * Atomically claims and removes {@code node}. Returns {@code true} if this thread won the CAS
   * (state 0&rarr;-1); {@code false} if another thread already claimed it.
   */
  static <V extends Cache.Val> boolean tryRemove(Node<V> node) {
    Val p = node.payload;
    if (p == null || !STATE.compareAndSet(p, 0, -1)) {
      return false;
    }
    removeFromList(node);
    return true;
  }
}
