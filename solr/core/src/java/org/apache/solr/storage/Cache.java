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
import java.util.concurrent.TimeUnit;

/**
 * Generic concurrent LRU cache with pin/unpin support.
 *
 * <p>Subclasses supply the concrete {@link Node} subtype (which carries the payload) and override
 * {@link #createPayload} to construct it. This class manages the doubly-linked LRU list, the
 * pin/unpin reference-count protocol, and the acquire/release/recycle lifecycle.
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
 *       same value (the subclass controls the concrete node type via {@link #createPayload}).
 * </ul>
 *
 * @param <V> the value type carried by each node
 */
class Cache<V extends Cache.Val> {

  // ---------------------------------------------------------------------------
  // Node
  // ---------------------------------------------------------------------------

  static class Val {
    /**
     * Reference count.
     *
     * <ul>
     *   <li>&ge;1 — pinned (not in LRU list)
     *   <li>0 — evictable (in LRU list)
     *   <li>-1 — permanently dead (evicted); node must not be used
     * </ul>
     */
    private volatile int refCount;

    Val(int initialRefCount) {
      this.refCount = initialRefCount;
    }

    int refCount() {
      return refCount;
    }
  }

  private static final VarHandle NEXT;
  private static final VarHandle REF_COUNT;

  static {
    try {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      NEXT = lookup.findVarHandle(Node.class, "next", Node.class);
      REF_COUNT = lookup.findVarHandle(Val.class, "refCount", int.class);
    } catch (ReflectiveOperationException e) {
      throw new Error(e);
    }
  }

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
  static final class Node<V extends Cache.Val> {

    /**
     * The payload managed by this cache entry. {@code null} for sentinel nodes. Non-final only in
     * order to enable nulling out (for GC eligibility) upon node reclaim.
     */
    private V payload;

    /** LRU list link toward the head (most-recently-used end). Guarded by atomic access */
    private volatile Node<V> next;

    /**
     * LRU list link toward the tail (least-recently-used end). Guarded by atomic access to {@link
     * #next}
     */
    private volatile Node<V> prev;

    Node(V payload, Node<V> prev) {
      this.payload = payload; // visibility guaranteed by callers incref'ing refCount before access
      this.prev = prev;
    }

    /** Sentinel constructor (head / tail / protocol sentinels). */
    Node() {
      this.payload = null;
    }

    /**
     * Return the associated payload. If called on a pinned node, is guaranteed to return non-null
     */
    V getPayload() {
      return payload;
    }

    boolean pinnable() {
      Val p = this.payload;
      return p != null && p.refCount >= 0;
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
  private final Node<V> RESERVED;
  private final Node<V> REMOVED;

  /** Cold queue: head sentinel (most-recently-used end). */
  private final Node<V> lruHead = new Node<>();

  /** Cold queue: tail sentinel (eviction end). */
  private final Node<V> lruTail = new Node<>();

  /**
   * Multiplier applied to the hot tail's age during eviction scoring. A hot node is evicted only
   * when the cold tail is at least {@code HOT_EVICTION_MULTIPLIER} times as stale, giving hot nodes
   * proportional protection regardless of absolute time scale.
   */
  private static final int HOT_EVICTION_SHIFT = 2;

  /** A year should be stale enough for our purposes, without risking overflow. */
  private static final long VERY_STALE = TimeUnit.DAYS.toNanos(365);

  /**
   * Returns the age (nanos since last unpin) of a node given its {@code lastUnpinNanos} stamp.
   * Returns a large sentinel age for never-used nodes ({@code lastUnpinNanos == 0}) so they are
   * always preferred for eviction over any node that has been genuinely used.
   */
  private static long age(long now, long lastUnpinNanos) {
    return lastUnpinNanos == 0 ? VERY_STALE : now - lastUnpinNanos;
  }

  @SuppressWarnings("unchecked")
  private Cache() {
    RESERVED = (Node<V>) RESERVED_PROTO;
    REMOVED = (Node<V>) REMOVED_PROTO;
    lruHead.next = lruTail;
    lruTail.prev = lruHead;
  }

  /**
   * Initializing constructor: wires {@code initialValues} into the LRU list in order (first value
   * at the head, last at the tail) using {@link #createPayload}. Equivalent to calling the no-arg
   * constructor followed by {@link #insertAtTail} for each value, but faster because no CAS is
   * needed during single-threaded initialization.
   */
  Cache(Iterable<V> initialValues) {
    this();
    Node<V> prev = lruHead;
    for (V value : initialValues) {
      Node<V> node = new Node<>(value, prev);
      prev.next = node;
      prev = node;
    }
    prev.next = lruTail;
    lruTail.prev = prev;
  }

  // ---------------------------------------------------------------------------
  // Node factory
  // ---------------------------------------------------------------------------

  /**
   * Creates a new node carrying {@code value}. Subclasses override to return a concrete subtype
   * (e.g. {@link BlockCache.Val}), ensuring that all nodes in the list are of the expected type.
   */
  @SuppressWarnings("unchecked")
  protected V createPayload(V oldValue, int initialRefCount) {
    return (V) new Val(initialRefCount);
  }

  // ---------------------------------------------------------------------------
  // LRU list operations
  // ---------------------------------------------------------------------------

  /**
   * Atomically acquires the exclusive right to modify {@code ref.next}. Spins while the link is
   * held by another thread (RESERVED), then CAS-swaps it to {@code reservation} and returns the
   * prior value. Returns REMOVED immediately if the node has already been spliced out.
   */
  @SuppressWarnings("unchecked")
  private Node<V> reserve(Node<V> ref, Node<V> reservation) {
    Node<V> next = ref.next;
    for (; ; ) {
      while (next == RESERVED_PROTO) {
        if (reservation == REMOVED_PROTO) {
          Thread.yield();
        }
        next = ref.next;
      }
      if (next == REMOVED_PROTO) {
        return next;
      }
      Node<V> extant = (Node<V>) NEXT.compareAndExchange(ref, next, reservation);
      if (extant == next) {
        return next;
      }
      next = extant;
    }
  }

  protected void insertAtHead(Node<V> listHead, Node<V> node, boolean recordAccess) {
    node.prev = listHead;
    Node<V> oldNext = reserve(listHead, RESERVED);
    assert oldNext != REMOVED_PROTO : "queue head sentinel should never be removed";
    node.next = oldNext;
    oldNext.prev = node;
    if (!NEXT.compareAndSet(listHead, RESERVED, node)) {
      throw new IllegalStateException("unexpected concurrent modification of listHead.next");
    }
  }

  private boolean removeFromList(Node<V> node) {
    Node<V> next = reserve(node, REMOVED);
    if (next == REMOVED_PROTO) {
      return false;
    }
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
    return true;
  }

  private void insertAtTail(V value) {
    for (; ; ) {
      Node<V> pred = lruTail.prev;
      Node<V> oldNext = reserve(pred, RESERVED);
      if (oldNext != lruTail) {
        if (oldNext == REMOVED_PROTO) {
          continue; // pred was concurrently removed; retry
        }
        // release reservation; pred is no longer tail's predecessor
        if (!NEXT.compareAndSet(pred, RESERVED, oldNext)) {
          throw new IllegalStateException();
        }
        continue;
      }
      Node<V> node = new Node<>(createPayload(value, 0), pred);
      node.next = lruTail;
      lruTail.prev = node;
      if (!NEXT.compareAndSet(pred, RESERVED, node)) {
        throw new IllegalStateException("unexpected concurrent modification during tail insertion");
      }
      return;
    }
  }

  // ---------------------------------------------------------------------------
  // Pin / unpin
  // ---------------------------------------------------------------------------

  private static final int UNPIN_SENTINEL = Integer.MIN_VALUE >> 1;

  /**
   * Pins {@code node} for the duration of a read, preventing eviction. Returns {@code false} if the
   * node is permanently dead (evicted); the caller must then fall back to loading.
   *
   * <p>If this is the <em>first</em> pin (refCount transitions 0&rarr;1), the node is removed from
   * the LRU list (it is no longer evictable). If the node is already pinned (refCount&gt;0), the
   * refcount is simply incremented with no list operation.
   */
  boolean pin(Node<V> node) {
    Val p = node.payload;
    if (p == null) {
      return false;
    }
    int rc = p.refCount;
    for (; ; ) {
      switch (rc) {
        case -1:
          return false;
        case UNPIN_SENTINEL:
          rc = p.refCount;
          continue;
      }
      assert rc >= 0;
      int witness = (int) REF_COUNT.compareAndExchange(p, rc, rc + 1);
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
  boolean unpin(Node<V> node, boolean recordAccess) {
    Val p = node.payload;
    assert p != null;
    int rc = p.refCount;
    for (; ; ) {
      if (rc == 1) {
        int witness = (int) REF_COUNT.compareAndExchange(p, 1, UNPIN_SENTINEL);
        if (witness == 1) {
          insertAtHead(lruHead, node, recordAccess);
          if (!REF_COUNT.compareAndSet(p, UNPIN_SENTINEL, 0)) {
            throw new IllegalStateException();
          }
          return true;
        } else {
          rc = witness;
        }
      } else {
        assert rc > 1;
        int witness = (int) REF_COUNT.compareAndExchange(p, rc, rc - 1);
        if (witness == rc) {
          return false;
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
   * evictable node from the tail of the list, then returns a fresh node (via {@link
   * #createPayload}) carrying the same value, pinned (refCount=1) and not yet in the list.
   *
   * <p>Returns {@code null} if the list is empty (all nodes are pinned).
   */
  final Node<V> acquireNode() {
    return acquireNode(null);
  }

  protected Node<V> acquireTail(Node<V> lruTail, Node<V> lruHead) {
    for (Node<V> candidate; (candidate = lruTail.prev) != lruHead; ) {
      Val p = candidate.payload;
      if (p != null && REF_COUNT.compareAndSet(p, 0, -1)) {
        return candidate;
      }
    }
    return null;
  }

  interface ValFunc<N> {
    N apply(N oldVal, int initialRefCount);
  }

  final Node<V> acquireNode(ValFunc<V> valFunc) {
    Node<V> candidate = acquireTail(lruTail, lruHead);
    if (candidate == null) {
      return null;
    }
    if (!removeFromList(candidate)) {
      throw new IllegalStateException();
    }
    V old = candidate.payload;
    candidate.payload = null; // ensure eligible for GC
    V newVal = valFunc == null ? createPayload(old, 1) : valFunc.apply(old, 1);
    return new Node<>(newVal, null);
  }

  /**
   * Explicitly releases a node when its owning resource is closed. If the node is still evictable
   * (refCount==0 and in the list), marks it dead and recycles its value back to the pool via {@link
   * #insertAtTail}, making it the highest-priority eviction candidate for reuse. Returns {@code
   * true} if the node was successfully released, {@code false} if the node was already evicted or
   * pinned (best-effort; caller need not retry).
   *
   * <p>Only pass <code>unconditional=true</code> if caller is the only possible owner of a ref to
   * this node (e.g., if CAS has failed).
   */
  boolean close(Node<V> node, boolean unconditional) {
    V p = node.payload;
    if (!unconditional && (p == null || !REF_COUNT.compareAndSet(p, 0, -1))) {
      // pinned or already dead — best effort, bail
      return false;
    }
    if (unconditional || removeFromList(node)) {
      node.payload = null; // ensure eligible for GC
      insertAtTail(p); // p cannot be null here
    } else {
      throw new IllegalStateException();
    }
    return true;
  }

  static class TsVal extends Val {

    /**
     * Nanosecond timestamp of the most recent {@link Cache#unpin}, or {@code 0} if never unpinned.
     * {@code 0} is treated as maximally stale, making never-used nodes the highest-priority
     * eviction candidates.
     */
    private long lastUnpinNanos;

    TsVal(int initialRefCount) {
      super(initialRefCount);
    }
  }

  private static long lastUnpinNanos(TsVal v) {
    return v == null ? 0 : v.lastUnpinNanos;
  }

  static class DualQueueCache<V extends TsVal> extends Cache<V> {

    /** Running memoization of last seen non-zero cold queue timestamp */
    private volatile long coldTs;

    /** Hot queue: head sentinel. Nodes promoted from cold are inserted here. */
    private final Node<V> hotHead = new Node<>();

    /** Hot queue: tail sentinel. */
    private final Node<V> hotTail = new Node<>();

    DualQueueCache(Iterable<V> initialValues) {
      super(initialValues);
      hotHead.next = hotTail;
      hotTail.prev = hotHead;
    }

    @Override
    protected final void insertAtHead(Node<V> head, Node<V> node, boolean recordAccess) {
      long prev = ((TsVal) node.payload).lastUnpinNanos;
      ((TsVal) node.payload).lastUnpinNanos = recordAccess ? System.nanoTime() : 0;
      super.insertAtHead(toHot(prev) ? hotHead : head, node, recordAccess);
    }

    @Override
    protected Node<V> acquireTail(Node<V> lruTail, Node<V> lruHead) {
      long coldCandidateTs;
      Node<V> candidate;
      long now = System.nanoTime();
      Val p;
      do {
        Node<V> coldCandidate = lruTail.prev, hotCandidate = hotTail.prev;
        if (coldCandidate == lruHead) {
          // cold queue is empty
          if (hotCandidate == hotHead) {
            // both queues empty
            return null;
          }
          candidate = hotCandidate;
          coldCandidateTs = 0;
        } else if (hotCandidate == hotHead) {
          candidate = coldCandidate;
          coldCandidateTs = lastUnpinNanos(coldCandidate.payload);
        } else {
          // Evict from hot only when cold is at least HOT_EVICTION_MULTIPLIER times as stale,
          // giving hot nodes proportional protection. age() maps lastUnpinNanos=0 to a large
          // sentinel so never-used cold nodes are always preferred over any real hot node.
          coldCandidateTs = lastUnpinNanos(coldCandidate.payload);
          candidate =
              age(now, coldCandidateTs)
                      < age(now, lastUnpinNanos(hotCandidate.payload)) >> HOT_EVICTION_SHIFT
                  ? hotCandidate
                  : coldCandidate;
        }
      } while ((p = candidate.payload) == null || !REF_COUNT.compareAndSet(p, 0, -1));
      if (coldCandidateTs != 0) {
        this.coldTs = coldCandidateTs;
      }
      return candidate;
    }

    private boolean toHot(long prev) {
      // Promote to hot if previously used and fresher than the last known cold-tail threshold.
      // coldTs tracks the most recently observed non-zero cold tail timestamp (updated by
      // acquireNode on each eviction). When zero (initial warm-up), fall back to hot.
      long threshold;
      return prev != 0 && ((threshold = coldTs) == 0 || threshold - prev < 0);
    }
  }
}
