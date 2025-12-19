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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/** Handles thread-safe dynamic unloading and on-demand reloading of backing resource. */
public class ReferenceHandler implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final LongAdder outstandingSize = new LongAdder();

  private final Closeable onClose;

  public ReferenceHandler(ExecutorService exec) {
    for (int i = PARALLEL_HEAD_FACTOR - 1; i >= 0; i--) {
      head[i] = new Ref(null, null, null, null);
    }
    Future<?>[] refQueueHandlers = new Future[removeOutstanding.length];
    int i = 0;
    for (ReferenceQueue<Object> q : removeOutstanding) {
      refQueueHandlers[i++] = exec.submit(() -> {
        Ref collected;
        while ((collected = (Ref) q.remove()) != null) {
          remove(collected);
        }
        return null;
      });
    }
    onClose = () -> {
      for (Future<?> f : refQueueHandlers) {
        try {
          f.cancel(true);
        } catch (Exception e) {
          log.warn("exception on close", e);
        }
      }
      for (Future<?> f : refQueueHandlers) {
        try {
          f.get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          // swallow; this is how we exit
        } catch (Exception e) {
          log.warn("exception on close", e);
        }
      }
    };
  }

  @Override
  public void close() throws IOException {
    onClose.close();
  }

  private static final int DEFAULT_PARALLEL_HEAD_FACTOR = 32;
  private static final int PARALLEL_HEAD_FACTOR;

  /**
   * Setting this to false ensures even (round-robin) utilization of refqueues. Assigning by thread
   * is fine at the time of refqueue <i>assignment</i>, but can yield hotspots that could increase
   * thread contention at time of ref collection (by GC threads).
   */
  private static final boolean ASSIGN_REFQUEUE_BY_THREAD =
      !"false".equals(System.getProperty("lucene.unload.assignRefQueueByThread"));

  static {
    String spec = System.getProperty("lucene.unload.parallelRefQueueCount");
    if (spec == null) {
      PARALLEL_HEAD_FACTOR = DEFAULT_PARALLEL_HEAD_FACTOR;
    } else {
      int v;
      try {
        v = Integer.parseInt(spec);
        if (v < 1 || Integer.bitCount(v) != 1) {
          log.warn("bad lucene.unload.parallelRefQueueCount spec: {}", spec);
          v = DEFAULT_PARALLEL_HEAD_FACTOR;
        }
      } catch (Throwable t) {
        log.warn("bad lucene.unload.parallelRefQueueCount spec: {} ({})", spec, t);
        v = DEFAULT_PARALLEL_HEAD_FACTOR;
      }
      PARALLEL_HEAD_FACTOR = v;
    }
    log.info("set static property PARALLEL_HEAD_FACTOR={}", PARALLEL_HEAD_FACTOR);
    log.info("set static property ASSIGN_REFQUEUE_BY_THREAD={}", ASSIGN_REFQUEUE_BY_THREAD);
  }

  private static final int PARALLEL_HEAD_MASK = PARALLEL_HEAD_FACTOR - 1;

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static final ReferenceQueue<Object>[] removeOutstanding =
      new ReferenceQueue[PARALLEL_HEAD_FACTOR];

  static {
    for (int i = PARALLEL_HEAD_FACTOR - 1; i >= 0; i--) {
      removeOutstanding[i] = new ReferenceQueue<>();
    }
  }

  private static final class Ref extends WeakReference<Object> {
    private final Runnable onCollection;
    private final AtomicReference<Ref> next = new AtomicReference<>();
    private volatile Ref prev;

    public Ref(
        Object referent, ReferenceQueue<? super Object> q, Runnable onCollection, Ref prev) {
      super(referent, q);
      this.onCollection = onCollection;
      this.prev = prev;
    }
  }

  private final Ref[] head = new Ref[PARALLEL_HEAD_FACTOR];

  private static final Ref RESERVED = new Ref(null, null, null, null);
  private static final Ref REMOVED = new Ref(null, null, null, null);

  private static final AtomicInteger ARBITRARY_REFQUEUE = new AtomicInteger();

  private Ref add(final Object o, Runnable onCollection) {
    int parallelIdx;
    if (ASSIGN_REFQUEUE_BY_THREAD) {
      parallelIdx = Thread.currentThread().hashCode() & PARALLEL_HEAD_MASK;
    } else {
      parallelIdx = ARBITRARY_REFQUEUE.getAndIncrement() & PARALLEL_HEAD_MASK;
    }
    outstandingSize.increment();
    Ref head = this.head[parallelIdx];
    try {
      final Ref ref = new Ref(o, removeOutstanding[parallelIdx], onCollection, head);
      Ref next = reserve(head, RESERVED);
      if (next != null) {
        next.prev = ref;
        ref.next.set(next);
      }
      if (!head.next.compareAndSet(RESERVED, ref)) {
        throw new IllegalStateException();
      }
      return ref;
    } finally {
      Reference.reachabilityFence(o);
    }
  }

  private static Ref reserve(Ref ref, Ref reservation) {
    Ref next = ref.next.get();
    for (; ; ) {
      while (next == RESERVED) {
        if (reservation == REMOVED) {
          Thread.yield();
        }
        next = ref.next.get();
      }
      Ref extant = ref.next.compareAndExchange(next, reservation);
      if (extant == next) {
        return next;
      } else {
        next = extant;
      }
    }
  }

  private void remove(final Ref ref) {
    try {
      ref.onCollection.run();
    } finally {
      Ref next = reserve(ref, REMOVED);
      outstandingSize.decrement();
      // now we have a lock on the link to next
      Ref prev;
      for (; ; ) {
        prev = ref.prev;
        if (prev.next.compareAndSet(ref, RESERVED)) {
          break;
        } else {
          Thread.yield();
        }
      }
      // now we have a lock on the link from prev
      if (next != null) {
        next.prev = prev;
      }
      if (!prev.next.compareAndSet(RESERVED, next)) {
        throw new IllegalStateException();
      }
    }
  }

  // visible for testing
  int nonEmptyRefQueueHeadCount() {
    return Math.toIntExact(Arrays.stream(head).filter((r) -> r.next.get() != null).count());
  }

  private static final Runnable NO_OP = () -> {};

  // visible for testing
  void addDummyReference(int byteSize) {
    add(new byte[byteSize], NO_OP);
  }
}
