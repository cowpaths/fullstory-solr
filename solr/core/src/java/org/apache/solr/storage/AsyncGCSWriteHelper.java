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

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Double-buffering async write helper for sequential streaming to a {@link WritableByteChannel}
 * (e.g. a GCS resumable-upload channel). Adapted from {@link AsyncDirectWriteHelper}: the DirectIO
 * / FileChannel / position-tracking concerns are removed; buffers are written sequentially to the
 * provided channel. The channel is closed (finalizing the upload) when this helper is closed.
 */
public class AsyncGCSWriteHelper implements Closeable {

  private int populatingBuffer = 0;
  private int consumingBuffer = 1;
  private final Struct[] buffers = new Struct[2];
  private final AtomicReference<Future<?>> future = new AtomicReference<>();
  private final WritableByteChannel channel;
  private final DirectBufferPool bufferPool;

  private enum Status {
    SYNC,
    ASYNC,
    FINISHED,
    FLUSH_ASYNC
  }

  private volatile Status status = Status.SYNC;
  private volatile int flushBufferIdx = -1;
  private static final Future<?> CLOSED = new CompletableFuture<>();

  public AsyncGCSWriteHelper(DirectBufferPool bufferPool, WritableByteChannel channel) {
    this.bufferPool = bufferPool;
    this.channel = channel;
    for (int i = 1; i >= 0; i--) {
      buffers[i] = new Struct(bufferPool, channel);
    }
  }

  public ByteBuffer init() {
    assert populatingBuffer == 0;
    return buffers[populatingBuffer].buffer;
  }

  /** No-op in sync mode; the channel is already open. */
  public void startSync() {}

  public void start(ExecutorService exec) {
    status = Status.ASYNC;
    Future<?> f = startWrite(exec);
    if (!future.compareAndSet(null, f)) {
      f.cancel(true);
      throw new IllegalStateException("started multiple times");
    }
  }

  private ByteBuffer syncSwap(ByteBuffer populated) throws IOException {
    Struct sync = buffers[populatingBuffer];
    assert Objects.equals(sync.buffer, populated);
    sync.writeFullBuffer();
    return populated.clear();
  }

  private Runnable swapConsume() {
    Struct releasing = buffers[consumingBuffer];
    Struct acquiring = buffers[consumingBuffer ^= 1];
    releasing.write.arrive();
    acquiring.read.arriveAndAwaitAdvance();
    return acquiring::writeFullBuffer;
  }

  public ByteBuffer write(ByteBuffer populated) throws IOException {
    switch (status) {
      case FINISHED:
      case FLUSH_ASYNC:
        throw new IllegalStateException();
      case SYNC:
        return syncSwap(populated);
      case ASYNC:
        break;
    }
    Struct releasing = buffers[populatingBuffer];
    Struct acquiring = buffers[populatingBuffer ^= 1];
    assert Objects.equals(releasing.buffer, populated);
    releasing.read.arrive();
    acquiring.write.arriveAndAwaitAdvance();
    return acquiring.buffer.clear();
  }

  private Future<?> startWrite(ExecutorService exec) {
    return exec.submit(
        () -> {
          Runnable writeFunction = swapConsume();
          while (status == Status.ASYNC) {
            writeFunction.run();
            writeFunction = swapConsume();
          }
          if (consumingBuffer != flushBufferIdx) {
            writeFunction.run();
            switch (status) {
              case FINISHED:
                buffers[consumingBuffer].write.arrive();
                return null;
              case FLUSH_ASYNC:
                status = Status.FINISHED;
                writeFunction = swapConsume();
                writeFinalPartial(buffers[consumingBuffer].buffer, writeFunction);
                break;
              default:
                throw new IllegalStateException();
            }
          } else if (status == Status.FLUSH_ASYNC) {
            status = Status.FINISHED;
            writeFinalPartial(buffers[consumingBuffer].buffer, writeFunction);
          }
          return null;
        });
  }

  /** Writes only the populated portion of the final (possibly partial) buffer. */
  private void writeFinalPartial(ByteBuffer buf, Runnable writeFunction) throws IOException {
    int remaining = buf.position();
    if (remaining > 0) {
      buf.flip();
      writeFunction.run();
    }
  }

  public void flush(ByteBuffer populated, boolean synchronous) throws IOException {
    switch (status) {
      case FINISHED:
      case FLUSH_ASYNC:
        throw new IllegalStateException("flushed multiple times");
      case SYNC:
        {
          status = Status.FINISHED;
          int remaining = populated.position();
          if (remaining > 0) {
            populated.flip();
            writeDirectly(populated);
          }
          return;
        }
      case ASYNC:
        flushBufferIdx = populatingBuffer;
        status = synchronous ? Status.FINISHED : Status.FLUSH_ASYNC;
        break;
    }
    Struct last = buffers[populatingBuffer];
    assert Objects.equals(last.buffer, populated);
    last.read.arrive();
    if (synchronous) {
      int remaining = populated.position();
      buffers[populatingBuffer ^ 1].write.arriveAndAwaitAdvance();
      if (remaining > 0) {
        populated.flip();
        writeDirectly(populated);
      }
    }
  }

  private void writeDirectly(ByteBuffer buf) throws IOException {
    while (buf.hasRemaining()) {
      channel.write(buf);
    }
  }

  @Override
  @SuppressWarnings("try")
  public void close() throws IOException {
    try {
      Future<?> f = future.getAndSet(CLOSED);
      if (f != null) {
        if (f == CLOSED) {
          throw new IllegalStateException("closed multiple times");
        }
        if (status == Status.ASYNC) {
          f.cancel(true);
        }
        try {
          f.get();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();
          if (cause instanceof IOException) {
            throw (IOException) cause;
          } else {
            throw new RuntimeException(e);
          }
        }
      }
    } finally {
      // Closing the channel finalizes the GCS resumable upload.
      try (WritableByteChannel ignored = channel) {
        bufferPool.release(buffers[0].buffer);
        bufferPool.release(buffers[1].buffer);
      }
    }
  }

  private static final class Struct {
    private final ByteBuffer buffer;
    private final Phaser read = new Phaser(2);
    private final Phaser write = new Phaser(2);
    private final WritableByteChannel channel;

    private Struct(DirectBufferPool bufferPool, WritableByteChannel channel) {
      this.buffer = bufferPool.get();
      this.channel = channel;
    }

    /** Rewinds the buffer and writes all bytes to the channel. Used for full-block writes. */
    void writeFullBuffer() {
      try {
        buffer.rewind();
        while (buffer.hasRemaining()) {
          channel.write(buffer);
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }
}
