package org.apache.solr.storage;

import java.io.Closeable;

/**
 * Implemented by Directory types backed by a {@link BlockCache}. Allows callers to open a
 * thread-scoped batch that groups {@link BlockCache} registrations during a bounded operation (e.g.
 * a merge), reducing PhantomReference count and associated GC overhead.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * try (var scope = dir.openBatchScope()) {
 *     doWork();
 * }
 * }</pre>
 */
public interface BlockCacheBatchScope {
  Closeable openBatchScope();
}
