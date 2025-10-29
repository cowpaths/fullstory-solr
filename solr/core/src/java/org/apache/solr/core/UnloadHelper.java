package org.apache.solr.core;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.ref.ReferenceQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.lucene.index.Unloader;
import org.apache.lucene.index.Unloader.BlockingRunnable;
import org.apache.lucene.index.Unloader.HoldingFlusher;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.handler.admin.MetricsHandler;
import org.apache.solr.metrics.MetricSuppliers;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class UnloadHelper<T extends Unloader.UnloadHelper>
    implements Supplier<T>, SolrMetricProducer {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final ScheduledExecutorService exec;
  private final SolrMetricsContext solrMetricsContext;
  private final Meter created;
  private final Meter loaded;
  private final Histogram loadTimeMillis;
  private final Meter unloaded;
  private final Meter closed;
  private final InfoStream infoStream = new UnloaderLoggingInfoStream();

  UnloadHelper(CoreContainer cc) {
    this.exec = cc.getUnloaderExecutor();
    MetricsHandler metricsHandler = cc.getMetricsHandler();
    if (metricsHandler == null) {
      solrMetricsContext = null;
      created = MetricSuppliers.NoOpMeterSupplier.INSTANCE.newMetric();
      loaded = MetricSuppliers.NoOpMeterSupplier.INSTANCE.newMetric();
      loadTimeMillis = MetricSuppliers.NoOpHistogramSupplier.INSTANCE.newMetric();
      unloaded = MetricSuppliers.NoOpMeterSupplier.INSTANCE.newMetric();
      closed = MetricSuppliers.NoOpMeterSupplier.INSTANCE.newMetric();
    } else {
      solrMetricsContext = metricsHandler.getSolrMetricsContext().getChildContext(this);
      String[] metricPath = new String[] {SolrInfoBean.Category.OTHER.toString(), "unloadable"};
      created = solrMetricsContext.meter("created", metricPath);
      loaded = solrMetricsContext.meter("loaded", metricPath);
      loadTimeMillis = solrMetricsContext.histogram("loadTimeMillis", metricPath);
      unloaded = solrMetricsContext.meter("unloaded", metricPath);
      closed = solrMetricsContext.meter("closed", metricPath);
    }
  }

  private ExecutorService refQueueExec;
  private volatile boolean closing = false;
  private Closeable refQueueHandling;

  private void handleRefQueues(
      ReferenceQueue<Object>[] queues,
      Consumer<Object> handler,
      HoldingFlusher flushHolding,
      AtomicReference<Boolean> handleRefQueue,
      LongSupplier holdingSize,
      LongSupplier outstandingSize) {
    if (closing || !handleRefQueue.compareAndSet(null, Boolean.TRUE)) {
      return;
    }
    refQueueExec =
        ExecutorUtil.newMDCAwareFixedThreadPool(
            queues.length << 1, new NamedThreadFactory("refQueueExec"));
    LongAdder activeRefQueueProcessors = new LongAdder();
    LongAdder collectedHoldingRefs = new LongAdder();
    LongAdder collectedRefs = new LongAdder();
    @SuppressWarnings("rawtypes")
    Future<?>[] refQueueFutures = new Future[queues.length << 1];
    for (int i = queues.length - 1; i >= 0; i--) {
      ReferenceQueue<Object> q = queues[i];
      refQueueFutures[i] =
          refQueueExec.submit(
              wrapTask(
                  handleRefQueue,
                  activeRefQueueProcessors,
                  () -> {
                    handler.accept(q.remove());
                    collectedRefs.increment();
                  }));
      int idx = i;
      long[] nextHoldUntil = new long[] {System.nanoTime()};
      refQueueFutures[queues.length + i] =
          refQueueExec.submit(
              wrapTask(
                  handleRefQueue,
                  activeRefQueueProcessors,
                  () -> {
                    collectedHoldingRefs.add(
                        flushHolding.flush(nextHoldUntil[0], idx, nextHoldUntil));
                  }));
    }
    this.refQueueHandling =
        () -> {
          handleRefQueue.set(false);
          for (Future<?> f : refQueueFutures) {
            f.cancel(true);
          }
        };
    if (solrMetricsContext == null) {
      return;
    }
    MetricsMap refQueueSize =
        new MetricsMap(
            map -> {
              long sizeHolding = holdingSize.getAsLong();
              long ramBytesUsedHolding = sizeHolding * Unloader.RAMBYTES_PER_HOLDINGREF;
              long size = outstandingSize.getAsLong();
              long ramBytesUsed = size * Unloader.RAMBYTES_PER_REF;
              map.put("activeProcessors", activeRefQueueProcessors.sum() + "/" + queues.length);
              map.put("holdRefsCollected", collectedHoldingRefs.sum());
              map.put("refsCollected", collectedRefs.sum());
              map.put("sizeHolding", sizeHolding);
              map.put("ramBytesUsedHolding", ramBytesUsedHolding);
              map.put("ramUsedHolding", RamUsageEstimator.humanReadableUnits(ramBytesUsedHolding));
              map.put("size", size);
              map.put("ramBytesUsed", ramBytesUsed);
              map.put("ramUsed", RamUsageEstimator.humanReadableUnits(ramBytesUsed));
              map.put(
                  "ramUsedOverall",
                  RamUsageEstimator.humanReadableUnits(ramBytesUsedHolding + ramBytesUsed));
            });
    solrMetricsContext.gauge(
        refQueueSize, true, "refQueue", SolrInfoBean.Category.OTHER.toString(), "unloadable");
  }

  @Override
  @SuppressWarnings("try")
  public void close() throws IOException {
    try (Closeable c = refQueueHandling) {
      closing = true;
    }
    SolrMetricProducer.super.close();
    ExecutorUtil.shutdownAndAwaitTermination(refQueueExec);
  }

  @Override
  @SuppressWarnings("unchecked")
  public T get() {
    return (T)
        new Unloader.AbstractUnloadHelper(exec, infoStream) {
          @Override
          public ScheduledExecutorService onCreation(Unloader<?> u) {
            created.mark();
            return super.onCreation(u);
          }

          @Override
          public void onLoad(long nanosSincePriorAccess, long loadTime) {
            loaded.mark();
            loadTimeMillis.update(TimeUnit.NANOSECONDS.toMillis(loadTime));
            super.onLoad(nanosSincePriorAccess, loadTime);
          }

          @Override
          public void onUnload(long nanosSinceLastAccess) {
            unloaded.mark();
            super.onUnload(nanosSinceLastAccess);
          }

          @Override
          public void onClose() {
            closed.mark();
            super.onClose();
          }

          @Override
          public void maybeHandleRefQueues(
              ReferenceQueue<Object>[] queues,
              Consumer<Object> handler,
              HoldingFlusher flushHolding,
              AtomicReference<Boolean> handleRefQueue,
              LongSupplier holdingSize,
              LongSupplier outstandingSize) {
            handleRefQueues(
                queues, handler, flushHolding, handleRefQueue, holdingSize, outstandingSize);
          }
        };
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    // do in ctor
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }

  private static final class UnloaderLoggingInfoStream extends InfoStream {
    @Override
    public void message(String component, String message) {
      if (log.isInfoEnabled()) {
        log.info("[{}][{}]: {}", component, Thread.currentThread().getName(), message);
      }
    }

    @Override
    public boolean isEnabled(String component) {
      return log.isInfoEnabled();
    }

    @Override
    public void close() {}
  }

  @SuppressWarnings("ReferenceEquality")
  private static Callable<Void> wrapTask(
      AtomicReference<Boolean> handleRefQueue,
      LongAdder activeRefQueueProcessors,
      BlockingRunnable r) {
    return () -> {
      activeRefQueueProcessors.increment();
      try {
        while (handleRefQueue.get() == Boolean.TRUE) {
          r.run();
        }
      } catch (InterruptedException ex) {
        if (handleRefQueue.get() == Boolean.TRUE) {
          // unexpected -- we've been interrupted but are still
          // supposed to be handling ref queue?
          handleRefQueue.set(false);
          log.error("unexpected interruption of ref queue processing", ex);
          throw ex;
        }
      } catch (Throwable t) {
        handleRefQueue.set(false);
        log.error("exception in ref queue processing", t);
        throw t;
      } finally {
        activeRefQueueProcessors.decrement();
      }
      log.info("normal exit of ref queue processing task");
      return null;
    };
  }
}
