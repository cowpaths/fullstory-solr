package org.apache.solr.core;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.lucene.index.Unloader;
import org.apache.lucene.util.InfoStream;
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
  private final Histogram lastAccessToReloadMillis;
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
      lastAccessToReloadMillis = MetricSuppliers.NoOpHistogramSupplier.INSTANCE.newMetric();
      unloaded = MetricSuppliers.NoOpMeterSupplier.INSTANCE.newMetric();
      closed = MetricSuppliers.NoOpMeterSupplier.INSTANCE.newMetric();
    } else {
      solrMetricsContext = metricsHandler.getSolrMetricsContext().getChildContext(this);
      String[] metricPath = new String[] {SolrInfoBean.Category.OTHER.toString(), "unloadable"};
      created = solrMetricsContext.meter("created", metricPath);
      loaded = solrMetricsContext.meter("loaded", metricPath);
      loadTimeMillis = solrMetricsContext.histogram("loadTimeMillis", metricPath);
      lastAccessToReloadMillis =
          solrMetricsContext.histogram("lastAccessToReloadMillis", metricPath);
      unloaded = solrMetricsContext.meter("unloaded", metricPath);
      closed = solrMetricsContext.meter("closed", metricPath);
    }
  }

  private volatile boolean closing = false;

  private void handleRefQueues(LongSupplier indirectTrackedCount, LongSupplier refsCollected) {
    if (closing) {
      return;
    }
    if (solrMetricsContext == null) {
      return;
    }
    MetricsMap refQueueSize =
        new MetricsMap(
            map -> {
              long totalLoaded = loaded.getCount();
              long totalUnloaded = unloaded.getCount();
              long totalCreated = created.getCount();
              long totalClosed = closed.getCount();
              long currentlyOpen = totalCreated - totalClosed;
              long currentlyLoaded = totalLoaded - totalUnloaded;
              map.put("indirectTrackedCount", indirectTrackedCount.getAsLong());
              map.put("refsCollected", refsCollected.getAsLong());
              map.put("currentlyOpen", currentlyOpen);
              map.put("loadedRatio", (double) currentlyLoaded / currentlyOpen);
            });
    solrMetricsContext.gauge(
        refQueueSize, true, "refQueue", SolrInfoBean.Category.OTHER.toString(), "unloadable");
  }

  @Override
  @SuppressWarnings("try")
  public void close() throws IOException {
    closing = true;
    SolrMetricProducer.super.close();
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
            lastAccessToReloadMillis.update(TimeUnit.NANOSECONDS.toMillis(nanosSincePriorAccess));
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
              AtomicBoolean handleRefQueue,
              LongSupplier indirectTrackedCount,
              LongSupplier refsCollected) {
            handleRefQueues(indirectTrackedCount, refsCollected);
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
}
