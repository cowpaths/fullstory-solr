package org.apache.solr.core;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.codahale.metrics.Snapshot;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.lucene.index.Unloader;
import org.apache.lucene.util.InfoStream;
import org.apache.solr.common.MapWriter;
import org.apache.solr.handler.admin.MetricsHandler;
import org.apache.solr.metrics.MetricSuppliers;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.util.stats.MetricUtils;
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
      loadTimeMillis =
          invertedHistogram(
              solrMetricsContext, "loadTimeMillis", metricPath, DEFAULT_WRITE_HISTOGRAM);
      lastAccessToReloadMillis =
          invertedHistogram(
              solrMetricsContext,
              "lastAccessToReloadMillis",
              metricPath,
              (snapshot, filter) -> {
                filter.accept("min_ms", TimeUnit.NANOSECONDS.toMillis(snapshot.getMin()));
                filter.accept("max_ms", TimeUnit.NANOSECONDS.toMillis(snapshot.getMax()));
                filter.accept("mean_ms", nsToMs(snapshot.getMean()));
                filter.accept("median_ms", nsToMs(snapshot.getMedian()));
                filter.accept("stddev_ms", nsToMs(snapshot.getStdDev()));
                filter.accept("p75_ms", nsToMs(snapshot.getValue(0.75)));
                filter.accept("p25_ms", nsToMs(snapshot.getValue(0.25)));
                filter.accept("p05_ms", nsToMs(snapshot.getValue(0.05)));
                filter.accept("p01_ms", nsToMs(snapshot.getValue(0.01)));
                filter.accept("p001_ms", nsToMs(snapshot.getValue(0.001)));
              });
      unloaded = solrMetricsContext.meter("unloaded", metricPath);
      closed = solrMetricsContext.meter("closed", metricPath);
    }
  }

  private static final long NANOS_PER_MILLI = TimeUnit.MILLISECONDS.toNanos(1);

  private static double nsToMs(double nanos) {
    return nanos / NANOS_PER_MILLI;
  }

  private static Histogram invertedHistogram(
      SolrMetricsContext ctx,
      String metricName,
      String[] metricPath,
      BiConsumer<Snapshot, BiConsumer<String, Object>> writeHistogram) {
    SolrMetricManager mgr = ctx.getMetricManager();
    final String name = SolrMetricManager.mkName(metricName, metricPath);
    ctx.registerMetricName(name);
    return mgr.registry(ctx.getRegistryName())
        .histogram(
            name, () -> new TimeHistogram(mgr.getHistogramSupplier().newMetric(), writeHistogram));
  }

  @SuppressWarnings("UnnecessaryLambda")
  private static final BiConsumer<Snapshot, BiConsumer<String, Object>> DEFAULT_WRITE_HISTOGRAM =
      (snapshot, filter) -> {
        filter.accept("min_ms", TimeUnit.NANOSECONDS.toMillis(snapshot.getMin()));
        filter.accept("max_ms", TimeUnit.NANOSECONDS.toMillis(snapshot.getMax()));
        filter.accept("mean_ms", nsToMs(snapshot.getMean()));
        filter.accept("median_ms", nsToMs(snapshot.getMedian()));
        filter.accept("stddev_ms", nsToMs(snapshot.getStdDev()));
        filter.accept("p75_ms", nsToMs(snapshot.get75thPercentile()));
        filter.accept("p95_ms", nsToMs(snapshot.get95thPercentile()));
        filter.accept("p99_ms", nsToMs(snapshot.get99thPercentile()));
        filter.accept("p999_ms", nsToMs(snapshot.get999thPercentile()));
      };

  private static final class TimeHistogram extends Histogram implements MetricUtils.SnapshotWriter {

    private final Histogram delegate;
    private final BiConsumer<Snapshot, BiConsumer<String, Object>> writeHistogram;

    private TimeHistogram(
        Histogram delegate, BiConsumer<Snapshot, BiConsumer<String, Object>> writeHistogram) {
      super(null);
      this.delegate = delegate;
      this.writeHistogram = writeHistogram;
    }

    @Override
    public void addSnapshot(
        MapWriter.EntryWriter ew, Snapshot snapshot, Predicate<CharSequence> propertyFilter) {
      BiConsumer<String, Object> filter =
          (k, v) -> {
            if (propertyFilter.test(k)) {
              ew.putNoEx(k, v);
            }
          };
      writeHistogram.accept(snapshot, filter);
    }

    @Override
    public void update(int value) {
      delegate.update(value);
    }

    @Override
    public void update(long value) {
      delegate.update(value);
    }

    @Override
    public long getCount() {
      return delegate.getCount();
    }

    @Override
    public Snapshot getSnapshot() {
      return delegate.getSnapshot();
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

  /**
   * How many keep-alive time ranges should we care about when considering recent history of a
   * resource for the purpose of avoiding thrashing.
   */
  private static final int KEEP_ALIVE_MULTIPLIER = 20;

  private static final long TIME_WINDOW_NANOS = KEEP_ALIVE_MULTIPLIER * Unloader.KEEP_ALIVE_NANOS;

  private static final long ACCEPTABLE_THRESHOLD = Unloader.KEEP_ALIVE_NANOS;

  /** Max amount of time we'll ever wait. 8x the configured keep-alive */
  private static final long MAX_THRESHOLD = Unloader.KEEP_ALIVE_NANOS << 3;

  @Override
  @SuppressWarnings("unchecked")
  public T get() {
    Histogram h =
        new Histogram(new SlidingTimeWindowReservoir(TIME_WINDOW_NANOS, TimeUnit.NANOSECONDS));
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
            loadTimeMillis.update(loadTime);
            lastAccessToReloadMillis.update(nanosSincePriorAccess);
            h.update(nanosSincePriorAccess);
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
          public long deferUnload(long nanosSinceLastAccess) {
            if (nanosSinceLastAccess >= MAX_THRESHOLD) {
              // we've waited the max amount of time; don't defer any longer
              return -1;
            }
            Snapshot s = h.getSnapshot();
            if (s.size() == 0) {
              // no recent loads; establish a baseline, don't defer
              return -1;
            }
            // get a pessimistic estimate (based on recent history) of how long we anticipate
            // we might enjoy the benefits of unloading if we were to unload now.
            long pessimisticExpectRemaining = (long) s.getValue(0.25) - nanosSinceLastAccess;
            if (pessimisticExpectRemaining >= ACCEPTABLE_THRESHOLD) {
              // we expect it's worth unloading; don't defer.
              return -1;
            } else {
              // wait a generous amount of time, up to `MAX_THRESHOLD`, to give a chance to
              // be kept alive without being closed.
              long generous = (long) s.getValue(0.75);
              return Math.min(generous, MAX_THRESHOLD) - nanosSinceLastAccess;
            }
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
