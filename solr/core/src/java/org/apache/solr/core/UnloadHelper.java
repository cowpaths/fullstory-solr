package org.apache.solr.core;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.codahale.metrics.Snapshot;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.Unloader;
import org.apache.lucene.index.UnloadingFieldsProducer;
import org.apache.lucene.util.InfoStream;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.admin.MetricsHandler;
import org.apache.solr.metrics.MetricSuppliers;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.util.stats.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class UnloadHelper<T extends Unloader.UnloadHelper>
    implements Supplier<T>, SolrMetricProducer {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // default to false
  private static final boolean TRACK_RELOADS =
      "true".equals(System.getProperty("lucene.unload.trackReloads"));

  private final String disableUnloadForField;
  private final int minSegSizeForUnload;
  private final ScheduledExecutorService exec;
  private final SolrMetricsContext solrMetricsContext;
  private final Meter created;
  private final Meter loaded;
  private final Histogram loadTimeMillis;
  private final Histogram lastAccessToReloadMillis;
  private final ConcurrentHashMap<StackTrace, Histogram> reloadFrom =
      TRACK_RELOADS ? new ConcurrentHashMap<>() : null;
  private final Meter unloaded;
  private final Meter closed;
  private final InfoStream infoStream = new UnloaderLoggingInfoStream();

  private volatile Object lastMinUnloadSpec;
  private volatile long minUnloadNanos = Unloader.KEEP_ALIVE_NANOS;
  private volatile int dynamicMinSegSize = -1;

  UnloadHelper(CoreContainer cc, String disableUnloadForField, int minSegSizeForUnload) {
    this.disableUnloadForField = disableUnloadForField;
    this.minSegSizeForUnload = minSegSizeForUnload;
    ZkController zkController = cc.getZkController();
    if (zkController != null) {
      // NOTE: this is scoped to the CoreContainer lifecycle, so we don't have to worry about
      // removing the clusterProps listener.
      log.info("adding clusterprops listener for minUnloadTime");
      zkController
          .getZkStateReader()
          .registerClusterPropertiesListener(
              (p) -> {
                Object o = p.get("minUnloadTime");
                if (Objects.equals(lastMinUnloadSpec, o)) {
                  // we get notified every time clusterprops changes; shortcircuit processing
                  // unless it's actually _our_ value that's changed.
                  return false;
                }
                lastMinUnloadSpec = o;
                long newVal;
                if (o instanceof Number) {
                  // millis
                  newVal = TimeUnit.MILLISECONDS.toNanos(((Number) o).longValue());
                } else if (o instanceof String) {
                  try {
                    newVal = Unloader.getNanos((String) o);
                  } catch (Exception ex) {
                    log.warn("problem parsing clusterprops `minUnloadTime` spec: {}", o, ex);
                    newVal = Unloader.KEEP_ALIVE_NANOS;
                  }
                } else {
                  // either null, or unrecognized type
                  newVal = Unloader.KEEP_ALIVE_NANOS;
                  if (o != null) {
                    log.warn(
                        "unrecognized type for clusterprops `minUnloadTime`: {} ({})",
                        o,
                        o.getClass());
                  }
                }
                if (newVal < 0) {
                  log.warn(
                      "clusterprops minUnloadTime should be >= 0; found {}, from {}", newVal, o);
                  minUnloadNanos = Unloader.KEEP_ALIVE_NANOS;
                } else {
                  log.info("set `minUnloadNanos` from clusterprops {} ({} nanos)", o, newVal);
                  minUnloadNanos = newVal;
                }
                return false;
              });
      zkController
          .getZkStateReader()
          .registerClusterPropertiesListener(
              (p) -> {
                Object o = null;
                try {
                  o = p.get("unloadMinSegSize");
                  int unloadMinSegSize;
                  if (o instanceof Number) {
                    unloadMinSegSize = ((Number) o).intValue();
                  } else if (o instanceof String) {
                    unloadMinSegSize = Integer.parseInt((String) o);
                  } else if (o != null) {
                    throw new IllegalArgumentException("unrecognized type");
                  } else {
                    unloadMinSegSize = -1;
                  }
                  if (unloadMinSegSize != dynamicMinSegSize) {
                    log.info(
                        "changed dynamic unloadMinSegSize; {} -> {}",
                        dynamicMinSegSize,
                        unloadMinSegSize);
                  }
                  dynamicMinSegSize = unloadMinSegSize;
                } catch (Exception e) {
                  log.warn("problem parsing unloadMinSegSize spec=\"{}\"", o, e);
                }
                return true;
              });
    }
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
          customizedHistogram(
              solrMetricsContext, "loadTimeMillis", metricPath, DEFAULT_WRITE_HISTOGRAM);
      lastAccessToReloadMillis =
          customizedHistogram(
              solrMetricsContext, "lastAccessToReloadMillis", metricPath, INVERTED_WRITE_HISTOGRAM);
      unloaded = solrMetricsContext.meter("unloaded", metricPath);
      closed = solrMetricsContext.meter("closed", metricPath);
      if (reloadFrom != null) {
        solrMetricsContext.gauge(
            new MetricsMap(
                m -> {
                  List<AbstractMap.SimpleImmutableEntry<StackTrace, Snapshot>> lst =
                      reloadFrom.entrySet().stream()
                          .map(
                              e ->
                                  new AbstractMap.SimpleImmutableEntry<>(
                                      e.getKey(), e.getValue().getSnapshot()))
                          .sorted(
                              (e1, e2) ->
                                  Integer.compare(e2.getValue().size(), e1.getValue().size()))
                          .collect(Collectors.toList());
                  int i = 0;
                  for (Map.Entry<StackTrace, Snapshot> e : lst) {
                    StackTrace key = e.getKey();
                    Snapshot value = e.getValue();
                    try {
                      m.put(
                          Integer.toString(i++),
                          (MapWriter)
                              ew -> {
                                ew.put("stackHash", key.hashCode());
                                ew.put("size", value.size());
                                INVERTED_WRITE_HISTOGRAM.accept(value, ew.getBiConsumer());
                                key.writeTo("stackTrace", ew);
                              });
                    } catch (IOException ex) {
                      log.warn("exception reporting reloads", ex);
                      break;
                    }
                  }
                }),
            true,
            "reloadFrom",
            metricPath);
      }
    }
  }

  private static final long NANOS_PER_MILLI = TimeUnit.MILLISECONDS.toNanos(1);

  private static double nsToMs(double nanos) {
    return nanos / NANOS_PER_MILLI;
  }

  private static Histogram customizedHistogram(
      SolrMetricsContext ctx,
      String metricName,
      String[] metricPath,
      BiConsumer<Snapshot, BiConsumer<CharSequence, Object>> writeHistogram) {
    SolrMetricManager mgr = ctx.getMetricManager();
    final String name = SolrMetricManager.mkName(metricName, metricPath);
    ctx.registerMetricName(name);
    return mgr.registry(ctx.getRegistryName())
        .histogram(
            name, () -> new TimeHistogram(mgr.getHistogramSupplier().newMetric(), writeHistogram));
  }

  @SuppressWarnings("UnnecessaryLambda")
  private static final BiConsumer<Snapshot, BiConsumer<CharSequence, Object>>
      DEFAULT_WRITE_HISTOGRAM =
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

  @SuppressWarnings("UnnecessaryLambda")
  private static final BiConsumer<Snapshot, BiConsumer<CharSequence, Object>>
      INVERTED_WRITE_HISTOGRAM =
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
          };

  private static final class TimeHistogram extends Histogram implements MetricUtils.SnapshotWriter {

    private final Histogram delegate;
    private final BiConsumer<Snapshot, BiConsumer<CharSequence, Object>> writeHistogram;

    private TimeHistogram(
        Histogram delegate, BiConsumer<Snapshot, BiConsumer<CharSequence, Object>> writeHistogram) {
      super(null);
      this.delegate = delegate;
      this.writeHistogram = writeHistogram;
    }

    @Override
    public void addSnapshot(
        MapWriter.EntryWriter ew, Snapshot snapshot, Predicate<CharSequence> propertyFilter) {
      BiConsumer<CharSequence, Object> filter =
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
    Histogram h;
    if (Unloader.ADAPTIVE_DEFER) {
      h = new Histogram(new SlidingTimeWindowReservoir(TIME_WINDOW_NANOS, TimeUnit.NANOSECONDS));
    } else {
      h = MetricSuppliers.NoOpHistogramSupplier.INSTANCE.newMetric();
    }
    return (T)
        new Unloader.AbstractUnloadHelper(exec, infoStream) {
          @Override
          public ScheduledExecutorService onCreation(Unloader<?> u) {
            created.mark();
            return super.onCreation(u);
          }

          @Override
          public void onLoad(long nanosSincePriorAccess, long loadTime, boolean initial) {
            loaded.mark();
            loadTimeMillis.update(loadTime);
            if (!initial) {
              // `nanosSincePriorAccess` is only relevant beyond the initial load (otherwise
              // there's no "prior access" to measure from).
              lastAccessToReloadMillis.update(nanosSincePriorAccess);
              h.update(nanosSincePriorAccess);
              StackTrace st;
              if (reloadFrom != null && (st = new StackTrace()).nonMerge()) {
                // we ignore merge-initiated reloads, since there's nothing we can do about these
                if (log.isInfoEnabled()) {
                  log.info(
                      "reloaded; stackHash={}, rid={}, field={}",
                      st.hashCode(),
                      rid(),
                      UnloadingFieldsProducer.FIELD_REQUESTED.get());
                }
                reloadFrom
                    .computeIfAbsent(st, (s) -> new Histogram(new ExponentiallyDecayingReservoir()))
                    .update(nanosSincePriorAccess);
              }
            }
            super.onLoad(nanosSincePriorAccess, loadTime, initial);
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
          public boolean disableUnload(Class<?> resourceClass, SegmentReadState srs) {
            if (disableUnloadForField == null) {
              return disableUnload0(srs);
            }
            FieldInfo fi = srs.fieldInfos.fieldInfo(disableUnloadForField);
            if (fi == null) {
              return disableUnload0(srs);
            }
            String formatName = fi.getAttribute(PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY);
            if (formatName == null || !formatName.startsWith("NoUnload")) {
              return true;
            } else {
              return disableUnload0(srs);
            }
          }

          private boolean disableUnload0(SegmentReadState srs) {
            int dynamic = dynamicMinSegSize;
            int threshold;
            if (dynamic < 0) {
              threshold = minSegSizeForUnload;
            } else {
              threshold = dynamicMinSegSize;
            }
            return srs.segmentInfo.maxDoc() < threshold;
          }

          @Override
          public long deferUnload(long nanosSinceLastAccess) {
            long minUnloadNanos = UnloadHelper.this.minUnloadNanos;
            if (nanosSinceLastAccess >= Math.max(MAX_THRESHOLD, minUnloadNanos)) {
              // we've waited the max amount of time; don't defer any longer
              return -1;
            }
            Snapshot s = h.getSnapshot();
            // get a pessimistic estimate (based on recent history) of how long we anticipate
            // we might enjoy the benefits of unloading if we were to unload now.
            @SuppressWarnings("unused")
            long pessimisticExpectRemaining;
            if (s.size() == 0
                || (pessimisticExpectRemaining = (long) s.getValue(0.25) - nanosSinceLastAccess)
                    >= ACCEPTABLE_THRESHOLD) {
              // at this point we either expect it's worth unloading, or (size==0) we have no
              // context with which to make a decision, so allow to proceed (establish a baseline)
              if (nanosSinceLastAccess >= minUnloadNanos) {
                // we meet the `minUnloadNanos` criterion; don't defer
                return -1;
              } else {
                // we wait until `minUnloadNanos` criterion may have been satisfied; but don't
                // wait longer than `MAX_THRESHOLD`, because we still want to check periodically
                // in case `minUnloadNanos` is reduced after being set very long.
                return Math.min(minUnloadNanos - nanosSinceLastAccess, MAX_THRESHOLD);
              }
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

  private static String rid() {
    SolrRequestInfo requestInfo = SolrRequestInfo.getRequestInfo();
    if (requestInfo == null) {
      return null;
    }
    SolrQueryRequest req = requestInfo.getReq();
    if (req == null) {
      return null;
    }
    SolrParams params = req.getParams();
    return params == null ? null : params.get("rid");
  }

  /**
   * Special class to allow stack traces to be used as map keys, and to optimize
   * equals/hashcode/toString for frequent use.
   */
  private static final class StackTrace {
    private final StackTraceElement[] elements;
    private Integer hashCode;
    private List<String> toString;

    private StackTrace() {
      this.elements = Thread.currentThread().getStackTrace();
    }

    private List<String> presentable() {
      List<String> cached = toString;
      if (cached != null) {
        return cached;
      }
      StringBuilder sb = new StringBuilder(512);
      String[] ret = new String[elements.length];
      for (int i = elements.length - 1; i >= 0; i--) {
        sb.setLength(0);
        StackTraceElement e = elements[i];
        sb.append(e.getClassName())
            .append('.')
            .append(e.getMethodName())
            .append('(')
            .append(e.getFileName())
            .append(':')
            .append(e.getLineNumber())
            .append(')');
        ret[i] = sb.toString();
      }
      return toString = List.of(ret);
    }

    @Override
    @SuppressWarnings({"EqualsUnsafeCast", "ReferenceEquality"})
    public boolean equals(Object other) {
      if (this == other) return true;
      if (other == null) return false;
      StackTraceElement[] a1 = elements;
      StackTraceElement[] a2 = ((StackTrace) other).elements;
      if (a1 == a2) {
        return true;
      }
      if (a1 == null || a2 == null) {
        return false;
      }
      int length = a1.length;
      if (a2.length != length) {
        return false;
      }

      for (int i = 0; i < length; i++) {
        StackTraceElement e1 = a1[i];
        StackTraceElement e2 = a2[i];

        if (e1 == e2) {
          continue;
        }
        if (e1 == null) {
          return false;
        }

        // Figure out whether the two elements are equal
        if (!e1.getClassName().equals(e2.getClassName())) {
          return false;
        }
        if (!e1.getMethodName().equals(e2.getMethodName())) {
          return false;
        }
        if (e1.getLineNumber() != e2.getLineNumber()) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int hashCode() {
      return hashCode == null ? (hashCode = hashCode(elements)) : hashCode;
    }

    private static int hashCode(StackTraceElement[] elements) {
      int result = 1;
      for (StackTraceElement e : elements) {
        result = 31 * result + hashCode(e);
      }
      return result;
    }

    private static int hashCode(StackTraceElement e) {
      int result = 31 * e.getClassName().hashCode() + e.getMethodName().hashCode();
      return 31 * result + e.getLineNumber();
    }

    public void writeTo(String key, MapWriter.EntryWriter ew) throws IOException {
      ew.put(
          key,
          (MapWriter)
              ew1 -> {
                int i = 0;
                for (String s : presentable()) {
                  ew1.put(Integer.toString(i++), s);
                }
              });
    }

    public boolean nonMerge() {
      return !"org.apache.lucene.index.ConcurrentMergeScheduler$MergeThread"
          .equals(elements[elements.length - 1].getClassName());
    }
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
