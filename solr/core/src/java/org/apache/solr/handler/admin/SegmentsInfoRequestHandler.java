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
package org.apache.solr.handler.admin;

import static org.apache.lucene.index.IndexOptions.DOCS;
import static org.apache.lucene.index.IndexOptions.DOCS_AND_FREQS;
import static org.apache.lucene.index.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
import static org.apache.solr.common.params.CommonParams.NAME;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergePolicy.MergeSpecification;
import org.apache.lucene.index.MergePolicy.OneMerge;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Terms;
import java.lang.reflect.Method;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.Version;
import org.apache.solr.common.luke.FieldFlag;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SegmentSearchStats;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.update.SolrIndexWriter;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This handler exposes information about last commit generation segments */
public class SegmentsInfoRequestHandler extends RequestHandlerBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String FIELD_INFO_PARAM = "fieldInfo";
  public static final String CORE_INFO_PARAM = "coreInfo";
  public static final String SIZE_INFO_PARAM = "sizeInfo";
  public static final String RAW_SIZE_PARAM = "rawSize";
  public static final String RAW_SIZE_SUMMARY_PARAM = "rawSizeSummary";
  public static final String RAW_SIZE_DETAILS_PARAM = "rawSizeDetails";
  public static final String RAW_SIZE_SAMPLING_PERCENT_PARAM = "rawSizeSamplingPercent";

  private static final List<String> FI_LEGEND;

  static {
    FI_LEGEND =
        Arrays.asList(
            FieldFlag.INDEXED.toString(),
            FieldFlag.DOC_VALUES.toString(),
            "xxx - DocValues type",
            FieldFlag.TERM_VECTOR_STORED.toString(),
            FieldFlag.OMIT_NORMS.toString(),
            FieldFlag.OMIT_TF.toString(),
            FieldFlag.OMIT_POSITIONS.toString(),
            FieldFlag.STORE_OFFSETS_WITH_POSITIONS.toString(),
            "p - field has payloads",
            "s - field uses soft deletes",
            ":x:x:x - point data dim : index dim : num bytes");
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    getSegmentsInfo(req, rsp);
    rsp.setHttpCaching(false);
  }

  private static final double GB = 1024.0 * 1024.0 * 1024.0;

  private void getSegmentsInfo(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    boolean withFieldInfo = req.getParams().getBool(FIELD_INFO_PARAM, false);
    boolean withCoreInfo = req.getParams().getBool(CORE_INFO_PARAM, false);
    boolean withSizeInfo = req.getParams().getBool(SIZE_INFO_PARAM, false);
    boolean withRawSizeInfo = req.getParams().getBool(RAW_SIZE_PARAM, false);
    boolean withRawSizeSummary = req.getParams().getBool(RAW_SIZE_SUMMARY_PARAM, false);
    boolean withRawSizeDetails = req.getParams().getBool(RAW_SIZE_DETAILS_PARAM, false);
    if (withRawSizeSummary || withRawSizeDetails) {
      withRawSizeInfo = true;
    }
    SolrIndexSearcher searcher = req.getSearcher();

    SegmentInfos infos = SegmentInfos.readLatestCommit(searcher.getIndexReader().directory());

    SimpleOrderedMap<Object> segmentInfos = new SimpleOrderedMap<>();

    SolrCore core = req.getCore();
    SimpleOrderedMap<Object> infosInfo = new SimpleOrderedMap<>();
    Version minVersion = infos.getMinSegmentLuceneVersion();
    if (minVersion != null) {
      infosInfo.add("minSegmentLuceneVersion", minVersion.toString());
    }
    Version commitVersion = infos.getCommitLuceneVersion();
    if (commitVersion != null) {
      infosInfo.add("commitLuceneVersion", commitVersion.toString());
    }
    infosInfo.add("numSegments", infos.size());
    infosInfo.add("segmentsFileName", infos.getSegmentsFileName());
    infosInfo.add("totalMaxDoc", infos.totalMaxDoc());
    infosInfo.add("userData", infos.userData);
    if (withCoreInfo) {
      SimpleOrderedMap<Object> coreInfo = new SimpleOrderedMap<>();
      infosInfo.add("core", coreInfo);
      coreInfo.add(
          "startTime", core.getStartTimeStamp().getTime() + "(" + core.getStartTimeStamp() + ")");
      coreInfo.add("dataDir", core.getDataDir());
      coreInfo.add("indexDir", core.getIndexDir());
      coreInfo.add("sizeInGB", (double) core.getIndexSize() / GB);

      RefCounted<IndexWriter> iwRef = core.getSolrCoreState().getIndexWriter(core);
      if (iwRef != null) {
        try {
          IndexWriter iw = iwRef.get();
          String iwConfigStr = iw.getConfig().toString();
          SimpleOrderedMap<Object> iwConfig = new SimpleOrderedMap<>();
          // meh ...
          String[] lines = iwConfigStr.split("\\n");
          for (String line : lines) {
            String[] parts = line.split("=");
            if (parts.length < 2) {
              continue;
            }
            iwConfig.add(parts[0], parts[1]);
          }
          coreInfo.add("indexWriterConfig", iwConfig);
        } finally {
          iwRef.decref();
        }
      }
    }
    SimpleOrderedMap<Object> segmentInfo;
    List<SegmentCommitInfo> sortable = new ArrayList<>(infos.asList());
    // Order by the number of live docs. The display is logarithmic so it is a little jumbled
    // visually
    sortable.sort(
        (s1, s2) -> (s2.info.maxDoc() - s2.getDelCount()) - (s1.info.maxDoc() - s1.getDelCount()));

    List<String> mergeCandidates = new ArrayList<>();
    SimpleOrderedMap<Object> runningMerges = getMergeInformation(req, infos, mergeCandidates);
    List<LeafReaderContext> leafContexts = searcher.getIndexReader().leaves();
    IndexSchema schema = req.getSchema();

    // Detect TemporalMergePolicy via reflection (it lives in the plugin, not Solr core).
    // The actual policy may be wrapped in FilterMergePolicy (e.g. SortingMergePolicy), so
    // we unwrap by walking the "in" field chain.
//    Object temporalPolicy = null;
//    Method getSegmentDateRangeMethod = null;
//    RefCounted<IndexWriter> iwRef2 = core.getSolrCoreState().getIndexWriter(core);
//    try {
//      MergePolicy mp = iwRef2.get().getConfig().getMergePolicy();
//      temporalPolicy = unwrapTemporalMergePolicy(mp);
//      if (temporalPolicy != null) {
//        getSegmentDateRangeMethod =
//            temporalPolicy.getClass().getMethod("getSegmentDateRange", SegmentCommitInfo.class);
//      }
//    } catch (NoSuchMethodException e) {
//      log.debug("TemporalMergePolicy found but getSegmentDateRange method not available", e);
//    } finally {
//      iwRef2.decref();
//    }

    for (SegmentCommitInfo segmentCommitInfo : sortable) {
      segmentInfo =
          getSegmentInfo(segmentCommitInfo, withSizeInfo, withFieldInfo, leafContexts, schema);
      if (mergeCandidates.contains(segmentCommitInfo.info.name)) {
        segmentInfo.add("mergeCandidate", true);
      }
//      if (getSegmentDateRangeMethod != null) {
//        try {
//          Object dateRange = getSegmentDateRangeMethod.invoke(temporalPolicy, segmentCommitInfo);
//          if (dateRange != null) {
//            long minDate = dateRange.getClass().getField("minDate").getLong(dateRange);
//            long maxDate = dateRange.getClass().getField("maxDate").getLong(dateRange);
//            segmentInfo.add("temporalMinDate", new Date(minDate));
//            segmentInfo.add("temporalMaxDate", new Date(maxDate));
//          }
//        } catch (Exception e) {
//          log.debug("Failed to extract temporal date range for segment {}",
//              segmentCommitInfo.info.name, e);
//        }
//      }
      SegmentDateRange segmentDateRange = extractDateRangeFromSegment(segmentCommitInfo);
      if (segmentDateRange != null) {
        segmentInfo.add("temporalMinDate", new Date(segmentDateRange.minDate));
        segmentInfo.add("temporalMaxDate", new Date(segmentDateRange.maxDate));
      }
      if (searcher.isSegmentStatsEnabled()) {
        SegmentSearchStats.Snapshot snap =
            searcher.getSegmentSearchStatsSnapshot(segmentCommitInfo.info.name);
        if (snap != null && snap.leafVisits > 0) {
          segmentInfo.add("segmentSearchLeafVisits", snap.leafVisits);
          segmentInfo.add("segmentSearchTotalTimeMs", snap.totalTimeMs);
          segmentInfo.add("segmentSearchAvgTimeMs", snap.avgTimeMs);
          segmentInfo.add("segmentSearchMinTimeMs", snap.minTimeMs);
          segmentInfo.add("segmentSearchMaxTimeMs", snap.maxTimeMs);
          segmentInfo.add(
              "segmentSearchHistogramType", SegmentSearchStats.Snapshot.DECAYING_HISTOGRAM_TYPE);
          segmentInfo.add("segmentSearchHistogramSampleSize", snap.histogramSampleSize);
          segmentInfo.add("segmentSearchHistogramUpdateCount", snap.histogramUpdateCount);
          segmentInfo.add("segmentSearchP50TimeMs", snap.p50TimeMs);
          segmentInfo.add("segmentSearchP90TimeMs", snap.p90TimeMs);
          segmentInfo.add("segmentSearchP95TimeMs", snap.p95TimeMs);
          segmentInfo.add("segmentSearchP99TimeMs", snap.p99TimeMs);
        }
      }
      segmentInfos.add((String) segmentInfo.get(NAME), segmentInfo);
    }

    rsp.add("info", infosInfo);
    if (runningMerges.size() > 0) {
      rsp.add("runningMerges", runningMerges);
    }
    if (withFieldInfo) {
      rsp.add("fieldInfoLegend", FI_LEGEND);
    }
    rsp.add("segments", segmentInfos);
    if (withRawSizeInfo) {
      IndexSizeEstimator estimator =
          new IndexSizeEstimator(
              searcher.getRawReader(), 20, 100, withRawSizeSummary, withRawSizeDetails);
      Object samplingPercentVal = req.getParams().get(RAW_SIZE_SAMPLING_PERCENT_PARAM);
      if (samplingPercentVal != null) {
        estimator.setSamplingPercent(Float.parseFloat(String.valueOf(samplingPercentVal)));
      }
      IndexSizeEstimator.Estimate estimate = estimator.estimate();
      SimpleOrderedMap<Object> estimateMap = new SimpleOrderedMap<>();
      // make the units more user-friendly
      estimateMap.add(IndexSizeEstimator.FIELDS_BY_SIZE, estimate.getHumanReadableFieldsBySize());
      estimateMap.add(IndexSizeEstimator.TYPES_BY_SIZE, estimate.getHumanReadableTypesBySize());
      if (estimate.getSummary() != null) {
        estimateMap.add(IndexSizeEstimator.SUMMARY, estimate.getSummary());
      }
      if (estimate.getDetails() != null) {
        estimateMap.add(IndexSizeEstimator.DETAILS, estimate.getDetails());
      }
      rsp.add("rawSize", estimateMap);
    }
  }

  private SimpleOrderedMap<Object> getSegmentInfo(
      SegmentCommitInfo segmentCommitInfo,
      boolean withSizeInfo,
      boolean withFieldInfos,
      List<LeafReaderContext> leafContexts,
      IndexSchema schema)
      throws IOException {
    SimpleOrderedMap<Object> segmentInfoMap = new SimpleOrderedMap<>();

    segmentInfoMap.add(NAME, segmentCommitInfo.info.name);
    segmentInfoMap.add("delCount", segmentCommitInfo.getDelCount());
    segmentInfoMap.add("softDelCount", segmentCommitInfo.getSoftDelCount());
    segmentInfoMap.add("hasFieldUpdates", segmentCommitInfo.hasFieldUpdates());
    segmentInfoMap.add("sizeInBytes", segmentCommitInfo.sizeInBytes());
    segmentInfoMap.add("size", segmentCommitInfo.info.maxDoc());
    Long timestamp = Long.parseLong(segmentCommitInfo.info.getDiagnostics().get("timestamp"));
    segmentInfoMap.add("age", new Date(timestamp));
    segmentInfoMap.add("source", segmentCommitInfo.info.getDiagnostics().get("source"));
    segmentInfoMap.add("version", segmentCommitInfo.info.getVersion().toString());
    // don't open a new SegmentReader - try to find the right one from the leaf contexts
    SegmentReader seg = null;
    for (LeafReaderContext lrc : leafContexts) {
      LeafReader leafReader = lrc.reader();
      leafReader = FilterLeafReader.unwrap(leafReader);
      if (leafReader instanceof SegmentReader) {
        SegmentReader sr = (SegmentReader) leafReader;
        if (sr.getSegmentInfo().info.equals(segmentCommitInfo.info)) {
          seg = sr;
          break;
        }
      }
    }
    if (seg != null) {
      LeafMetaData metaData = seg.getMetaData();
      if (metaData != null) {
        segmentInfoMap.add("createdVersionMajor", metaData.getCreatedVersionMajor());
        segmentInfoMap.add("minVersion", metaData.getMinVersion().toString());
        if (metaData.getSort() != null) {
          segmentInfoMap.add("sort", metaData.getSort().toString());
        }
      }
    }
    if (!segmentCommitInfo.info.getDiagnostics().isEmpty()) {
      segmentInfoMap.add("diagnostics", segmentCommitInfo.info.getDiagnostics());
    }
    if (!segmentCommitInfo.info.getAttributes().isEmpty()) {
      segmentInfoMap.add("attributes", segmentCommitInfo.info.getAttributes());
    }
    if (withSizeInfo) {
      Directory dir = segmentCommitInfo.info.dir;
      List<Pair<String, Long>> files =
          segmentCommitInfo.files().stream()
              .map(
                  f -> {
                    long size = -1;
                    try {
                      size = dir.fileLength(f);
                    } catch (IOException e) {
                    }
                    return new Pair<String, Long>(f, size);
                  })
              .sorted(
                  (p1, p2) -> {
                    if (p1.second() > p2.second()) {
                      return -1;
                    } else if (p1.second() < p2.second()) {
                      return 1;
                    } else {
                      return 0;
                    }
                  })
              .collect(Collectors.toList());
      if (!files.isEmpty()) {
        SimpleOrderedMap<Object> topFiles = new SimpleOrderedMap<>();
        for (int i = 0; i < Math.min(files.size(), 5); i++) {
          Pair<String, Long> p = files.get(i);
          topFiles.add(p.first(), RamUsageEstimator.humanReadableUnits(p.second()));
        }
        segmentInfoMap.add("largestFiles", topFiles);
      }
    }
    if (withFieldInfos) {
      if (seg == null) {
        log.debug(
            "Skipping segment info - not available as a SegmentReader: {}", segmentCommitInfo);
      } else {
        FieldInfos fis = seg.getFieldInfos();
        SimpleOrderedMap<Object> fields = new SimpleOrderedMap<>();
        for (FieldInfo fi : fis) {
          fields.add(fi.name, getFieldInfo(seg, fi, schema));
        }
        segmentInfoMap.add("fields", fields);
      }
    }

    return segmentInfoMap;
  }

  private SimpleOrderedMap<Object> getFieldInfo(
      SegmentReader reader, FieldInfo fi, IndexSchema schema) {
    SimpleOrderedMap<Object> fieldFlags = new SimpleOrderedMap<>();
    StringBuilder flags = new StringBuilder();
    IndexOptions opts = fi.getIndexOptions();
    flags.append((opts != IndexOptions.NONE) ? FieldFlag.INDEXED.getAbbreviation() : '-');
    DocValuesType dvt = fi.getDocValuesType();
    if (dvt != DocValuesType.NONE) {
      flags.append(FieldFlag.DOC_VALUES.getAbbreviation());
      switch (dvt) {
        case NUMERIC:
          flags.append("num");
          break;
        case BINARY:
          flags.append("bin");
          break;
        case SORTED:
          flags.append("srt");
          break;
        case SORTED_NUMERIC:
          flags.append("srn");
          break;
        case SORTED_SET:
          flags.append("srs");
          break;
        default:
          flags.append("???"); // should not happen
      }
    } else {
      flags.append("----");
    }
    flags.append((fi.hasVectors()) ? FieldFlag.TERM_VECTOR_STORED.getAbbreviation() : '-');
    flags.append((fi.omitsNorms()) ? FieldFlag.OMIT_NORMS.getAbbreviation() : '-');

    flags.append((DOCS == opts) ? FieldFlag.OMIT_TF.getAbbreviation() : '-');

    flags.append((DOCS_AND_FREQS == opts) ? FieldFlag.OMIT_POSITIONS.getAbbreviation() : '-');

    flags.append(
        (DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS == opts)
            ? FieldFlag.STORE_OFFSETS_WITH_POSITIONS.getAbbreviation()
            : '-');

    flags.append((fi.hasPayloads() ? "p" : "-"));
    flags.append((fi.isSoftDeletesField() ? "s" : "-"));
    if (fi.getPointDimensionCount() > 0 || fi.getPointIndexDimensionCount() > 0) {
      flags.append(":");
      flags.append(fi.getPointDimensionCount()).append(':');
      flags.append(fi.getPointIndexDimensionCount()).append(':');
      flags.append(fi.getPointNumBytes());
    }

    fieldFlags.add("flags", flags.toString());
    try {
      Terms terms = reader.terms(fi.name);
      if (terms != null) {
        fieldFlags.add("docCount", terms.getDocCount());
        fieldFlags.add("termCount", terms.size());
        fieldFlags.add("sumDocFreq", terms.getSumDocFreq());
        fieldFlags.add("sumTotalTermFreq", terms.getSumTotalTermFreq());
      }
    } catch (Exception e) {
      log.debug("Exception retrieving term stats for field {}", fi.name, e);
    }

    // probably too much detail?
    //    Map<String, String> attributes = fi.attributes();
    //    if (!attributes.isEmpty()) {
    //      fieldFlags.add("attributes", attributes);
    //    }

    // check compliance of the index with the current schema
    SchemaField sf = schema.getFieldOrNull(fi.name);
    boolean hasPoints = fi.getPointDimensionCount() > 0 || fi.getPointIndexDimensionCount() > 0;

    if (sf != null) {
      fieldFlags.add("schemaType", sf.getType().getTypeName());
      SimpleOrderedMap<Object> nonCompliant = new SimpleOrderedMap<>();
      if (sf.hasDocValues()
          && fi.getDocValuesType() == DocValuesType.NONE
          && fi.getIndexOptions() != IndexOptions.NONE) {
        nonCompliant.add(
            "docValues", "schema=" + sf.getType().getUninversionType(sf) + ", segment=false");
      }
      if (!sf.hasDocValues() && fi.getDocValuesType() != DocValuesType.NONE) {
        nonCompliant.add("docValues", "schema=false, segment=" + fi.getDocValuesType().toString());
      }
      if (!sf.isPolyField()) { // difficult to find all sub-fields in a general way
        if (sf.indexed() != ((fi.getIndexOptions() != IndexOptions.NONE) || hasPoints)) {
          nonCompliant.add(
              "indexed", "schema=" + sf.indexed() + ", segment=" + fi.getIndexOptions());
        }
      }
      if (!hasPoints && (sf.omitNorms() != fi.omitsNorms())) {
        nonCompliant.add("omitNorms", "schema=" + sf.omitNorms() + ", segment=" + fi.omitsNorms());
      }
      if (sf.storeTermVector() != fi.hasVectors()) {
        nonCompliant.add(
            "termVectors", "schema=" + sf.storeTermVector() + ", segment=" + fi.hasVectors());
      }
      if (sf.storeOffsetsWithPositions()
          != (fi.getIndexOptions() == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)) {
        nonCompliant.add(
            "storeOffsetsWithPositions",
            "schema=" + sf.storeOffsetsWithPositions() + ", segment=" + fi.getIndexOptions());
      }

      if (nonCompliant.size() > 0) {
        nonCompliant.add("schemaField", sf.toString());
        fieldFlags.add("nonCompliant", nonCompliant);
      }
    } else {
      fieldFlags.add("schemaType", "(UNKNOWN)");
    }
    return fieldFlags;
  }

  // returns a map of currently running merges, and populates a list of candidate segments for merge
  private SimpleOrderedMap<Object> getMergeInformation(
      SolrQueryRequest req, SegmentInfos infos, List<String> mergeCandidates) throws IOException {
    SimpleOrderedMap<Object> result = new SimpleOrderedMap<>();
    RefCounted<IndexWriter> refCounted =
        req.getCore().getSolrCoreState().getIndexWriter(req.getCore());
    try {
      IndexWriter indexWriter = refCounted.get();
      if (indexWriter instanceof SolrIndexWriter) {
        result.addAll(((SolrIndexWriter) indexWriter).getRunningMerges());
      }
      // get chosen merge policy
      MergePolicy mp = indexWriter.getConfig().getMergePolicy();
      // Find merges
      MergeSpecification findMerges = mp.findMerges(MergeTrigger.EXPLICIT, infos, indexWriter);
      if (findMerges != null && findMerges.merges != null && findMerges.merges.size() > 0) {
        for (OneMerge merge : findMerges.merges) {
          // TODO: add merge grouping
          for (SegmentCommitInfo mergeSegmentInfo : merge.segments) {
            mergeCandidates.add(mergeSegmentInfo.info.name);
          }
        }
      }

      return result;
    } finally {
      refCounted.decref();
    }
  }

  private static final String TEMPORAL_MERGE_POLICY_CLASS = "mn.fs.solr.index.TemporalMergePolicy";

  /**
   * Walk the FilterMergePolicy wrapper chain to find a TemporalMergePolicy. Returns null if not
   * found.
   */
  private static Object unwrapTemporalMergePolicy(MergePolicy mp) {
    MergePolicy current = mp;
    while (current != null) {
      if (current.getClass().getName().equals(TEMPORAL_MERGE_POLICY_CLASS)) {
        return current;
      }
      // FilterMergePolicy stores the delegate in a protected field "in"
      try {
        java.lang.reflect.Field inField = current.getClass().getSuperclass().getDeclaredField("in");
        inField.setAccessible(true);
        Object inner = inField.get(current);
        if (inner instanceof MergePolicy) {
          current = (MergePolicy) inner;
        } else {
          break;
        }
      } catch (NoSuchFieldException | IllegalAccessException e) {
        break;
      }
    }
    return null;
  }

  @Override
  public String getDescription() {
    return "Lucene segments info.";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }

  @Override
  public Name getPermissionName(AuthorizationContext request) {
    return Name.METRICS_READ_PERM;
  }



  /**
   * Extract date range (min/max timestamps) from a single segment by reading point values.
   *
   * @param segmentInfo the segment to read from
   * @return the date range, or null if the temporal field is not present or has no values
   * @throws IOException if there's an error reading the segment
   */
  private static SegmentDateRange extractDateRangeFromSegment(SegmentCommitInfo segmentInfo)
          throws IOException {
    SegmentInfo si = segmentInfo.info;

    // Track compound directory separately to ensure we never close si.dir
    Directory compoundDir = null;
    try {
      Directory readerDir =
              si.getUseCompoundFile()
                      ? (compoundDir = si.getCodec().compoundFormat().getCompoundReader(si.dir, si, IOContext.DEFAULT))
                      : si.dir;

      FieldInfos fieldInfos =
              si.getCodec().fieldInfosFormat().read(readerDir, si, "", IOContext.READONCE);

      String temporalField = "EventStart";
      // Validate that the temporal field exists and is a point field
      FieldInfo fieldInfo = fieldInfos.fieldInfo(temporalField);
      if (fieldInfo == null) {
        return null;
      }

      if (fieldInfo.getPointDimensionCount() == 0) {
        log.warn(
                "Segment "
                        + si.name
                        + ": temporal field '"
                        + temporalField
                        + "' is not indexed as a point field (found: "
                        + fieldInfo
                        + "). "
                        + "Skipping this segment for date-tiered merging. This may occur with legacy segments "
                        + "or after schema changes.");
        return null;
      }

      // Read point values using the codec
      PointsFormat pointsFormat = si.getCodec().pointsFormat();
      try (PointsReader pointsReader =
                   pointsFormat.fieldsReader(
                           new SegmentReadState(readerDir, si, fieldInfos, IOContext.READONCE))) {

        PointValues pointValues = pointsReader.getValues(temporalField);
        if (pointValues == null) {
          return null;
        }

        byte[] minPackedValue = pointValues.getMinPackedValue();
        byte[] maxPackedValue = pointValues.getMaxPackedValue();

        if (minPackedValue == null || maxPackedValue == null) {
          return null;
        }

        // Decode the packed values as longs, we have to make an assumption here
        // since we don't have the schema :/
        long minDate = LongPoint.decodeDimension(minPackedValue, 0);
        long maxDate = LongPoint.decodeDimension(maxPackedValue, 0);

        // Convert to milliseconds based on detected unit
        long divisor = getTemporalFieldDivisor(maxDate);
        long minDateMillis, maxDateMillis;
        if (divisor < 0) {
          long multiplier = -divisor;
          minDateMillis = minDate * multiplier;
          maxDateMillis = maxDate * multiplier;
        } else {
          minDateMillis = minDate / divisor;
          maxDateMillis = maxDate / divisor;
        }

        return new SegmentDateRange(minDateMillis, maxDateMillis);
      }
    } finally {
      // Close compound directory if we opened it (never close si.dir)
      if (compoundDir != null) {
        compoundDir.close();
      }
    }
  }

  private static long getTemporalFieldDivisor(long maxValue) {
    if (maxValue > 100_000_000_000_000L) {
      // Values > 10^14 are microseconds
      return 1_000;
    } else if (maxValue > 100_000_000_000L) {
      // Values > 10^11 are milliseconds
      return 1;
    } else {
      // Values <= 10^11 are seconds
      return -1000;
    }
  }

  public static class SegmentDateRange {
    public final long minDate;
    public final long maxDate;

    public SegmentDateRange(long minDate, long maxDate) {
      this.minDate = minDate;
      this.maxDate = maxDate;
    }
  }

}
