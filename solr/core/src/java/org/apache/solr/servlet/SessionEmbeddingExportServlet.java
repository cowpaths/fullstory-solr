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
package org.apache.solr.servlet;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.storage.CompressingDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletOutputStream;
import javax.servlet.UnavailableException;
import javax.servlet.WriteListener;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.invoke.MethodHandles;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.solr.util.circuitbreaker.CircuitBreakerRegistry.getTimesTrippedMetrics;

/**
 * FullStory: a simple servlet to produce a few prometheus metrics. This servlet exists for
 * backwards compatibility and will be removed in favor of the native prometheus-exporter.
 */
public final class SessionEmbeddingExportServlet extends BaseSolrServlet {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // values less than this threshold are considered invalid; mark the invalid values instead of
  // failing the call.


  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException, UnavailableException {
    String collection = request.getParameter("coll");
    if (collection == null) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "coll is a required parameter");
      return;
    }
    String exportTypeParam = request.getParameter("type");
    if (exportTypeParam == null) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "type is a required parameter. Valid values are: " + Arrays.toString(ExportType.values()));
      return;
    }
    ExportType exportType = ExportType.fromString(exportTypeParam);


    String protocol = request.getScheme(); // http or https
    int port = request.getServerPort(); // port number
    SolrClient solrClient = new HttpSolrClient.Builder(protocol + "://localhost:" + port + "/solr/" + collection).build();

    SolrQuery query = new SolrQuery();
    query.set("q", "SessionVectorGroup:*");
    query.set("sort", "SessionStart desc");
    query.set("fl", "SessionId,SessionVector,SessionVectorGroup,SessionVectorGroupDotProduct,SessionSummary");
    query.set("rows", "10000");

    try {
      QueryResponse qRsp = solrClient.query(query);
      SolrDocumentList results = qRsp.getResults();
      response.setContentType("text/csv");
      try (PrintWriter out = response.getWriter()) {
        if (exportType == ExportType.META) {
          out.println("SessionId\tSessionVectorGroup(euclidean)\tSessionVectorGroup(dot product)\tSessionSummary");
          results.forEach(doc -> {
            out.println(doc.getFieldValue("SessionId") + "\t" + doc.getFieldValue("SessionVectorGroup") + "\t" + doc.getFieldValue("SessionVectorDotProductGroup") + "\t" + doc.getFieldValue("SessionSummary"));
          });
          response.setHeader("Content-Disposition", "attachment; filename=\"" + collection + "-session-meta.tsv\"");
        } else if (exportType == ExportType.VECTOR) {
          results.forEach(doc -> {
            @SuppressWarnings("unchecked")
            List<Float> sessionVector = (List<Float>) doc.getFieldValue("SessionVector");
            String vectorString = sessionVector.stream()
                    .map(String::valueOf)
                    .collect(Collectors.joining("\t"));
            out.println(vectorString);
          });
          response.setHeader("Content-Disposition", "attachment; filename=\"" + collection + "-session-vector.tsv\"");
        }
      }



    } catch (SolrServerException e) {
      throw new RuntimeException(e);
    }

    // Process the response as needed
  }
  public enum ExportType {
    VECTOR, META;

    public static ExportType fromString(String value) {
      for (ExportType type : ExportType.values()) {
        if (type.name().equalsIgnoreCase(value)) {
          return type;
        }
      }
      throw new IllegalArgumentException("Unknown export type: " + value);
    }
  }
}
