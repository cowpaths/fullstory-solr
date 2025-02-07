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

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableField;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.BasicResultContext;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.UnavailableException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * FullStory: a simple servlet to produce a few prometheus metrics. This servlet exists for
 * backwards compatibility and will be removed in favor of the native prometheus-exporter.
 */
public final class SessionEmbeddingExportServlet extends BaseSolrServlet {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // values less than this threshold are considered invalid; mark the invalid values instead of
  // failing the call.


  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException, UnavailableException {
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

    SolrDispatchFilter filter = getSolrDispatchFilter(request);
    CoreContainer cores = filter.getCores();

    List<String> allCoreNames = cores.getAllCoreNames();//hacky...
    Optional<String> coreName = allCoreNames.stream().filter(name -> name.startsWith(collection)).findFirst();

    if (!coreName.isPresent()) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No core found for collection: " + collection);
      return;
    }

    SolrCore core = cores.getCore(coreName.get());


    SolrQuery query = new SolrQuery();
    query.set("q", "SessionVectorGroup:*");
    query.set("sort", "SessionStart desc");
    query.set("fl", "UserIdSessionId,SessionVector,SessionVectorGroup,SessionVectorDotProductGroup,SessionSummary");
    query.set("rows", "10000");

    SolrQueryResponse selectRsp = new SolrQueryResponse();
    try (SolrQueryRequest queryReq = new SolrQueryRequestBase(core, query) {}) {
      core.getRequestHandler("/select").handleRequest(queryReq, selectRsp);
      Exception cause = selectRsp.getException();
      if (cause != null) {
        throw new RuntimeException(cause);
      }

      BasicResultContext brc = (BasicResultContext) selectRsp.getResponse();

      Iterator<SolrDocument> results = brc.getProcessedDocuments();


      response.setContentType("text/tsv");
      try (PrintWriter out = response.getWriter()) {
        if (exportType == ExportType.META) {
          out.println("UserIdSessionId\tSessionVectorGroup(euclidean)\tSessionVectorGroup(dot product)\tSessionSummary");
          results.forEachRemaining(doc -> {
            String summary = ("(Group " + doc.getFieldValue("SessionVectorGroup") + ")" + ((IndexableField)doc.getFieldValue("SessionSummary")).stringValue()).replaceAll("\n", " ");
            out.println(doc.getFieldValue("UserIdSessionId") + "\t" + doc.getFieldValue("SessionVectorGroup") + "\t" + doc.getFieldValue("SessionVectorDotProductGroup") + "\t" + summary);
          });
          response.setHeader("Content-Disposition", "attachment; filename=\"" + collection + "-session-meta.tsv\"");
        } else if (exportType == ExportType.VECTOR) {
          results.forEachRemaining(doc -> {
            @SuppressWarnings("unchecked")
            List<Field> sessionVector = (List<Field>) doc.getFieldValue("SessionVector");
            String vectorString = sessionVector.stream()
                    .map(f -> String.valueOf(f.numericValue()))
                    .collect(Collectors.joining("\t"));
            out.println(vectorString);
          });
          response.setHeader("Content-Disposition", "attachment; filename=\"" + collection + "-session-vector.tsv\"");
        }
      }



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


  static SolrDispatchFilter getSolrDispatchFilter(HttpServletRequest request) throws IOException {
    Object value = request.getAttribute(HttpSolrCall.class.getName());
    if (!(value instanceof HttpSolrCall)) {
      throw new IOException(
              String.format(
                      Locale.ROOT, "request attribute %s does not exist.", HttpSolrCall.class.getName()));
    }
    return ((HttpSolrCall) value).solrDispatchFilter;
  }
}
