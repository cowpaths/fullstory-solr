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

package org.apache.solr.util.circuitbreaker;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.SolrRequest.SolrRequestType;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

/**
 * Default base class to define circuit breaker plugins for Solr. <b>Still experimental, may
 * change</b>
 *
 * <p>There are two (typical) ways to use circuit breakers:
 *
 * <ol>
 *   <li>Have them checked at admission control by default (use CircuitBreakerRegistry for the
 *       same).
 *   <li>Use the circuit breaker in a specific code path(s).
 * </ol>
 *
 * @lucene.experimental
 */
public abstract class CircuitBreaker implements NamedListInitializedPlugin, Closeable {
  public static final String SYSPROP_SOLR_CIRCUITBREAKER_ERRORCODE =
      "solr.circuitbreaker.errorcode";
  private static SolrException.ErrorCode errorCode = resolveExceptionErrorCode();
  // Only query requests are checked by default
  private Set<SolrRequestType> requestTypes = Set.of(SolrRequestType.QUERY);
  private boolean warnOnly;
  private final List<SolrRequestType> SUPPORTED_TYPES =
      List.of(SolrRequestType.QUERY, SolrRequestType.UPDATE);

  @Override
  public void init(NamedList<?> args) {
    SolrPluginUtils.invokeSetters(this, args);
    if (args.getBooleanArg("warnOnly") != null) {
      setWarnOnly(args.getBooleanArg("warnOnly"));
    }
  }

  public CircuitBreaker() {
    // Early abort if custom error code system property is wrong
    errorCode = resolveExceptionErrorCode();
  }

  /** Check if circuit breaker is tripped. */
  public abstract boolean isTripped();

  /** Get error message when the circuit breaker triggers */
  public abstract String getErrorMessage();

  /**
   * Get http error code, defaults to 429 (TOO_MANY_REQUESTS) but can be overridden with system
   * property {@link #SYSPROP_SOLR_CIRCUITBREAKER_ERRORCODE}
   */
  public static SolrException.ErrorCode getExceptionErrorCode() {
    return errorCode;
  }

  private static SolrException.ErrorCode resolveExceptionErrorCode() {
    int intCode = SolrException.ErrorCode.TOO_MANY_REQUESTS.code;
    String strCode = System.getProperty(SYSPROP_SOLR_CIRCUITBREAKER_ERRORCODE);
    if (strCode != null) {
      try {
        intCode = Integer.parseInt(strCode);
      } catch (NumberFormatException nfe) {
        intCode = SolrException.ErrorCode.UNKNOWN.code;
      }
    }
    SolrException.ErrorCode errorCode = SolrException.ErrorCode.getErrorCode(intCode);
    if (errorCode != SolrException.ErrorCode.UNKNOWN) {
      return errorCode;
    } else {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          String.format(
              Locale.ROOT,
              "Invalid error code %s specified for circuit breaker system property %s.",
              strCode,
              SYSPROP_SOLR_CIRCUITBREAKER_ERRORCODE));
    }
  }

  @Override
  public void close() throws IOException {
    // Nothing to do by default
  }

  /**
   * Set the request types for which this circuit breaker should be checked. If not called, the
   * circuit breaker will be checked for the {@link SolrRequestType#QUERY} request type only.
   *
   * @param requestTypes list of strings representing request types
   * @throws IllegalArgumentException if the request type is not valid
   */
  public void setRequestTypes(List<String> requestTypes) {
    this.requestTypes =
        requestTypes.stream()
            .map(t -> SolrRequestType.valueOf(t.toUpperCase(Locale.ROOT)))
            .peek(
                t -> {
                  if (!SUPPORTED_TYPES.contains(t)) {
                    throw new IllegalArgumentException(
                        String.format(
                            Locale.ROOT,
                            "Request type %s is not supported for circuit breakers",
                            t.name()));
                  }
                })
            .collect(Collectors.toSet());
  }

  public void setWarnOnly(boolean warnOnly) {
    this.warnOnly = warnOnly;
  }

  public boolean isWarnOnly() {
    return warnOnly;
  }

  public Set<SolrRequestType> getRequestTypes() {
    return requestTypes;
  }

  public abstract CircuitBreaker setThreshold(double threshold);
}
