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
package org.apache.solr.handler.component;

import com.codahale.metrics.Counter;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

public class TimeLimitedHttpShardHandlerFactory extends HttpShardHandlerFactory {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private long slowNodeTimeout;
  private boolean dryRun;
  private boolean initialized;

  static final String TIMEOUT_CONFIG_KEY = "slowNodeTimeout"; //config key for timeout in millisec
  static final String DRY_RUN_CONFIG_KEY = "dryRun";

  private SlowNodeDetector slowNodeDetector;
  private static final long SLOW_NODE_TTL = 60000; // 1 minute
  private SolrMetricsContext solrMetricsContext;
  Counter cancelledSlowNodeRequests;

  /**
   * Get {@link ShardHandler} that times out on slow shards.
   * Take note the returned ShardHandler is expected to handle a single batch of identical requests submitted
   * sequentially
   **/
  @Override
  public ShardHandler getShardHandler() {
    if (!initialized) {
      throw new RuntimeException(TimeLimitedHttpShardHandlerFactory.class.getSimpleName() + " is not initialized, run init() first or check if there are any exceptions during init()");
    }
    return new TimeLimitedHttpShardHandler(this, slowNodeTimeout, dryRun, slowNodeDetector, (timedOutTasks) -> cancelledSlowNodeRequests.inc(timedOutTasks.size()));
  }

  /**
   * For test
   * @param slowNodeDetector
   */
  void setSlowNodeDetector(SlowNodeDetector slowNodeDetector) {
    this.slowNodeDetector = slowNodeDetector;
  }

  @Override
  public void init(PluginInfo info) {
    super.init(info);
    NamedList<?> args = info.initArgs;
    Object minWaitObject = args.get(TIMEOUT_CONFIG_KEY);
    if (minWaitObject == null) {
      throw new IllegalArgumentException("Missing required parameter: " + TIMEOUT_CONFIG_KEY + " for " + TimeLimitedHttpShardHandlerFactory.class.getSimpleName() + " in solr config");
    }
    slowNodeTimeout = Long.parseLong(minWaitObject.toString());

    Object dryRunObject = args.get(DRY_RUN_CONFIG_KEY);
    if (dryRunObject != null) {
      dryRun = Boolean.parseBoolean(dryRunObject.toString());
    }
    log.debug("Initialized {} with, timeout {}, and dryRun {}", TimeLimitedHttpShardHandlerFactory.class.getSimpleName(), slowNodeTimeout, dryRun);

    slowNodeDetector = new SlowNodeDetector.Builder().withSlowNodeTtl(SLOW_NODE_TTL).build();

    initialized = true;
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    super.initializeMetrics(parentContext, scope);
    solrMetricsContext = parentContext.getChildContext(this);
    String expandedScope = SolrMetricManager.mkName(scope, SolrInfoBean.Category.QUERY.name());
    cancelledSlowNodeRequests = solrMetricsContext.counter("cancelledSlowNodeRequests", expandedScope);
  }
}
