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

import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.PluginInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

public class TimeLimitedHttpShardHandlerFactory extends HttpShardHandlerFactory {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private long minWait;
  private double waitMultiplier;
  private boolean dryRun;
  private boolean initialized;

  static final String MIN_WAIT_CONFIG_KEY = "minWait"; //config key for minimum wait time in millisec - only consider limiting requests that take longer than this
  static final String WAIT_MULTIPLIER_CONFIG_KEY = "waitMultiplier"; //config key for wait time multiplier - poll/wait for this multiplied by longest shard response latency observed so far. Interrupt the pending requests if such timeout is reached
  static final String DRY_RUN_CONFIG_KEY = "dryRun";

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
    return new TimeLimitedHttpShardHandler(this, minWait, waitMultiplier, dryRun);
  }

  @Override
  public void init(PluginInfo info) {
    super.init(info);
    NamedList<?> args = info.initArgs;
    Object minWaitObject = args.get(MIN_WAIT_CONFIG_KEY);
    if (minWaitObject == null) {
      throw new IllegalArgumentException("Missing required parameter: " + MIN_WAIT_CONFIG_KEY + " for " + TimeLimitedHttpShardHandlerFactory.class.getSimpleName() + " in solr config");
    }
    minWait = Long.parseLong(minWaitObject.toString());

    Object waitMultiplierObject = args.get(WAIT_MULTIPLIER_CONFIG_KEY);
    if (waitMultiplierObject == null) {
      throw new IllegalArgumentException("Missing required parameter: " + WAIT_MULTIPLIER_CONFIG_KEY + " for " + TimeLimitedHttpShardHandlerFactory.class.getSimpleName() + " in solr config");
    }
    waitMultiplier = Double.parseDouble(waitMultiplierObject.toString());

    Object dryRunObject = args.get(DRY_RUN_CONFIG_KEY);
    if (dryRunObject != null) {
      dryRun = Boolean.parseBoolean(dryRunObject.toString());
    }
    log.debug("Initialized {} with, minWait {}, waitMultiplier {}, and dryRun {}", TimeLimitedHttpShardHandlerFactory.class.getSimpleName(), minWait, waitMultiplier, dryRun);

    initialized = true;
  }
}
