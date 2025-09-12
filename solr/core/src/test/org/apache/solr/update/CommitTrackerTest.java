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
package org.apache.solr.update;

import java.lang.invoke.MethodHandles;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.util.LogLevel;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@LogLevel("org.apache.solr.update=INFO")
public class CommitTrackerTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Test
  public void testAlignCommitTime() {
    long time1 = CommitTracker.alignCommitMaxTime("A", 60000, 0);
    assertTrue(time1 >= 0 && time1 < 60000 * 2); // at most jitter a whole commitMaxTime
    long time2 = CommitTracker.alignCommitMaxTime("A", 60000, 1000);
    assertTrue(time2 >= 0 && time2 < 60000 * 2); // at most jitter a whole commitMaxTime
    // jitter should be same for same collection now 1sec has past hence adjusted time should be
    // 1sec less (wait for 1 less sec)
    assertEquals(time1 - 1000, time2);
    long time3 = CommitTracker.alignCommitMaxTime("A", 60000, 60000);
    assertEquals(time1, time3); // after 1 commitMaxTime, should be the same wait time

    long time4 = CommitTracker.alignCommitMaxTime("B", 60000, 0);
    assertTrue(time4 >= 0 && time4 < 60000 * 2); // at most jitter a whole commitMaxTime
    assertNotEquals(time1, time4); // different collection different jitter
  }
}
