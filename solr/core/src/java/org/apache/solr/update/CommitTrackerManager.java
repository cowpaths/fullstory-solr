package org.apache.solr.update;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.ZkStateReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * De-couple the commit tracker config management logic from CommitTracker instance. <br>
 * This only tracks and manage the ext.alignCommitTime override property for now.
 */
public class CommitTrackerManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String ALIGN_COMMIT_TIME_KEY =
      ClusterProperties.EXT_PROPRTTY_PREFIX + "alignCommitTime";
  public static final String SOFT_COMMIT_MAX_TIME_KEY =
          ClusterProperties.EXT_PROPRTTY_PREFIX + "softAutoCommit.maxTime";
  public static final String HARD_COMMIT_MAX_TIME_KEY =
          ClusterProperties.EXT_PROPRTTY_PREFIX + "autoCommit.maxTime";
  private static volatile Boolean alignCommitTimeOverride;
  private static volatile Long hardCommitMaxTimeOverride;
  private static volatile Long softCommitMaxTimeOverride;

  private CommitTrackerManager() {}

  public static void init(ZkStateReader zkStateReader) {
    zkStateReader.registerClusterPropertiesListener(
        (Map<String, Object> properties) -> {
          alignCommitTimeOverride = (Boolean) properties.get(ALIGN_COMMIT_TIME_KEY);
          log.info(
              "{} change detected serving alignCommitTimeOverride: {}",
              ALIGN_COMMIT_TIME_KEY,
              alignCommitTimeOverride);

          hardCommitMaxTimeOverride = (Long) properties.get(HARD_COMMIT_MAX_TIME_KEY);
          log.info(
                  "{} change detected serving hardCommitMaxTimeOverride: {}",
                  HARD_COMMIT_MAX_TIME_KEY,
                  hardCommitMaxTimeOverride);

          softCommitMaxTimeOverride = (Long) properties.get(SOFT_COMMIT_MAX_TIME_KEY);
          log.info(
                  "{} change detected serving softCommitMaxTimeOverride: {}",
                  SOFT_COMMIT_MAX_TIME_KEY,
                  softCommitMaxTimeOverride);
          return false;
        });
  }

  public static Boolean getAlignCommitTimeOverride() {
    return alignCommitTimeOverride;
  }
  public static Long getHardCommitMaxTimeOverride() {
    return hardCommitMaxTimeOverride;
  }
  public static Long getSoftCommitMaxTimeOverride() {
    return softCommitMaxTimeOverride;
  }
}
