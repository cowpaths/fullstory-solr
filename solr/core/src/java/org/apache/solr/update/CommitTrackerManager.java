package org.apache.solr.update;

import java.util.Map;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.ZkStateReader;

public class CommitTrackerManager {
  public static final String ALIGN_COMMIT_TIME =
      ClusterProperties.EXT_PROPRTTY_PREFIX + "alignCommitTime";
  private static volatile Boolean alignCommitTimeOverride;

  private CommitTrackerManager() {}

  public static void init(ZkStateReader zkStateReader) {
    zkStateReader.registerClusterPropertiesListener(
        (Map<String, Object> properties) -> {
          alignCommitTimeOverride = (Boolean) properties.get(ALIGN_COMMIT_TIME);
          return false;
        });
  }

  public static Boolean getAlignCommitTimeOverride() {
    return alignCommitTimeOverride;
  }
}
