package org.apache.solr.update;

import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.ZkStateReader;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class CommitTrackerManager {
  public static final String ADJUST_COMMIT_TIME =
          ClusterProperties.EXT_PROPRTTY_PREFIX + "adjustCommitTime";
  private static final boolean DEFAULT_ADJUST_COMMIT_TIME = true; //adjust by default
  private static final AtomicBoolean adjustCommitTime = new AtomicBoolean(DEFAULT_ADJUST_COMMIT_TIME);
  private CommitTrackerManager() {
  }
  public static void init(ZkStateReader zkStateReader) {
    zkStateReader.registerClusterPropertiesListener((Map<String, Object> properties) -> {
      Boolean val = (Boolean) properties.get(ADJUST_COMMIT_TIME);
      adjustCommitTime.set(Objects.requireNonNullElse(val, DEFAULT_ADJUST_COMMIT_TIME));
      return false;
    });
  }

  public static boolean isAdjustCommitTime() {
    return adjustCommitTime.get();
  }

}
