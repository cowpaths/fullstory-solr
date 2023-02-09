package org.apache.solr.cloud;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.NodeRoles;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.BeforeClass;
import org.junit.Test;

public class SplitShardWithNodeRoleTest extends SolrCloudTestCase {
  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2).addConfig("conf", configset("cloud-minimal")).configure();
    System.setProperty(NodeRoles.NODE_ROLES_PROP, "data:off,coordinator:on");
    JettySolrRunner coordinator1 = null;

    try {
      coordinator1 = cluster.startJettySolrRunner();
      coordinator1 = cluster.startJettySolrRunner();
    } finally {
      System.clearProperty(NodeRoles.NODE_ROLES_PROP);
    }

    JettySolrRunner overseer1 = null;
    JettySolrRunner overseer2 = null;
    System.setProperty(NodeRoles.NODE_ROLES_PROP, "data:off,overseer:preferred,coordinator:off");
    try {
      overseer1 = cluster.startJettySolrRunner();
      overseer2 = cluster.startJettySolrRunner();
    } finally {
      System.clearProperty(NodeRoles.NODE_ROLES_PROP);
    }

    Thread.sleep(10000);
    String overseerLeader = coordinator1.getCoreContainer().getZkController().getOverseerLeader();
    String msg =
        String.format(
            "Overseer leader should be from overseer %d or %d  node but %s",
            overseer1.getLocalPort(), overseer2.getLocalPort(), overseerLeader);
    assertTrue(
        msg,
        overseerLeader.contains(String.valueOf(overseer1.getLocalPort()))
            || overseerLeader.contains(String.valueOf(overseer2.getLocalPort())));
  }

  @Test
  public void testSolrClusterWithNodeRoleWithSingleReplica() throws Exception {
    doSplit("coll_NO_HA", 1, 1, 0);
  }

  @Test
  public void testSolrClusterWithNodeRoleWithHA() throws Exception {
    doSplit("coll_HA", 1, 1, 1);
  }

  public void doSplit(String collName, int shard, int nrtReplica, int pullReplica)
      throws Exception {
    CloudSolrClient client = cluster.getSolrClient();
    CollectionAdminRequest.createCollection(collName, "conf", shard, nrtReplica, 0, pullReplica)
        .setPerReplicaState(true)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collName, shard, nrtReplica + pullReplica);
    UpdateRequest ur = new UpdateRequest();
    for (int i = 0; i < 10; i++) {
      SolrInputDocument doc2 = new SolrInputDocument();
      doc2.addField("id", "" + i);
      ur.add(doc2);
    }

    ur.commit(client, collName);

    CollectionAdminRequest.SplitShard splitShard =
        CollectionAdminRequest.splitShard(collName).setShardName("shard1");
    splitShard.process(cluster.getSolrClient());
    waitForState(
        "Timed out waiting for sub shards to be active. Number of active shards="
            + cluster
                .getSolrClient()
                .getClusterState()
                .getCollection(collName)
                .getActiveSlices()
                .size(),
        collName,
        activeClusterShape(shard + 1, 3 * (nrtReplica + pullReplica)));
  }
}
