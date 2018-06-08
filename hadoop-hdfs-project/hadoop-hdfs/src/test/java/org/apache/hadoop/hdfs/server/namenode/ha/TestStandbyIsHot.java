/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.ha;

import static org.junit.Assert.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.log4j.Level;
import org.junit.Test;

/**
 * The hotornot.com of unit tests: makes sure that the standby not only
 * has namespace information, but also has the correct block reports, etc.
 */
public class TestStandbyIsHot {
  protected static final Log LOG = LogFactory.getLog(
      TestStandbyIsHot.class);
  private static final String TEST_FILE_DATA = "hello highly available world";
  private static final String TEST_FILE = "/testStandbyIsHot";
  private static final Path TEST_FILE_PATH = new Path(TEST_FILE);

  static {
    DFSTestUtil.setNameNodeLogLevel(Level.ALL);
  }

  @Test(timeout=60000)
  public void testStandbyIsHot() throws Exception {
    Configuration conf = new Configuration();
    // We read from the standby to watch block locations
    HAUtil.setAllowStandbyReads(conf, true);
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHATopology())
      .numDataNodes(3)
      .build();
    try {
      cluster.waitActive();
      cluster.transitionToActive(0);
      
      NameNode nn1 = cluster.getNameNode(0);
      NameNode nn2 = cluster.getNameNode(1);
      
      FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);
      
      Thread.sleep(1000);
      System.err.println("==================================");
      DFSTestUtil.writeFile(fs, TEST_FILE_PATH, TEST_FILE_DATA);
      // Have to force an edit log roll so that the standby catches up
      nn1.getRpcServer().rollEditLog();
      System.err.println("==================================");

      // Trigger immediate heartbeats and block reports so
      // that the active "trusts" all of the DNs
      cluster.triggerHeartbeats();
      cluster.triggerBlockReports();

      // Change replication
      LOG.info("Changing replication to 1");
      fs.setReplication(TEST_FILE_PATH, (short)1);

      nn1.getRpcServer().rollEditLog();

      // Change back to 3
      LOG.info("Changing replication to 3");
      fs.setReplication(TEST_FILE_PATH, (short)3);
      BlockManagerTestUtil.computeAllPendingWork(
          nn1.getNamesystem().getBlockManager());
      nn1.getRpcServer().rollEditLog();
      fail("TODO: add assertions");
    } finally {
      cluster.shutdown();
    }
  }
}
