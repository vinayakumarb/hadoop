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

package org.apache.hadoop.hdfs.server.namenode;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.EnumSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.io.EnumSetWritable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Race between two threads simultaneously calling
 * FSNamesystem.getAdditionalBlock().
 */
public class TestAddBlockRetry {
  public static final Log LOG = LogFactory.getLog(TestAddBlockRetry.class);

  private static final short REPLICATION = 3;

  private Configuration conf;
  private MiniDFSCluster cluster;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf)

      .build();
    cluster.waitActive();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /*
   * Since NameNode will not persist any locations of the block, addBlock()
   * retry call after restart NN should re-select the locations and return to
   * client. refer HDFS-5257
   */
  @Test
  public void testAddBlockRetryShouldReturnBlockWithLocations()
      throws Exception {
    final String src = "/testAddBlockRetryShouldReturnBlockWithLocations";
    NamenodeProtocols nameNodeRpc = cluster.getNameNodeRpc();
    // create file
    nameNodeRpc.create(src, FsPermission.getFileDefault(), "clientName",
        new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE)), true,
        (short) 3, 1024, null, null);
    // start first addBlock()
    LOG.info("Starting first addBlock for " + src);
    LocatedBlock lb1 = nameNodeRpc.addBlock(src, "clientName", null, null,
        HdfsConstants.GRANDFATHER_INODE_ID, null, null);

    cluster.restartNameNode();
    nameNodeRpc = cluster.getNameNodeRpc();
    LocatedBlock lb2 = nameNodeRpc.addBlock(src, "clientName", null, null,
        HdfsConstants.GRANDFATHER_INODE_ID, null, null);
    assertEquals("Blocks are not equal", lb1.getBlock(), lb2.getBlock());
  }
}
