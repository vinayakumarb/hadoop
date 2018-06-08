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
package org.apache.hadoop.hdfs.server.blockmanagement;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.hdfs.server.namenode.ha.HAContext;
import org.apache.hadoop.hdfs.server.namenode.ha.HAState;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestBlockManager {
  /**
   * Some of these tests exercise code which has some randomness involved -
   * ie even if there's a bug, they may pass because the random node selection
   * chooses the correct result.
   * 
   * Since they're true unit tests and run quickly, we loop them a number
   * of times trying to trigger the incorrect behavior.
   */
  private static final int NUM_TEST_ITERS = 30;
  private static final int BLOCK_SIZE = 64*1024;
  private static final Log LOG = LogFactory.getLog(TestBlockManager.class);

  private FSNamesystem fsn;
  private BlockManager bm;
  private long mockINodeId;


  @Before
  public void setupMockCluster() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY,
             "need to set a dummy value here so it assumes a multi-rack cluster");
    fsn = Mockito.mock(FSNamesystem.class);
    Mockito.doReturn(true).when(fsn).hasWriteLock();
    Mockito.doReturn(true).when(fsn).hasReadLock();
    Mockito.doReturn(true).when(fsn).isRunning();
    //Make shouldPopulaeReplQueues return true
    HAContext haContext = Mockito.mock(HAContext.class);
    HAState haState = Mockito.mock(HAState.class);
    Mockito.when(haContext.getState()).thenReturn(haState);
    Mockito.when(haState.shouldPopulateReplQueues()).thenReturn(true);
    Mockito.when(fsn.getHAContext()).thenReturn(haContext);
    bm = new BlockManager(fsn, false, conf);
    mockINodeId = INodeId.ROOT_INODE_ID + 1;
  }

  @Test
  public void testLegacyBlockInInvalidateBlocks() {
    final long legancyGenerationStampLimit = 10000;
    BlockIdManager bim = Mockito.mock(BlockIdManager.class);

    when(bim.getLegacyGenerationStampLimit())
        .thenReturn(legancyGenerationStampLimit);
    when(bim.isLegacyBlock(any(Block.class))).thenCallRealMethod();

    InvalidateBlocks ibs = new InvalidateBlocks(100, 30000, bim);

    Block legacy = new Block(-1, 10, legancyGenerationStampLimit / 10);
    ibs.add(legacy, false);
    assertEquals(1, ibs.getBlocks());
  }
}
