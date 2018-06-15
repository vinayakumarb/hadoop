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
import static org.junit.Assert.fail;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.junit.Test;

/**
 * This class provides tests for {@link BlockUnderConstructionFeature} class
 */
public class TestBlockUnderConstructionFeature {
  @Test
  public void testInitializeBlockRecovery() throws Exception {
    BlockInfo blockInfo = new BlockInfo(
        new Block(Block.EMPTY_BLOCK_ID, 0, GenerationStamp.LAST_RESERVED_STAMP),
        (short) 3);
    blockInfo.convertToBlockUnderConstruction(BlockUCState.UNDER_CONSTRUCTION);

    // Recovery attempt #1.
    blockInfo.getUnderConstructionFeature().initializeBlockRecovery(blockInfo,
        true);
    fail("TODO: Assertion not implemented yet!!");
  }
}
