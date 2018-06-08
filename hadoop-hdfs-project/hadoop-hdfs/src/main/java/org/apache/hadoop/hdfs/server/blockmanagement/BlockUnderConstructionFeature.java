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

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState.COMPLETE;

/**
 * Represents the under construction feature of a Block.
 * This is usually the last block of a file opened for write or append.
 */
public class BlockUnderConstructionFeature {
  private BlockUCState blockUCState;

  /**
   * The new generation stamp, which this block will have
   * after the recovery succeeds. Also used as a recovery id to identify
   * the right recovery if any of the abandoned recoveries re-appear.
   */
  private long blockRecoveryId = 0;

  /**
   * The block source to use in the event of copy-on-write truncate.
   */
  private BlockInfo truncateBlock;

  public BlockUnderConstructionFeature(Block blk, BlockUCState state) {
    assert getBlockUCState() != COMPLETE :
        "BlockUnderConstructionFeature cannot be in COMPLETE state";
    this.blockUCState = state;
  }

  /**
   * Return the state of the block under construction.
   * @see BlockUCState
   */
  public BlockUCState getBlockUCState() {
    return blockUCState;
  }

  void setBlockUCState(BlockUCState s) {
    blockUCState = s;
  }

  public long getBlockRecoveryId() {
    return blockRecoveryId;
  }

  /** Get recover block */
  public BlockInfo getTruncateBlock() {
    return truncateBlock;
  }

  public void setTruncateBlock(BlockInfo recoveryBlock) {
    this.truncateBlock = recoveryBlock;
  }

  /**
   * Set {@link #blockUCState} to {@link BlockUCState#COMMITTED}.
   */
  void commit() {
    blockUCState = BlockUCState.COMMITTED;
  }

  /**
   * Initialize lease recovery for this block.
   * Find the first alive data-node starting from the previous primary and
   * make it primary.
   * @param blockInfo Block to be recovered
   * @param recoveryId Recovery ID (new gen stamp)
   * @param startRecovery Issue recovery command to datanode if true.
   */
  public void initializeBlockRecovery(BlockInfo blockInfo, long recoveryId,
      boolean startRecovery) {
    setBlockUCState(BlockUCState.UNDER_RECOVERY);
    blockRecoveryId = recoveryId;
    if (!startRecovery) {
      return;
    }
  }

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder(100);
    appendUCParts(b);
    return b.toString();
  }

  private void appendUCParts(StringBuilder sb) {
    sb.append("{UCState=").append(blockUCState)
      .append(", truncateBlock=").append(truncateBlock)
      .append("}");
  }
}
