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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;

import java.io.IOException;

/**
 * BlockIdManager allocates the generation stamps and the block ID. The
 * {@link FSNamesystem} is responsible for persisting the allocations in the
 * {@link FSEditLog}.
 */
public class BlockIdManager {
  /**
   * The global generation stamp for legacy blocks with randomly
   * generated block IDs.
   */
  private final GenerationStamp legacyGenerationStamp = new GenerationStamp();
  /**
   * The global generation stamp for this file system.
   */
  private final GenerationStamp generationStamp = new GenerationStamp();
  /**
   * The value of the generation stamp when the first switch to sequential
   * block IDs was made. Blocks with generation stamps below this value
   * have randomly allocated block IDs. Blocks with generation stamps above
   * this value had sequentially allocated block IDs. Read from the fsImage
   * (or initialized as an offset from the V1 (legacy) generation stamp on
   * upgrade).
   */
  private long legacyGenerationStampLimit;
  /**
   * The global block ID space for this file system.
   */
  private final SequentialBlockIdGenerator blockIdGenerator;

  public BlockIdManager(BlockManager blockManager) {
    this.legacyGenerationStampLimit =
        HdfsConstants.GRANDFATHER_GENERATION_STAMP;
    this.blockIdGenerator = new SequentialBlockIdGenerator(blockManager);
  }

  /**
   * Upgrades the generation stamp for the filesystem
   * by reserving a sufficient range for all existing blocks.
   * Should be invoked only during the first upgrade to
   * sequential block IDs.
   */
  public long upgradeLegacyGenerationStamp() {
    Preconditions.checkState(generationStamp.getCurrentValue() ==
      GenerationStamp.LAST_RESERVED_STAMP);
    generationStamp.skipTo(legacyGenerationStamp.getCurrentValue() +
      HdfsServerConstants.RESERVED_LEGACY_GENERATION_STAMPS);

    legacyGenerationStampLimit = generationStamp.getCurrentValue();
    return generationStamp.getCurrentValue();
  }

  /**
   * Sets the generation stamp that delineates random and sequentially
   * allocated block IDs.
   *
   * @param stamp set generation stamp limit to this value
   */
  public void setLegacyGenerationStampLimit(long stamp) {
    Preconditions.checkState(legacyGenerationStampLimit ==
        HdfsConstants.GRANDFATHER_GENERATION_STAMP);
    legacyGenerationStampLimit = stamp;
  }

  /**
   * Gets the value of the generation stamp that delineates sequential
   * and random block IDs.
   */
  public long getGenerationStampAtblockIdSwitch() {
    return legacyGenerationStampLimit;
  }

  @VisibleForTesting
  SequentialBlockIdGenerator getBlockIdGenerator() {
    return blockIdGenerator;
  }

  /**
   * Sets the maximum allocated contiguous block ID for this filesystem. This is
   * the basis for allocating new block IDs.
   * @param blockId
   */
  public void setLastAllocatedBlockId(byte[] blockId) {
    blockIdGenerator.skipTo(0L);//TODO : Fix skipping
  }

  /**
   * Gets the maximum sequentially allocated contiguous block ID for this
   * filesystem
   */
  public long getLastAllocatedBlockId() {
    return blockIdGenerator.getCurrentValue();
  }

  /**
   * Sets the current generation stamp for legacy blocks
   */
  public void setLegacyGenerationStamp(long stamp) {
    legacyGenerationStamp.setCurrentValue(stamp);
  }

  /**
   * Gets the current generation stamp for legacy blocks
   */
  public long getLegacyGenerationStamp() {
    return legacyGenerationStamp.getCurrentValue();
  }

  /**
   * Gets the current generation stamp for this filesystem
   */
  public void setGenerationStamp(long stamp) {
    generationStamp.setCurrentValue(stamp);
  }

  public long getGenerationStamp() {
    return generationStamp.getCurrentValue();
  }

  public long getLegacyGenerationStampLimit() {
    return legacyGenerationStampLimit;
  }

  /**
   * Increments, logs and then returns the block ID
   */
  byte[] nextBlockId() {
    //Convert long to byte[]
    return Block.generateBlockId(blockIdGenerator.nextValue());
  }

  void clear() {
    legacyGenerationStamp.setCurrentValue(GenerationStamp.LAST_RESERVED_STAMP);
    generationStamp.setCurrentValue(GenerationStamp.LAST_RESERVED_STAMP);
    getBlockIdGenerator().setCurrentValue(SequentialBlockIdGenerator
      .LAST_RESERVED_BLOCK_ID);
    legacyGenerationStampLimit = HdfsConstants.GRANDFATHER_GENERATION_STAMP;
  }
}
