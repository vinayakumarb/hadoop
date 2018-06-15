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

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.util.LightWeightGSet;

import static org.apache.hadoop.hdfs.server.namenode.INodeId.INVALID_INODE_ID;

/**
 * For a given block (or an erasure coding block group), BlockInfo class
 * maintains 1) the {@link BlockCollection} it is part of, and 2) datanodes
 * where the replicas of the block, or blocks belonging to the erasure coding
 * block group, are stored.
 */
@InterfaceAudience.Private
public class BlockInfo extends Block
    implements LightWeightGSet.LinkedElement {

  public static final BlockInfo[] EMPTY_ARRAY = {};

  /**
   * Replication factor.
   */
  private short replication;

  /**
   * Block collection ID.
   */
  private long bcId;

  /** For implementing {@link LightWeightGSet.LinkedElement} interface. */
  private LightWeightGSet.LinkedElement nextLinkedElement;


  private BlockUnderConstructionFeature uc;

  /**
   * Construct an entry for blocksmap
   * @param size the block's replication factor, or the total number of blocks
   *             in the block group
   */
  public BlockInfo(short size) {
    this.bcId = INVALID_INODE_ID;
    this.replication = size;
  }

  public BlockInfo(Block blk, short size) {
    super(blk);
    this.bcId = INVALID_INODE_ID;
    this.replication = size;
  }

  public short getReplication() {
    return replication;
  }

  public void setReplication(short repl) {
    this.replication = repl;
  }

  public long getBlockCollectionId() {
    return bcId;
  }

  public void setBlockCollectionId(long id) {
    this.bcId = id;
  }

  public void delete() {
    setBlockCollectionId(INVALID_INODE_ID);
  }

  public boolean isDeleted() {
    return bcId == INVALID_INODE_ID;
  }

  @Override
  public int hashCode() {
    // Super implementation is sufficient
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    // Sufficient to rely on super's implementation
    return (this == obj) || super.equals(obj);
  }

  @Override
  public LightWeightGSet.LinkedElement getNext() {
    return nextLinkedElement;
  }

  @Override
  public void setNext(LightWeightGSet.LinkedElement next) {
    this.nextLinkedElement = next;
  }

  /* UnderConstruction Feature related */

  public BlockUnderConstructionFeature getUnderConstructionFeature() {
    return uc;
  }

  public BlockUCState getBlockUCState() {
    return uc == null ? BlockUCState.COMPLETE : uc.getBlockUCState();
  }

  /**
   * Is this block complete?
   *
   * @return true if the state of the block is {@link BlockUCState#COMPLETE}
   */
  public boolean isComplete() {
    return getBlockUCState().equals(BlockUCState.COMPLETE);
  }

  public final boolean isCompleteOrCommitted() {
    final BlockUCState state = getBlockUCState();
    return state.equals(BlockUCState.COMPLETE) ||
        state.equals(BlockUCState.COMMITTED);
  }

  /**
   * Add/Update the under construction feature.
   */
  public void convertToBlockUnderConstruction(BlockUCState s) {
    if (isComplete()) {
      uc = new BlockUnderConstructionFeature(this, s);
    } else {
      // the block is already under construction
      uc.setBlockUCState(s);
    }
  }

  /**
   * Convert an under construction block to complete.
   */
  void convertToCompleteBlock() {
    assert getBlockUCState() != BlockUCState.COMPLETE :
        "Trying to convert a COMPLETE block";
    uc = null;
  }

  /**
   * Commit block's length and generation stamp as reported by the client.
   * Set block state to {@link BlockUCState#COMMITTED}.
   * @param block - contains client reported block length and generation
   * @return staleReplica's List.
   * @throws IOException if block ids are inconsistent.
   */
  void commitBlock(Block block) throws IOException {
    if (getBlockId() != block.getBlockId()) {
      throw new IOException("Trying to commit inconsistent block: id = "
          + block.getBlockId() + ", expected id = " + getBlockId());
    }
    Preconditions.checkState(!isComplete());
    uc.commit();
    this.setNumBytes(block.getNumBytes());
  }
}
