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
package org.apache.hadoop.hdfs.protocol;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Identifies a Block uniquely across the block pools
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ExtendedBlock {
  private String poolId;
  private Block block;

  public ExtendedBlock() {
    this(null, Block.EMPTY_BLOCK_ID, 0);
  }

  public ExtendedBlock(final ExtendedBlock b) {
    this(b.poolId, new Block(b.block));
  }

  public ExtendedBlock(final String poolId, final byte[] blockId) {
    this(poolId, blockId, 0);
  }

  public ExtendedBlock(String poolId, Block b) {
    this.poolId = poolId != null ? poolId.intern() : null;
    this.block = b;
  }

  public ExtendedBlock(final String poolId, final byte[] blkid, final long len) {
    this.poolId = poolId != null ? poolId.intern() : null;
    block = new Block(blkid, len, 0);
  }

  public String getBlockPoolId() {
    return poolId;
  }

  public long getNumBytes() {
    return block.getNumBytes();
  }

  public byte[] getBlockId() {
    return block.getBlockId();
  }

  public void setBlockId(final byte[] bid) {
    block.setBlockId(bid);
  }

  public void setNumBytes(final long len) {
    block.setNumBytes(len);
  }

  public void set(String poolId, Block blk) {
    this.poolId = poolId != null ? poolId.intern() : null;
    this.block = blk;
  }

  public static Block getLocalBlock(final ExtendedBlock b) {
    return b == null ? null : b.getLocalBlock();
  }

  public Block getLocalBlock() {
    return block;
  }

  @Override // Object
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ExtendedBlock)) {
      return false;
    }
    ExtendedBlock b = (ExtendedBlock)o;
    return b.block.equals(block) &&
        (b.poolId != null ? b.poolId.equals(poolId) : poolId == null);
  }

  @Override // Object
  public int hashCode() {
    return new HashCodeBuilder(31, 17).
        append(poolId).
        append(block).
        toHashCode();
  }

  @Override // Object
  public String toString() {
    return poolId + ":" + block;
  }
}
