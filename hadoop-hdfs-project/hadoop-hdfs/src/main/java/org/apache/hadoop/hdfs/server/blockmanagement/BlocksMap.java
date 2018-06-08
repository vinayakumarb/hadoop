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

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.LightWeightGSet;

/**
 * This class maintains the map from a block to its metadata.
 * block's metadata currently includes blockCollection it belongs to and
 * the datanodes that store the block.
 */
class BlocksMap {

  /** Constant {@link LightWeightGSet} capacity. */
  private final int capacity;
  
  private GSet<Block, BlockInfo> blocks;

  private final LongAdder totalReplicatedBlocks = new LongAdder();

  BlocksMap(int capacity) {
    // Use 2% of total memory to size the GSet capacity
    this.capacity = capacity;
    this.blocks = new LightWeightGSet<Block, BlockInfo>(capacity) {
      @Override
      public Iterator<BlockInfo> iterator() {
        SetIterator iterator = new SetIterator();
        /*
         * Not tracking any modifications to set. As this set will be used
         * always under FSNameSystem lock, modifications will not cause any
         * ConcurrentModificationExceptions. But there is a chance of missing
         * newly added elements during iteration.
         */
        iterator.setTrackModification(false);
        return iterator;
      }
    };
  }


  void close() {
    clear();
    blocks = null;
  }
  
  void clear() {
    if (blocks != null) {
      blocks.clear();
      totalReplicatedBlocks.reset();
    }
  }

  /**
   * Add block b belonging to the specified block collection to the map.
   */
  BlockInfo addBlockCollection(BlockInfo b, BlockCollection bc) {
    BlockInfo info = blocks.get(b);
    if (info != b) {
      info = b;
      blocks.put(info);
      incrementBlockStat(info);
    }
    info.setBlockCollectionId(bc.getId());
    return info;
  }

  /**
   * Remove the block from the block map;
   * remove it from all data-node lists it belongs to;
   * and remove all data-node locations associated with the block.
   */
  void removeBlock(BlockInfo block) {
    BlockInfo blockInfo = blocks.remove(block);
    if (blockInfo == null) {
      return;
    }
    decrementBlockStat(block);
  }

  /**
   * Check if BlocksMap contains the block.
   *
   * @param b Block to check
   * @return true if block is in the map, otherwise false
   */
  boolean containsBlock(Block b) {
    return blocks.contains(b);
  }

  /** Returns the block object if it exists in the map. */
  BlockInfo getStoredBlock(Block b) {
    return blocks.get(b);
  }

  int size() {
    if (blocks != null) {
      return blocks.size();
    } else {
      return 0;
    }
  }

  Iterable<BlockInfo> getBlocks() {
    return blocks;
  }
  
  /** Get the capacity of the HashMap that stores blocks */
  int getCapacity() {
    return capacity;
  }

  private void incrementBlockStat(BlockInfo block) {
    totalReplicatedBlocks.increment();
  }

  private void decrementBlockStat(BlockInfo block) {
    totalReplicatedBlocks.decrement();
    assert totalReplicatedBlocks.longValue() >= 0 :
        "Total number of replicated blocks should be non-negative";
  }
}
