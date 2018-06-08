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

import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdfs.DFSUtil;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;

/**
 * Keeps a Collection for every named machine containing blocks
 * that have recently been invalidated and are thought to live
 * on the machine in question.
 */
@InterfaceAudience.Private
class InvalidateBlocks {
  private LightWeightHashSet<Block> blocks = new LightWeightHashSet<>();
  private final LongAdder numBlocks = new LongAdder();
  private final int blockInvalidateLimit;
  private final BlockIdManager blockIdManager;

  /**
   * The period of pending time for block invalidation since the NameNode
   * startup
   */
  private final long pendingPeriodInMs;
  /** the startup time */
  private final long startupTime = Time.monotonicNow();

  InvalidateBlocks(final int blockInvalidateLimit, long pendingPeriodInMs,
                   final BlockIdManager blockIdManager) {
    this.blockInvalidateLimit = blockInvalidateLimit;
    this.pendingPeriodInMs = pendingPeriodInMs;
    this.blockIdManager = blockIdManager;
    printBlockDeletionTime(BlockManager.LOG);
  }

  private void printBlockDeletionTime(final Logger log) {
    log.info("{} is set to {}",
        DFSConfigKeys.DFS_NAMENODE_STARTUP_DELAY_BLOCK_DELETION_SEC_KEY,
        DFSUtil.durationToString(pendingPeriodInMs));
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy MMM dd HH:mm:ss");
    Calendar calendar = new GregorianCalendar();
    calendar.add(Calendar.SECOND, (int) (this.pendingPeriodInMs / 1000));
    log.info("The block deletion will start around {}",
        sdf.format(calendar.getTime()));
  }

  /**
   * @return The total number of blocks to be invalidated.
   */
  long numBlocks() {
    return getBlocks();
  }

  /**
   * @return The total number of blocks of type
   * {@link org.apache.hadoop.hdfs.protocol.BlockType#CONTIGUOUS}
   * to be invalidated.
   */
  long getBlocks() {
    return numBlocks.longValue();
  }

  /**
   * @return true if the given storage has the given block listed for
   * invalidation. Blocks are compared including their generation stamps:
   * if a block is pending invalidation but with a different generation stamp,
   * returns false.
   */
  synchronized boolean contains(final Block block) {
    Block blockInSet = blocks.getElement(block);
    return blockInSet != null &&
        block.getGenerationStamp() == blockInSet.getGenerationStamp();
  }

  /**
   * Add a block to the block collection which will be
   * invalidated on the specified datanode.
   */
  synchronized void add(final Block block, final boolean log) {
    if (blocks.add(block)) {
      numBlocks.increment();
      if (log) {
        NameNode.blockStateChangeLog.debug("BLOCK* {}: add {}",
            getClass().getSimpleName(), block);
      }
    }
  }

  /** Remove the block from the specified storage. */
  synchronized void remove(final Block block) {
    if (blocks.remove(block)) {
      numBlocks.decrement();
    }
  }

  private void dumpBlockSet(final PrintWriter out) {
    if (blocks != null && blocks.size() > 0) {
      out.println(StringUtils.join(',', blocks));
    }
  }
  /** Print the contents to out. */
  synchronized void dump(final PrintWriter out) {
    out.println("Metasave: Blocks " + numBlocks()
        + " waiting deletion");
    dumpBlockSet(out);
  }

  /**
   * @return the remianing pending time
   */
  @VisibleForTesting
  long getInvalidationDelay() {
    return pendingPeriodInMs - (Time.monotonicNow() - startupTime);
  }

  synchronized void clear() {
    blocks.clear();
    numBlocks.reset();
  }
}
