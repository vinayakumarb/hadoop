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
import java.io.PrintWriter;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import javax.management.ObjectName;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier.AccessMode;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.INodesInPath;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.namenode.ha.HAContext;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.util.LightWeightGSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Keeps information related to the blocks stored in the Hadoop cluster.
 * For block state management, it tries to maintain the  safety
 * property of "# of live replicas == # of expected redundancy" under
 * any events such as decommission, namenode failover, datanode failure.
 *
 * The motivation of maintenance mode is to allow admins quickly repair nodes
 * without paying the cost of decommission. Thus with maintenance mode,
 * # of live replicas doesn't have to be equal to # of expected redundancy.
 * If any of the replica is in maintenance mode, the safety property
 * is extended as follows. These property still apply for the case of zero
 * maintenance replicas, thus we can use these safe property for all scenarios.
 * a. # of live replicas >= # of min replication for maintenance.
 * b. # of live replicas <= # of expected redundancy.
 * c. # of live replicas and maintenance replicas >= # of expected redundancy.
 *
 * For regular replication, # of min live replicas for maintenance is determined
 * by DFS_NAMENODE_MAINTENANCE_REPLICATION_MIN_KEY. This number has to <=
 * DFS_NAMENODE_REPLICATION_MIN_KEY.
 * For erasure encoding, # of min live replicas for maintenance is
 * BlockInfoStriped#getRealDataBlockNum.
 *
 * Another safety property is to satisfy the block placement policy. While the
 * policy is configurable, the replicas the policy is applied to are the live
 * replicas + maintenance replicas.
 */
@InterfaceAudience.Private
public class BlockManager implements BlockStatsMXBean {

  public static final Logger LOG = LoggerFactory.getLogger(BlockManager.class);
  public static final Logger blockLog = NameNode.blockStateChangeLog;

  private static final String QUEUE_REASON_CORRUPT_STATE =
    "it has the wrong state or generation stamp";

  private static final String QUEUE_REASON_FUTURE_GENSTAMP =
    "generation stamp is in the future";

  private static final long BLOCK_RECOVERY_TIMEOUT_MULTIPLIER = 30;

  private final Namesystem namesystem;

  // Block pool ID used by this namenode
  private String blockPoolId;

  /** flag indicating whether replication queues have been initialized */
  private boolean initializedReplQueues;

  private final long startupDelayBlockDeletionInMs;
  private ObjectName mxBeanName;
  /** Used by metrics */
  public long getPendingDeletionBlocksCount() {
    return invalidateBlocks.numBlocks();
  }
  /** Used by metrics */
  public long getStartupDelayBlockDeletionInMs() {
    return startupDelayBlockDeletionInMs;
  }

  /**
   * Mapping: Block -> { BlockCollection, datanodes, self ref }
   * Updated only in response to client-sent information.
   */
  final BlocksMap blocksMap;

  /**
   * Blocks to be invalidated.
   * For a striped block to invalidate, we should track its individual internal
   * blocks.
   */
  private final InvalidateBlocks invalidateBlocks;
  
  /** Stores information about block recovery attempts. */
  private final PendingRecoveryBlocks pendingRecoveryBlocks;

  /** The maximum number of replicas allowed for a block */
  public final short maxReplication;
  /** Minimum copies needed or else write is disallowed */
  public final short minReplication;
  /** Default number of replicas */
  public final int defaultReplication;
  final float blocksInvalidateWorkPct;
  private final BlockStoragePolicySuite storagePolicySuite;

  /** Check whether name system is running before terminating */
  private boolean checkNSRunning = true;

  private final BlockIdManager blockIdManager;

  public BlockManager(final Namesystem namesystem, boolean haEnabled,
      final Configuration conf) throws IOException {
    this.namesystem = namesystem;
    this.blockIdManager = new BlockIdManager(this);
    startupDelayBlockDeletionInMs = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_STARTUP_DELAY_BLOCK_DELETION_SEC_KEY,
        DFSConfigKeys.DFS_NAMENODE_STARTUP_DELAY_BLOCK_DELETION_SEC_DEFAULT) * 1000L;
    int blockInvalidateLimit = conf
        .getInt(DFSConfigKeys.DFS_BLOCK_INVALIDATE_LIMIT_KEY,
            DFSConfigKeys.DFS_BLOCK_INVALIDATE_LIMIT_DEFAULT);
    invalidateBlocks = new InvalidateBlocks(
        blockInvalidateLimit,
        startupDelayBlockDeletionInMs,
        blockIdManager);

    // Compute the map capacity by allocating 2% of total memory
    blocksMap = new BlocksMap(
        LightWeightGSet.computeCapacity(2.0, "BlocksMap"));
    storagePolicySuite = BlockStoragePolicySuite.createDefaultSuite();

    this.defaultReplication = conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY,
        DFSConfigKeys.DFS_REPLICATION_DEFAULT);

    final int maxR = conf.getInt(DFSConfigKeys.DFS_REPLICATION_MAX_KEY,
        DFSConfigKeys.DFS_REPLICATION_MAX_DEFAULT);
    final int minR = conf.getInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY,
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_DEFAULT);
    if (minR <= 0)
      throw new IOException("Unexpected configuration parameters: "
          + DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY
          + " = " + minR + " <= 0");
    if (maxR > Short.MAX_VALUE)
      throw new IOException("Unexpected configuration parameters: "
          + DFSConfigKeys.DFS_REPLICATION_MAX_KEY
          + " = " + maxR + " > " + Short.MAX_VALUE);
    if (minR > maxR)
      throw new IOException("Unexpected configuration parameters: "
          + DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY
          + " = " + minR + " > "
          + DFSConfigKeys.DFS_REPLICATION_MAX_KEY
          + " = " + maxR);
    this.minReplication = (short)minR;
    this.maxReplication = (short)maxR;
    this.blocksInvalidateWorkPct = DFSUtil.getInvalidateWorkPctPerIteration(conf);

    long heartbeatIntervalSecs = conf.getTimeDuration(
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT, TimeUnit.SECONDS);
    long blockRecoveryTimeout = getBlockRecoveryTimeout(heartbeatIntervalSecs);
    pendingRecoveryBlocks = new PendingRecoveryBlocks(blockRecoveryTimeout);

    LOG.info("defaultReplication         = {}", defaultReplication);
    LOG.info("maxReplication             = {}", maxReplication);
    LOG.info("minReplication             = {}", minReplication);
  }

  public BlockStoragePolicy getStoragePolicy(final String policyName) {
    return storagePolicySuite.getPolicy(policyName);
  }

  public BlockStoragePolicy getStoragePolicy(final byte policyId) {
    return storagePolicySuite.getPolicy(policyId);
  }

  public BlockStoragePolicy[] getStoragePolicies() {
    return storagePolicySuite.getAllPolicies();
  }

  public void setBlockPoolId(String blockPoolId) {
    this.blockPoolId = blockPoolId;
  }

  public String getBlockPoolId() {
    return blockPoolId;
  }

  public BlockStoragePolicySuite getStoragePolicySuite() {
    return storagePolicySuite;
  }

  public void activate(Configuration conf, long blockTotal) {
    mxBeanName = MBeans.register("NameNode", "BlockStats", this);
  }

  public void close() {
    blocksMap.close();
  }

  /** Dump meta data to out. */
  public void metaSave(PrintWriter out) {
    // Dump blocks that are waiting to be deleted
    invalidateBlocks.dump(out);
  }

  public short getMinReplication() {
    return minReplication;
  }

  public short getMinStorageNum(BlockInfo block) {
    return minReplication;
  }

  /**
   * Commit a block of a file
   * 
   * @param block block to be committed
   * @param commitBlock - contains client reported block length and generation
   * @return true if the block is changed to committed state.
   * @throws IOException if the block does not have at least a minimal number
   * of replicas reported from data-nodes.
   */
  private boolean commitBlock(final BlockInfo block,
      final Block commitBlock) throws IOException {
    if (block.getBlockUCState() == BlockUCState.COMMITTED)
      return false;
    assert block.getNumBytes() <= commitBlock.getNumBytes() :
        "commitBlock length is less than the stored one "
            + commitBlock.getNumBytes() + " vs. " + block.getNumBytes();
    if(block.getGenerationStamp() != commitBlock.getGenerationStamp()) {
      throw new IOException("Commit block with mismatching GS. NN has " +
          block + ", client submits " + commitBlock);
    }
    block.commitBlock(commitBlock);
    return true;
  }
  
  /**
   * Commit the last block of the file and mark it as complete if it has
   * meets the minimum redundancy requirement
   * 
   * @param bc block collection
   * @param commitBlock - contains client reported block length and generation
   * @param iip - INodes in path to bc
   * @return true if the last block is changed to committed state.
   * @throws IOException if the block does not have at least a minimal number
   * of replicas reported from data-nodes.
   */
  public boolean commitOrCompleteLastBlock(BlockCollection bc,
      Block commitBlock, INodesInPath iip) throws IOException {
    if(commitBlock == null)
      return false; // not committing, this is a block allocation retry
    BlockInfo lastBlock = bc.getLastBlock();
    if(lastBlock == null)
      return false; // no blocks in file yet
    if(lastBlock.isComplete())
      return false; // already completed (e.g. by syncBlock)
    
    final boolean committed = commitBlock(lastBlock, commitBlock);

    if (committed) {
      completeBlock(lastBlock, iip, false);
    } else if (pendingRecoveryBlocks.isUnderRecovery(lastBlock)) {
      // We've just finished recovery for this block, complete
      // the block forcibly disregarding number of replicas.
      // This is to ignore minReplication, the block will be closed
      // and then replicated out.
      completeBlock(lastBlock, iip, true);
    }
    return committed;
  }

  /**
   * Convert a specified block of the file to a complete block.
   * @param curBlock - block to be completed
   * @param iip - INodes in path to file containing curBlock; if null,
   *              this will be resolved internally
   * @param force - force completion of the block
   * @throws IOException if the block does not have at least a minimal number
   * of replicas reported from data-nodes.
   */
  private void completeBlock(BlockInfo curBlock, INodesInPath iip,
      boolean force) throws IOException {
    if (curBlock.isComplete()) {
      return;
    }

    if (!force && curBlock.getBlockUCState() != BlockUCState.COMMITTED) {
      throw new IOException(
          "Cannot complete block: block has not been COMMITTED by the client");
    }

    convertToCompleteBlock(curBlock, iip);
  }

  /**
   * Convert a specified block of the file to a complete block.
   * Skips validity checking and safe mode block total updates; use
   * {@link BlockManager#completeBlock} to include these.
   * @param curBlock - block to be completed
   * @param iip - INodes in path to file containing curBlock; if null,
   *              this will be resolved internally
   * @throws IOException if the block does not have at least a minimal number
   * of replicas reported from data-nodes.
   */
  private void convertToCompleteBlock(BlockInfo curBlock, INodesInPath iip)
      throws IOException {
    curBlock.convertToCompleteBlock();
    namesystem.getFSDirectory().updateSpaceForCompleteBlock(curBlock, iip);
  }

  /**
   * Force the given block in the given file to be marked as complete,
   * regardless of whether enough replicas are present. This is necessary
   * when tailing edit logs as a Standby.
   */
  public void forceCompleteBlock(final BlockInfo block) throws IOException {
    block.commitBlock(block);
    completeBlock(block, null, true);
  }

  /**
   * Convert the last block of the file to an under construction block.<p>
   * The block is converted only if the file has blocks and the last one
   * is a partial block (its size is less than the preferred block size).
   * The converted block is returned to the client.
   * The client uses the returned block locations to form the data pipeline
   * for this block.<br>
   * The methods returns null if there is no partial block at the end.
   * The client is supposed to allocate a new block with the next call.
   *
   * @param bc file
   * @param bytesToRemove num of bytes to remove from block
   * @return the last block locations if the block is partial or null otherwise
   */
  public LocatedBlock convertLastBlockToUnderConstruction(
      BlockCollection bc, long bytesToRemove) throws IOException {
    BlockInfo lastBlock = bc.getLastBlock();
    if (lastBlock == null ||
       bc.getPreferredBlockSize() == lastBlock.getNumBytes() - bytesToRemove) {
      return null;
    }
    assert lastBlock == getStoredBlock(lastBlock) :
      "last block of the file is not in blocksMap";

    // convert the last block to under construction. note no block replacement
    // is happening
    bc.convertLastBlockToUC(lastBlock);

    final long fileLength = bc.computeContentSummary(
        getStoragePolicySuite()).getLength();
    final long pos = fileLength - lastBlock.getNumBytes();
    return createLocatedBlock(null, lastBlock, pos,
        BlockTokenIdentifier.AccessMode.WRITE);
  }

  private void createLocatedBlockList(
      LocatedBlockBuilder locatedBlocks,
      final BlockInfo[] blocks,
      final long offset, final long length,
      final AccessMode mode) throws IOException {
    int curBlk;
    long curPos = 0, blkSize = 0;
    int nrBlocks = (blocks[0].getNumBytes() == 0) ? 0 : blocks.length;
    for (curBlk = 0; curBlk < nrBlocks; curBlk++) {
      blkSize = blocks[curBlk].getNumBytes();
      assert blkSize > 0 : "Block of size 0";
      if (curPos + blkSize > offset) {
        break;
      }
      curPos += blkSize;
    }

    if (nrBlocks > 0 && curBlk == nrBlocks)   // offset >= end of file
      return;

    long endOff = offset + length;
    do {
      locatedBlocks.addBlock(
          createLocatedBlock(locatedBlocks, blocks[curBlk], curPos, mode));
      curPos += blocks[curBlk].getNumBytes();
      curBlk++;
    } while (curPos < endOff 
          && curBlk < blocks.length
          && !locatedBlocks.isBlockMax());
    return;
  }

  private LocatedBlock createLocatedBlock(LocatedBlockBuilder locatedBlocks,
      final BlockInfo[] blocks,
      final long endPos, final AccessMode mode) throws IOException {
    int curBlk;
    long curPos = 0;
    int nrBlocks = (blocks[0].getNumBytes() == 0) ? 0 : blocks.length;
    for (curBlk = 0; curBlk < nrBlocks; curBlk++) {
      long blkSize = blocks[curBlk].getNumBytes();
      if (curPos + blkSize >= endPos) {
        break;
      }
      curPos += blkSize;
    }
    
    return createLocatedBlock(locatedBlocks, blocks[curBlk], curPos, mode);
  }

  private LocatedBlock createLocatedBlock(LocatedBlockBuilder locatedBlocks,
      final BlockInfo blk, final long pos, final AccessMode mode)
          throws IOException {
    final LocatedBlock lb = createLocatedBlock(locatedBlocks, blk, pos);
    return lb;
  }

  /** @return a LocatedBlock for the given block */
  private LocatedBlock createLocatedBlock(LocatedBlockBuilder locatedBlocks,
      final BlockInfo blk, final long pos) throws IOException {
    final ExtendedBlock eb = new ExtendedBlock(getBlockPoolId(), blk);
    return null == locatedBlocks ?
        newLocatedBlock(eb, pos) :
        locatedBlocks.newLocatedBlock(eb, pos);
  }

  /** Create a LocatedBlocks. */
  public LocatedBlocks createLocatedBlocks(final BlockInfo[] blocks,
      final long fileSizeExcludeBlocksUnderConstruction,
      final boolean isFileUnderConstruction, final long offset,
      final long length, final boolean needBlockToken, final boolean inSnapshot,
      FileEncryptionInfo feInfo)
      throws IOException {
    assert namesystem.hasReadLock();
    if (blocks == null) {
      return null;
    } else if (blocks.length == 0) {
      return new LocatedBlocks(0, isFileUnderConstruction,
          Collections.<LocatedBlock> emptyList(), null, false, feInfo);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("blocks = {}", java.util.Arrays.asList(blocks));
      }
      final AccessMode mode = needBlockToken? BlockTokenIdentifier.AccessMode.READ: null;

      LocatedBlockBuilder locatedBlocks = new LocatedBlockBuilder(Integer.MAX_VALUE)
          .fileLength(fileSizeExcludeBlocksUnderConstruction)
          .lastUC(isFileUnderConstruction)
          .encryption(feInfo);

      createLocatedBlockList(locatedBlocks, blocks, offset, length, mode);
      if (!inSnapshot) {
        final BlockInfo last = blocks[blocks.length - 1];
        final long lastPos = last.isComplete()?
            fileSizeExcludeBlocksUnderConstruction - last.getNumBytes()
            : fileSizeExcludeBlocksUnderConstruction;

        locatedBlocks
          .lastBlock(createLocatedBlock(locatedBlocks, last, lastPos, mode))
          .lastComplete(last.isComplete());
      } else {
        locatedBlocks
          .lastBlock(createLocatedBlock(locatedBlocks, blocks,
              fileSizeExcludeBlocksUnderConstruction, mode))
          .lastComplete(true);
      }
      LocatedBlocks locations = locatedBlocks.build();
      return locations;
    }
  }

  /**
   * Clamp the specified replication between the minimum and the maximum
   * replication levels.
   */
  public short adjustReplication(short replication) {
    return replication < minReplication? minReplication
        : replication > maxReplication? maxReplication: replication;
  }

  /**
   * Check whether the replication parameter is within the range
   * determined by system configuration and throw an exception if it's not.
   *
   * @param src the path to the target file
   * @param replication the requested replication factor
   * @param clientName the name of the client node making the request
   * @throws java.io.IOException thrown if the requested replication factor
   * is out of bounds
   */
   public void verifyReplication(String src,
                          short replication,
                          String clientName) throws IOException {
    String err = null;
    if (replication > maxReplication) {
      err = " exceeds maximum of " + maxReplication;
    } else if (replication < minReplication) {
      err = " is less than the required minimum of " + minReplication;
    }

    if (err != null) {
      throw new IOException("Requested replication factor of " + replication
          + err + " for " + src
          + (clientName == null? "": ", clientName=" + clientName));
    }
  }

  /**
   * Adds block to list of blocks which will be invalidated on specified
   * datanode and log the operation
   */
  void addToInvalidates(final Block block) {
    if (!isPopulatingReplQueues()) {
      return;
    }
    invalidateBlocks.add(block, true);
  }

  /**
   * Removes the blocks from blocksmap and updates the safemode blocks total.
   * @param blocks An instance of {@link BlocksMapUpdateInfo} which contains a
   *               list of blocks that need to be removed from blocksMap
   */
  public void removeBlocksAndUpdateSafemodeTotal(BlocksMapUpdateInfo blocks) {
    assert namesystem.hasWriteLock();
    for (BlockInfo b : blocks.getToDeleteList()) {
      removeBlock(b);
    }
  }

  /**
   * For each block in the name-node verify whether it belongs to any file,
   * extra or low redundancy. Place it into the respective queue.
   */

  /** Set replication for the blocks. */
  public void setReplication(
      final short oldRepl, final short newRepl, final BlockInfo b) {
    if (newRepl == oldRepl) {
      return;
    }

    // update neededReconstruction priority queues
    b.setReplication(newRepl);
  }

  public int getTotalBlocks() {
    return blocksMap.size();
  }

  public void removeBlock(BlockInfo block) {
    assert namesystem.hasWriteLock();
    // No need to ACK blocks that are being removed entirely
    // from the namespace, since the removal of the associated
    // file already removes them from the block map below.
    block.setNumBytes(Long.MAX_VALUE);
    addToInvalidates(block);
    removeBlockFromMap(block);
  }

  public BlockInfo getStoredBlock(Block block) {
    return blocksMap.getStoredBlock(block);
  }

  public BlockInfo addBlockCollection(BlockInfo block,
      BlockCollection bc) {
    return blocksMap.addBlockCollection(block, bc);
  }

  /**
   * Do some check when adding a block to blocksmap.
   * For HDFS-7994 to check whether then block is a NonEcBlockUsingStripedID.
   *
   */
  public BlockInfo addBlockCollectionWithCheck(
      BlockInfo block, BlockCollection bc) {
    return addBlockCollection(block, bc);
  }

  public void removeBlockFromMap(BlockInfo block) {
    blocksMap.removeBlock(block);
  }

  public int getCapacity() {
    return blocksMap.getCapacity();
  }

  /**
   * Clear all queues that hold decisions previously made by
   * this NameNode.
   */
  public void clearQueues() {
    invalidateBlocks.clear();
  }

  public static LocatedBlock newLocatedBlock(ExtendedBlock b, long startOffset) {
    // startOffset is unknown
    return new LocatedBlock(b, startOffset);
  }

  public void shutdown() {
    blocksMap.close();
    MBeans.unregister(mxBeanName);
    mxBeanName = null;
  }
  
  public void clear() {
    blockIdManager.clear();
    clearQueues();
    blocksMap.clear();
  }

  /**
   * Initialize replication queues.
   */
  public void initializeReplQueues() {
    LOG.info("initializing replication queues");
    initializedReplQueues = true;
  }

  /**
   * Check if replication queues are to be populated
   * @return true when node is HAState.Active and not in the very first safemode
   */
  public boolean isPopulatingReplQueues() {
    if (!shouldPopulateReplQueues()) {
      return false;
    }
    return initializedReplQueues;
  }

  public void setInitializedReplQueues(boolean v) {
    this.initializedReplQueues = v;
  }

  public boolean shouldPopulateReplQueues() {
    HAContext haContext = namesystem.getHAContext();
    if (haContext == null || haContext.getState() == null)
      return false;
    return haContext.getState().shouldPopulateReplQueues();
  }

  /**
   * Notification of a successful block recovery.
   * @param block for which the recovery succeeded
   */
  public void successfulBlockRecovery(BlockInfo block) {
    pendingRecoveryBlocks.remove(block);
  }

  /**
   * Checks whether a recovery attempt has been made for the given block.
   * If so, checks whether that attempt has timed out.
   * @param b block for which recovery is being attempted
   * @return true if no recovery attempt has been made or
   *         the previous attempt timed out
   */
  public boolean addBlockRecoveryAttempt(BlockInfo b) {
    return pendingRecoveryBlocks.add(b);
  }

  public BlockIdManager getBlockIdManager() {
    return blockIdManager;
  }

  public long nextGenerationStamp(boolean legacyBlock) throws IOException {
    return blockIdManager.nextGenerationStamp(legacyBlock);
  }

  public boolean isLegacyBlock(Block block) {
    return blockIdManager.isLegacyBlock(block);
  }

  public long nextBlockId() {
    return blockIdManager.nextBlockId();
  }

  boolean isGenStampInFuture(Block block) {
    return blockIdManager.isGenStampInFuture(block);
  }

  private static long getBlockRecoveryTimeout(long heartbeatIntervalSecs) {
    return TimeUnit.SECONDS.toMillis(heartbeatIntervalSecs *
        BLOCK_RECOVERY_TIMEOUT_MULTIPLIER);
  }

  @VisibleForTesting
  public void setBlockRecoveryTimeout(long blockRecoveryTimeout) {
    pendingRecoveryBlocks.setRecoveryTimeoutInterval(blockRecoveryTimeout);
  }
}
