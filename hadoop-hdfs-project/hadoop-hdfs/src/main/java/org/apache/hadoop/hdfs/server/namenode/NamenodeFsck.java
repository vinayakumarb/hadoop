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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataEncryptionKeyFactory;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tracing.TraceUtils;
import org.apache.hadoop.util.Time;
import org.apache.htrace.core.Tracer;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class provides rudimentary checking of DFS volumes for errors and
 * sub-optimal conditions.
 * <p>The tool scans all files and directories, starting from an indicated
 *  root path. The following abnormal conditions are detected and handled:</p>
 * <ul>
 *  <li>detect files with under-replicated or over-replicated blocks</li>
 *  </ul>
 *  Additionally, the tool collects a detailed overall DFS statistics, and
 *  optionally can print detailed statistics on block locations and replication
 *  factors of each file.
 */
@InterfaceAudience.Private
public class NamenodeFsck implements DataEncryptionKeyFactory {
  public static final Log LOG = LogFactory.getLog(NameNode.class.getName());

  // return string marking fsck status
  public static final String CORRUPT_STATUS = "is CORRUPT";
  public static final String HEALTHY_STATUS = "is HEALTHY";
  public static final String DECOMMISSIONING_STATUS = "is DECOMMISSIONING";
  public static final String DECOMMISSIONED_STATUS = "is DECOMMISSIONED";
  public static final String ENTERING_MAINTENANCE_STATUS =
      "is ENTERING MAINTENANCE";
  public static final String IN_MAINTENANCE_STATUS = "is IN MAINTENANCE";
  public static final String NONEXISTENT_STATUS = "does not exist";
  public static final String FAILURE_STATUS = "FAILED";
  public static final String UNDEFINED = "undefined";

  private final NameNode namenode;
  private final BlockManager blockManager;
  private final InetAddress remoteAddress;

  private long totalDirs = 0L;
  private long totalSymlinks = 0L;

  private String lostFound = null;
  private boolean showFiles = false;
  private boolean showOpenFiles = false;
  private boolean showBlocks = false;
  private boolean showStoragePolcies = false;
  private boolean showprogress = false;
  private boolean showCorruptFileBlocks = false;

  private Tracer tracer;

  /**
   * True if we encountered an internal error during FSCK, such as not being
   * able to delete a corrupt file.
   */
  private boolean internalError = false;

  /**
   * True if the user specified the -delete option.
   *
   * Whe this option is in effect, we will delete corrupted files.
   */
  private boolean doDelete = false;

  String path = "/";

  private String blockIds = null;

  // We return back N files that are corrupt; the list of files returned is
  // ordered by block id; to allow continuation support, pass in the last block
  // # from previous call
  private final String[] currentCookie = new String[] { null };

  private final Configuration conf;
  private final PrintWriter out;
  private List<String> snapshottableDirs = null;

  private StoragePolicySummary storageTypeSummary = null;

  /**
   * Filesystem checker.
   * @param conf configuration (namenode config)
   * @param namenode namenode that this fsck is going to use
   * @param pmap key=value[] map passed to the http servlet as url parameters
   * @param out output stream to write the fsck output
   * @param remoteAddress source address of the fsck request
   */
  NamenodeFsck(Configuration conf, NameNode namenode, Map<String, String[]> pmap,
      PrintWriter out, InetAddress remoteAddress) {
    this.conf = conf;
    this.namenode = namenode;
    this.blockManager = namenode.getNamesystem().getBlockManager();
    this.out = out;
    this.remoteAddress = remoteAddress;
    this.tracer = new Tracer.Builder("NamenodeFsck").
        conf(TraceUtils.wrapHadoopConf("namenode.fsck.htrace.", conf)).
        build();

    for (Iterator<String> it = pmap.keySet().iterator(); it.hasNext();) {
      String key = it.next();
      if (key.equals("path")) { this.path = pmap.get("path")[0]; }
      else if (key.equals("delete")) { this.doDelete = true; }
      else if (key.equals("files")) { this.showFiles = true; }
      else if (key.equals("blocks")) { this.showBlocks = true; }
      else if (key.equals("storagepolicies")) {
        this.showStoragePolcies = true;
      } else if (key.equals("showprogress")) {
        this.showprogress = true;
      } else if (key.equals("openforwrite")) {
        this.showOpenFiles = true;
      } else if (key.equals("listcorruptfileblocks")) {
        this.showCorruptFileBlocks = true;
      } else if (key.equals("startblockafter")) {
        this.currentCookie[0] = pmap.get("startblockafter")[0];
      } else if (key.equals("includeSnapshots")) {
        this.snapshottableDirs = new ArrayList<String>();
      } else if (key.equals("blockId")) {
        this.blockIds = pmap.get("blockId")[0];
      }
    }
  }

  /**
   * Check block information given a blockId number
   *
  */
  public void blockIdCK(String blockId) {

    if(blockId == null) {
      out.println("Please provide valid blockId!");
      return;
    }

    try {
      //get blockInfo
      Block block = new Block(
          Block.generateBlockId(Block.filename2id(blockId)));
      //find which file this block belongs to
      BlockInfo blockInfo = blockManager.getStoredBlock(block);
      if(blockInfo == null) {
        out.println("Block "+ blockId +" " + NONEXISTENT_STATUS);
        LOG.warn("Block "+ blockId + " " + NONEXISTENT_STATUS);
        return;
      }
      final INodeFile iNode = namenode.getNamesystem().getBlockCollection(blockInfo);
      out.println("Block Id: " + blockId);
      out.println("Block belongs to: "+iNode.getFullPathName());
    } catch (Exception e){
      String errMsg = "Fsck on blockId '" + blockId;
      LOG.warn(errMsg, e);
      out.println(e.getMessage());
      out.print("\n\n" + errMsg);
      LOG.warn("Error in looking up block", e);
    }
  }

  /**
   * Check files on DFS, starting from the indicated path.
   */
  public void fsck() {
    final long startTime = Time.monotonicNow();
    try {
      if(blockIds != null) {
        String[] blocks = blockIds.split(" ");
        StringBuilder sb = new StringBuilder();
        sb.append("FSCK started by " +
            UserGroupInformation.getCurrentUser() + " from " +
            remoteAddress + " at " + new Date());
        out.println(sb);
        sb.append(" for blockIds: \n");
        for (String blk: blocks) {
          if(blk == null || !blk.contains(Block.BLOCK_FILE_PREFIX)) {
            out.println("Incorrect blockId format: " + blk);
            continue;
          }
          out.print("\n");
          blockIdCK(blk);
          sb.append(blk + "\n");
        }
        LOG.info(sb);
        namenode.getNamesystem().logFsckEvent("/", remoteAddress);
        out.flush();
        return;
      }

      String msg = "FSCK started by " + UserGroupInformation.getCurrentUser()
          + " from " + remoteAddress + " for path " + path + " at " + new Date();
      LOG.info(msg);
      out.println(msg);
      namenode.getNamesystem().logFsckEvent(path, remoteAddress);

      if (snapshottableDirs != null) {
        SnapshottableDirectoryStatus[] snapshotDirs =
            namenode.getRpcServer().getSnapshottableDirListing();
        if (snapshotDirs != null) {
          for (SnapshottableDirectoryStatus dir : snapshotDirs) {
            snapshottableDirs.add(dir.getFullPath().toString());
          }
        }
      }

      final HdfsFileStatus file = namenode.getRpcServer().getFileInfo(path);
      if (file != null) {

        if (showCorruptFileBlocks) {
          listCorruptFileBlocks();
          return;
        }

        if (this.showStoragePolcies) {
          storageTypeSummary = new StoragePolicySummary(
              namenode.getNamesystem().getBlockManager().getStoragePolicies());
        }

        Result replRes = new ReplicationResult(conf);
        Result ecRes = new ErasureCodingResult(conf);

        check(path, file, replRes, ecRes);

        out.print("\nStatus: ");
        out.println(replRes.isHealthy() && ecRes.isHealthy() ? "HEALTHY" : "CORRUPT");
        out.println(" Total dirs:\t\t\t" + totalDirs);
        out.println(" Total symlinks:\t\t" + totalSymlinks);
        out.println("\nReplicated Blocks:");
        out.println(replRes);
        out.println("\nErasure Coded Block Groups:");
        out.println(ecRes);

        if (this.showStoragePolcies) {
          out.print(storageTypeSummary);
        }

        out.println("FSCK ended at " + new Date() + " in "
            + (Time.monotonicNow() - startTime + " milliseconds"));

        // If there were internal errors during the fsck operation, we want to
        // return FAILURE_STATUS, even if those errors were not immediately
        // fatal.  Otherwise many unit tests will pass even when there are bugs.
        if (internalError) {
          throw new IOException("fsck encountered internal errors!");
        }

        // DFSck client scans for the string HEALTHY/CORRUPT to check the status
        // of file system and return appropriate code. Changing the output
        // string might break testcases. Also note this must be the last line
        // of the report.
        if (replRes.isHealthy() && ecRes.isHealthy()) {
          out.print("\n\nThe filesystem under path '" + path + "' " + HEALTHY_STATUS);
        } else {
          out.print("\n\nThe filesystem under path '" + path + "' " + CORRUPT_STATUS);
        }

      } else {
        out.print("\n\nPath '" + path + "' " + NONEXISTENT_STATUS);
      }
    } catch (Exception e) {
      String errMsg = "Fsck on path '" + path + "' " + FAILURE_STATUS;
      LOG.warn(errMsg, e);
      out.println("FSCK ended at " + new Date() + " in "
          + (Time.monotonicNow() - startTime + " milliseconds"));
      out.println(e.getMessage());
      out.print("\n\n" + errMsg);
    } finally {
      out.close();
    }
  }

  private void listCorruptFileBlocks() throws IOException {
    final List<String> corrputBlocksFiles = namenode.getNamesystem()
        .listCorruptFileBlocksWithSnapshot(path, snapshottableDirs,
            currentCookie);
    int numCorruptFiles = corrputBlocksFiles.size();
    String filler;
    if (numCorruptFiles > 0) {
      filler = Integer.toString(numCorruptFiles);
    } else if (currentCookie[0].equals("0")) {
      filler = "no";
    } else {
      filler = "no more";
    }
    out.println("Cookie:\t" + currentCookie[0]);
    for (String s : corrputBlocksFiles) {
      out.println(s);
    }
    out.println("\n\nThe filesystem under path '" + path + "' has " + filler
        + " CORRUPT files");
    out.println();
  }

  @VisibleForTesting
  void check(String parent, HdfsFileStatus file, Result replRes, Result ecRes)
      throws IOException {
    String path = file.getFullName(parent);
    if (showprogress &&
        (totalDirs + totalSymlinks + replRes.totalFiles + ecRes.totalFiles)
            % 100 == 0) {
      out.println();
      out.flush();
    }

    if (file.isDirectory()) {
      checkDir(path, replRes, ecRes);
      return;
    }
    if (file.isSymlink()) {
      if (showFiles) {
        out.println(path + " <symlink>");
      }
      totalSymlinks++;
      return;
    }
    LocatedBlocks blocks = getBlockLocations(path, file);
    if (blocks == null) { // the file is deleted
      return;
    }

    final Result r = replRes;
    collectFileSummary(path, file, r, blocks);
    collectBlocksSummary(parent, file, r, blocks);
  }

  private void checkDir(String path, Result replRes, Result ecRes) throws IOException {
    if (snapshottableDirs != null && snapshottableDirs.contains(path)) {
      String snapshotPath = (path.endsWith(Path.SEPARATOR) ? path : path
          + Path.SEPARATOR)
          + HdfsConstants.DOT_SNAPSHOT_DIR;
      HdfsFileStatus snapshotFileInfo = namenode.getRpcServer().getFileInfo(
          snapshotPath);
      check(snapshotPath, snapshotFileInfo, replRes, ecRes);
    }
    byte[] lastReturnedName = HdfsFileStatus.EMPTY_NAME;
    DirectoryListing thisListing;
    if (showFiles) {
      out.println(path + " <dir>");
    }
    totalDirs++;
    do {
      assert lastReturnedName != null;
      thisListing = namenode.getRpcServer().getListing(
          path, lastReturnedName, false);
      if (thisListing == null) {
        return;
      }
      HdfsFileStatus[] files = thisListing.getPartialListing();
      for (int i = 0; i < files.length; i++) {
        check(path, files[i], replRes, ecRes);
      }
      lastReturnedName = thisListing.getLastName();
    } while (thisListing.hasMore());
  }

  private LocatedBlocks getBlockLocations(String path, HdfsFileStatus file)
      throws IOException {
    long fileLen = file.getLen();
    LocatedBlocks blocks = null;
    final FSNamesystem fsn = namenode.getNamesystem();
    fsn.readLock();
    try {
      blocks = FSDirStatAndListingOp.getBlockLocations(
          fsn.getFSDirectory(), fsn.getPermissionChecker(),
          path, 0, fileLen, false)
          .blocks;
    } catch (FileNotFoundException fnfe) {
      blocks = null;
    } finally {
      fsn.readUnlock("fsckGetBlockLocations");
    }
    return blocks;
  }

  private void collectFileSummary(String path, HdfsFileStatus file, Result res,
      LocatedBlocks blocks) throws IOException {
    long fileLen = file.getLen();
    boolean isOpen = blocks.isUnderConstruction();
    if (isOpen && !showOpenFiles) {
      // We collect these stats about open files to report with default options
      res.totalOpenFilesSize += fileLen;
      res.totalOpenFilesBlocks += blocks.locatedBlockCount();
      res.totalOpenFiles++;
      return;
    }
    res.totalFiles++;
    res.totalSize += fileLen;
    res.totalBlocks += blocks.locatedBlockCount();
    String redundancyPolicy;
    redundancyPolicy = "replicated: replication=" +
        file.getReplication() + ",";

    if (showOpenFiles && isOpen) {
      out.print(path + " " + fileLen + " bytes, " + redundancyPolicy + " " +
        blocks.locatedBlockCount() + " block(s), OPENFORWRITE: ");
    } else if (showFiles) {
      out.print(path + " " + fileLen + " bytes, " + redundancyPolicy + " " +
        blocks.locatedBlockCount() + " block(s): ");
    } else if (showprogress) {
      out.print('.');
    }
  }

  private void collectBlocksSummary(String parent, HdfsFileStatus file,
      Result res, LocatedBlocks blocks) throws IOException {
    String path = file.getFullName(parent);
    boolean isOpen = blocks.isUnderConstruction();
    if (isOpen && !showOpenFiles) {
      return;
    }
    int missing = 0;
    int corrupt = 0;
    long missize = 0;
    long corruptSize = 0;
    int underReplicatedPerFile = 0;
    int misReplicatedPerFile = 0;
    StringBuilder report = new StringBuilder();
    int blockNumber = 0;
    final LocatedBlock lastBlock = blocks.getLastLocatedBlock();
    for (LocatedBlock lBlk : blocks.getLocatedBlocks()) {
      ExtendedBlock block = lBlk.getBlock();
      if (!blocks.isLastBlockComplete() && lastBlock != null &&
          lastBlock.getBlock().equals(block)) {
        // this is the last block and this is not complete. ignore it since
        // it is under construction
        continue;
      }

      final BlockInfo storedBlock = blockManager.getStoredBlock(
          block.getLocalBlock());
      final int minReplication = blockManager.getMinStorageNum(storedBlock);
      // count expected replicas
      short targetFileReplication;
      targetFileReplication = file.getReplication();
      res.numExpectedReplicas += targetFileReplication;
      // report
      String blkName = block.toString();
      report.append(blockNumber + ". " + blkName + " len=" +
          block.getNumBytes());
      report.append('\n');
      blockNumber++;
    }

    //display under construction block info.
    if (!blocks.isLastBlockComplete() && lastBlock != null) {
      ExtendedBlock block = lastBlock.getBlock();
      String blkName = block.toString();
      BlockInfo storedBlock = blockManager.getStoredBlock(
          block.getLocalBlock());
      report.append('\n');
      report.append("Under Construction Block:\n");
      report.append(blockNumber).append(". ").append(blkName);
      report.append(" len=").append(block.getNumBytes());
    }

    if (showFiles) {
      out.print(" OK\n");
      if (showBlocks) {
        out.print(report + "\n");
      }
    }
  }

  @Override
  public DataEncryptionKey newDataEncryptionKey() throws IOException {
    return namenode.getRpcServer().getDataEncryptionKey();
  }

  /*
   * XXX (ab) See comment above for copyBlock().
   *
   * Pick the best node from which to stream the data.
   * That's the local one, if available.
   */
  private DatanodeInfo bestNode(DFSClient dfs, DatanodeInfo[] nodes,
                                TreeSet<DatanodeInfo> deadNodes) throws IOException {
    if ((nodes == null) ||
        (nodes.length - deadNodes.size() < 1)) {
      throw new IOException("No live nodes contain current block");
    }
    DatanodeInfo chosenNode;
    do {
      chosenNode = nodes[ThreadLocalRandom.current().nextInt(nodes.length)];
    } while (deadNodes.contains(chosenNode));
    return chosenNode;
  }

  /**
   * FsckResult of checking, plus overall DFS statistics.
   */
  @VisibleForTesting
  static class Result {
    final List<String> missingIds = new ArrayList<String>();
    long missingSize = 0L;
    long corruptFiles = 0L;
    long corruptBlocks = 0L;
    long corruptSize = 0L;
    long totalBlocks = 0L;
    long numExpectedReplicas = 0L;
    long totalOpenFilesBlocks = 0L;
    long totalFiles = 0L;
    long totalOpenFiles = 0L;
    long totalSize = 0L;
    long totalOpenFilesSize = 0L;
    long totalReplicas = 0L;

    /**
     * DFS is considered healthy if there are no missing blocks.
     */
    boolean isHealthy() {
      return ((missingIds.size() == 0) && (corruptBlocks == 0));
    }

    /** Add a missing block name, plus its size. */
    void addMissing(String id, long size) {
      missingIds.add(id);
      missingSize += size;
    }

    /** Add a corrupt block. */
    void addCorrupt(long size) {
      corruptBlocks++;
      corruptSize += size;
    }

    /** Return the actual replication factor. */
    float getReplicationFactor() {
      if (totalBlocks == 0)
        return 0.0f;
      return (float) (totalReplicas) / (float) totalBlocks;
    }
  }

  @VisibleForTesting
  static class ReplicationResult extends Result {
    final short replication;
    final short minReplication;

    ReplicationResult(Configuration conf) {
      this.replication = (short)conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY,
                                            DFSConfigKeys.DFS_REPLICATION_DEFAULT);
      this.minReplication = (short)conf.getInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY,
                                            DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_DEFAULT);
    }

    @Override
    public String toString() {
      StringBuilder res = new StringBuilder();
      res.append(" Total size:\t").append(totalSize).append(" B");
      if (totalOpenFilesSize != 0) {
        res.append(" (Total open files size: ").append(totalOpenFilesSize)
            .append(" B)");
      }
      res.append("\n Total files:\t").append(totalFiles);
      if (totalOpenFiles != 0) {
        res.append(" (Files currently being written: ").append(totalOpenFiles)
            .append(")");
      }
      res.append("\n Total blocks (validated):\t").append(totalBlocks);
      if (totalBlocks > 0) {
        res.append(" (avg. block size ").append((totalSize / totalBlocks))
            .append(" B)");
      }
      if (totalOpenFilesBlocks != 0) {
        res.append(" (Total open file blocks (not validated): ").append(
            totalOpenFilesBlocks).append(")");
      }
      if (corruptFiles > 0) {
        res.append("\n  ********************************");
        if(corruptFiles>0) {
          res.append(
              "\n  CORRUPT FILES:\t").append(corruptFiles);
          if (missingSize > 0) {
            res.append("\n  MISSING BLOCKS:\t").append(missingIds.size()).append(
                "\n  MISSING SIZE:\t\t").append(missingSize).append(" B");
          }
          if (corruptBlocks > 0) {
            res.append("\n  CORRUPT BLOCKS: \t").append(corruptBlocks).append(
                "\n  CORRUPT SIZE:\t\t").append(corruptSize).append(" B");
          }
        }
        res.append("\n  ********************************");
      }
      res.append("\n Default replication factor:\t").append(replication)
          .append("\n Average block replication:\t").append(
              getReplicationFactor()).append("\n Missing blocks:\t\t").append(
              missingIds.size()).append("\n Corrupt blocks:\t\t").append(
              corruptBlocks);
      return res.toString();
    }
  }

  @VisibleForTesting
  static class ErasureCodingResult extends Result {

    ErasureCodingResult(Configuration conf) {
    }

    @Override
    public String toString() {
      StringBuilder res = new StringBuilder();
      res.append(" Total size:\t").append(totalSize).append(" B");
      if (totalOpenFilesSize != 0) {
        res.append(" (Total open files size: ").append(totalOpenFilesSize)
            .append(" B)");
      }
      res.append("\n Total files:\t").append(totalFiles);
      if (totalOpenFiles != 0) {
        res.append(" (Files currently being written: ").append(totalOpenFiles)
            .append(")");
      }
      res.append("\n Total block groups (validated):\t").append(totalBlocks);
      if (totalBlocks > 0) {
        res.append(" (avg. block group size ").append((totalSize / totalBlocks))
            .append(" B)");
      }
      if (totalOpenFilesBlocks != 0) {
        res.append(" (Total open file block groups (not validated): ").append(
            totalOpenFilesBlocks).append(")");
      }
      if (corruptFiles > 0) {
        res.append("\n  ********************************");
        if(corruptFiles>0) {
          res.append(
              "\n  CORRUPT FILES:\t").append(corruptFiles);
          if (missingSize > 0) {
            res.append("\n  MISSING BLOCK GROUPS:\t").append(missingIds.size()).append(
                "\n  MISSING SIZE:\t\t").append(missingSize).append(" B");
          }
          if (corruptBlocks > 0) {
            res.append("\n  CORRUPT BLOCK GROUPS: \t").append(corruptBlocks).append(
                "\n  CORRUPT SIZE:\t\t").append(corruptSize).append(" B");
          }
        }
        res.append("\n  ********************************");
      }
      res.append("\n Average block group size:\t").append(
          getReplicationFactor()).append("\n Missing block groups:\t\t").append(
          missingIds.size()).append("\n Corrupt block groups:\t\t").append(
          corruptBlocks);
      return res.toString();
    }
  }
}
