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
package org.apache.hadoop.hdfs;

import org.apache.hadoop.fs.CompositeCrcFileChecksum;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32CastagnoliFileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32GzipFileChecksum;
import org.apache.hadoop.fs.Options.ChecksumCombineMode;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.hdfs.protocol.BlockChecksumType;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.CrcComposer;
import org.apache.hadoop.util.CrcUtil;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Utility classes to compute file checksum for both replicated and striped
 * files.
 */
final class FileChecksumHelper {
  static final Logger LOG =
      LoggerFactory.getLogger(FileChecksumHelper.class);

  private FileChecksumHelper() {}

  /**
   * A common abstract class to compute file checksum.
   */
  static abstract class FileChecksumComputer {
    private final String src;
    private final long length;
    private final DFSClient client;
    private final ClientProtocol namenode;
    private final ChecksumCombineMode combineMode;
    private final BlockChecksumType blockChecksumType;
    private final DataOutputBuffer blockChecksumBuf = new DataOutputBuffer();

    private FileChecksum fileChecksum;
    private LocatedBlocks blockLocations;

    private int timeout;
    private List<LocatedBlock> locatedBlocks;
    private long remaining = 0L;

    private int bytesPerCRC = -1;
    private DataChecksum.Type crcType = DataChecksum.Type.DEFAULT;
    private long crcPerBlock = 0;
    private boolean isRefetchBlocks = false;
    private int lastRetriedIndex = -1;

    /**
     * Constructor that accepts all the input parameters for the computing.
     */
    FileChecksumComputer(String src, long length,
                         LocatedBlocks blockLocations,
                         ClientProtocol namenode,
                         DFSClient client,
                         ChecksumCombineMode combineMode) throws IOException {
      this.src = src;
      this.length = length;
      this.blockLocations = blockLocations;
      this.namenode = namenode;
      this.client = client;
      this.combineMode = combineMode;
      switch (combineMode) {
      case MD5MD5CRC:
        this.blockChecksumType = BlockChecksumType.MD5CRC;
        break;
      case COMPOSITE_CRC:
        this.blockChecksumType = BlockChecksumType.COMPOSITE_CRC;
        break;
      default:
        throw new IOException("Unknown ChecksumCombineMode: " + combineMode);
      }

      this.remaining = length;

      if (blockLocations != null) {
        if (src.contains(HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR_SEPARATOR)) {
          this.remaining = Math.min(length, blockLocations.getFileLength());
        }
        this.locatedBlocks = blockLocations.getLocatedBlocks();
      }
    }

    String getSrc() {
      return src;
    }

    long getLength() {
      return length;
    }

    DFSClient getClient() {
      return client;
    }

    ClientProtocol getNamenode() {
      return namenode;
    }

    ChecksumCombineMode getCombineMode() {
      return combineMode;
    }

    BlockChecksumType getBlockChecksumType() {
      return blockChecksumType;
    }

    DataOutputBuffer getBlockChecksumBuf() {
      return blockChecksumBuf;
    }

    FileChecksum getFileChecksum() {
      return fileChecksum;
    }

    LocatedBlocks getBlockLocations() {
      return blockLocations;
    }

    void refetchBlocks() throws IOException {
      this.blockLocations = getClient().getBlockLocations(getSrc(),
          getLength());
      this.locatedBlocks = getBlockLocations().getLocatedBlocks();
      this.isRefetchBlocks = false;
    }

    int getTimeout() {
      return timeout;
    }

    void setTimeout(int timeout) {
      this.timeout = timeout;
    }

    List<LocatedBlock> getLocatedBlocks() {
      return locatedBlocks;
    }

    long getRemaining() {
      return remaining;
    }

    void setRemaining(long remaining) {
      this.remaining = remaining;
    }

    int getBytesPerCRC() {
      return bytesPerCRC;
    }

    void setBytesPerCRC(int bytesPerCRC) {
      this.bytesPerCRC = bytesPerCRC;
    }

    DataChecksum.Type getCrcType() {
      return crcType;
    }

    void setCrcType(DataChecksum.Type crcType) {
      this.crcType = crcType;
    }

    long getCrcPerBlock() {
      return crcPerBlock;
    }

    void setCrcPerBlock(long crcPerBlock) {
      this.crcPerBlock = crcPerBlock;
    }

    boolean isRefetchBlocks() {
      return isRefetchBlocks;
    }

    void setRefetchBlocks(boolean refetchBlocks) {
      this.isRefetchBlocks = refetchBlocks;
    }

    int getLastRetriedIndex() {
      return lastRetriedIndex;
    }

    void setLastRetriedIndex(int lastRetriedIndex) {
      this.lastRetriedIndex = lastRetriedIndex;
    }

    /**
     * Perform the file checksum computing. The intermediate results are stored
     * in the object and will be used later.
     * @throws IOException
     */
    void compute() throws IOException {
      /**
       * request length is 0 or the file is empty, return one with the
       * magic entry that matches what previous hdfs versions return.
       */
      if (locatedBlocks == null || locatedBlocks.isEmpty()) {
        // Explicitly specified here in case the default DataOutputBuffer
        // buffer length value is changed in future. This matters because the
        // fixed value 32 has to be used to repeat the magic value for previous
        // HDFS version.
        final int lenOfZeroBytes = 32;
        byte[] emptyBlockMd5 = new byte[lenOfZeroBytes];
        MD5Hash fileMD5 = MD5Hash.digest(emptyBlockMd5);
        fileChecksum =  new MD5MD5CRC32GzipFileChecksum(0, 0, fileMD5);
      } else {
        checksumBlocks();
        fileChecksum = makeFinalResult();
      }
    }

    /**
     * Compute block checksums block by block and append the raw bytes of the
     * block checksums into getBlockChecksumBuf().
     *
     * @throws IOException
     */
    abstract void checksumBlocks() throws IOException;

    /**
     * Make final file checksum result given the per-block or per-block-group
     * checksums collected into getBlockChecksumBuf().
     */
    FileChecksum makeFinalResult() throws IOException {
      switch (combineMode) {
      case MD5MD5CRC:
        return makeMd5CrcResult();
      case COMPOSITE_CRC:
        return makeCompositeCrcResult();
      default:
        throw new IOException("Unknown ChecksumCombineMode: " + combineMode);
      }
    }

    FileChecksum makeMd5CrcResult() {
      //compute file MD5
      final MD5Hash fileMD5 = MD5Hash.digest(blockChecksumBuf.getData());
      switch (crcType) {
      case CRC32:
        return new MD5MD5CRC32GzipFileChecksum(bytesPerCRC,
            crcPerBlock, fileMD5);
      case CRC32C:
        return new MD5MD5CRC32CastagnoliFileChecksum(bytesPerCRC,
            crcPerBlock, fileMD5);
      default:
        // we will get here when crcType is "NULL".
        return null;
      }
    }

    FileChecksum makeCompositeCrcResult() throws IOException {
      long blockSizeHint = 0;
      if (locatedBlocks.size() > 0) {
        blockSizeHint = locatedBlocks.get(0).getBlockSize();
      }
      CrcComposer crcComposer =
          CrcComposer.newCrcComposer(getCrcType(), blockSizeHint);
      byte[] blockChecksumBytes = blockChecksumBuf.getData();

      long sumBlockLengths = 0;
      for (int i = 0; i < locatedBlocks.size() - 1; ++i) {
        LocatedBlock block = locatedBlocks.get(i);
        // For everything except the last LocatedBlock, we expect getBlockSize()
        // to accurately reflect the number of file bytes digested in the block
        // checksum.
        sumBlockLengths += block.getBlockSize();
        int blockCrc = CrcUtil.readInt(blockChecksumBytes, i * 4);

        crcComposer.update(blockCrc, block.getBlockSize());
        LOG.debug(
            "Added blockCrc 0x{} for block index {} of size {}",
            Integer.toString(blockCrc, 16), i, block.getBlockSize());
      }

      // NB: In some cases the located blocks have their block size adjusted
      // explicitly based on the requested length, but not all cases;
      // these numbers may or may not reflect actual sizes on disk.
      long reportedLastBlockSize =
          blockLocations.getLastLocatedBlock().getBlockSize();
      long consumedLastBlockLength = reportedLastBlockSize;
      if (length - sumBlockLengths < reportedLastBlockSize) {
        LOG.warn(
            "Last block length {} is less than reportedLastBlockSize {}",
            length - sumBlockLengths, reportedLastBlockSize);
        consumedLastBlockLength = length - sumBlockLengths;
      }
      // NB: blockChecksumBytes.length may be much longer than actual bytes
      // written into the DataOutput.
      int lastBlockCrc = CrcUtil.readInt(
          blockChecksumBytes, 4 * (locatedBlocks.size() - 1));
      crcComposer.update(lastBlockCrc, consumedLastBlockLength);
      LOG.debug(
          "Added lastBlockCrc 0x{} for block index {} of size {}",
          Integer.toString(lastBlockCrc, 16),
          locatedBlocks.size() - 1,
          consumedLastBlockLength);

      int compositeCrc = CrcUtil.readInt(crcComposer.digest(), 0);
      return new CompositeCrcFileChecksum(
          compositeCrc, getCrcType(), bytesPerCRC);
    }
    /**
     * Close an IO stream pair.
     */
    void close(IOStreamPair pair) {
      if (pair != null) {
        IOUtils.closeStream(pair.in);
        IOUtils.closeStream(pair.out);
      }
    }
  }

  /**
   * Replicated file checksum computer.
   */
  static class ReplicatedFileChecksumComputer extends FileChecksumComputer {
    private int blockIdx;

    ReplicatedFileChecksumComputer(String src, long length,
                                   LocatedBlocks blockLocations,
                                   ClientProtocol namenode,
                                   DFSClient client,
                                   ChecksumCombineMode combineMode)
        throws IOException {
      super(src, length, blockLocations, namenode, client, combineMode);
    }

    @Override
    void checksumBlocks() throws IOException {
      // get block checksum for each block
      for (blockIdx = 0;
           blockIdx < getLocatedBlocks().size() && getRemaining() >= 0;
           blockIdx++) {
        if (isRefetchBlocks()) {  // refetch to get fresh tokens
          refetchBlocks();
        }

        LocatedBlock locatedBlock = getLocatedBlocks().get(blockIdx);

        if (!checksumBlock(locatedBlock)) {
          throw new PathIOException(
              getSrc(), "Fail to get block MD5 for " + locatedBlock);
        }
      }
    }

    /**
     * Return true when sounds good to continue or retry, false when severe
     * condition or totally failed.
     */
    private boolean checksumBlock(LocatedBlock locatedBlock) {
      ExtendedBlock block = locatedBlock.getBlock();
      if (getRemaining() < block.getNumBytes()) {
        block.setNumBytes(getRemaining());
      }
      //TODO: Read the data and compute checksum
      return false;
    }
  }
}
