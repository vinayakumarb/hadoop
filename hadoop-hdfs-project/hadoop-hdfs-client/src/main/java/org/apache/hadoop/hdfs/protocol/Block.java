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

import java.io.*;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.StringUtils;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**************************************************
 * A Block is a Hadoop FS primitive, identified by a
 * long.
 *
 **************************************************/
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class Block implements Writable, Comparable<Block> {
  public static final int BLOCK_ID_LENGTH = 24;
  public static final byte[] EMPTY_BLOCK_ID = new byte[BLOCK_ID_LENGTH];
  public static final String BLOCK_FILE_PREFIX = "blk_";
  public static final String METADATA_EXTENSION = ".meta";
  static {                                      // register a ctor
    WritableFactories.setFactory(Block.class, new WritableFactory() {
      @Override
      public Writable newInstance() { return new Block(); }
    });
  }

  public static final Pattern blockFilePattern = Pattern
      .compile(BLOCK_FILE_PREFIX + "(-??\\d++)$");
  public static final Pattern metaFilePattern = Pattern
      .compile(BLOCK_FILE_PREFIX + "(-??\\d++)_(\\d++)\\" + METADATA_EXTENSION
          + "$");
  public static final Pattern metaOrBlockFilePattern = Pattern
      .compile(BLOCK_FILE_PREFIX + "(-??\\d++)(_(\\d++)\\" + METADATA_EXTENSION
          + ")?$");

  public static long filename2id(String name) {
    Matcher m = blockFilePattern.matcher(name);
    return m.matches() ? Long.parseLong(m.group(1)) : 0;
  }

  /**
   * Get the blockId from the name of the meta or block file
   */
  public static long getBlockId(String metaOrBlockFile) {
    Matcher m = metaOrBlockFilePattern.matcher(metaOrBlockFile);
    return m.matches() ? Long.parseLong(m.group(1)) : 0;
  }

  private byte[] blockId;
  private long numBytes;

  public Block() {this(EMPTY_BLOCK_ID, 0, 0);}

  public Block(final byte[] blkid, final long len, final long generationStamp) {
    set(blkid, len, generationStamp);
  }

  public Block(final byte[] blkid) {
    this(blkid, 0, HdfsConstants.GRANDFATHER_GENERATION_STAMP);
  }

  public Block(Block blk) {
    this(blk.blockId, blk.numBytes, 0L);
  }

  public void set(byte[] blkid, long len, long genStamp) {
    this.blockId = blkid;
    this.numBytes = len;
  }
  /**
   */
  public byte[] getBlockId() {
    return blockId;
  }

  public void setBlockId(byte[] bid) {
    Preconditions.checkArgument(bid != null && bid.length == BLOCK_ID_LENGTH);
    blockId = bid;
  }

  /**
   */
  public String getBlockName() {
    return new StringBuilder().append(BLOCK_FILE_PREFIX)
        .append(blockId).toString();
  }

  /**
   */
  public long getNumBytes() {
    return numBytes;
  }
  public void setNumBytes(long len) {
    this.numBytes = len;
  }

  public long getGenerationStamp() {
    return 0L;
  }

  public void setGenerationStamp(long stamp) {}

  /**
   * A helper method to output the string representation of the Block portion of
   * a derived class' instance.
   *
   * @param b the target object
   * @return the string representation of the block
   */
  public static String toString(final Block b) {
    StringBuilder sb = new StringBuilder();
    sb.append(BLOCK_FILE_PREFIX).
       append(b.blockId).append("_").
       append(0);
    return sb.toString();
  }

  /**
   */
  @Override
  public String toString() {
    return toString(this);
  }

  public void appendStringTo(StringBuilder sb) {
    sb.append(BLOCK_FILE_PREFIX)
      .append(StringUtils.byteToHexString(blockId));
  }


  /////////////////////////////////////
  // Writable
  /////////////////////////////////////
  @Override // Writable
  public void write(DataOutput out) throws IOException {
    writeHelper(out);
  }

  @Override // Writable
  public void readFields(DataInput in) throws IOException {
    readHelper(in);
  }

  final void writeHelper(DataOutput out) throws IOException {
    out.write(blockId);
    out.writeLong(numBytes);
  }

  final void readHelper(DataInput in) throws IOException {
    byte[] id = new byte[BLOCK_ID_LENGTH];
    in.readFully(id);
    this.blockId = id;
    this.numBytes = in.readLong();
    if (numBytes < 0) {
      throw new IOException("Unexpected block size: " + numBytes);
    }
  }

  @Override // Comparable
  public int compareTo(@Nonnull Block b) {
    for (int i = 0; i < blockId.length; i++) {
      if (blockId[i] == b.blockId[i]) {
        continue;
      }
      return blockId[i] < b.blockId[i] ? -1 : 1;
    }
    return 0;
  }

  @Override // Object
  public boolean equals(Object o) {
    return this == o || o instanceof Block && compareTo((Block) o) == 0;
  }

  /**
   * @return true if the two blocks have the same block ID and the same
   * generation stamp, or if both blocks are null.
   */
  public static boolean matchingIdAndGenStamp(Block a, Block b) {
    if (a == b) return true; // same block, or both null
    // only one null
    return !(a == null || b == null) && Arrays.equals(a.blockId, b.blockId);
  }

  @Override // Object
  public int hashCode() {
    return Arrays.hashCode(blockId);
  }

  @VisibleForTesting
  public static byte[] generateBlockId(long id) {
    byte[] b = new byte[BLOCK_ID_LENGTH];
    for (int i = 0; i < Long.BYTES; i++) {
      b[i] = (byte) (id >>> 8 * (Long.BYTES - (i + 1)));
    }
    return b;
  }
}
