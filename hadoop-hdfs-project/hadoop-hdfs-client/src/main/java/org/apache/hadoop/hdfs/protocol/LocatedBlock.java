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

import java.util.Arrays;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;

import com.google.common.collect.Lists;

/**
 * Associates a block with the Datanodes that contain its replicas
 * and other block metadata (E.g. the file offset associated with this
 * block, whether it is corrupt, a location is cached in memory,
 * security token, etc).
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class LocatedBlock {

  private final ExtendedBlock b;
  private long offset;  // offset of the first byte of the block in the file

  public LocatedBlock(ExtendedBlock b, long startOffset) {
    this.b = b;
    this.offset = startOffset;
  }

  public ExtendedBlock getBlock() {
    return b;
  }

  public long getStartOffset() {
    return offset;
  }

  public long getBlockSize() {
    return b.getNumBytes();
  }

  public void setStartOffset(long value) {
    this.offset = value;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{" + b
        + "; getBlockSize()=" + getBlockSize()
        + "; offset=" + offset
        + "}";
  }
}
