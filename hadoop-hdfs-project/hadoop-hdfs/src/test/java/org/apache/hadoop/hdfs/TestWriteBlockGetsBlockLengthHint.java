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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;


/**
 * Test to verify that the DFSClient passes the expected block length to
 * the DataNode via DataTransferProtocol.
 */
public class TestWriteBlockGetsBlockLengthHint {
  static final long DEFAULT_BLOCK_LENGTH = 1024;
  static final long EXPECTED_BLOCK_LENGTH = DEFAULT_BLOCK_LENGTH * 2;

  @Test
  public void blockLengthHintIsPropagated() throws IOException {
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    final Path path = new Path("/" + METHOD_NAME + ".dat");

    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_LENGTH);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();

    try {
      cluster.waitActive();

      // FsDatasetChecker#createRbw asserts during block creation if the test
      // fails.
      DFSTestUtil.createFile(
          cluster.getFileSystem(),
          path,
          4096,  // Buffer size.
          EXPECTED_BLOCK_LENGTH,
          EXPECTED_BLOCK_LENGTH,
          (short) 1,
          0x1BAD5EED);
    } finally {
      cluster.shutdown();
    }
  }
}
