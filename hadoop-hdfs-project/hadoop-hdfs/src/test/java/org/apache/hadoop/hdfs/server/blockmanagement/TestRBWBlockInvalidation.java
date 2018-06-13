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

import static org.junit.Assert.assertEquals;

import java.io.Closeable;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

/**
 * Test when RBW block is removed. Invalidation of the corrupted block happens
 * and then the under replicated block gets replicated to the datanode.
 */
public class TestRBWBlockInvalidation {
  private static final Log LOG = LogFactory.getLog(TestRBWBlockInvalidation.class);

  /**
   * Regression test for HDFS-4799, a case where, upon restart, if there
   * were RWR replicas with out-of-date genstamps, the NN could accidentally
   * delete good replicas instead of the bad replicas.
   */
  @Test(timeout=120000)
  public void testRWRInvalidation() throws Exception {
    Configuration conf = new HdfsConfiguration();

    // Speed up the test a bit with faster heartbeats.
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);

    int numFiles = 10;
    // Test with a bunch of separate files, since otherwise the test may
    // fail just due to "good luck", even if a bug is present.
    List<Path> testPaths = Lists.newArrayList();
    for (int i = 0; i < numFiles; i++) {
      testPaths.add(new Path("/test" + i));
    }
    
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .build();
    try {
      List<FSDataOutputStream> streams = Lists.newArrayList();
      try {
        // Open the test files and write some data to each
        for (Path path : testPaths) {
          FSDataOutputStream out = cluster.getFileSystem().create(path, (short)2);
          streams.add(out);

          out.writeBytes("old gs data\n");
          out.hflush();
        }

        for (Path path : testPaths) {
          DFSTestUtil.waitReplication(cluster.getFileSystem(), path, (short)2);
        }

        // Write some more data and flush again. This data will only
        // be in the latter genstamp copy of the blocks.
        for (int i = 0; i < streams.size(); i++) {
          Path path = testPaths.get(i);
          FSDataOutputStream out = streams.get(i);

          out.writeBytes("new gs data\n");
          out.hflush();

          // Set replication so that only one node is necessary for this block,
          // and close it.
          cluster.getFileSystem().setReplication(path, (short)1);
          out.close();
        }

        for (Path path : testPaths) {
          DFSTestUtil.waitReplication(cluster.getFileSystem(), path, (short)1);
        }

        // Upon restart, there will be two replicas, one with an old genstamp
        // and one current copy. This test wants to ensure that the old genstamp
        // copy is the one that is deleted.

        LOG.info("=========================== restarting cluster");
        cluster.restartNameNode();
        
        cluster.waitActive();

        waitForNumTotalBlocks(cluster, numFiles);
        // Make sure we can still read the blocks.
        for (Path path : testPaths) {
          String ret = DFSTestUtil.readFile(cluster.getFileSystem(), path);
          assertEquals("old gs data\n" + "new gs data\n", ret);
        }
      } finally {
        IOUtils.cleanup(LOG, streams.toArray(new Closeable[0]));
      }
    } finally {
      cluster.shutdown();
    }

  }

  private void waitForNumTotalBlocks(final MiniDFSCluster cluster,
      final int numTotalBlocks) throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {

      @Override
      public Boolean get() {
        try {
          // Wait total blocks
          if (cluster.getNamesystem().getBlocksTotal() == numTotalBlocks) {
            return true;
          }
        } catch (Exception ignored) {
          // Ignore the exception
        }

        return false;
      }
    }, 1000, 60000);
  }
}
