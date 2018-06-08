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

import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Test;



/**
 * This tests InterDataNodeProtocol for block handling. 
 */
public class TestNamenodeCapacityReport {
  private static final Log LOG = LogFactory.getLog(TestNamenodeCapacityReport.class);

  /**
   * The following test first creates a file.
   * It verifies the block information from a datanode.
   * Then, it updates the block with new information and verifies again. 
   */
  @Test
  public void testVolumeSize() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      final FSNamesystem namesystem = cluster.getNamesystem();
      long used, remaining, configCapacity;
      float percentUsed, percentRemaining;
      
      long diskCapacity = 0L;//TODO
      
      configCapacity = namesystem.getCapacityTotal();
      used = namesystem.getCapacityUsed();
      remaining = namesystem.getCapacityRemaining();
      percentUsed = namesystem.getPercentUsed();
      percentRemaining = namesystem.getPercentRemaining();
      LOG.info("Data node directory " + cluster.getDataDirectory());
           
      LOG.info("Name node diskCapacity " + diskCapacity + " configCapacity "
          + configCapacity + " used " + used
          + " remaining " + remaining
          + " remaining " + remaining + " percentUsed " + percentUsed 
          + " percentRemaining " + percentRemaining);
      
      // Ensure new total capacity reported excludes the reserved space
      assertTrue(configCapacity == diskCapacity);
      
      // Ensure new total capacity reported excludes the reserved space
      // There will be 5% space reserved in ext filesystem which is not
      // considered.
      assertTrue(configCapacity >= (used + remaining));

      // Ensure percent used is calculated based on used and present capacity
      assertTrue(percentUsed == DFSUtilClient.getPercentUsed(used,
                                                             configCapacity));

      // Ensure percent used is calculated based on used and present capacity
      assertTrue(percentRemaining == ((float)remaining * 100.0f)/(float)configCapacity);

      //Adding testcase for non-dfs used where we need to consider
      // reserved replica also.
      final int fileCount = 5;
      final DistributedFileSystem fs = cluster.getFileSystem();
      // create streams and hsync to force datastreamers to start
      DFSOutputStream[] streams = new DFSOutputStream[fileCount];
      for (int i=0; i < fileCount; i++) {
        streams[i] = (DFSOutputStream)fs.create(new Path("/f"+i))
            .getWrappedStream();
        streams[i].write("1".getBytes());
        streams[i].hsync();
      }
      assertTrue(configCapacity > (namesystem.getCapacityUsed() + namesystem
          .getCapacityRemaining()));
      // There is a chance that nonDFS usage might have slightly due to
      // testlogs, So assume 1MB other files used within this gap
      assertTrue(
          (namesystem.getCapacityUsed() + namesystem.getCapacityRemaining()
              + fileCount * fs
              .getDefaultBlockSize()) - configCapacity < 1 * 1024);
    }
    finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  private static final float EPSILON = 0.0001f;
}
