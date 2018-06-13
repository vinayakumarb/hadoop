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

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.server.namenode.top.TopConf;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIO.POSIX.NoMlockCacheManipulator;
import org.apache.hadoop.net.ServerSocketUtil;
import org.apache.hadoop.util.VersionInfo;
import org.junit.Test;
import org.eclipse.jetty.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.net.BindException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Class for testing {@link NameNodeMXBean} implementation
 */
public class TestNameNodeMXBean {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestNameNodeMXBean.class);

  /**
   * Used to assert equality between doubles
   */
  private static final double DELTA = 0.000001;

  static {
    NativeIO.POSIX.setCacheManipulator(new NoMlockCacheManipulator());
  }

  @SuppressWarnings({ "unchecked" })
  @Test
  public void testNameNodeMXBeanInfo() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      FSNamesystem fsn = cluster.getNameNode().namesystem;

      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanName = new ObjectName(
          "Hadoop:service=NameNode,name=NameNodeInfo");
      // get attribute "ClusterId"
      String clusterId = (String) mbs.getAttribute(mxbeanName, "ClusterId");
      assertEquals(fsn.getClusterId(), clusterId);
      // get attribute "BlockPoolId"
      String blockpoolId = (String) mbs.getAttribute(mxbeanName, 
          "BlockPoolId");
      assertEquals(fsn.getBlockPoolId(), blockpoolId);
      // get attribute "Version"
      String version = (String) mbs.getAttribute(mxbeanName, "Version");
      assertEquals(fsn.getVersion(), version);
      assertTrue(version.equals(VersionInfo.getVersion()
          + ", r" + VersionInfo.getRevision()));
      // get attribute "Used"
      Long used = (Long) mbs.getAttribute(mxbeanName, "Used");
      assertEquals(fsn.getUsed(), used.longValue());
      // get attribute "Total"
      Long total = (Long) mbs.getAttribute(mxbeanName, "Total");
      assertEquals(fsn.getTotal(), total.longValue());
      // get attribute "safemode"
      String safemode = (String) mbs.getAttribute(mxbeanName, "Safemode");
      assertEquals(fsn.getSafemode(), safemode);
      // get attribute percentremaining
      Float percentremaining = (Float) (mbs.getAttribute(mxbeanName,
          "PercentRemaining"));
      assertEquals(fsn.getPercentRemaining(), percentremaining, DELTA);
      // get attribute Totalblocks
      Long totalblocks = (Long) (mbs.getAttribute(mxbeanName, "TotalBlocks"));
      assertEquals(fsn.getTotalBlocks(), totalblocks.longValue());
      String nameJournalStatus = (String) (mbs.getAttribute(mxbeanName,
          "NameJournalStatus"));
      assertEquals("Bad value for NameJournalStatus",
          fsn.getNameJournalStatus(), nameJournalStatus);
      // get attribute JournalTransactionInfo
      String journalTxnInfo = (String) mbs.getAttribute(mxbeanName,
          "JournalTransactionInfo");
      assertEquals("Bad value for NameTxnIds", fsn.getJournalTransactionInfo(),
          journalTxnInfo);
      // get attribute "CompileInfo"
      String compileInfo = (String) mbs.getAttribute(mxbeanName, "CompileInfo");
      assertEquals("Bad value for CompileInfo", fsn.getCompileInfo(),
          compileInfo);
      // get attribute CorruptFiles
      String corruptFiles = (String) (mbs.getAttribute(mxbeanName,
          "CorruptFiles"));
      assertEquals("Bad value for CorruptFiles", fsn.getCorruptFiles(),
          corruptFiles);
      // get attribute NameDirStatuses
      String nameDirStatuses = (String) (mbs.getAttribute(mxbeanName,
          "NameDirStatuses"));
      assertEquals(fsn.getNameDirStatuses(), nameDirStatuses);
      Map<String, Map<String, String>> statusMap =
        (Map<String, Map<String, String>>) JSON.parse(nameDirStatuses);
      Collection<URI> nameDirUris = cluster.getNameDirs(0);
      for (URI nameDirUri : nameDirUris) {
        File nameDir = new File(nameDirUri);
        System.out.println("Checking for the presence of " + nameDir +
            " in active name dirs.");
        assertTrue(statusMap.get("active").containsKey(
            nameDir.getAbsolutePath()));
      }
      assertEquals(2, statusMap.get("active").size());
      assertEquals(0, statusMap.get("failed").size());

      // This will cause the first dir to fail.
      File failedNameDir = new File(nameDirUris.iterator().next());
      assertEquals(0, FileUtil.chmod(
          new File(failedNameDir, "current").getAbsolutePath(), "000"));
      cluster.getNameNodeRpc().rollEditLog();
      
      nameDirStatuses = (String) (mbs.getAttribute(mxbeanName,
          "NameDirStatuses"));
      statusMap = (Map<String, Map<String, String>>) JSON.parse(nameDirStatuses);
      for (URI nameDirUri : nameDirUris) {
        File nameDir = new File(nameDirUri);
        String expectedStatus =
            nameDir.equals(failedNameDir) ? "failed" : "active";
        System.out.println("Checking for the presence of " + nameDir +
            " in " + expectedStatus + " name dirs.");
        assertTrue(statusMap.get(expectedStatus).containsKey(
            nameDir.getAbsolutePath()));
      }
      assertEquals(1, statusMap.get("active").size());
      assertEquals(1, statusMap.get("failed").size());
      assertNull("RollingUpgradeInfo should be null when there is no rolling"
          + " upgrade", mbs.getAttribute(mxbeanName, "RollingUpgradeStatus"));
    } finally {
      if (cluster != null) {
        for (URI dir : cluster.getNameDirs(0)) {
          FileUtil.chmod(
            new File(new File(dir), "current").getAbsolutePath(), "755");
        }
        cluster.shutdown();
      }
    }
  }

  @Test(timeout=120000)
  @SuppressWarnings("unchecked")
  public void testTopUsers() throws Exception {
    final Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanNameFsns = new ObjectName(
          "Hadoop:service=NameNode,name=FSNamesystemState");
      FileSystem fs = cluster.getFileSystem();
      final Path path = new Path("/");
      final int NUM_OPS = 10;
      for (int i=0; i< NUM_OPS; i++) {
        fs.listStatus(path);
        fs.setTimes(path, 0, 1);
      }
      String topUsers =
          (String) (mbs.getAttribute(mxbeanNameFsns, "TopUserOpCounts"));
      ObjectMapper mapper = new ObjectMapper();
      Map<String, Object> map = mapper.readValue(topUsers, Map.class);
      assertTrue("Could not find map key timestamp", 
          map.containsKey("timestamp"));
      assertTrue("Could not find map key windows", map.containsKey("windows"));
      List<Map<String, List<Map<String, Object>>>> windows =
          (List<Map<String, List<Map<String, Object>>>>) map.get("windows");
      assertEquals("Unexpected num windows", 3, windows.size());
      for (Map<String, List<Map<String, Object>>> window : windows) {
        final List<Map<String, Object>> ops = window.get("ops");
        assertEquals("Unexpected num ops", 4, ops.size());
        for (Map<String, Object> op: ops) {
          if (op.get("opType").equals("datanodeReport")) {
            continue;
          }
          final long count = Long.parseLong(op.get("totalCount").toString());
          final String opType = op.get("opType").toString();
          final int expected;
          if (opType.equals(TopConf.ALL_CMDS)) {
            expected = 2 * NUM_OPS + 2;
          } else if (opType.equals("datanodeReport")) {
            expected = 2;
          } else {
            expected = NUM_OPS;
          }
          assertEquals("Unexpected total count", expected, count);
        }
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test(timeout=120000)
  public void testTopUsersDisabled() throws Exception {
    final Configuration conf = new Configuration();
    // Disable nntop
    conf.setBoolean(DFSConfigKeys.NNTOP_ENABLED_KEY, false);
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanNameFsns = new ObjectName(
          "Hadoop:service=NameNode,name=FSNamesystemState");
      FileSystem fs = cluster.getFileSystem();
      final Path path = new Path("/");
      final int NUM_OPS = 10;
      for (int i=0; i< NUM_OPS; i++) {
        fs.listStatus(path);
        fs.setTimes(path, 0, 1);
      }
      String topUsers =
          (String) (mbs.getAttribute(mxbeanNameFsns, "TopUserOpCounts"));
      assertNull("Did not expect to find TopUserOpCounts bean!", topUsers);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test(timeout=120000)
  public void testTopUsersNoPeriods() throws Exception {
    final Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.NNTOP_ENABLED_KEY, true);
    conf.set(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, "");
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanNameFsns = new ObjectName(
          "Hadoop:service=NameNode,name=FSNamesystemState");
      FileSystem fs = cluster.getFileSystem();
      final Path path = new Path("/");
      final int NUM_OPS = 10;
      for (int i=0; i< NUM_OPS; i++) {
        fs.listStatus(path);
        fs.setTimes(path, 0, 1);
      }
      String topUsers =
          (String) (mbs.getAttribute(mxbeanNameFsns, "TopUserOpCounts"));
      assertNotNull("Expected TopUserOpCounts bean!", topUsers);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test(timeout = 120000)
  public void testQueueLength() throws Exception {
    final Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanNameFs =
          new ObjectName("Hadoop:service=NameNode,name=FSNamesystem");
      int queueLength = (int) mbs.getAttribute(mxbeanNameFs, "LockQueueLength");
      assertEquals(0, queueLength);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test(timeout = 120000)
  public void testNNDirectorySize() throws Exception{
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    MiniDFSCluster cluster = null;
    for (int i = 0; i < 5; i++) {
      try{
        // Have to specify IPC ports so the NNs can talk to each other.
        int[] ports = ServerSocketUtil.getPorts(2);
        MiniDFSNNTopology topology = new MiniDFSNNTopology()
            .addNameservice(new MiniDFSNNTopology.NSConf("ns1")
                .addNN(new MiniDFSNNTopology.NNConf("nn1").setIpcPort(ports[0]))
                .addNN(
                    new MiniDFSNNTopology.NNConf("nn2").setIpcPort(ports[1])));

        cluster = new MiniDFSCluster.Builder(conf)
            .nnTopology(topology)
            .build();
        break;
      } catch (BindException e) {
        // retry if race on ports given by ServerSocketUtil#getPorts
        continue;
      }
    }
    if (cluster == null) {
      fail("failed to start mini cluster.");
    }
    FileSystem fs = null;
    try {
      cluster.waitActive();

      FSNamesystem nn0 = cluster.getNamesystem(0);
      FSNamesystem nn1 = cluster.getNamesystem(1);
      checkNNDirSize(cluster.getNameDirs(0), nn0.getNameDirSize());
      checkNNDirSize(cluster.getNameDirs(1), nn1.getNameDirSize());
      cluster.transitionToActive(0);
      fs = cluster.getFileSystem(0);
      DFSTestUtil.createFile(fs, new Path("/file"), 0, (short) 1, 0L);

      //rollEditLog
      HATestUtil.waitForStandbyToCatchUp(cluster.getNameNode(0),
          cluster.getNameNode(1));
      checkNNDirSize(cluster.getNameDirs(0), nn0.getNameDirSize());
      checkNNDirSize(cluster.getNameDirs(1), nn1.getNameDirSize());

      //Test metric after call saveNamespace
      DFSTestUtil.createFile(fs, new Path("/file"), 0, (short) 1, 0L);
      nn0.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      nn0.saveNamespace(0, 0);
      checkNNDirSize(cluster.getNameDirs(0), nn0.getNameDirSize());
    } finally {
      cluster.shutdown();
    }
  }

  @SuppressWarnings("unchecked")
  private void checkNNDirSize(Collection<URI> nameDirUris, String metric){
    Map<String, Long> nnDirMap =
        (Map<String, Long>) JSON.parse(metric);
    assertEquals(nameDirUris.size(), nnDirMap.size());
    for (URI dirUrl : nameDirUris) {
      File dir = new File(dirUrl);
      assertEquals(nnDirMap.get(dir.getAbsolutePath()).longValue(),
          FileUtils.sizeOfDirectory(dir));
    }
  }

  @Test
  public void testTotalBlocksMetrics() throws Exception {
    MiniDFSCluster cluster = null;
    FSNamesystem namesystem = null;
    DistributedFileSystem fs = null;
    try {
      Configuration conf = new HdfsConfiguration();
      int totalSize = 0;
      int blockSize = 10*1024*1024;
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);

      cluster = new MiniDFSCluster.Builder(conf)
          .build();
      namesystem = cluster.getNamesystem();
      fs = cluster.getFileSystem();
      verifyTotalBlocksMetrics(0L, namesystem.getTotalBlocks());

      // create small file
      Path replDirPath = new Path("/replicated");
      Path replFileSmall = new Path(replDirPath, "replfile_small");
      final short factor = 3;
      DFSTestUtil.createFile(fs, replFileSmall, blockSize, factor, 0);
      DFSTestUtil.waitReplication(fs, replFileSmall, factor);

      // create learge file
      Path replFileLarge = new Path(replDirPath, "replfile_large");
      DFSTestUtil.createFile(fs, replFileLarge, 2 * blockSize, factor, 0);
      DFSTestUtil.waitReplication(fs, replFileLarge, factor);

      // delete replicated files
      fs.delete(replDirPath, true);
      verifyTotalBlocksMetrics(0L, namesystem.getTotalBlocks());

    } finally {
      if (fs != null) {
        try {
          fs.close();
        } catch (Exception e) {
          throw e;
        }
      }
      if (namesystem != null) {
        try {
          namesystem.close();
        } catch (Exception e) {
          throw e;
        }
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  void verifyTotalBlocksMetrics(long expectedTotalReplicatedBlocks, long actualTotalBlocks)
      throws Exception {
    long expectedTotalBlocks = expectedTotalReplicatedBlocks;
    assertEquals("Unexpected total blocks!", expectedTotalBlocks,
        actualTotalBlocks);
  }
}
