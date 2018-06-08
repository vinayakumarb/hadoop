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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.server.namenode.NamenodeFsck.ErasureCodingResult;
import org.apache.hadoop.hdfs.server.namenode.NamenodeFsck.ReplicationResult;
import org.apache.hadoop.hdfs.server.namenode.NamenodeFsck.Result;
import org.apache.hadoop.hdfs.tools.DFSck;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * A JUnit test for doing fsck.
 */
public class TestFsck {
  private static final Log LOG =
      LogFactory.getLog(TestFsck.class.getName());

  static final String AUDITLOG_FILE =
      GenericTestUtils.getTempPath("TestFsck-audit.log");
  
  // Pattern for: 
  // allowed=true ugi=name ip=/address cmd=FSCK src=/ dst=null perm=null
  static final Pattern FSCK_PATTERN = Pattern.compile(
      "allowed=.*?\\s" +
      "ugi=.*?\\s" + 
      "ip=/\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\s" + 
      "cmd=fsck\\ssrc=\\/\\sdst=null\\s" + 
      "perm=null\\s" + "proto=.*");
  static final Pattern GET_FILE_INFO_PATTERN = Pattern.compile(
      "allowed=.*?\\s" +
      "ugi=.*?\\s" + 
      "ip=/\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\s" + 
      "cmd=getfileinfo\\ssrc=\\/\\sdst=null\\s" + 
      "perm=null\\s" + "proto=.*");

  static final Pattern NUM_MISSING_BLOCKS_PATTERN = Pattern.compile(
      ".*Missing blocks:\t\t([0123456789]*).*");

  static final Pattern NUM_CORRUPT_BLOCKS_PATTERN = Pattern.compile(
      ".*Corrupt blocks:\t\t([0123456789]*).*");
  
  private static final String LINE_SEPARATOR =
      System.getProperty("line.separator");

  static String runFsck(Configuration conf, int expectedErrCode, 
                        boolean checkErrorCode, String... path)
                        throws Exception {
    ByteArrayOutputStream bStream = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(bStream, true);
    GenericTestUtils.setLogLevel(FSPermissionChecker.LOG, Level.ALL);
    int errCode = ToolRunner.run(new DFSck(conf, out), path);
    LOG.info("OUTPUT = " + bStream.toString());
    if (checkErrorCode) {
      assertEquals(expectedErrCode, errCode);
    }
    GenericTestUtils.setLogLevel(FSPermissionChecker.LOG, Level.INFO);
    return bStream.toString();
  }

  private MiniDFSCluster cluster = null;
  private Configuration conf = null;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
  }

  @After
  public void tearDown() throws Exception {
    shutdownCluster();
  }

  private void shutdownCluster() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /** do fsck. */
  @Test
  public void testFsck() throws Exception {
    DFSTestUtil util = new DFSTestUtil.Builder().setName("TestFsck").
        setNumFiles(20).build();
    FileSystem fs = null;
    final long precision = 1L;
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY,
        precision);
    File builderBaseDir = new File(GenericTestUtils.getRandomizedTempPath());
    cluster = new MiniDFSCluster.Builder(conf, builderBaseDir)
        .numDataNodes(4).build();
    fs = cluster.getFileSystem();
    final String fileName = "/srcdat";
    util.createFiles(fs, fileName);
    util.waitReplication(fs, fileName, (short)3);
    final Path file = new Path(fileName);
    long aTime = fs.getFileStatus(file).getAccessTime();
    Thread.sleep(precision);
    setupAuditLogs();
    String outStr = runFsck(conf, 0, true, "/");
    verifyAuditLogs();
    assertEquals(aTime, fs.getFileStatus(file).getAccessTime());
    System.out.println(outStr);
    assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
    shutdownCluster();

    // restart the cluster; bring up namenode but not the data nodes
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(0).format(false).build();
    outStr = runFsck(conf, 1, true, "/");
    // expect the result is corrupt
    assertTrue(outStr.contains(NamenodeFsck.CORRUPT_STATUS));
    System.out.println(outStr);

    // bring up data nodes & cleanup cluster
    cluster.startDataNodes(conf, 4, true, null, null);
    cluster.waitActive();
    cluster.waitClusterUp();
    fs = cluster.getFileSystem();
    util.cleanup(fs, "/srcdat");
  }

  /** Sets up log4j logger for auditlogs. */
  private void setupAuditLogs() throws IOException {
    File file = new File(AUDITLOG_FILE);
    if (file.exists()) {
      file.delete();
    }
    Logger logger = ((Log4JLogger) FSNamesystem.auditLog).getLogger();
    logger.setLevel(Level.INFO);
    PatternLayout layout = new PatternLayout("%m%n");
    RollingFileAppender appender =
        new RollingFileAppender(layout, AUDITLOG_FILE);
    logger.addAppender(appender);
  }
  
  private void verifyAuditLogs() throws IOException {
    // Turn off the logs
    Logger logger = ((Log4JLogger) FSNamesystem.auditLog).getLogger();
    logger.setLevel(Level.OFF);

    BufferedReader reader = null;
    try {
      // Audit log should contain one getfileinfo and one fsck
      reader = new BufferedReader(new FileReader(AUDITLOG_FILE));
      String line;

      // one extra getfileinfo stems from resolving the path
      //
      for (int i = 0; i < 2; i++) {
        line = reader.readLine();
        assertNotNull(line);
        assertTrue("Expected getfileinfo event not found in audit log",
            GET_FILE_INFO_PATTERN.matcher(line).matches());
      }
      line = reader.readLine();
      assertNotNull(line);
      assertTrue("Expected fsck event not found in audit log", FSCK_PATTERN
          .matcher(line).matches());
      assertNull("Unexpected event in audit log", reader.readLine());
    } finally {
      // Close the reader and remove the appender to release the audit log file
      // handle after verifying the content of the file.
      if (reader != null) {
        reader.close();
      }
      if (logger != null) {
        logger.removeAllAppenders();
      }
    }
  }
  
  @Test
  public void testFsckNonExistent() throws Exception {
    DFSTestUtil util = new DFSTestUtil.Builder().setName("TestFsck").
        setNumFiles(20).build();
    FileSystem fs = null;
    File builderBaseDir = new File(GenericTestUtils.getRandomizedTempPath());
    cluster = new MiniDFSCluster.Builder(conf, builderBaseDir)
        .numDataNodes(4).build();
    fs = cluster.getFileSystem();
    util.createFiles(fs, "/srcdat");
    util.waitReplication(fs, "/srcdat", (short)3);
    String outStr = runFsck(conf, 0, true, "/non-existent");
    assertEquals(-1, outStr.indexOf(NamenodeFsck.HEALTHY_STATUS));
    System.out.println(outStr);
    util.cleanup(fs, "/srcdat");
  }

  /** Test fsck with permission set on inodes. */
  @Test
  public void testFsckPermission() throws Exception {
    final DFSTestUtil util = new DFSTestUtil.Builder().
        setName(getClass().getSimpleName()).setNumFiles(20).build();

    // Create a cluster with the current user, write some files
    File builderBaseDir = new File(GenericTestUtils.getRandomizedTempPath());
    cluster = new MiniDFSCluster.Builder(conf, builderBaseDir)
        .numDataNodes(4).build();
    final MiniDFSCluster c2 = cluster;
    final String dir = "/dfsck";
    final Path dirpath = new Path(dir);
    final FileSystem fs = c2.getFileSystem();

    util.createFiles(fs, dir);
    util.waitReplication(fs, dir, (short) 3);
    fs.setPermission(dirpath, new FsPermission((short) 0700));

    // run DFSck as another user, should fail with permission issue
    UserGroupInformation fakeUGI = UserGroupInformation.createUserForTesting(
        "ProbablyNotARealUserName", new String[] {"ShangriLa"});
    fakeUGI.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        System.out.println(runFsck(conf, -1, true, dir));
        return null;
      }
    });

    // set permission and try DFSck again as the fake user, should succeed
    fs.setPermission(dirpath, new FsPermission((short) 0777));
    fakeUGI.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        final String outStr = runFsck(conf, 0, true, dir);
        System.out.println(outStr);
        assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
        return null;
      }
    });

    util.cleanup(fs, dir);
  }

  @Test
  public void testFsckOpenFiles() throws Exception {
    DFSTestUtil util = new DFSTestUtil.Builder().setName("TestFsck").
        setNumFiles(4).build();
    FileSystem fs = null;
    File builderBaseDir = new File(GenericTestUtils.getRandomizedTempPath());
    cluster = new MiniDFSCluster.Builder(conf, builderBaseDir)
        .numDataNodes(4).build();
    String topDir = "/srcdat";
    String randomString = "HADOOP  ";
    fs = cluster.getFileSystem();
    cluster.waitActive();
    util.createFiles(fs, topDir);
    util.waitReplication(fs, topDir, (short)3);
    String outStr = runFsck(conf, 0, true, "/");
    assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
    // Open a file for writing and do not close for now
    Path openFile = new Path(topDir + "/openFile");
    FSDataOutputStream out = fs.create(openFile);
    int writeCount = 0;
    while (writeCount != 100) {
      out.write(randomString.getBytes());
      writeCount++;
    }
    ((DFSOutputStream) out.getWrappedStream()).hflush();
    // We expect the filesystem to be HEALTHY and show one open file
    outStr = runFsck(conf, 0, true, topDir);
    System.out.println(outStr);
    assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
    assertFalse(outStr.contains("OPENFORWRITE"));
    // Use -openforwrite option to list open files
    outStr = runFsck(conf, 0, true, topDir, "-files", "-blocks",
        "-locations", "-openforwrite");
    System.out.println(outStr);
    assertTrue(outStr.contains("OPENFORWRITE"));
    assertTrue(outStr.contains("Under Construction Block:"));
    assertTrue(outStr.contains("openFile"));
    // Close the file
    out.close();
    // Now, fsck should show HEALTHY fs and should not show any open files
    outStr = runFsck(conf, 0, true, topDir);
    System.out.println(outStr);
    assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
    assertFalse(outStr.contains("OPENFORWRITE"));
    assertFalse(outStr.contains("Under Construction Block:"));
    util.cleanup(fs, topDir);
  }

  /** Test if fsck can return -1 in case of failure.
   * 
   * @throws Exception
   */
  @Test
  public void testFsckError() throws Exception {
    // bring up a one-node cluster
    File builderBaseDir = new File(GenericTestUtils.getRandomizedTempPath());
    cluster = new MiniDFSCluster.Builder(conf, builderBaseDir).build();
    String fileName = "/test.txt";
    Path filePath = new Path(fileName);
    FileSystem fs = cluster.getFileSystem();

    // create a one-block file
    DFSTestUtil.createFile(fs, filePath, 1L, (short)1, 1L);
    DFSTestUtil.waitReplication(fs, filePath, (short)1);

    // intentionally corrupt NN data structure
    INodeFile node = (INodeFile) cluster.getNamesystem().dir.getINode(
        fileName, DirOp.READ);
    final BlockInfo[] blocks = node.getBlocks();
    assertEquals(blocks.length, 1);
    blocks[0].setNumBytes(-1L);  // set the block length to be negative

    // run fsck and expect a failure with -1 as the error code
    String outStr = runFsck(conf, -1, true, fileName);
    System.out.println(outStr);
    assertTrue(outStr.contains(NamenodeFsck.FAILURE_STATUS));

    // clean up file system
    fs.delete(filePath, true);
  }

  /**
   * Test for checking fsck command on illegal arguments should print the proper
   * usage.
   */
  @Test
  public void testToCheckTheFsckCommandOnIllegalArguments() throws Exception {
    // bring up a one-node cluster
    File builderBaseDir = new File(GenericTestUtils.getRandomizedTempPath());
    cluster = new MiniDFSCluster.Builder(conf, builderBaseDir).build();
    String fileName = "/test.txt";
    Path filePath = new Path(fileName);
    FileSystem fs = cluster.getFileSystem();

    // create a one-block file
    DFSTestUtil.createFile(fs, filePath, 1L, (short) 1, 1L);
    DFSTestUtil.waitReplication(fs, filePath, (short) 1);

    // passing illegal option
    String outStr = runFsck(conf, -1, true, fileName, "-thisIsNotAValidFlag");
    System.out.println(outStr);
    assertTrue(!outStr.contains(NamenodeFsck.HEALTHY_STATUS));

    // passing multiple paths are arguments
    outStr = runFsck(conf, -1, true, "/", fileName);
    System.out.println(outStr);
    assertTrue(!outStr.contains(NamenodeFsck.HEALTHY_STATUS));
    // clean up file system
    fs.delete(filePath, true);
  }
  
  /** Test fsck with FileNotFound. */
  @Test
  public void testFsckFileNotFound() throws Exception {

    // Number of replicas to actually start
    final short numReplicas = 1;

    NameNode namenode = mock(NameNode.class);
    NetworkTopology nettop = mock(NetworkTopology.class);
    Map<String, String[]> pmap = new HashMap<>();
    Writer result = new StringWriter();
    PrintWriter out = new PrintWriter(result, true);
    InetAddress remoteAddress = InetAddress.getLocalHost();
    FSNamesystem fsName = mock(FSNamesystem.class);
    FSDirectory fsd = mock(FSDirectory.class);
    BlockManager blockManager = mock(BlockManager.class);
    INodesInPath iip = mock(INodesInPath.class);

    when(namenode.getNamesystem()).thenReturn(fsName);
    when(fsName.getBlockManager()).thenReturn(blockManager);
    when(fsName.getFSDirectory()).thenReturn(fsd);
    when(fsd.getFSNamesystem()).thenReturn(fsName);
    when(fsd.resolvePath(anyObject(), anyString(), any(DirOp.class))).thenReturn(iip);

    NamenodeFsck fsck = new NamenodeFsck(conf, namenode, pmap, out,
        remoteAddress);

    String pathString = "/tmp/testFile";

    HdfsFileStatus file = new HdfsFileStatus.Builder()
        .length(123L)
        .replication(1)
        .blocksize(128 * 1024L)
        .mtime(123123123L)
        .atime(123123120L)
        .perm(FsPermission.getDefault())
        .owner("foo")
        .group("bar")
        .path(DFSUtil.string2Bytes(pathString))
        .fileId(312321L)
        .children(1)
        .storagePolicy(HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED)
        .build();
    Result replRes = new ReplicationResult(conf);
    Result ecRes = new ErasureCodingResult(conf);

    try {
      fsck.check(pathString, file, replRes, ecRes);
    } catch (Exception e) {
      fail("Unexpected exception " + e.getMessage());
    }
    assertTrue(replRes.isHealthy());
  }

  /** Test fsck with symlinks in the filesystem. */
  @Test
  public void testFsckSymlink() throws Exception {
    final DFSTestUtil util = new DFSTestUtil.Builder().
        setName(getClass().getSimpleName()).setNumFiles(1).build();

    FileSystem fs = null;
    final long precision = 1L;
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY,
        precision);
    File builderBaseDir = new File(GenericTestUtils.getRandomizedTempPath());
    cluster = new MiniDFSCluster.Builder(conf, builderBaseDir)
        .numDataNodes(4).build();
    fs = cluster.getFileSystem();
    final String fileName = "/srcdat";
    util.createFiles(fs, fileName);
    final FileContext fc = FileContext.getFileContext(
        cluster.getConfiguration(0));
    final Path file = new Path(fileName);
    final Path symlink = new Path("/srcdat-symlink");
    fc.createSymlink(file, symlink, false);
    util.waitReplication(fs, fileName, (short)3);
    long aTime = fc.getFileStatus(symlink).getAccessTime();
    Thread.sleep(precision);
    setupAuditLogs();
    String outStr = runFsck(conf, 0, true, "/");
    verifyAuditLogs();
    assertEquals(aTime, fc.getFileStatus(symlink).getAccessTime());
    System.out.println(outStr);
    assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
    assertTrue(outStr.contains("Total symlinks:\t\t1"));
    util.cleanup(fs, fileName);
  }

  /**
   * Test for including the snapshot files in fsck report.
   */
  @Test
  public void testFsckForSnapshotFiles() throws Exception {
    File builderBaseDir = new File(GenericTestUtils.getRandomizedTempPath());
    cluster = new MiniDFSCluster.Builder(conf, builderBaseDir).numDataNodes(1)
        .build();
    String runFsck = runFsck(conf, 0, true, "/", "-includeSnapshots",
        "-files");
    assertTrue(runFsck.contains("HEALTHY"));
    final String fileName = "/srcdat";
    DistributedFileSystem hdfs = cluster.getFileSystem();
    Path file1 = new Path(fileName);
    DFSTestUtil.createFile(hdfs, file1, 1024, (short) 1, 1000L);
    hdfs.allowSnapshot(new Path("/"));
    hdfs.createSnapshot(new Path("/"), "mySnapShot");
    runFsck = runFsck(conf, 0, true, "/", "-includeSnapshots", "-files");
    assertTrue(runFsck.contains("/.snapshot/mySnapShot/srcdat"));
    runFsck = runFsck(conf, 0, true, "/", "-files");
    assertFalse(runFsck.contains("mySnapShot"));
  }

  /**
   * Test for blockIdCK.
   */

  @Test
  public void testBlockIdCK() throws Exception {

    final short replFactor = 2;
    short numDn = 2;
    final long blockSize = 512;

    String[] racks = {"/rack1", "/rack2"};
    String[] hosts = {"host1", "host2"};

    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 2);

    DistributedFileSystem dfs = null;
    File builderBaseDir = new File(GenericTestUtils.getRandomizedTempPath());
    cluster = new MiniDFSCluster.Builder(conf, builderBaseDir)
        .numDataNodes(numDn).hosts(hosts).racks(racks).build();

    assertNotNull("Failed Cluster Creation", cluster);
    cluster.waitClusterUp();
    dfs = cluster.getFileSystem();
    assertNotNull("Failed to get FileSystem", dfs);

    DFSTestUtil util = new DFSTestUtil.Builder().
        setName(getClass().getSimpleName()).setNumFiles(1).build();
    //create files
    final String pathString = new String("/testfile");
    final Path path = new Path(pathString);
    util.createFile(dfs, path, 1024, replFactor, 1000L);
    util.waitReplication(dfs, path, replFactor);
    StringBuilder sb = new StringBuilder();
    for (LocatedBlock lb: util.getAllBlocks(dfs, path)){
      sb.append(lb.getBlock().getLocalBlock().getBlockName()+" ");
    }
    String[] bIds = sb.toString().split(" ");

    //run fsck
    //illegal input test
    String runFsckResult = runFsck(conf, 0, true, "/", "-blockId",
        "not_a_block_id");
    assertTrue(runFsckResult.contains("Incorrect blockId format:"));

    //general test
    runFsckResult = runFsck(conf, 0, true, "/", "-blockId", sb.toString());
    assertTrue(runFsckResult.contains(bIds[0]));
    assertTrue(runFsckResult.contains(bIds[1]));
    assertTrue(runFsckResult.contains(
        "Block replica on datanode/rack: host1/rack1 is HEALTHY"));
    assertTrue(runFsckResult.contains(
        "Block replica on datanode/rack: host2/rack2 is HEALTHY"));
  }

  private void writeFile(final DistributedFileSystem dfs,
      Path dir, String fileName) throws IOException {
    Path filePath = new Path(dir.toString() + Path.SEPARATOR + fileName);
    final FSDataOutputStream out = dfs.create(filePath);
    out.writeChars("teststring");
    out.close();
  }

  private void writeFile(final DistributedFileSystem dfs,
      String dirName, String fileName, String storagePolicy)
      throws IOException {
    Path dirPath = new Path(dirName);
    dfs.mkdirs(dirPath);
    dfs.setStoragePolicy(dirPath, storagePolicy);
    writeFile(dfs, dirPath, fileName);
  }

  /**
   * Test storage policy display.
   */
  @Test
  public void testStoragePoliciesCK() throws Exception {
    File builderBaseDir = new File(GenericTestUtils.getRandomizedTempPath());
    cluster = new MiniDFSCluster.Builder(conf, builderBaseDir)
        .numDataNodes(3)
        .storageTypes(
            new StorageType[] {StorageType.DISK, StorageType.ARCHIVE})
        .build();
    cluster.waitActive();
    final DistributedFileSystem dfs = cluster.getFileSystem();
    writeFile(dfs, "/testhot", "file", "HOT");
    writeFile(dfs, "/testwarm", "file", "WARM");
    writeFile(dfs, "/testcold", "file", "COLD");
    String outStr = runFsck(conf, 0, true, "/", "-storagepolicies");
    assertTrue(outStr.contains("DISK:3(HOT)"));
    assertTrue(outStr.contains("DISK:1,ARCHIVE:2(WARM)"));
    assertTrue(outStr.contains("ARCHIVE:3(COLD)"));
    assertTrue(outStr.contains("All blocks satisfy specified storage policy."));
    dfs.setStoragePolicy(new Path("/testhot"), "COLD");
    dfs.setStoragePolicy(new Path("/testwarm"), "COLD");
    outStr = runFsck(conf, 0, true, "/", "-storagepolicies");
    assertTrue(outStr.contains("DISK:3(HOT)"));
    assertTrue(outStr.contains("DISK:1,ARCHIVE:2(WARM)"));
    assertTrue(outStr.contains("ARCHIVE:3(COLD)"));
    assertFalse(outStr.contains(
        "All blocks satisfy specified storage policy."));
  }
}
