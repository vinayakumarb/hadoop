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
package org.apache.hadoop.hdfs.tools;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.text.StrBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurationUtil;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.TestRefreshUserMappings;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.DefaultImpersonationProvider;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

/**
 * set/clrSpaceQuote are tested in {@link org.apache.hadoop.hdfs.TestQuota}.
 */
public class TestDFSAdmin {
  private static final Log LOG = LogFactory.getLog(TestDFSAdmin.class);
  private Configuration conf = null;
  private MiniDFSCluster cluster;
  private DFSAdmin admin;
  private NameNode namenode;
  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private static final PrintStream OLD_OUT = System.out;
  private static final PrintStream OLD_ERR = System.err;
  private String tempResource = null;
  private static final int NUM_DATANODES = 2;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 3);
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 512);
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR,
        GenericTestUtils.getRandomizedTempPath());
    restartCluster();

    admin = new DFSAdmin(conf);
  }

  private void redirectStream() {
    System.setOut(new PrintStream(out));
    System.setErr(new PrintStream(err));
  }

  private void resetStream() {
    out.reset();
    err.reset();
  }

  @After
  public void tearDown() throws Exception {
    try {
      System.out.flush();
      System.err.flush();
    } finally {
      System.setOut(OLD_OUT);
      System.setErr(OLD_ERR);
    }

    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }

    resetStream();
    if (tempResource != null) {
      File f = new File(tempResource);
      FileUtils.deleteQuietly(f);
      tempResource = null;
    }
  }

  private void restartCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
    cluster = new MiniDFSCluster.Builder(conf)
        .build();
    cluster.waitActive();
    namenode = cluster.getNameNode();
  }

  private void getReconfigurableProperties(String nodeType, String address,
      final List<String> outs, final List<String> errs) throws IOException {
    reconfigurationOutErrFormatter("getReconfigurableProperties", nodeType,
        address, outs, errs);
  }

  private void getReconfigurationStatus(String nodeType, String address,
      final List<String> outs, final List<String> errs) throws IOException {
    reconfigurationOutErrFormatter("getReconfigurationStatus", nodeType,
        address, outs, errs);
  }

  private void reconfigurationOutErrFormatter(String methodName,
      String nodeType, String address, final List<String> outs,
      final List<String> errs) throws IOException {
    ByteArrayOutputStream bufOut = new ByteArrayOutputStream();
    PrintStream outStream = new PrintStream(bufOut);
    ByteArrayOutputStream bufErr = new ByteArrayOutputStream();
    PrintStream errStream = new PrintStream(bufErr);

    if (methodName.equals("getReconfigurableProperties")) {
      admin.getReconfigurableProperties(
          nodeType,
          address,
          outStream,
          errStream);
    } else if (methodName.equals("getReconfigurationStatus")) {
      admin.getReconfigurationStatus(nodeType, address, outStream, errStream);
    } else if (methodName.equals("startReconfiguration")) {
      admin.startReconfiguration(nodeType, address, outStream, errStream);
    }

    scanIntoList(bufOut, outs);
    scanIntoList(bufErr, errs);
  }

  private static void scanIntoList(
      final ByteArrayOutputStream baos,
      final List<String> list) {
    final Scanner scanner = new Scanner(baos.toString());
    while (scanner.hasNextLine()) {
      list.add(scanner.nextLine());
    }
    scanner.close();
  }

  @Test(timeout = 30000)
  public void testNameNodeGetReconfigurableProperties() throws IOException {
    final String address = namenode.getHostAndPort();
    final List<String> outs = Lists.newArrayList();
    final List<String> errs = Lists.newArrayList();
    getReconfigurableProperties("namenode", address, outs, errs);
    assertEquals(6, outs.size());
    assertEquals(DFS_HEARTBEAT_INTERVAL_KEY, outs.get(1));
    assertEquals(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, outs.get(2));
    assertEquals(errs.size(), 0);
  }

  void awaitReconfigurationFinished(final String nodeType,
      final String address, final List<String> outs, final List<String> errs)
      throws TimeoutException, IOException, InterruptedException {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        outs.clear();
        errs.clear();
        try {
          getReconfigurationStatus(nodeType, address, outs, errs);
        } catch (IOException e) {
          LOG.error(String.format(
              "call getReconfigurationStatus on %s[%s] failed.", nodeType,
              address), e);
        }
        return !outs.isEmpty() && outs.get(0).contains("finished");

      }
    }, 100, 100 * 100);
  }

  @Test(timeout = 30000)
  public void testNameNodeGetReconfigurationStatus() throws IOException,
      InterruptedException, TimeoutException {
    ReconfigurationUtil ru = mock(ReconfigurationUtil.class);
    namenode.setReconfigurationUtil(ru);
    final String address = namenode.getHostAndPort();

    List<ReconfigurationUtil.PropertyChange> changes =
        new ArrayList<>();
    changes.add(new ReconfigurationUtil.PropertyChange(
        DFS_HEARTBEAT_INTERVAL_KEY, String.valueOf(6),
        namenode.getConf().get(DFS_HEARTBEAT_INTERVAL_KEY)));
    changes.add(new ReconfigurationUtil.PropertyChange(
        "randomKey", "new123", "old456"));
    when(ru.parseChangedProperties(any(Configuration.class),
        any(Configuration.class))).thenReturn(changes);
    assertThat(admin.startReconfiguration("namenode", address), is(0));

    final List<String> outs = Lists.newArrayList();
    final List<String> errs = Lists.newArrayList();
    awaitReconfigurationFinished("namenode", address, outs, errs);

    // verify change
    assertEquals(
        DFS_HEARTBEAT_INTERVAL_KEY + " has wrong value",
        6,
        namenode
          .getConf()
          .getLong(DFS_HEARTBEAT_INTERVAL_KEY,
                DFS_HEARTBEAT_INTERVAL_DEFAULT));

    int offset = 1;
    assertThat(outs.get(offset), containsString("SUCCESS: Changed property "
        + DFS_HEARTBEAT_INTERVAL_KEY));
    assertThat(outs.get(offset + 1),
        is(allOf(containsString("From:"), containsString("3"))));
    assertThat(outs.get(offset + 2),
        is(allOf(containsString("To:"), containsString("6"))));
  }

  private static String scanIntoString(final ByteArrayOutputStream baos) {
    final StrBuilder sb = new StrBuilder();
    final Scanner scanner = new Scanner(baos.toString());
    while (scanner.hasNextLine()) {
      sb.appendln(scanner.nextLine());
    }
    scanner.close();
    return sb.toString();
  }

  @Test(timeout = 180000)
  public void testReportCommand() throws Exception {
    tearDown();
    redirectStream();

    // init conf
    final Configuration dfsConf = new HdfsConfiguration();
    dfsConf.setInt(
        DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 500);
    dfsConf.setLong(DFS_HEARTBEAT_INTERVAL_KEY, 1);
    final Path baseDir = new Path(
        PathUtils.getTestDir(getClass()).getAbsolutePath(),
        GenericTestUtils.getMethodName());
    dfsConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.toString());

    try(MiniDFSCluster miniCluster = new MiniDFSCluster
        .Builder(dfsConf)
        .build()) {

      miniCluster.waitActive();

      final DFSAdmin dfsAdmin = new DFSAdmin(dfsConf);
      final DFSClient client = miniCluster.getFileSystem().getClient();

      // Verify report command for all counts to be zero
      resetStream();
      assertEquals(0, ToolRunner.run(dfsAdmin, new String[] {"-report"}));

      final short replFactor = 1;
      final long fileLength = 512L;
      final DistributedFileSystem fs = miniCluster.getFileSystem();
      final Path file = new Path(baseDir, "/corrupted");
      DFSTestUtil.createFile(fs, file, fileLength, replFactor, 12345L);
      DFSTestUtil.waitReplication(fs, file, replFactor);
      final ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, file);
      LocatedBlocks lbs = miniCluster.getFileSystem().getClient().
          getNamenode().getBlockLocations(
          file.toString(), 0, fileLength);
      assertTrue("Unexpected block type: " + lbs.get(0),
          lbs.get(0) instanceof LocatedBlock);
      // Verify report command for all counts to be zero
      resetStream();
      assertEquals(0, ToolRunner.run(dfsAdmin, new String[] {"-report"}));
    }
  }

  @Test(timeout = 300000L)
  public void testListOpenFiles() throws Exception {
    redirectStream();

    final Configuration dfsConf = new HdfsConfiguration();
    dfsConf.setInt(
        DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 500);
    dfsConf.setLong(DFS_HEARTBEAT_INTERVAL_KEY, 1);
    dfsConf.setLong(DFSConfigKeys.DFS_NAMENODE_LIST_OPENFILES_NUM_RESPONSES, 5);
    final Path baseDir = new Path(
        PathUtils.getTestDir(getClass()).getAbsolutePath(),
        GenericTestUtils.getMethodName());
    dfsConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.toString());

    final int numDataNodes = 3;
    final int numClosedFiles = 25;
    final int numOpenFiles = 15;

    try(MiniDFSCluster miniCluster = new MiniDFSCluster
        .Builder(dfsConf)
        .build()) {
      final short replFactor = 1;
      final long fileLength = 512L;
      final FileSystem fs = miniCluster.getFileSystem();
      final Path parentDir = new Path("/tmp/files/");

      fs.mkdirs(parentDir);
      HashSet<Path> closedFileSet = new HashSet<>();
      for (int i = 0; i < numClosedFiles; i++) {
        Path file = new Path(parentDir, "closed-file-" + i);
        DFSTestUtil.createFile(fs, file, fileLength, replFactor, 12345L);
        closedFileSet.add(file);
      }

      HashMap<Path, FSDataOutputStream> openFilesMap = new HashMap<>();
      for (int i = 0; i < numOpenFiles; i++) {
        Path file = new Path(parentDir, "open-file-" + i);
        DFSTestUtil.createFile(fs, file, fileLength, replFactor, 12345L);
        FSDataOutputStream outputStream = fs.append(file);
        openFilesMap.put(file, outputStream);
      }

      final DFSAdmin dfsAdmin = new DFSAdmin(dfsConf);
      assertEquals(0, ToolRunner.run(dfsAdmin,
          new String[]{"-listOpenFiles"}));
      verifyOpenFilesListing(closedFileSet, openFilesMap);

      for (int count = 0; count < numOpenFiles; count++) {
        closedFileSet.addAll(DFSTestUtil.closeOpenFiles(openFilesMap, 1));
        resetStream();
        assertEquals(0, ToolRunner.run(dfsAdmin,
            new String[]{"-listOpenFiles"}));
        verifyOpenFilesListing(closedFileSet, openFilesMap);
      }

      // test -listOpenFiles command with option <path>
      openFilesMap.clear();
      Path file;
      HashMap<Path, FSDataOutputStream> openFiles1 = new HashMap<>();
      HashMap<Path, FSDataOutputStream> openFiles2 = new HashMap<>();
      for (int i = 0; i < numOpenFiles; i++) {
        if (i % 2 == 0) {
          file = new Path(new Path("/tmp/files/a"), "open-file-" + i);
        } else {
          file = new Path(new Path("/tmp/files/b"), "open-file-" + i);
        }

        DFSTestUtil.createFile(fs, file, fileLength, replFactor, 12345L);
        FSDataOutputStream outputStream = fs.append(file);

        if (i % 2 == 0) {
          openFiles1.put(file, outputStream);
        } else {
          openFiles2.put(file, outputStream);
        }
        openFilesMap.put(file, outputStream);
      }

      resetStream();
      // list all open files
      assertEquals(0,
          ToolRunner.run(dfsAdmin, new String[] {"-listOpenFiles"}));
      verifyOpenFilesListing(null, openFilesMap);

      resetStream();
      // list open files under directory path /tmp/files/a
      assertEquals(0, ToolRunner.run(dfsAdmin,
          new String[] {"-listOpenFiles", "-path", "/tmp/files/a"}));
      verifyOpenFilesListing(null, openFiles1);

      resetStream();
      // list open files without input path
      assertEquals(-1, ToolRunner.run(dfsAdmin,
          new String[] {"-listOpenFiles", "-path"}));
      // verify the error
      String outStr = scanIntoString(err);
      assertTrue(outStr.contains("listOpenFiles: option"
          + " -path requires 1 argument"));

      resetStream();
      // list open files with empty path
      assertEquals(0, ToolRunner.run(dfsAdmin,
          new String[] {"-listOpenFiles", "-path", ""}));
      // all the open files will be listed
      verifyOpenFilesListing(null, openFilesMap);

      resetStream();
      // list invalid path file
      assertEquals(0, ToolRunner.run(dfsAdmin,
          new String[] {"-listOpenFiles", "-path", "/invalid_path"}));
      outStr = scanIntoString(out);
      for (Path openFilePath : openFilesMap.keySet()) {
        assertThat(outStr, not(containsString(openFilePath.toString())));
      }
      DFSTestUtil.closeOpenFiles(openFilesMap, openFilesMap.size());
    }
  }

  private void verifyOpenFilesListing(HashSet<Path> closedFileSet,
      HashMap<Path, FSDataOutputStream> openFilesMap) {
    final String outStr = scanIntoString(out);
    LOG.info("dfsadmin -listOpenFiles output: \n" + out);
    if (closedFileSet != null) {
      for (Path closedFilePath : closedFileSet) {
        assertThat(outStr,
            not(containsString(closedFilePath.toString() +
                System.lineSeparator())));
      }
    }

    for (Path openFilePath : openFilesMap.keySet()) {
      assertThat(outStr, is(containsString(openFilePath.toString() +
          System.lineSeparator())));
    }
  }

  @Test
  public void testRefreshProxyUser() throws Exception {
    Path dirPath = new Path("/testdir1");
    Path subDirPath = new Path("/testdir1/subdir1");
    UserGroupInformation loginUserUgi =  UserGroupInformation.getLoginUser();
    String proxyUser = "fakeuser";
    String realUser = loginUserUgi.getShortUserName();

    UserGroupInformation proxyUgi =
        UserGroupInformation.createProxyUserForTesting(proxyUser,
            loginUserUgi, loginUserUgi.getGroupNames());

    // create a directory as login user and re-assign it to proxy user
    loginUserUgi.doAs(new PrivilegedExceptionAction<Integer>() {
      @Override
      public Integer run() throws Exception {
        cluster.getFileSystem().mkdirs(dirPath);
        cluster.getFileSystem().setOwner(dirPath, proxyUser,
            proxyUgi.getPrimaryGroupName());
        return 0;
      }
    });

    // try creating subdirectory inside the directory as proxy user,
    // This should fail because of the current user hasn't still been proxied
    try {
      proxyUgi.doAs(new PrivilegedExceptionAction<Integer>() {
        @Override public Integer run() throws Exception {
          cluster.getFileSystem().mkdirs(subDirPath);
          return 0;
        }
      });
    } catch (RemoteException re) {
      Assert.assertTrue(re.unwrapRemoteException()
          instanceof AccessControlException);
      Assert.assertTrue(re.unwrapRemoteException().getMessage()
          .equals("User: " + realUser +
              " is not allowed to impersonate " + proxyUser));
    }

    // refresh will look at configuration on the server side
    // add additional resource with the new value
    // so the server side will pick it up
    String userKeyGroups = DefaultImpersonationProvider.getTestProvider().
        getProxySuperuserGroupConfKey(realUser);
    String userKeyHosts = DefaultImpersonationProvider.getTestProvider().
        getProxySuperuserIpConfKey(realUser);
    String rsrc = "testGroupMappingRefresh_rsrc.xml";
    tempResource = TestRefreshUserMappings.addNewConfigResource(rsrc,
        userKeyGroups, "*", userKeyHosts, "*");

    String[] args = new String[]{"-refreshSuperUserGroupsConfiguration"};
    admin.run(args);

    // After proxying the fakeuser, the mkdir should work
    proxyUgi.doAs(new PrivilegedExceptionAction<Integer>() {
      @Override
      public Integer run() throws Exception {
        cluster.getFileSystem().mkdirs(dirPath);
        return 0;
      }
    });
  }
}