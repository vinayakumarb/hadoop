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

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;
import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.LAZY_PERSIST;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.UnhandledException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster.NameNodeInfo;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.hdfs.tools.JMXGet;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.mockito.internal.util.reflection.Whitebox;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/** Utilities for HDFS tests */
public class DFSTestUtil {

  private static final Log LOG = LogFactory.getLog(DFSTestUtil.class);

  private static final Random gen = new Random();
  private static final String[] dirNames = {
    "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine"
  };

  private final int maxLevels;
  private final int maxSize;
  private final int minSize;
  private final int nFiles;
  private MyFile[] files;

  /** Creates a new instance of DFSTestUtil
   *
   * @param nFiles Number of files to be created
   * @param maxLevels Maximum number of directory levels
   * @param maxSize Maximum size for file
   * @param minSize Minimum size for file
   */
  private DFSTestUtil(int nFiles, int maxLevels, int maxSize, int minSize) {
    this.nFiles = nFiles;
    this.maxLevels = maxLevels;
    this.maxSize = maxSize;
    this.minSize = minSize;
  }

  /** Creates a new instance of DFSTestUtil
   *
   * @param testName Name of the test from where this utility is used
   * @param nFiles Number of files to be created
   * @param maxLevels Maximum number of directory levels
   * @param maxSize Maximum size for file
   * @param minSize Minimum size for file
   */
  public DFSTestUtil(String testName, int nFiles, int maxLevels, int maxSize,
      int minSize) {
    this.nFiles = nFiles;
    this.maxLevels = maxLevels;
    this.maxSize = maxSize;
    this.minSize = minSize;
  }

  /**
   * when formatting a namenode - we must provide clusterid.
   * @param conf
   * @throws IOException
   */
  public static void formatNameNode(Configuration conf) throws IOException {
    String clusterId = StartupOption.FORMAT.getClusterId();
    if(clusterId == null || clusterId.isEmpty())
      StartupOption.FORMAT.setClusterId("testClusterID");
    // Use a copy of conf as it can be altered by namenode during format.
    NameNode.format(new Configuration(conf));
  }

  /**
   * Create a new HA-enabled configuration.
   */
  public static Configuration newHAConfiguration(final String logicalName) {
    Configuration conf = new Configuration();
    addHAConfiguration(conf, logicalName);
    return conf;
  }

  /**
   * Add a new HA configuration.
   */
  public static void addHAConfiguration(Configuration conf,
      final String logicalName) {
    String nsIds = conf.get(DFSConfigKeys.DFS_NAMESERVICES);
    if (nsIds == null) {
      conf.set(DFSConfigKeys.DFS_NAMESERVICES, logicalName);
    } else { // append the nsid
      conf.set(DFSConfigKeys.DFS_NAMESERVICES, nsIds + "," + logicalName);
    }
    conf.set(DFSUtil.addKeySuffixes(HdfsClientConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX,
            logicalName), "nn1,nn2");
    conf.set(HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX +
            "." + logicalName,
            ConfiguredFailoverProxyProvider.class.getName());
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
  }

  public static void setFakeHttpAddresses(Configuration conf,
      final String logicalName) {
    conf.set(DFSUtil.addKeySuffixes(
        DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY,
        logicalName, "nn1"), "127.0.0.1:12345");
    conf.set(DFSUtil.addKeySuffixes(
        DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY,
        logicalName, "nn2"), "127.0.0.1:12346");
  }

  public static void setEditLogForTesting(FSNamesystem fsn, FSEditLog newLog) {
    // spies are shallow copies, must allow async log to restart its thread
    // so it has the new copy
    newLog.restart();
    Whitebox.setInternalState(fsn.getFSImage(), "editLog", newLog);
    Whitebox.setInternalState(fsn.getFSDirectory(), "editLog", newLog);
  }

  /** class MyFile contains enough information to recreate the contents of
   * a single file.
   */
  private class MyFile {

    private String name = "";
    private final int size;
    private final long seed;

    MyFile() {
      int nLevels = gen.nextInt(maxLevels);
      if (nLevels != 0) {
        int[] levels = new int[nLevels];
        for (int idx = 0; idx < nLevels; idx++) {
          levels[idx] = gen.nextInt(10);
        }
        StringBuffer sb = new StringBuffer();
        for (int idx = 0; idx < nLevels; idx++) {
          sb.append(dirNames[levels[idx]]);
          sb.append("/");
        }
        name = sb.toString();
      }
      long fidx = -1;
      while (fidx < 0) { fidx = gen.nextLong(); }
      name = name + Long.toString(fidx);
      size = minSize + gen.nextInt(maxSize - minSize);
      seed = gen.nextLong();
    }

    String getName() { return name; }
    int getSize() { return size; }
    long getSeed() { return seed; }
  }

  public void createFiles(FileSystem fs, String topdir) throws IOException {
    createFiles(fs, topdir, (short)3);
  }

  public static byte[] readFileAsBytes(FileSystem fs, Path fileName) throws IOException {
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      IOUtils.copyBytes(fs.open(fileName), os, 1024);
      return os.toByteArray();
    }
  }

  /** create nFiles with random names and directory hierarchies
   *  with random (but reproducible) data in them.
   */
  public void createFiles(FileSystem fs, String topdir,
                   short replicationFactor) throws IOException {
    files = new MyFile[nFiles];

    for (int idx = 0; idx < nFiles; idx++) {
      files[idx] = new MyFile();
    }

    Path root = new Path(topdir);

    for (int idx = 0; idx < nFiles; idx++) {
      createFile(fs, new Path(root, files[idx].getName()), files[idx].getSize(),
          replicationFactor, files[idx].getSeed());
    }
  }

  public static String readFile(FileSystem fs, Path fileName)
      throws IOException {
    byte buf[] = readFileBuffer(fs, fileName);
	return new String(buf, 0, buf.length);
  }

  public static byte[] readFileBuffer(FileSystem fs, Path fileName)
      throws IOException {
    try (ByteArrayOutputStream os = new ByteArrayOutputStream();
         FSDataInputStream in = fs.open(fileName)) {
      IOUtils.copyBytes(in, os, 1024, true);
      return os.toByteArray();
    }
  }

  public static void createFile(FileSystem fs, Path fileName, long fileLen,
      short replFactor, long seed) throws IOException {
    createFile(fs, fileName, 1024, fileLen, fs.getDefaultBlockSize(fileName),
        replFactor, seed);
  }

  public static void createFile(FileSystem fs, Path fileName, int bufferLen,
      long fileLen, long blockSize, short replFactor, long seed)
      throws IOException {
    createFile(fs, fileName, false, bufferLen, fileLen, blockSize, replFactor,
      seed, false);
  }

  public static void createFile(FileSystem fs, Path fileName,
      boolean isLazyPersist, int bufferLen, long fileLen, long blockSize,
      short replFactor, long seed, boolean flush) throws IOException {
        createFile(fs, fileName, isLazyPersist, bufferLen, fileLen, blockSize,
          replFactor, seed, flush, null);
  }

  public static void createFile(FileSystem fs, Path fileName,
      boolean isLazyPersist, int bufferLen, long fileLen, long blockSize,
      short replFactor, long seed, boolean flush,
      InetSocketAddress[] favoredNodes) throws IOException {
    assert bufferLen > 0;
    if (!fs.mkdirs(fileName.getParent())) {
      throw new IOException("Mkdirs failed to create " +
          fileName.getParent().toString());
    }
    EnumSet<CreateFlag> createFlags = EnumSet.of(CREATE);
    createFlags.add(OVERWRITE);
    if (isLazyPersist) {
      createFlags.add(LAZY_PERSIST);
    }
    try (FSDataOutputStream out = (favoredNodes == null) ?
        fs.create(fileName, FsPermission.getFileDefault(), createFlags,
            fs.getConf().getInt(IO_FILE_BUFFER_SIZE_KEY, 4096), replFactor,
            blockSize, null)
        :
        ((DistributedFileSystem) fs).create(fileName, FsPermission.getDefault(),
            true, bufferLen, replFactor, blockSize, null, favoredNodes)
    ) {
      if (fileLen > 0) {
        byte[] toWrite = new byte[bufferLen];
        Random rb = new Random(seed);
        long bytesToWrite = fileLen;
        while (bytesToWrite > 0) {
          rb.nextBytes(toWrite);
          int bytesToWriteNext = (bufferLen < bytesToWrite) ? bufferLen
              : (int) bytesToWrite;

          out.write(toWrite, 0, bytesToWriteNext);
          bytesToWrite -= bytesToWriteNext;
        }
        if (flush) {
          out.hsync();
        }
      }
    }
  }

  public static byte[] calculateFileContentsFromSeed(long seed, int length) {
    Random rb = new Random(seed);
    byte val[] = new byte[length];
    rb.nextBytes(val);
    return val;
  }

  /** check if the files have been copied correctly. */
  public boolean checkFiles(FileSystem fs, String topdir) throws IOException {
    Path root = new Path(topdir);

    for (int idx = 0; idx < nFiles; idx++) {
      Path fPath = new Path(root, files[idx].getName());
      try (FSDataInputStream in = fs.open(fPath)) {
        byte[] toRead = new byte[files[idx].getSize()];
        byte[] toCompare = new byte[files[idx].getSize()];
        Random rb = new Random(files[idx].getSeed());
        rb.nextBytes(toCompare);
        in.readFully(0, toRead);
        for (int i = 0; i < toRead.length; i++) {
          if (toRead[i] != toCompare[i]) {
            return false;
          }
        }
      }
    }

    return true;
  }

  void setReplication(FileSystem fs, String topdir, short value)
                                              throws IOException {
    Path root = new Path(topdir);
    for (int idx = 0; idx < nFiles; idx++) {
      Path fPath = new Path(root, files[idx].getName());
      fs.setReplication(fPath, value);
    }
  }

  /** return list of filenames created as part of createFiles */
  public String[] getFileNames(String topDir) {
    if (nFiles == 0)
      return new String[]{};
    else {
      String[] fileNames =  new String[nFiles];
      for (int idx=0; idx < nFiles; idx++) {
        fileNames[idx] = topDir + "/" + files[idx].getName();
      }
      return fileNames;
    }
  }

  /**
   * Wait for the given file to reach the given replication factor.
   * @throws TimeoutException if we fail to sufficiently replicate the file
   */
  public static void waitReplication(FileSystem fs, Path fileName, short replFactor)
      throws IOException, InterruptedException, TimeoutException {
    boolean correctReplFactor;
    final int ATTEMPTS = 40;
    int count = 0;

    do {
      correctReplFactor = true;
      BlockLocation locs[] = fs.getFileBlockLocations(
        fs.getFileStatus(fileName), 0, Long.MAX_VALUE);
      count++;
      return;
    } while (!correctReplFactor && count < ATTEMPTS);
  }

  /** delete directory and everything underneath it.*/
  public void cleanup(FileSystem fs, String topdir) throws IOException {
    Path root = new Path(topdir);
    fs.delete(root, true);
    files = null;
  }

  public static ExtendedBlock getFirstBlock(FileSystem fs, Path path) throws IOException {
    try (HdfsDataInputStream in = (HdfsDataInputStream) fs.open(path)) {
      in.readByte();
      return in.getCurrentBlock();
    }
  }

  public static List<LocatedBlock> getAllBlocks(FSDataInputStream in)
      throws IOException {
    return ((HdfsDataInputStream) in).getAllBlocks();
  }

  public static List<LocatedBlock> getAllBlocks(FileSystem fs, Path path)
      throws IOException {
    try (HdfsDataInputStream in = (HdfsDataInputStream) fs.open(path)) {
      return in.getAllBlocks();
    }
  }

  public static String readFile(File f) throws IOException {
    try (BufferedReader in = new BufferedReader(new FileReader(f))) {
      StringBuilder b = new StringBuilder();
      int c;
      while ((c = in.read()) != -1) {
        b.append((char) c);
      }
      return b.toString();
    }
  }

  public static byte[] readFileAsBytes(File f) throws IOException {
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      IOUtils.copyBytes(new FileInputStream(f), os, 1024);
      return os.toByteArray();
    }
  }

  /* Write the given bytes to the given file */
  public static void writeFile(FileSystem fs, Path p, byte[] bytes)
      throws IOException {
    if (fs.exists(p)) {
      fs.delete(p, true);
    }
    try (InputStream is = new ByteArrayInputStream(bytes);
      FSDataOutputStream os = fs.create(p)) {
      IOUtils.copyBytes(is, os, bytes.length);
    }
  }

  /* Write the given bytes to the given file using the specified blockSize */
  public static void writeFile(
      FileSystem fs, Path p, byte[] bytes, long blockSize)
      throws IOException {
    if (fs.exists(p)) {
      fs.delete(p, true);
    }
    try (InputStream is = new ByteArrayInputStream(bytes);
        FSDataOutputStream os = fs.create(
            p, false, 4096, fs.getDefaultReplication(p), blockSize)) {
      IOUtils.copyBytes(is, os, bytes.length);
    }
  }

  /* Write the given string to the given file */
  public static void writeFile(FileSystem fs, Path p, String s)
      throws IOException {
    writeFile(fs, p, s.getBytes());
  }

  /* Append the given string to the given file */
  public static void appendFile(FileSystem fs, Path p, String s)
      throws IOException {
    assert fs.exists(p);
    try (InputStream is = new ByteArrayInputStream(s.getBytes());
      FSDataOutputStream os = fs.append(p)) {
      IOUtils.copyBytes(is, os, s.length());
    }
  }

  /**
   * Append specified length of bytes to a given file
   * @param fs The file system
   * @param p Path of the file to append
   * @param length Length of bytes to append to the file
   * @throws IOException
   */
  public static void appendFile(FileSystem fs, Path p, int length)
      throws IOException {
    assert fs.exists(p);
    assert length >= 0;
    byte[] toAppend = new byte[length];
    Random random = new Random();
    random.nextBytes(toAppend);
    try (FSDataOutputStream out = fs.append(p)) {
      out.write(toAppend);
    }
  }

  /**
   * Append specified length of bytes to a given file, starting with new block.
   * @param fs The file system
   * @param p Path of the file to append
   * @param length Length of bytes to append to the file
   * @throws IOException
   */
  public static void appendFileNewBlock(DistributedFileSystem fs,
      Path p, int length) throws IOException {
    assert length >= 0;
    byte[] toAppend = new byte[length];
    Random random = new Random();
    random.nextBytes(toAppend);
    appendFileNewBlock(fs, p, toAppend);
  }

  /**
   * Append specified bytes to a given file, starting with new block.
   *
   * @param fs The file system
   * @param p Path of the file to append
   * @param bytes The data to append
   * @throws IOException
   */
  public static void appendFileNewBlock(DistributedFileSystem fs,
      Path p, byte[] bytes) throws IOException {
    assert fs.exists(p);
    try (FSDataOutputStream out = fs.append(p,
        EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK), 4096, null)) {
      out.write(bytes);
    }
  }

  /**
   * @return url content as string (UTF-8 encoding assumed)
   */
  public static String urlGet(URL url) throws IOException {
    return new String(urlGetBytes(url), Charsets.UTF_8);
  }

  /**
   * @return URL contents as a byte array
   */
  public static byte[] urlGetBytes(URL url) throws IOException {
    URLConnection conn = url.openConnection();
    HttpURLConnection hc = (HttpURLConnection)conn;

    assertEquals(HttpURLConnection.HTTP_OK, hc.getResponseCode());
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    IOUtils.copyBytes(conn.getInputStream(), out, 4096, true);
    return out.toByteArray();
  }

  /**
   * mock class to get group mapping for fake users
   *
   */
  static class MockUnixGroupsMapping extends ShellBasedUnixGroupsMapping {
    static Map<String, String []> fakeUser2GroupsMap;
    private static final List<String> defaultGroups;
    static {
      defaultGroups = new ArrayList<String>(1);
      defaultGroups.add("supergroup");
      fakeUser2GroupsMap = new HashMap<String, String[]>();
    }

    @Override
    public List<String> getGroups(String user) throws IOException {
      boolean found = false;

      // check to see if this is one of fake users
      List<String> l = new ArrayList<String>();
      for(String u : fakeUser2GroupsMap.keySet()) {
        if(user.equals(u)) {
          found = true;
          for(String gr : fakeUser2GroupsMap.get(u)) {
            l.add(gr);
          }
        }
      }

      // default
      if(!found) {
        l =  super.getGroups(user);
        if(l.size() == 0) {
          System.out.println("failed to get real group for " + user +
              "; using default");
          return defaultGroups;
        }
      }
      return l;
    }
  }

  /**
   * update the configuration with fake class for mapping user to groups
   * @param conf
   * @param map - user to groups mapping
   */
  static public void updateConfWithFakeGroupMapping
    (Configuration conf, Map<String, String []> map) {
    if(map!=null) {
      MockUnixGroupsMapping.fakeUser2GroupsMap = map;
    }

    // fake mapping user to groups
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        DFSTestUtil.MockUnixGroupsMapping.class,
        ShellBasedUnixGroupsMapping.class);

  }

  /**
   * Get a FileSystem instance as specified user in a doAs block.
   */
  static public FileSystem getFileSystemAs(UserGroupInformation ugi,
      final Configuration conf) throws IOException {
    try {
      return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          return FileSystem.get(conf);
        }
      });
    } catch (InterruptedException e) {
      throw (InterruptedIOException)new InterruptedIOException().initCause(e);
    }
  }

  public static byte[] generateSequentialBytes(int start, int length) {
    byte[] result = new byte[length];

    for (int i = 0; i < length; i++) {
      result[i] = (byte) ((start + i) % 127);
    }

    return result;
  }

  public static Statistics getStatistics(FileSystem fs) {
    return FileSystem.getStatistics(fs.getUri().getScheme(), fs.getClass());
  }

  /**
   * Load file into byte[]
   */
  public static byte[] loadFile(String filename) throws IOException {
    File file = new File(filename);
    try (DataInputStream in = new DataInputStream(new FileInputStream(file))) {
      byte[] content = new byte[(int) file.length()];
      in.readFully(content);
      return content;
    }
  }

  public static void setFederatedConfiguration(MiniDFSCluster cluster,
      Configuration conf) {
    Set<String> nameservices = new HashSet<String>();
    for (NameNodeInfo info : cluster.getNameNodeInfos()) {
      assert info.nameserviceId != null;
      nameservices.add(info.nameserviceId);
      conf.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_RPC_ADDRESS_KEY,
          info.nameserviceId), DFSUtil.createUri(HdfsConstants.HDFS_URI_SCHEME,
              info.nameNode.getNameNodeAddress()).toString());
      conf.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
          info.nameserviceId), DFSUtil.createUri(HdfsConstants.HDFS_URI_SCHEME,
              info.nameNode.getNameNodeAddress()).toString());
    }
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, Joiner.on(",")
        .join(nameservices));
  }

  public static void setFederatedHAConfiguration(MiniDFSCluster cluster,
      Configuration conf) {
    Map<String, List<String>> nameservices = Maps.newHashMap();
    for (NameNodeInfo info : cluster.getNameNodeInfos()) {
      Preconditions.checkState(info.nameserviceId != null);
      List<String> nns = nameservices.get(info.nameserviceId);
      if (nns == null) {
        nns = Lists.newArrayList();
        nameservices.put(info.nameserviceId, nns);
      }
      nns.add(info.nnId);

      conf.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_RPC_ADDRESS_KEY,
          info.nameserviceId, info.nnId),
          DFSUtil.createUri(HdfsConstants.HDFS_URI_SCHEME,
          info.nameNode.getNameNodeAddress()).toString());
      conf.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
          info.nameserviceId, info.nnId),
          DFSUtil.createUri(HdfsConstants.HDFS_URI_SCHEME,
          info.nameNode.getNameNodeAddress()).toString());
    }
    for (Map.Entry<String, List<String>> entry : nameservices.entrySet()) {
      conf.set(DFSUtil.addKeySuffixes(DFS_HA_NAMENODES_KEY_PREFIX,
          entry.getKey()), Joiner.on(",").join(entry.getValue()));
      conf.set(HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX + "."
          + entry.getKey(), ConfiguredFailoverProxyProvider.class.getName());
    }
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, Joiner.on(",")
        .join(nameservices.keySet()));
  }

  /** Copy one file's contents into the other **/
  public static void copyFile(File src, File dest) throws IOException {
    FileUtils.copyFile(src, dest);
  }

  public static class Builder {
    private int maxLevels = 3;
    private int maxSize = 8*1024;
    private int minSize = 1;
    private int nFiles = 1;

    public Builder() {
    }

    public Builder setName(String string) {
      return this;
    }

    public Builder setNumFiles(int nFiles) {
      this.nFiles = nFiles;
      return this;
    }

    public Builder setMaxLevels(int maxLevels) {
      this.maxLevels = maxLevels;
      return this;
    }

    public Builder setMaxSize(int maxSize) {
      this.maxSize = maxSize;
      return this;
    }

    public Builder setMinSize(int minSize) {
      this.minSize = minSize;
      return this;
    }

    public DFSTestUtil build() {
      return new DFSTestUtil(nFiles, maxLevels, maxSize, minSize);
    }
  }

  /**
   * Run a set of operations and generate all edit logs
   */
  public static void runOperations(MiniDFSCluster cluster,
      DistributedFileSystem filesystem, Configuration conf, long blockSize,
      int nnIndex) throws IOException {
    // create FileContext for rename2
    FileContext fc = FileContext.getFileContext(cluster.getURI(0), conf);

    // OP_ADD 0
    final Path pathFileCreate = new Path("/file_create");
    FSDataOutputStream s = filesystem.create(pathFileCreate);
    // OP_CLOSE 9
    s.close();
    // OP_APPEND 47
    FSDataOutputStream s2 = filesystem.append(pathFileCreate, 4096, null);
    s2.close();

    // OP_UPDATE_BLOCKS 25
    final String updateBlockFile = "/update_blocks";
    FSDataOutputStream fout = filesystem.create(new Path(updateBlockFile), true, 4096, (short)1, 4096L);
    fout.write(1);
    fout.hflush();
    long fileId = ((DFSOutputStream)fout.getWrappedStream()).getFileId();
    DFSClient dfsclient = DFSClientAdapter.getDFSClient(filesystem);
    LocatedBlocks blocks = dfsclient.getNamenode().getBlockLocations(updateBlockFile, 0, Integer.MAX_VALUE);
    dfsclient.getNamenode().abandonBlock(blocks.get(0).getBlock(), fileId, updateBlockFile, dfsclient.clientName);
    fout.close();

    // OP_SET_STORAGE_POLICY 45
    filesystem.setStoragePolicy(pathFileCreate,
        HdfsConstants.HOT_STORAGE_POLICY_NAME);
    // OP_RENAME_OLD 1
    final Path pathFileMoved = new Path("/file_moved");
    filesystem.rename(pathFileCreate, pathFileMoved);
    // OP_DELETE 2
    filesystem.delete(pathFileMoved, false);
    // OP_MKDIR 3
    Path pathDirectoryMkdir = new Path("/directory_mkdir");
    filesystem.mkdirs(pathDirectoryMkdir);
    // OP_ALLOW_SNAPSHOT 29
    filesystem.allowSnapshot(pathDirectoryMkdir);
    // OP_DISALLOW_SNAPSHOT 30
    filesystem.disallowSnapshot(pathDirectoryMkdir);
    // OP_CREATE_SNAPSHOT 26
    String ssName = "snapshot1";
    filesystem.allowSnapshot(pathDirectoryMkdir);
    filesystem.createSnapshot(pathDirectoryMkdir, ssName);
    // OP_RENAME_SNAPSHOT 28
    String ssNewName = "snapshot2";
    filesystem.renameSnapshot(pathDirectoryMkdir, ssName, ssNewName);
    // OP_DELETE_SNAPSHOT 27
    filesystem.deleteSnapshot(pathDirectoryMkdir, ssNewName);
    // OP_SET_REPLICATION 4
    s = filesystem.create(pathFileCreate);
    s.close();
    filesystem.setReplication(pathFileCreate, (short)1);
    // OP_SET_PERMISSIONS 7
    Short permission = 0777;
    filesystem.setPermission(pathFileCreate, new FsPermission(permission));
    // OP_SET_OWNER 8
    filesystem.setOwner(pathFileCreate, new String("newOwner"), null);
    // OP_CLOSE 9 see above
    // OP_SET_GENSTAMP 10 see above
    // OP_SET_NS_QUOTA 11 obsolete
    // OP_CLEAR_NS_QUOTA 12 obsolete
    // OP_TIMES 13
    long mtime = 1285195527000L; // Wed, 22 Sep 2010 22:45:27 GMT
    long atime = mtime;
    filesystem.setTimes(pathFileCreate, mtime, atime);
    // OP_SET_QUOTA 14
    filesystem.setQuota(pathDirectoryMkdir, 1000L,
        HdfsConstants.QUOTA_DONT_SET);
    // OP_SET_QUOTA_BY_STORAGETYPE
    filesystem.setQuotaByStorageType(pathDirectoryMkdir, StorageType.SSD, 888L);
    // OP_RENAME 15
    fc.rename(pathFileCreate, pathFileMoved, Rename.NONE);
    // OP_CONCAT_DELETE 16
    Path   pathConcatTarget = new Path("/file_concat_target");
    Path[] pathConcatFiles  = new Path[2];
    pathConcatFiles[0]      = new Path("/file_concat_0");
    pathConcatFiles[1]      = new Path("/file_concat_1");

    long length = blockSize * 3; // multiple of blocksize for concat
    short replication = 1;
    long seed = 1;
    DFSTestUtil.createFile(filesystem, pathConcatTarget, length, replication,
        seed);
    DFSTestUtil.createFile(filesystem, pathConcatFiles[0], length, replication,
        seed);
    DFSTestUtil.createFile(filesystem, pathConcatFiles[1], length, replication,
        seed);
    filesystem.concat(pathConcatTarget, pathConcatFiles);

    // OP_TRUNCATE 46
    length = blockSize * 2;
    DFSTestUtil.createFile(filesystem, pathFileCreate, length, replication,
        seed);
    filesystem.truncate(pathFileCreate, blockSize);

    // OP_SYMLINK 17
    Path pathSymlink = new Path("/file_symlink");
    fc.createSymlink(pathConcatTarget, pathSymlink, false);

    // OP_REASSIGN_LEASE 22
    String filePath = "/hard-lease-recovery-test";
    byte[] bytes = "foo-bar-baz".getBytes();
    DFSClientAdapter.stopLeaseRenewer(filesystem);
    FSDataOutputStream leaseRecoveryPath = filesystem.create(new Path(filePath));
    leaseRecoveryPath.write(bytes);
    leaseRecoveryPath.hflush();
    // Set the hard lease timeout to 1 second.
    cluster.setLeasePeriod(60 * 1000, 1000, nnIndex);
    // wait for lease recovery to complete
    LocatedBlocks locatedBlocks;
    do {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {}
      locatedBlocks = DFSClientAdapter.callGetBlockLocations(
          cluster.getNameNodeRpc(nnIndex), filePath, 0L, bytes.length);
    } while (locatedBlocks.isUnderConstruction());
    // OP_SET_ACL
    List<AclEntry> aclEntryList = Lists.newArrayList();
    aclEntryList.add(
        new AclEntry.Builder()
            .setPermission(FsAction.READ_WRITE)
            .setScope(AclEntryScope.ACCESS)
            .setType(AclEntryType.USER)
            .build());
    aclEntryList.add(
        new AclEntry.Builder()
            .setName("user")
            .setPermission(FsAction.READ_WRITE)
            .setScope(AclEntryScope.ACCESS)
            .setType(AclEntryType.USER)
            .build());
    aclEntryList.add(
        new AclEntry.Builder()
            .setPermission(FsAction.WRITE)
            .setScope(AclEntryScope.ACCESS)
            .setType(AclEntryType.GROUP)
            .build());
    aclEntryList.add(
        new AclEntry.Builder()
            .setPermission(FsAction.NONE)
            .setScope(AclEntryScope.ACCESS)
            .setType(AclEntryType.OTHER)
            .build());
    filesystem.setAcl(pathConcatTarget, aclEntryList);
    // OP_SET_XATTR
    filesystem.setXAttr(pathConcatTarget, "user.a1",
        new byte[]{0x31, 0x32, 0x33});
    filesystem.setXAttr(pathConcatTarget, "user.a2",
        new byte[]{0x37, 0x38, 0x39});
    // OP_REMOVE_XATTR
    filesystem.removeXAttr(pathConcatTarget, "user.a2");
  }

  public static void abortStream(DFSOutputStream out) throws IOException {
    out.abort();
  }

  public static byte[] asArray(ByteBuffer buf) {
    byte arr[] = new byte[buf.remaining()];
    buf.duplicate().get(arr);
    return arr;
  }

  public static void checkComponentsEquals(byte[][] expected, byte[][] actual) {
    assertEquals("expected: " + DFSUtil.byteArray2PathString(expected)
        + ", actual: " + DFSUtil.byteArray2PathString(actual), expected.length,
        actual.length);
    int i = 0;
    for (byte[] e : expected) {
      byte[] actualComponent = actual[i++];
      assertTrue("expected: " + DFSUtil.bytes2String(e) + ", actual: "
          + DFSUtil.bytes2String(actualComponent),
          Arrays.equals(e, actualComponent));
    }
  }

  /**
   * Verify that two files have the same contents.
   *
   * @param fs The file system containing the two files.
   * @param p1 The path of the first file.
   * @param p2 The path of the second file.
   * @param len The length of the two files.
   * @throws IOException
   */
  public static void verifyFilesEqual(FileSystem fs, Path p1, Path p2, int len)
      throws IOException {
    try (FSDataInputStream in1 = fs.open(p1);
         FSDataInputStream in2 = fs.open(p2)) {
      for (int i = 0; i < len; i++) {
        assertEquals("Mismatch at byte " + i, in1.read(), in2.read());
      }
    }
  }

  /**
   * Verify that two files have different contents.
   *
   * @param fs The file system containing the two files.
   * @param p1 The path of the first file.
   * @param p2 The path of the second file.
   * @param len The length of the two files.
   * @throws IOException
   */
  public static void verifyFilesNotEqual(FileSystem fs, Path p1, Path p2,
      int len) throws IOException {
    try (FSDataInputStream in1 = fs.open(p1);
         FSDataInputStream in2 = fs.open(p2)) {
      for (int i = 0; i < len; i++) {
        if (in1.read() != in2.read()) {
          return;
        }
      }
      fail("files are equal, but should not be");
    }
  }

  /**
   * Helper function to create a key in the Key Provider. Defaults
   * to the first indexed NameNode's Key Provider.
   *
   * @param keyName The name of the key to create
   * @param cluster The cluster to create it in
   * @param conf Configuration to use
   */
  public static void createKey(String keyName, MiniDFSCluster cluster,
                                Configuration conf)
          throws NoSuchAlgorithmException, IOException {
    createKey(keyName, cluster, 0, conf);
  }

  /**
   * Helper function to create a key in the Key Provider.
   *
   * @param keyName The name of the key to create
   * @param cluster The cluster to create it in
   * @param idx The NameNode index
   * @param conf Configuration to use
   */
  public static void createKey(String keyName, MiniDFSCluster cluster,
                               int idx, Configuration conf)
      throws NoSuchAlgorithmException, IOException {
    NameNode nn = cluster.getNameNode(idx);
    KeyProvider provider = nn.getNamesystem().getProvider();
    final KeyProvider.Options options = KeyProvider.options(conf);
    options.setDescription(keyName);
    options.setBitLength(128);
    provider.createKey(keyName, options);
    provider.flush();
  }

  public static void toolRun(Tool tool, String cmd, int retcode, String contain)
      throws Exception {
    String [] cmds = StringUtils.split(cmd, ' ');
    System.out.flush();
    System.err.flush();
    PrintStream origOut = System.out;
    PrintStream origErr = System.err;
    String output = null;
    int ret = 0;
    try {
      ByteArrayOutputStream bs = new ByteArrayOutputStream(1024);
      try (PrintStream out = new PrintStream(bs)) {
        System.setOut(out);
        System.setErr(out);
        ret = tool.run(cmds);
        System.out.flush();
        System.err.flush();
      }
      output = bs.toString();
    } finally {
      System.setOut(origOut);
      System.setErr(origErr);
    }
    System.out.println("Output for command: " + cmd + " retcode: " + ret);
    if (output != null) {
      System.out.println(output);
    }
    assertEquals(retcode, ret);
    if (contain != null) {
      assertTrue("The real output is: " + output + ".\n It should contain: "
          + contain, output.contains(contain));
    }
  }

  public static void FsShellRun(String cmd, int retcode, String contain,
      Configuration conf) throws Exception {
    FsShell shell = new FsShell(new Configuration(conf));
    toolRun(shell, cmd, retcode, contain);
  }

  public static void DFSAdminRun(String cmd, int retcode, String contain,
      Configuration conf) throws Exception {
    DFSAdmin admin = new DFSAdmin(new Configuration(conf));
    toolRun(admin, cmd, retcode, contain);
  }

  public static void FsShellRun(String cmd, Configuration conf)
      throws Exception {
    FsShellRun(cmd, 0, null, conf);
  }

  public static void setNameNodeLogLevel(Level level) {
    GenericTestUtils.setLogLevel(FSNamesystem.LOG, level);
    GenericTestUtils.setLogLevel(BlockManager.LOG, level);
    GenericTestUtils.setLogLevel(LeaseManager.LOG, level);
    GenericTestUtils.setLogLevel(NameNode.LOG, level);
    GenericTestUtils.setLogLevel(NameNode.stateChangeLog, level);
    GenericTestUtils.setLogLevel(NameNode.blockStateChangeLog, level);
  }

  /**
   * Get the NamenodeProtocol RPC proxy for the NN associated with this
   * DFSClient object
   *
   * @param nameNodeUri the URI of the NN to get a proxy for.
   *
   * @return the Namenode RPC proxy associated with this DFSClient object
   */
  @VisibleForTesting
  public static NamenodeProtocol getNamenodeProtocolProxy(Configuration conf,
      URI nameNodeUri, UserGroupInformation ugi)
      throws IOException {
    return NameNodeProxies.createNonHAProxy(conf,
        DFSUtilClient.getNNAddress(nameNodeUri), NamenodeProtocol.class, ugi,
        false).getProxy();
  }

  /**
   * Get the RefreshUserMappingsProtocol RPC proxy for the NN associated with
   * this DFSClient object
   *
   * @param nameNodeUri the URI of the NN to get a proxy for.
   *
   * @return the RefreshUserMappingsProtocol RPC proxy associated with this
   * DFSClient object
   */
  @VisibleForTesting
  public static RefreshUserMappingsProtocol getRefreshUserMappingsProtocolProxy(
      Configuration conf, URI nameNodeUri) throws IOException {
    final AtomicBoolean nnFallbackToSimpleAuth = new AtomicBoolean(false);
    return NameNodeProxies.createProxy(conf,
        nameNodeUri, RefreshUserMappingsProtocol.class,
        nnFallbackToSimpleAuth).getProxy();
  }

  public static void waitForMetric(final JMXGet jmx, final String metricName, final int expectedValue)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          final int currentValue = Integer.parseInt(jmx.getValue(metricName));
          LOG.info("Waiting for " + metricName +
                       " to reach value " + expectedValue +
                       ", current value = " + currentValue);
          return currentValue == expectedValue;
        } catch (Exception e) {
          throw new UnhandledException("Test failed due to unexpected exception", e);
        }
      }
    }, 1000, 60000);
  }

  /**
   * Close current file system and create a new instance as given
   * {@link UserGroupInformation}.
   */
  public static FileSystem login(final FileSystem fs,
      final Configuration conf, final UserGroupInformation ugi)
          throws IOException, InterruptedException {
    if (fs != null) {
      fs.close();
    }
    return DFSTestUtil.getFileSystemAs(ugi, conf);
  }

  /**
   * Test if the given {@link FileStatus} user, group owner and its permission
   * are expected, throw {@link AssertionError} if any value is not expected.
   */
  public static void verifyFilePermission(FileStatus stat, String owner,
      String group, FsAction u, FsAction g, FsAction o) {
    if(stat != null) {
      if(!Strings.isNullOrEmpty(owner)) {
        assertEquals(owner, stat.getOwner());
      }
      if(!Strings.isNullOrEmpty(group)) {
        assertEquals(group, stat.getGroup());
      }
      FsPermission permission = stat.getPermission();
      if(u != null) {
        assertEquals(u, permission.getUserAction());
      }
      if (g != null) {
        assertEquals(g, permission.getGroupAction());
      }
      if (o != null) {
        assertEquals(o, permission.getOtherAction());
      }
    }
  }

  public static void verifyDelete(FsShell shell, FileSystem fs, Path path,
      boolean shouldExistInTrash) throws Exception {
    Path trashPath = Path.mergePaths(shell.getCurrentTrashDir(path), path);

    verifyDelete(shell, fs, path, trashPath, shouldExistInTrash);
  }

  public static void verifyDelete(FsShell shell, FileSystem fs, Path path,
      Path trashPath, boolean shouldExistInTrash) throws Exception {
    assertTrue(path + " file does not exist", fs.exists(path));

    // Verify that trashPath has a path component named ".Trash"
    Path checkTrash = trashPath;
    while (!checkTrash.isRoot() && !checkTrash.getName().equals(".Trash")) {
      checkTrash = checkTrash.getParent();
    }
    assertEquals("No .Trash component found in trash path " + trashPath,
        ".Trash", checkTrash.getName());

    String[] argv = new String[]{"-rm", "-r", path.toString()};
    int res = ToolRunner.run(shell, argv);
    assertEquals("rm failed", 0, res);
    if (shouldExistInTrash) {
      assertTrue("File not in trash : " + trashPath, fs.exists(trashPath));
    } else {
      assertFalse("File in trash : " + trashPath, fs.exists(trashPath));
    }
  }

  public static Map<Path, FSDataOutputStream> createOpenFiles(FileSystem fs,
      String filePrefix, int numFilesToCreate) throws IOException {
    final Map<Path, FSDataOutputStream> filesCreated = new HashMap<>();
    final byte[] buffer = new byte[(int) (1024 * 1.75)];
    final Random rand = new Random(0xFEED0BACL);
    for (int i = 0; i < numFilesToCreate; i++) {
      Path file = new Path("/" + filePrefix + "-" + i);
      FSDataOutputStream stm = fs.create(file, true, 1024, (short) 1, 1024);
      rand.nextBytes(buffer);
      stm.write(buffer);
      filesCreated.put(file, stm);
    }
    return filesCreated;
  }

  public static HashSet<Path> closeOpenFiles(
      HashMap<Path, FSDataOutputStream> openFilesMap,
      int numFilesToClose) throws IOException {
    HashSet<Path> closedFiles = new HashSet<>();
    for (Iterator<Entry<Path, FSDataOutputStream>> it =
         openFilesMap.entrySet().iterator(); it.hasNext();) {
      Entry<Path, FSDataOutputStream> entry = it.next();
      LOG.info("Closing file: " + entry.getKey());
      entry.getValue().close();
      closedFiles.add(entry.getKey());
      it.remove();
      numFilesToClose--;
      if (numFilesToClose == 0) {
        break;
      }
    }
    return closedFiles;
  }

  /**
   * Check the correctness of the snapshotDiff report.
   * Make sure all items in the passed entries are in the snapshotDiff
   * report.
   */
  public static void verifySnapshotDiffReport(DistributedFileSystem fs,
      Path dir, String from, String to,
      DiffReportEntry... entries) throws IOException {
    SnapshotDiffReport report = fs.getSnapshotDiffReport(dir, from, to);
    // reverse the order of from and to
    SnapshotDiffReport inverseReport = fs
        .getSnapshotDiffReport(dir, to, from);
    LOG.info(report.toString());
    LOG.info(inverseReport.toString() + "\n");

    assertEquals(entries.length, report.getDiffList().size());
    assertEquals(entries.length, inverseReport.getDiffList().size());

    for (DiffReportEntry entry : entries) {
      if (entry.getType() == DiffType.MODIFY) {
        assertTrue(report.getDiffList().contains(entry));
        assertTrue(inverseReport.getDiffList().contains(entry));
      } else if (entry.getType() == DiffType.DELETE) {
        assertTrue(report.getDiffList().contains(entry));
        assertTrue(inverseReport.getDiffList().contains(
            new DiffReportEntry(DiffType.CREATE, entry.getSourcePath())));
      } else if (entry.getType() == DiffType.CREATE) {
        assertTrue(report.getDiffList().contains(entry));
        assertTrue(inverseReport.getDiffList().contains(
            new DiffReportEntry(DiffType.DELETE, entry.getSourcePath())));
      }
    }
  }
}
