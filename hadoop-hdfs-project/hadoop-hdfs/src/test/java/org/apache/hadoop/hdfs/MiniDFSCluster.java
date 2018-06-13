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

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_STANDBY_CHECKPOINTS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_EXTENSION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMESERVICES;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMESERVICE_ID;
import static org.apache.hadoop.hdfs.server.common.Util.fileAsURI;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ha.HAServiceProtocol.RequestSource;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.MiniDFSNNTopology.NNConf;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * This class creates a single-process DFS cluster for junit testing.
 * The data directories for non-simulated DFS are under the testing directory.
 * For simulated data nodes, no underlying fs storage is used.
 */
@InterfaceAudience.LimitedPrivate({"HBase", "HDFS", "Hive", "MapReduce", "Pig"})
@InterfaceStability.Unstable
public class MiniDFSCluster implements AutoCloseable {

  private static final String NAMESERVICE_ID_PREFIX = "nameserviceId";
  private static final Log LOG = LogFactory.getLog(MiniDFSCluster.class);
  /** System property to set the data dir: {@value} */
  public static final String PROP_TEST_BUILD_DATA =
      GenericTestUtils.SYSPROP_TEST_DATA_DIR;
  /** Configuration option to set the data dir: {@value} */
  public static final String HDFS_MINIDFS_BASEDIR = "hdfs.minidfs.basedir";
  /** Configuration option to set the provided data dir: {@value} */
  public static final String HDFS_MINIDFS_BASEDIR_PROVIDED =
      "hdfs.minidfs.basedir.provided";
  public static final String  DFS_NAMENODE_SAFEMODE_EXTENSION_TESTING_KEY
      = DFS_NAMENODE_SAFEMODE_EXTENSION_KEY + ".testing";

  static { DefaultMetricsSystem.setMiniClusterMode(true); }
  /**
   * Class to construct instances of MiniDFSClusters with specific options.
   */
  public static class Builder {
    private int nameNodePort = 0;
    private int nameNodeHttpPort = 0;
    private final Configuration conf;
    private boolean format = true;
    private boolean manageNameDfsDirs = true;
    private boolean manageNameDfsSharedDirs = true;
    private boolean enableManagedDfsDirsRedundancy = true;
    private StartupOption option = null;
    private String clusterId = null;
    private boolean waitSafeMode = true;
    private boolean setupHostsFile = false;
    private MiniDFSNNTopology nnTopology = null;
    private boolean checkExitOnShutdown = true;
    private boolean skipFsyncForTesting = true;

    public Builder(Configuration conf) {
      this.conf = conf;
      if (null == conf.get(HDFS_MINIDFS_BASEDIR)) {
        conf.set(HDFS_MINIDFS_BASEDIR,
            new File(getBaseDirectory()).getAbsolutePath());
      }
    }

    public Builder(Configuration conf, File basedir) {
      this.conf = conf;
      if (null == basedir) {
        throw new IllegalArgumentException(
            "MiniDFSCluster base directory cannot be null");
      }
      String cdir = conf.get(HDFS_MINIDFS_BASEDIR);
      if (cdir != null) {
        throw new IllegalArgumentException(
            "MiniDFSCluster base directory already defined (" + cdir + ")");
      }
      conf.set(HDFS_MINIDFS_BASEDIR, basedir.getAbsolutePath());
    }

    /**
     * Default: 0
     */
    public Builder nameNodePort(int val) {
      this.nameNodePort = val;
      return this;
    }

    /**
     * Default: 0
     */
    public Builder nameNodeHttpPort(int val) {
      this.nameNodeHttpPort = val;
      return this;
    }

    /**
     * Default: true
     */
    public Builder format(boolean val) {
      this.format = val;
      return this;
    }

    /**
     * Default: true
     */
    public Builder manageNameDfsDirs(boolean val) {
      this.manageNameDfsDirs = val;
      return this;
    }

    /**
     * Default: true
     */
    public Builder manageNameDfsSharedDirs(boolean val) {
      this.manageNameDfsSharedDirs = val;
      return this;
    }

    /**
     * Default: true
     */
    public Builder enableManagedDfsDirsRedundancy(boolean val) {
      this.enableManagedDfsDirsRedundancy = val;
      return this;
    }

    /**
     * Default: null
     */
    public Builder startupOption(StartupOption val) {
      this.option = val;
      return this;
    }

    /**
     * Default: true
     */
    public Builder waitSafeMode(boolean val) {
      this.waitSafeMode = val;
      return this;
    }

    /**
     * Default: true
     */
    public Builder checkExitOnShutdown(boolean val) {
      this.checkExitOnShutdown = val;
      return this;
    }

    /**
     * Default: null
     */
    public Builder clusterId(String cid) {
      this.clusterId = cid;
      return this;
    }

    /**
     * Default: a single namenode.
     * See {@link MiniDFSNNTopology#simpleFederatedTopology(int)} to set up
     * federated nameservices
     */
    public Builder nnTopology(MiniDFSNNTopology topology) {
      this.nnTopology = topology;
      return this;
    }

    /**
     * Default: true
     * When true, we skip fsync() calls for speed improvements.
     */
    public Builder skipFsyncForTesting(boolean val) {
      this.skipFsyncForTesting = val;
      return this;
    }

    /**
     * Construct the actual MiniDFSCluster
     */
    public MiniDFSCluster build() throws IOException {
      return new MiniDFSCluster(this);
    }
  }
  
  /**
   * Used by builder to create and return an instance of MiniDFSCluster
   */
  protected MiniDFSCluster(Builder builder) throws IOException {
    if (builder.nnTopology == null) {
      // If no topology is specified, build a single NN. 
      builder.nnTopology = MiniDFSNNTopology.simpleSingleNN(
          builder.nameNodePort, builder.nameNodeHttpPort);
    }
    final int numNameNodes = builder.nnTopology.countNameNodes();
    LOG.info("starting cluster: numNameNodes=" + numNameNodes);

    initMiniDFSCluster(builder.conf,
                       builder.format,
                       builder.manageNameDfsDirs,
                       builder.manageNameDfsSharedDirs,
                       builder.enableManagedDfsDirsRedundancy,
                       builder.option,
                       builder.clusterId,
                       builder.waitSafeMode, builder.nnTopology,
                       builder.checkExitOnShutdown,
                       builder.skipFsyncForTesting);
  }
  
  private Configuration conf;
  private Multimap<String, NameNodeInfo> namenodes = ArrayListMultimap.create();
  private File base_dir;
  private File data_dir;
  private boolean waitSafeMode = true;
  private boolean federation;
  private boolean checkExitOnShutdown = true;
  private Set<FileSystem> fileSystems = Sets.newHashSet();

  private List<long[]> storageCap = Lists.newLinkedList();

  /**
   * A unique instance identifier for the cluster. This
   * is used to disambiguate HA filesystems in the case where
   * multiple MiniDFSClusters are used in the same test suite.
   */
  private int instanceId;
  private static int instanceCount = 0;

  /**
   * Stores the information related to a namenode in the cluster
   */
  public static class NameNodeInfo {
    public NameNode nameNode;
    Configuration conf;
    String nameserviceId;
    String nnId;
    StartupOption startOpt;
    NameNodeInfo(NameNode nn, String nameserviceId, String nnId,
        StartupOption startOpt, Configuration conf) {
      this.nameNode = nn;
      this.nameserviceId = nameserviceId;
      this.nnId = nnId;
      this.startOpt = startOpt;
      this.conf = conf;
    }

    public void setStartOpt(StartupOption startOpt) {
      this.startOpt = startOpt;
    }

    public String getNameserviceId() {
      return this.nameserviceId;
    }

    public String getNamenodeId() {
      return this.nnId;
    }
  }

  /**
   * This null constructor is used only when wishing to start a data node cluster
   * without a name node (ie when the name node is started elsewhere).
   */
  public MiniDFSCluster() {
    synchronized (MiniDFSCluster.class) {
      instanceId = instanceCount++;
    }
  }

  /**
   * Modify the config and start up the servers with the given operation.
   * Servers will be started on free ports.
   * <p>
   * The caller must manage the creation of NameNode and DataNode directories
   * and have already set {@link DFSConfigKeys#DFS_NAMENODE_NAME_DIR_KEY}
   * in the given conf.
   *
   * @param conf the base configuration to use in starting the servers.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param nameNodeOperation the operation with which to start the servers.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   */
  @Deprecated // in 22 to be removed in 24. Use MiniDFSCluster.Builder instead
  public MiniDFSCluster(Configuration conf,
                        int numDataNodes,
                        StartupOption nameNodeOperation) throws IOException {
    this(0, conf, false, false, nameNodeOperation);
  }

  /**
   * Modify the config and start up the servers.  The rpc and info ports for
   * servers are guaranteed to use free ports.
   * <p>
   * NameNode and DataNode directory creation and configuration will be
   * managed by this class.
   *
   * @param conf the base configuration to use in starting the servers.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param format if true, format the NameNode and DataNodes before starting up
   * @param racks array of strings indicating the rack that each DataNode is on
   */
  @Deprecated // in 22 to be removed in 24. Use MiniDFSCluster.Builder instead
  public MiniDFSCluster(Configuration conf,
                        int numDataNodes,
                        boolean format,
                        String[] racks) throws IOException {
    this(0, conf, format, true, null);
  }

  /**
   * Modify the config and start up the servers.  The rpc and info ports for
   * servers are guaranteed to use free ports.
   * <p>
   * NameNode and DataNode directory creation and configuration will be
   * managed by this class.
   *
   * @param conf the base configuration to use in starting the servers.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param format if true, format the NameNode and DataNodes before starting up
   * @param racks array of strings indicating the rack that each DataNode is on
   * @param hosts array of strings indicating the hostname for each DataNode
   */
  @Deprecated // in 22 to be removed in 24. Use MiniDFSCluster.Builder instead
  public MiniDFSCluster(Configuration conf,
                        int numDataNodes,
                        boolean format,
                        String[] racks, String[] hosts) throws IOException {
    this(0, conf, format, true, null);
  }

  /**
   * NOTE: if possible, the other constructors that don't have nameNode port
   * parameter should be used as they will ensure that the servers use free
   * ports.
   * <p>
   * Modify the config and start up the servers.
   *
   * @param nameNodePort suggestion for which rpc port to use.  caller should
   *          use getNameNodePort() to get the actual port used.
   * @param conf the base configuration to use in starting the servers.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param format if true, format the NameNode and DataNodes before starting
   *          up
   * @param manageDfsDirs if true, the data directories for servers will be
   *          created and {@link DFSConfigKeys#DFS_NAMENODE_NAME_DIR_KEY}
   *          will be set in the conf
   * @param operation the operation with which to start the servers.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   */
  @Deprecated // in 22 to be removed in 24. Use MiniDFSCluster.Builder instead
  public MiniDFSCluster(int nameNodePort,
                        Configuration conf,
                        int numDataNodes,
                        boolean format,
                        boolean manageDfsDirs,
                        StartupOption operation,
                        String[] racks) throws IOException {
    this(nameNodePort, conf, format, manageDfsDirs, operation);
  }

  /**
   * NOTE: if possible, the other constructors that don't have nameNode port
   * parameter should be used as they will ensure that the servers use free ports.
   * <p>
   * Modify the config and start up the servers.
   *
   * @param nameNodePort suggestion for which rpc port to use.  caller should
   *          use getNameNodePort() to get the actual port used.
   * @param conf the base configuration to use in starting the servers.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param format if true, format the NameNode and DataNodes before starting up
   * @param manageDfsDirs if true, the data directories for servers will be
   *          created and {@link DFSConfigKeys#DFS_NAMENODE_NAME_DIR_KEY}
   *          will be set in the conf
   * @param operation the operation with which to start the servers.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   * @param simulatedCapacities array of capacities of the simulated data nodes
   */
  @Deprecated // in 22 to be removed in 24. Use MiniDFSCluster.Builder instead
  public MiniDFSCluster(int nameNodePort,
                        Configuration conf,
                        int numDataNodes,
                        boolean format,
                        boolean manageDfsDirs,
                        StartupOption operation,
                        String[] racks,
                        long[] simulatedCapacities) throws IOException {
    this(nameNodePort, conf, format, manageDfsDirs, operation);
  }

  /**
   * NOTE: if possible, the other constructors that don't have nameNode port
   * parameter should be used as they will ensure that the servers use free ports.
   * <p>
   * Modify the config and start up the servers.
   *  @param nameNodePort suggestion for which rpc port to use.  caller should
   *          use getNameNodePort() to get the actual port used.
   * @param conf the base configuration to use in starting the servers.  This
   *          will be modified as necessary.
   * @param format if true, format the NameNode and DataNodes before starting up
   * @param manageNameDfsDirs if true, the data directories for servers will be
*          created and {@link DFSConfigKeys#DFS_NAMENODE_NAME_DIR_KEY} and
*          will be set in
*          the conf
   * @param operation the operation with which to start the servers.  If null
*          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   */
  @Deprecated // in 22 to be removed in 24. Use MiniDFSCluster.Builder instead
  public MiniDFSCluster(int nameNodePort, Configuration conf, boolean format,
      boolean manageNameDfsDirs, StartupOption operation) throws IOException {
    initMiniDFSCluster(conf, format,
                       manageNameDfsDirs, true, true, operation,
        null, true, MiniDFSNNTopology.simpleSingleNN(nameNodePort, 0),
                       true, true);
  }

  private void initMiniDFSCluster(Configuration conf, boolean format, boolean manageNameDfsDirs,
      boolean manageNameDfsSharedDirs, boolean enableManagedDfsDirsRedundancy,
      StartupOption startOpt, String clusterId, boolean waitSafeMode,
      MiniDFSNNTopology nnTopology, boolean checkExitOnShutdown,
      boolean skipFsyncForTesting)
  throws IOException {
    boolean success = false;
    try {
      ExitUtil.disableSystemExit();

      // Re-enable symlinks for tests, see HADOOP-10020 and HADOOP-10052
      FileSystem.enableSymlinks();

      synchronized (MiniDFSCluster.class) {
        instanceId = instanceCount++;
      }

      this.conf = conf;
      base_dir = new File(determineDfsBaseDir());
      data_dir = new File(base_dir, "data");
      this.waitSafeMode = waitSafeMode;
      this.checkExitOnShutdown = checkExitOnShutdown;

      int safemodeExtension = conf.getInt(
          DFS_NAMENODE_SAFEMODE_EXTENSION_TESTING_KEY, 0);
      conf.setInt(DFS_NAMENODE_SAFEMODE_EXTENSION_KEY, safemodeExtension);

      // In an HA cluster, in order for the StandbyNode to perform checkpoints,
      // it needs to know the HTTP port of the Active. So, if ephemeral ports
      // are chosen, disable checkpoints for the test.
      if (!nnTopology.allHttpPortsSpecified() &&
          nnTopology.isHA()) {
        LOG.info("MiniDFSCluster disabling checkpointing in the Standby node " +
            "since no HTTP ports have been specified.");
        conf.setBoolean(DFS_HA_STANDBY_CHECKPOINTS_KEY, false);
      }
      if (!nnTopology.allIpcPortsSpecified() &&
          nnTopology.isHA()) {
        LOG.info("MiniDFSCluster disabling log-roll triggering in the "
            + "Standby node since no IPC ports have been specified.");
        conf.setInt(DFS_HA_LOGROLL_PERIOD_KEY, -1);
      }

      EditLogFileOutputStream.setShouldSkipFsyncForTesting(skipFsyncForTesting);
    
      federation = nnTopology.isFederated();
      try {
        createNameNodesAndSetConf(
            nnTopology, manageNameDfsDirs, manageNameDfsSharedDirs,
            enableManagedDfsDirsRedundancy,
            format, startOpt, clusterId);
      } catch (IOException ioe) {
        LOG.error("IOE creating namenodes. Permissions dump:\n" +
            createPermissionsDiagnosisString(data_dir), ioe);
        throw ioe;
      }
      if (format) {
        if (data_dir.exists() && !FileUtil.fullyDelete(data_dir)) {
          throw new IOException("Cannot remove data directory: " + data_dir +
              createPermissionsDiagnosisString(data_dir));
        }
      }
    
      if (startOpt == StartupOption.RECOVER) {
        return;
      }

      waitClusterUp();
      //make sure ProxyUsers uses the latest conf
      ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
      success = true;
    } finally {
      if (!success) {
        shutdown();
      }
    }
  }
  
  /**
   * @return a debug string which can help diagnose an error of why
   * a given directory might have a permissions error in the context
   * of a test case
   */
  private String createPermissionsDiagnosisString(File path) {
    StringBuilder sb = new StringBuilder();
    while (path != null) { 
      sb.append("path '" + path + "': ").append("\n");
      sb.append("\tabsolute:").append(path.getAbsolutePath()).append("\n");
      sb.append("\tpermissions: ");
      sb.append(path.isDirectory() ? "d": "-");
      sb.append(FileUtil.canRead(path) ? "r" : "-");
      sb.append(FileUtil.canWrite(path) ? "w" : "-");
      sb.append(FileUtil.canExecute(path) ? "x" : "-");
      sb.append("\n");
      path = path.getParentFile();
    }
    return sb.toString();
  }

  private void createNameNodesAndSetConf(MiniDFSNNTopology nnTopology,
      boolean manageNameDfsDirs, boolean manageNameDfsSharedDirs,
      boolean enableManagedDfsDirsRedundancy, boolean format,
      StartupOption operation, String clusterId) throws IOException {
    // do the basic namenode configuration
    configureNameNodes(nnTopology, federation, conf);

    int nnCounter = 0;
    int nsCounter = 0;
    // configure each NS independently
    for (MiniDFSNNTopology.NSConf nameservice : nnTopology.getNameservices()) {
      configureNameService(nameservice, nsCounter++, manageNameDfsSharedDirs,
          manageNameDfsDirs, enableManagedDfsDirsRedundancy,
          format, operation, clusterId, nnCounter);
      nnCounter += nameservice.getNNs().size();
    }

    for (NameNodeInfo nn : namenodes.values()) {
      Configuration nnConf = nn.conf;
      for (NameNodeInfo nnInfo : namenodes.values()) {
        if (nn.equals(nnInfo)) {
          continue;
        }
       copyKeys(conf, nnConf, nnInfo.nameserviceId, nnInfo.nnId);
      }
    }
  }

  private static void copyKeys(Configuration srcConf, Configuration destConf,
      String nameserviceId, String nnId) {
    String key = DFSUtil.addKeySuffixes(DFS_NAMENODE_RPC_ADDRESS_KEY,
      nameserviceId, nnId);
    destConf.set(key, srcConf.get(key));

    copyKey(srcConf, destConf, nameserviceId, nnId,
        DFS_NAMENODE_HTTP_ADDRESS_KEY);
    copyKey(srcConf, destConf, nameserviceId, nnId,
        DFS_NAMENODE_HTTPS_ADDRESS_KEY);
    copyKey(srcConf, destConf, nameserviceId, nnId,
        DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY);
    copyKey(srcConf, destConf, nameserviceId, nnId,
        DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY);
  }

  private static void copyKey(Configuration srcConf, Configuration destConf,
      String nameserviceId, String nnId, String baseKey) {
    String key = DFSUtil.addKeySuffixes(baseKey, nameserviceId, nnId);
    String val = srcConf.get(key);
    if (val != null) {
      destConf.set(key, srcConf.get(key));
    }
  }

  /**
   * Do the rest of the NN configuration for things like shared edits,
   * as well as directory formatting, etc. for a single nameservice
   * @param nnCounter the count of the number of namenodes already configured/started. Also,
   *                  acts as the <i>index</i> to the next NN to start (since indicies start at 0).
   * @throws IOException
   */
  private void configureNameService(MiniDFSNNTopology.NSConf nameservice, int nsCounter,
      boolean manageNameDfsSharedDirs, boolean manageNameDfsDirs, boolean
      enableManagedDfsDirsRedundancy, boolean format,
      StartupOption operation, String clusterId,
      final int nnCounter) throws IOException{
    String nsId = nameservice.getId();
    String lastDefaultFileSystem = null;

    // If HA is enabled on this nameservice, enumerate all the namenodes
    // in the configuration. Also need to set a shared edits dir
    int numNNs = nameservice.getNNs().size();
    if (numNNs > 1 && manageNameDfsSharedDirs) {
      URI sharedEditsUri = getSharedEditsDir(nnCounter, nnCounter + numNNs - 1);
      conf.set(DFS_NAMENODE_SHARED_EDITS_DIR_KEY, sharedEditsUri.toString());
      // Clean out the shared edits dir completely, including all subdirectories.
      FileUtil.fullyDelete(new File(sharedEditsUri));
    }

    // Now format first NN and copy the storage directory from that node to the others.
    int nnIndex = nnCounter;
    Collection<URI> prevNNDirs = null;
    for (NNConf nn : nameservice.getNNs()) {
      initNameNodeConf(conf, nsId, nsCounter, nn.getNnId(), manageNameDfsDirs,
          manageNameDfsDirs,  nnIndex);
      Collection<URI> namespaceDirs = FSNamesystem.getNamespaceDirs(conf);
      if (format) {
        // delete the existing namespaces
        for (URI nameDirUri : namespaceDirs) {
          File nameDir = new File(nameDirUri);
          if (nameDir.exists() && !FileUtil.fullyDelete(nameDir)) {
            throw new IOException("Could not fully delete " + nameDir);
          }
        }

        // delete the checkpoint directories, if they exist
        Collection<URI> checkpointDirs = Util.stringCollectionAsURIs(conf
            .getTrimmedStringCollection(DFS_NAMENODE_CHECKPOINT_DIR_KEY));
        for (URI checkpointDirUri : checkpointDirs) {
          File checkpointDir = new File(checkpointDirUri);
          if (checkpointDir.exists() && !FileUtil.fullyDelete(checkpointDir)) {
            throw new IOException("Could not fully delete " + checkpointDir);
          }
        }
      }

      boolean formatThisOne = format;
      // if we are looking at not the first NN
      if (nnIndex++ > nnCounter && format) {
        // Don't format the second, third, etc NN in an HA setup - that
        // would result in it having a different clusterID,
        // block pool ID, etc. Instead, copy the name dirs
        // from the previous one.
        formatThisOne = false;
        assert (null != prevNNDirs);
        copyNameDirs(prevNNDirs, namespaceDirs, conf);
      }

      if (formatThisOne) {
        // Allow overriding clusterID for specific NNs to test
        // misconfiguration.
        if (nn.getClusterId() == null) {
          StartupOption.FORMAT.setClusterId(clusterId);
        } else {
          StartupOption.FORMAT.setClusterId(nn.getClusterId());
        }
        DFSTestUtil.formatNameNode(conf);
      }
      prevNNDirs = namespaceDirs;
    }

    // create all the namenodes in the namespace
    nnIndex = nnCounter;
    for (NNConf nn : nameservice.getNNs()) {
      Configuration hdfsConf = new Configuration(conf);
      initNameNodeConf(hdfsConf, nsId, nsCounter, nn.getNnId(), manageNameDfsDirs,
          enableManagedDfsDirsRedundancy, nnIndex++);
      createNameNode(hdfsConf, false, operation,
          clusterId, nsId, nn.getNnId());
      // Record the last namenode uri
      lastDefaultFileSystem = hdfsConf.get(FS_DEFAULT_NAME_KEY);
    }
    if (!federation && lastDefaultFileSystem != null) {
      // Set the default file system to the actual bind address of NN.
      conf.set(FS_DEFAULT_NAME_KEY, lastDefaultFileSystem);
    }
  }

  /**
   * Do the basic NN configuration for the topology. Does not configure things like the shared
   * edits directories
   * @param nnTopology
   * @param federation
   * @param conf
   * @throws IOException
   */
  public static void configureNameNodes(MiniDFSNNTopology nnTopology, boolean federation,
      Configuration conf) throws IOException {
    Preconditions.checkArgument(nnTopology.countNameNodes() > 0,
        "empty NN topology: no namenodes specified!");

    if (!federation && nnTopology.countNameNodes() == 1) {
      NNConf onlyNN = nnTopology.getOnlyNameNode();
      // we only had one NN, set DEFAULT_NAME for it. If not explicitly
      // specified initially, the port will be 0 to make NN bind to any
      // available port. It will be set to the right address after
      // NN is started.
      conf.set(FS_DEFAULT_NAME_KEY, "hdfs://127.0.0.1:" + onlyNN.getIpcPort());
    }

    List<String> allNsIds = Lists.newArrayList();
    for (MiniDFSNNTopology.NSConf nameservice : nnTopology.getNameservices()) {
      if (nameservice.getId() != null) {
        allNsIds.add(nameservice.getId());
      }
    }

    if (!allNsIds.isEmpty()) {
      conf.set(DFS_NAMESERVICES, Joiner.on(",").join(allNsIds));
    }

    for (MiniDFSNNTopology.NSConf nameservice : nnTopology.getNameservices()) {
      String nsId = nameservice.getId();

      Preconditions.checkArgument(
          !federation || nsId != null,
          "if there is more than one NS, they must have names");

      // First set up the configuration which all of the NNs
      // need to have - have to do this a priori before starting
      // *any* of the NNs, so they know to come up in standby.
      List<String> nnIds = Lists.newArrayList();
      // Iterate over the NNs in this nameservice
      for (NNConf nn : nameservice.getNNs()) {
        nnIds.add(nn.getNnId());

        initNameNodeAddress(conf, nameservice.getId(), nn);
      }

      // If HA is enabled on this nameservice, enumerate all the namenodes
      // in the configuration. Also need to set a shared edits dir
      if (nnIds.size() > 1) {
        conf.set(DFSUtil.addKeySuffixes(DFS_HA_NAMENODES_KEY_PREFIX, nameservice.getId()), Joiner
            .on(",").join(nnIds));
      }
    }
  }
  
  public URI getSharedEditsDir(int minNN, int maxNN) throws IOException {
    return formatSharedEditsDir(base_dir, minNN, maxNN);
  }
  
  public static URI formatSharedEditsDir(File baseDir, int minNN, int maxNN)
      throws IOException {
    return fileAsURI(new File(baseDir, "shared-edits-" +
        minNN + "-through-" + maxNN));
  }
  
  public NameNodeInfo[] getNameNodeInfos() {
    return this.namenodes.values().toArray(new NameNodeInfo[0]);
  }

  /**
   * @param nsIndex index of the namespace id to check
   * @return all the namenodes bound to the given namespace index
   */
  public NameNodeInfo[] getNameNodeInfos(int nsIndex) {
    int i = 0;
    for (String ns : this.namenodes.keys()) {
      if (i++ == nsIndex) {
        return this.namenodes.get(ns).toArray(new NameNodeInfo[0]);
      }
    }
    return null;
  }

  /**
   * @param nameservice id of nameservice to read
   * @return all the namenodes bound to the given namespace index
   */
  public NameNodeInfo[] getNameNodeInfos(String nameservice) {
    for (String ns : this.namenodes.keys()) {
      if (nameservice.equals(ns)) {
        return this.namenodes.get(ns).toArray(new NameNodeInfo[0]);
      }
    }
    return null;
  }


  private void initNameNodeConf(Configuration conf, String nameserviceId, int nsIndex, String nnId,
      boolean manageNameDfsDirs, boolean enableManagedDfsDirsRedundancy, int nnIndex)
      throws IOException {
    if (nameserviceId != null) {
      conf.set(DFS_NAMESERVICE_ID, nameserviceId);
    }
    if (nnId != null) {
      conf.set(DFS_HA_NAMENODE_ID_KEY, nnId);
    }
    if (manageNameDfsDirs) {
      if (enableManagedDfsDirsRedundancy) {
        File[] files = getNameNodeDirectory(nsIndex, nnIndex);
        conf.set(DFS_NAMENODE_NAME_DIR_KEY, fileAsURI(files[0]) + "," + fileAsURI(files[1]));
        files = getCheckpointDirectory(nsIndex, nnIndex);
        conf.set(DFS_NAMENODE_CHECKPOINT_DIR_KEY, fileAsURI(files[0]) + "," + fileAsURI(files[1]));
      } else {
        File[] files = getNameNodeDirectory(nsIndex, nnIndex);
        conf.set(DFS_NAMENODE_NAME_DIR_KEY, fileAsURI(files[0]).toString());
        files = getCheckpointDirectory(nsIndex, nnIndex);
        conf.set(DFS_NAMENODE_CHECKPOINT_DIR_KEY, fileAsURI(files[0]).toString());
      }
    }
  }

  private File[] getNameNodeDirectory(int nameserviceIndex, int nnIndex) {
    return getNameNodeDirectory(base_dir, nameserviceIndex, nnIndex);
  }

  public static File[] getNameNodeDirectory(String base_dir, int nsIndex, int nnIndex) {
    return getNameNodeDirectory(new File(base_dir), nsIndex, nnIndex);
  }

  public static File[] getNameNodeDirectory(File base_dir, int nsIndex, int nnIndex) {
    File[] files = new File[2];
    files[0] = new File(base_dir, "name-" + nsIndex + "-" + (2 * nnIndex + 1));
    files[1] = new File(base_dir, "name-" + nsIndex + "-" + (2 * nnIndex + 2));
    return files;
  }

  public File[] getCheckpointDirectory(int nsIndex, int nnIndex) {
    return getCheckpointDirectory(base_dir, nsIndex, nnIndex);
  }

  public static File[] getCheckpointDirectory(String base_dir, int nsIndex, int nnIndex) {
    return getCheckpointDirectory(new File(base_dir), nsIndex, nnIndex);
  }

  public static File[] getCheckpointDirectory(File base_dir, int nsIndex, int nnIndex) {
    File[] files = new File[2];
    files[0] = new File(base_dir, "namesecondary-" + nsIndex + "-" + (2 * nnIndex + 1));
    files[1] = new File(base_dir, "namesecondary-" + nsIndex + "-" + (2 * nnIndex + 2));
    return files;
  }


  public static void copyNameDirs(Collection<URI> srcDirs, Collection<URI> dstDirs,
      Configuration dstConf) throws IOException {
    URI srcDir = Lists.newArrayList(srcDirs).get(0);
    FileSystem dstFS = FileSystem.getLocal(dstConf).getRaw();
    for (URI dstDir : dstDirs) {
      Preconditions.checkArgument(!dstDir.equals(srcDir),
          "src and dst are the same: " + dstDir);
      File dstDirF = new File(dstDir);
      if (dstDirF.exists()) {
        if (!FileUtil.fullyDelete(dstDirF)) {
          throw new IOException("Unable to delete: " + dstDirF);
        }
      }
      LOG.info("Copying namedir from primary node dir "
          + srcDir + " to " + dstDir);
      FileUtil.copy(
          new File(srcDir),
          dstFS, new Path(dstDir), false, dstConf);
    }
  }

  /**
   * Initialize the address and port for this NameNode. In the
   * non-federated case, the nameservice and namenode ID may be
   * null.
   */
  private static void initNameNodeAddress(Configuration conf,
      String nameserviceId, NNConf nnConf) {
    // Set NN-specific specific key
    String key = DFSUtil.addKeySuffixes(
        DFS_NAMENODE_HTTP_ADDRESS_KEY, nameserviceId,
        nnConf.getNnId());
    conf.set(key, "127.0.0.1:" + nnConf.getHttpPort());

    key = DFSUtil.addKeySuffixes(
        DFS_NAMENODE_RPC_ADDRESS_KEY, nameserviceId,
        nnConf.getNnId());
    conf.set(key, "127.0.0.1:" + nnConf.getIpcPort());
  }
  
  private static String[] createArgs(StartupOption operation) {
    if (operation == StartupOption.ROLLINGUPGRADE) {
      return new String[]{operation.getName(),
          operation.getRollingUpgradeStartupOption().name()};
    }
    String[] args = (operation == null ||
        operation == StartupOption.FORMAT ||
        operation == StartupOption.REGULAR) ?
            new String[] {} : new String[] {operation.getName()};
    return args;
  }

  private void createNameNode(Configuration hdfsConf, boolean format, StartupOption operation,
      String clusterId, String nameserviceId, String nnId) throws IOException {
    // Format and clean out DataNode directories
    if (format) {
      DFSTestUtil.formatNameNode(hdfsConf);
    }
    if (operation == StartupOption.UPGRADE){
      operation.setClusterId(clusterId);
    }

    String[] args = createArgs(operation);
    NameNode nn =  NameNode.createNameNode(args, hdfsConf);
    if (operation == StartupOption.RECOVER) {
      return;
    }
    // After the NN has started, set back the bound ports into
    // the conf
    hdfsConf.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_RPC_ADDRESS_KEY,
        nameserviceId, nnId), nn.getNameNodeAddressHostPortString());
    if (nn.getHttpAddress() != null) {
      hdfsConf.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_HTTP_ADDRESS_KEY,
          nameserviceId, nnId), NetUtils.getHostPortString(nn.getHttpAddress()));
    }
    if (nn.getHttpsAddress() != null) {
      hdfsConf.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_HTTPS_ADDRESS_KEY,
          nameserviceId, nnId), NetUtils.getHostPortString(nn.getHttpsAddress()));
    }
    if (hdfsConf.get(DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY) != null) {
      hdfsConf.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY,
          nameserviceId, nnId),
          hdfsConf.get(DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY));
    }
    copyKeys(hdfsConf, conf, nameserviceId, nnId);
    DFSUtil.setGenericConf(hdfsConf, nameserviceId, nnId,
        DFS_NAMENODE_HTTP_ADDRESS_KEY);
    NameNodeInfo info = new NameNodeInfo(nn, nameserviceId, nnId,
        operation, hdfsConf);
    namenodes.put(nameserviceId, info);
  }

  /**
   * @return URI of the namenode from a single namenode MiniDFSCluster
   */
  public URI getURI() {
    checkSingleNameNode();
    return getURI(0);
  }
  
  /**
   * @return URI of the given namenode in MiniDFSCluster
   */
  public URI getURI(int nnIndex) {
    String hostPort =
        getNN(nnIndex).nameNode.getNameNodeAddressHostPortString();
    URI uri = null;
    try {
      uri = new URI("hdfs://" + hostPort);
    } catch (URISyntaxException e) {
      NameNode.LOG.warn("unexpected URISyntaxException", e);
    }
    return uri;
  }
  
  public int getInstanceId() {
    return instanceId;
  }

  /**
   * @return Configuration of for the given namenode
   */
  public Configuration getConfiguration(int nnIndex) {
    return getNN(nnIndex).conf;
  }

  private NameNodeInfo getNN(int nnIndex) {
    int count = 0;
    for (NameNodeInfo nn : namenodes.values()) {
      if (count == nnIndex) {
        return nn;
      }
      count++;
    }
    return null;
  }

  /**
   * wait for the cluster to get out of safemode.
   */
  public void waitClusterUp() throws IOException {
  }

  /**
   * Finalize the namenode. Block pools corresponding to the namenode are
   * finalized on the datanode.
   */
  private void finalizeNamenode(NameNode nn, Configuration conf) throws Exception {
    if (nn == null) {
      throw new IllegalStateException("Attempting to finalize "
                                      + "Namenode but it is not running");
    }
    ToolRunner.run(new DFSAdmin(conf), new String[]{"-finalizeUpgrade"});
  }

  /**
   * If the NameNode is running, attempt to finalize a previous upgrade.
   * When this method return, the NameNode should be finalized, but
   * DataNodes may not be since that occurs asynchronously.
   *
   * @throws IllegalStateException if the Namenode is not running.
   */
  public void finalizeCluster(Configuration conf) throws Exception {
    for (NameNodeInfo nnInfo : namenodes.values()) {
      if (nnInfo == null) {
        throw new IllegalStateException("Attempting to finalize "
            + "Namenode but it is not running");
      }
      finalizeNamenode(nnInfo.nameNode, nnInfo.conf);
    }
  }

  public int getNumNameNodes() {
    return namenodes.size();
  }
  
  /**
   * Gets the started NameNode.  May be null.
   */
  public NameNode getNameNode() {
    checkSingleNameNode();
    return getNameNode(0);
  }
  
  /**
   * Get an instance of the NameNode's RPC handler.
   */
  public NamenodeProtocols getNameNodeRpc() {
    checkSingleNameNode();
    return getNameNodeRpc(0);
  }
  
  /**
   * Get an instance of the NameNode's RPC handler.
   */
  public NamenodeProtocols getNameNodeRpc(int nnIndex) {
    return getNameNode(nnIndex).getRpcServer();
  }
  
  /**
   * Gets the NameNode for the index.  May be null.
   */
  public NameNode getNameNode(int nnIndex) {
    return getNN(nnIndex).nameNode;
  }
  
  /**
   * Return the {@link FSNamesystem} object.
   * @return {@link FSNamesystem} object.
   */
  public FSNamesystem getNamesystem() {
    checkSingleNameNode();
    return NameNodeAdapter.getNamesystem(getNN(0).nameNode);
  }

  public FSNamesystem getNamesystem(int nnIndex) {
    return NameNodeAdapter.getNamesystem(getNN(nnIndex).nameNode);
  }

  /**
   * Gets the rpc port used by the NameNode, because the caller
   * supplied port is not necessarily the actual port used.
   * Assumption: cluster has a single namenode
   */     
  public int getNameNodePort() {
    checkSingleNameNode();
    return getNameNodePort(0);
  }
    
  /**
   * Gets the rpc port used by the NameNode at the given index, because the
   * caller supplied port is not necessarily the actual port used.
   */     
  public int getNameNodePort(int nnIndex) {
    return getNN(nnIndex).nameNode.getNameNodeAddress().getPort();
  }

  /**
   * @return the service rpc port used by the NameNode at the given index.
   */     
  public int getNameNodeServicePort(int nnIndex) {
    return getNN(nnIndex).nameNode.getServiceRpcAddress().getPort();
  }
    
  /**
   * Shutdown all the nodes in the cluster.
   */
  public void shutdown() {
      shutdown(false);
  }
    
  /**
   * Shutdown all the nodes in the cluster.
   */
  public void shutdown(boolean deleteDfsDir) {
    shutdown(deleteDfsDir, true);
  }

  /**
   * Shutdown all the nodes in the cluster.
   */
  public void shutdown(boolean deleteDfsDir, boolean closeFileSystem) {
    LOG.info("Shutting down the Mini HDFS Cluster");
    if (checkExitOnShutdown)  {
      if (ExitUtil.terminateCalled()) {
        LOG.fatal("Test resulted in an unexpected exit",
            ExitUtil.getFirstExitException());
        ExitUtil.resetFirstExitException();
        throw new AssertionError("Test resulted in an unexpected exit");
      }
    }
    if (closeFileSystem) {
      for (FileSystem fs : fileSystems) {
        try {
          fs.close();
        } catch (IOException ioe) {
          LOG.warn("Exception while closing file system", ioe);
        }
      }
      fileSystems.clear();
    }
    for (NameNodeInfo nnInfo : namenodes.values()) {
      if (nnInfo == null) continue;
      stopAndJoinNameNode(nnInfo.nameNode);
    }
    ShutdownHookManager.get().clearShutdownHooks();
    if (base_dir != null) {
      if (deleteDfsDir) {
        FileUtil.fullyDelete(base_dir);
      } else {
        FileUtil.fullyDeleteOnExit(base_dir);
      }
    }
  }

  /**
   * Shutdown all the namenodes.
   */
  public synchronized void shutdownNameNodes() {
    for (int i = 0; i < namenodes.size(); i++) {
      shutdownNameNode(i);
    }
  }
  
  /**
   * Shutdown the namenode at a given index.
   */
  public synchronized void shutdownNameNode(int nnIndex) {
    NameNodeInfo info = getNN(nnIndex);
    stopAndJoinNameNode(info.nameNode);
    info.nnId = null;
    info.nameNode = null;
    info.nameserviceId = null;
  }

  /**
   * Fully stop the NameNode by stop and join.
   */
  private void stopAndJoinNameNode(NameNode nn) {
    if (nn == null) {
      return;
    }
    LOG.info("Shutting down the namenode");
    nn.stop();
    nn.join();
    nn.joinHttpServer();
  }

  /**
   * Restart all namenodes.
   */
  public synchronized void restartNameNodes() throws IOException {
    for (int i = 0; i < namenodes.size(); i++) {
      restartNameNode(i, false);
    }
    waitActive();
  }
  
  /**
   * Restart the namenode.
   */
  public synchronized void restartNameNode(String... args) throws IOException {
    checkSingleNameNode();
    restartNameNode(0, true, args);
  }

  /**
   * Restart the namenode. Optionally wait for the cluster to become active.
   */
  public synchronized void restartNameNode(boolean waitActive)
      throws IOException {
    checkSingleNameNode();
    restartNameNode(0, waitActive);
  }
  
  /**
   * Restart the namenode at a given index.
   */
  public synchronized void restartNameNode(int nnIndex) throws IOException {
    restartNameNode(nnIndex, true);
  }

  /**
   * Restart the namenode at a given index. Optionally wait for the cluster
   * to become active.
   */
  public synchronized void restartNameNode(int nnIndex, boolean waitActive,
      String... args) throws IOException {
    NameNodeInfo info = getNN(nnIndex);
    StartupOption startOpt = info.startOpt;

    shutdownNameNode(nnIndex);
    if (args.length != 0) {
      startOpt = null;
    } else {
      args = createArgs(startOpt);
    }

    NameNode nn = NameNode.createNameNode(args, info.conf);
    info.nameNode = nn;
    info.setStartOpt(startOpt);
    if (waitActive) {
      waitClusterUp();
      LOG.info("Restarted the namenode");
      waitActive();
    }
  }

  /**
   * Returns true if the NameNode is running and is out of Safe Mode
   * or if waiting for safe mode is disabled.
   */
  public boolean isNameNodeUp(int nnIndex) {
    NameNode nameNode = getNN(nnIndex).nameNode;
    if (nameNode == null) {
      return false;
    }
    long[] sizes;
    sizes = NameNodeAdapter.getStats(nameNode.getNamesystem());
    boolean isUp = false;
    synchronized (this) {
      isUp = ((!nameNode.isInSafeMode() || !waitSafeMode) &&
          sizes[ClientProtocol.GET_STATS_CAPACITY_IDX] != 0);
    }
    return isUp;
  }

  /**
   * Returns true if all the NameNodes are running and is out of Safe Mode.
   */
  public boolean isClusterUp() {
    for (int index = 0; index < namenodes.size(); index++) {
      if (!isNameNodeUp(index)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Get a client handle to the DFS cluster with a single namenode.
   */
  public DistributedFileSystem getFileSystem() throws IOException {
    checkSingleNameNode();
    return getFileSystem(0);
  }

  /**
   * Get a client handle to the DFS cluster for the namenode at given index.
   */
  public DistributedFileSystem getFileSystem(int nnIndex) throws IOException {
    return (DistributedFileSystem) addFileSystem(FileSystem.get(getURI(nnIndex),
        getNN(nnIndex).conf));
  }

  /**
   * Get another FileSystem instance that is different from FileSystem.get(conf).
   * This simulating different threads working on different FileSystem instances.
   */
  public FileSystem getNewFileSystemInstance(int nnIndex) throws IOException {
    return addFileSystem(FileSystem.newInstance(getURI(nnIndex), getNN(nnIndex).conf));
  }

  private <T extends FileSystem> T addFileSystem(T fs) {
    fileSystems.add(fs);
    return fs;
  }

  /**
   * @return a http URL
   */
  public String getHttpUri(int nnIndex) {
    return "http://"
        + getNN(nnIndex).conf
            .get(DFS_NAMENODE_HTTP_ADDRESS_KEY);
  }

  /**
   * Get the directories where the namenode stores its image.
   */
  public Collection<URI> getNameDirs(int nnIndex) {
    return FSNamesystem.getNamespaceDirs(getNN(nnIndex).conf);
  }

  /**
   * Get the directories where the namenode stores its edits.
   */
  public Collection<URI> getNameEditsDirs(int nnIndex) throws IOException {
    return FSNamesystem.getNamespaceEditsDirs(getNN(nnIndex).conf);
  }
  
  public void transitionToActive(int nnIndex) throws IOException,
      ServiceFailedException {
    getNameNode(nnIndex).getRpcServer().transitionToActive(
        new StateChangeRequestInfo(RequestSource.REQUEST_BY_USER_FORCED));
  }
  
  public void transitionToStandby(int nnIndex) throws IOException,
      ServiceFailedException {
    getNameNode(nnIndex).getRpcServer().transitionToStandby(
        new StateChangeRequestInfo(RequestSource.REQUEST_BY_USER_FORCED));
  }
  
   /** Wait until the given namenode gets registration from all the datanodes */
  public void waitActive(int nnIndex) throws IOException {
    if (namenodes.size() == 0 || getNN(nnIndex) == null || getNN(nnIndex).nameNode == null) {
      return;
    }

    NameNodeInfo info = getNN(nnIndex);
    InetSocketAddress addr = info.nameNode.getServiceRpcAddress();
    assert addr.getPort() != 0;
    DFSClient client = new DFSClient(addr, conf);
    //Internally client will retry until namenode is up.
    client.namenode.getStats();
    client.close();
  }

  /**
   * Wait until the cluster is active and running.
   */
  public void waitActive() throws IOException {
    for (int index = 0; index < namenodes.size(); index++) {
      int failedCount = 0;
      while (true) {
        try {
          waitActive(index);
          break;
        } catch (IOException e) {
          failedCount++;
          // Cached RPC connection to namenode, if any, is expected to fail once
          if (failedCount > 1) {
            LOG.warn("Tried waitActive() " + failedCount
                + " time(s) and failed, giving up.  "
                + StringUtils.stringifyException(e));
            throw e;
          }
        }
      }
    }
    LOG.info("Cluster is active");
  }

  public void printNNs() {
    for (int i = 0; i < namenodes.size(); i++) {
      LOG.info("Have namenode " + i + ", info:" + getNN(i));
      LOG.info(" has namenode: " + getNN(i).nameNode);
    }
  }

  /**
   * Set the softLimit and hardLimit of client lease periods
   */
  public void setLeasePeriod(long soft, long hard) {
    NameNodeAdapter.setLeasePeriod(getNamesystem(), soft, hard);
  }
  
  public void setLeasePeriod(long soft, long hard, int nnIndex) {
    NameNodeAdapter.setLeasePeriod(getNamesystem(nnIndex), soft, hard);
  }
  
  public void setWaitSafeMode(boolean wait) {
    this.waitSafeMode = wait;
  }

  /**
   * Get the base directory for this MiniDFS instance.
   * <p/>
   * Within the MiniDFCluster class and any subclasses, this method should be
   * used instead of {@link #getBaseDirectory()} which doesn't support
   * configuration-specific base directories.
   * <p/>
   * First the Configuration property {@link #HDFS_MINIDFS_BASEDIR} is fetched.
   * If non-null, this is returned.
   * If this is null, then {@link #getBaseDirectory()} is called.
   * @return the base directory for this instance.
   */
  protected String determineDfsBaseDir() {
    if (conf != null) {
      final String dfsdir = conf.get(HDFS_MINIDFS_BASEDIR, null);
      if (dfsdir != null) {
        return dfsdir;
      }
    }
    return getBaseDirectory();
  }

  /**
   * Get the base directory for any DFS cluster whose configuration does
   * not explicitly set it. This is done via
   * {@link GenericTestUtils#getTestDir()}.
   * @return a directory for use as a miniDFS filesystem.
   */
  public static String getBaseDirectory() {
    return GenericTestUtils.getTestDir("dfs").getAbsolutePath()
        + File.separator;
  }

  /**
   * Return all block files in given directory (recursive search).
   */
  public static List<File> getAllBlockFiles(File storageDir) {
    List<File> results = new ArrayList<File>();
    File[] files = storageDir.listFiles();
    if (files == null) {
      return null;
    }
    for (File f : files) {
      if (f.getName().startsWith(Block.BLOCK_FILE_PREFIX) &&
          !f.getName().endsWith(Block.METADATA_EXTENSION)) {
        results.add(f);
      } else if (f.isDirectory()) {
        List<File> subdirResults = getAllBlockFiles(f);
        if (subdirResults != null) {
          results.addAll(subdirResults);
        }
      }
    }
    return results;
  }

  /**
   * Return all block metadata files in given directory (recursive search)
   */
  public static List<File> getAllBlockMetadataFiles(File storageDir) {
    List<File> results = new ArrayList<File>();
    File[] files = storageDir.listFiles();
    if (files == null) {
      return null;
    }
    for (File f : files) {
      if (f.getName().startsWith(Block.BLOCK_FILE_PREFIX) &&
              f.getName().endsWith(Block.METADATA_EXTENSION)) {
        results.add(f);
      } else if (f.isDirectory()) {
        List<File> subdirResults = getAllBlockMetadataFiles(f);
        if (subdirResults != null) {
          results.addAll(subdirResults);
        }
      }
    }
    return results;
  }

  /**
   * Shut down a cluster if it is not null
   * @param cluster cluster reference or null
   */
  public static void shutdownCluster(MiniDFSCluster cluster) {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  /**
   * Throw an exception if the MiniDFSCluster is not started with a single
   * namenode
   */
  private void checkSingleNameNode() {
    if (namenodes.size() != 1) {
      throw new IllegalArgumentException("Namenode index is needed");
    }
  }

  /**
   * Add a namenode to a federated cluster and start it. Configuration of
   * datanodes in the cluster is refreshed to register with the new namenode.
   * 
   * @return newly started namenode
   */
  public void addNameNode(Configuration conf, int namenodePort)
      throws IOException {
    if(!federation)
      throw new IOException("cannot add namenode to non-federated cluster");

    int nameServiceIndex = namenodes.keys().size();
    String nameserviceId = NAMESERVICE_ID_PREFIX + (namenodes.keys().size() + 1);

    String nameserviceIds = conf.get(DFS_NAMESERVICES);
    nameserviceIds += "," + nameserviceId;
    conf.set(DFS_NAMESERVICES, nameserviceIds);
  
    String nnId = null;
    initNameNodeAddress(conf, nameserviceId,
        new NNConf(nnId).setIpcPort(namenodePort));
    // figure out the current number of NNs
    NameNodeInfo[] infos = this.getNameNodeInfos(nameserviceId);
    int nnIndex = infos == null ? 0 : infos.length;
    initNameNodeConf(conf, nameserviceId, nameServiceIndex, nnId, true, true, nnIndex);
    createNameNode(conf, true, null, null, nameserviceId, nnId);
    // Wait for new namenode to get registrations from all the datanodes
    waitActive(nnIndex);
  }

  private void addToFile(String p, String address) throws IOException {
    File f = new File(p);
    f.createNewFile();
    PrintWriter writer = new PrintWriter(new FileWriter(f, true));
    try {
      writer.println(address);
    } finally {
      writer.close();
    }
  }

  @Override
  public void close() {
    shutdown();
  }
}
