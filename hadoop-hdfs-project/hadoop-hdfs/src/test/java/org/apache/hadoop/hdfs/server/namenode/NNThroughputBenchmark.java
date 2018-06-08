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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

/**
 * Main class for a series of name-node benchmarks.
 * 
 * Each benchmark measures throughput and average execution time 
 * of a specific name-node operation, e.g. file creation or block reports.
 * 
 * The benchmark does not involve any other hadoop components
 * except for the name-node. Each operation is executed
 * by calling directly the respective name-node method.
 * The name-node here is real all other components are simulated.
 *
 * For usage, please see <a href="http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/Benchmarking.html#NNThroughputBenchmark">the documentation</a>.
 * Meanwhile, if you change the usage of this program, please also update the
 * documentation accordingly.
 */
public class NNThroughputBenchmark implements Tool {
  private static final Log LOG = LogFactory.getLog(NNThroughputBenchmark.class);
  private static final int BLOCK_SIZE = 16;
  private static final String GENERAL_OPTIONS_USAGE =
      "[-keepResults] | [-logLevel L] | [-UGCacheRefreshCount G]";

  static Configuration config;
  static NameNode nameNode;
  static NamenodeProtocol nameNodeProto;
  static ClientProtocol clientProto;
  static RefreshUserMappingsProtocol refreshUserMappingsProto;
  static String bpid = null;

  NNThroughputBenchmark(Configuration conf) throws IOException {
    config = conf;
    // We do not need many handlers, since each thread simulates a handler
    // by calling name-node methods directly
    // Turn off minimum block size verification
    config.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);
    // set exclude file
    config.set(DFSConfigKeys.DFS_HOSTS_EXCLUDE,
      "${hadoop.tmp.dir}/dfs/hosts/exclude");
    File excludeFile = new File(config.get(DFSConfigKeys.DFS_HOSTS_EXCLUDE,
      "exclude"));
    if(!excludeFile.exists()) {
      if(!excludeFile.getParentFile().exists() && !excludeFile.getParentFile().mkdirs())
        throw new IOException("NNThroughputBenchmark: cannot mkdir " + excludeFile);
    }
    new FileOutputStream(excludeFile).close();
    // set include file
    config.set(DFSConfigKeys.DFS_HOSTS, "${hadoop.tmp.dir}/dfs/hosts/include");
    File includeFile = new File(config.get(DFSConfigKeys.DFS_HOSTS, "include"));
    new FileOutputStream(includeFile).close();
  }

  void close() {
    if(nameNode != null)
      nameNode.stop();
  }

  static void setNameNodeLoggingLevel(Level logLevel) {
    LOG.fatal("Log level = " + logLevel.toString());
    // change log level to NameNode logs
    DFSTestUtil.setNameNodeLogLevel(logLevel);
    GenericTestUtils.setLogLevel(LogManager.getLogger(
            NetworkTopology.class.getName()), logLevel);
    GenericTestUtils.setLogLevel(LogManager.getLogger(
            Groups.class.getName()), logLevel);
  }

  /**
   * Base class for collecting operation statistics.
   * 
   * Overload this class in order to run statistics for a 
   * specific name-node operation.
   */
  abstract class OperationStatsBase {
    protected static final String BASE_DIR_NAME = "/nnThroughputBenchmark";
    protected static final String OP_ALL_NAME = "all";
    protected static final String OP_ALL_USAGE = "-op all <other ops options>";

    protected final String baseDir;
    protected short replication;
    protected int  numThreads = 0;        // number of threads
    protected int  numOpsRequired = 0;    // number of operations requested
    protected int  numOpsExecuted = 0;    // number of operations executed
    protected long cumulativeTime = 0;    // sum of times for each op
    protected long elapsedTime = 0;       // time from start to finish
    protected boolean keepResults = false;// don't clean base directory on exit
    protected Level logLevel;             // logging level, ERROR by default
    protected int ugcRefreshCount = 0;    // user group cache refresh count

    protected List<StatsDaemon> daemons;

    /**
     * Operation name.
     */
    abstract String getOpName();

    /**
     * Parse command line arguments.
     * 
     * @param args arguments
     * @throws IOException
     */
    abstract void parseArguments(List<String> args) throws IOException;

    /**
     * Generate inputs for each daemon thread.
     * 
     * @param opsPerThread number of inputs for each thread.
     * @throws IOException
     */
    abstract void generateInputs(int[] opsPerThread) throws IOException;

    /**
     * This corresponds to the arg1 argument of 
     * {@link #executeOp(int, int, String)}, which can have different meanings
     * depending on the operation performed.
     * 
     * @param daemonId id of the daemon calling this method
     * @return the argument
     */
    abstract String getExecutionArgument(int daemonId);

    /**
     * Execute name-node operation.
     * 
     * @param daemonId id of the daemon calling this method.
     * @param inputIdx serial index of the operation called by the deamon.
     * @param arg1 operation specific argument.
     * @return time of the individual name-node call.
     * @throws IOException
     */
    abstract long executeOp(int daemonId, int inputIdx, String arg1) throws IOException;

    /**
     * Print the results of the benchmarking.
     */
    abstract void printResults();

    OperationStatsBase() {
      baseDir = BASE_DIR_NAME + "/" + getOpName();
      replication = (short) config.getInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
      numOpsRequired = 10;
      numThreads = 3;
      logLevel = Level.ERROR;
      ugcRefreshCount = Integer.MAX_VALUE;
    }

    void benchmark() throws IOException {
      daemons = new ArrayList<StatsDaemon>();
      long start = 0;
      try {
        numOpsExecuted = 0;
        cumulativeTime = 0;
        if(numThreads < 1)
          return;
        int tIdx = 0; // thread index < nrThreads
        int opsPerThread[] = new int[numThreads];
        for(int opsScheduled = 0; opsScheduled < numOpsRequired; 
                                  opsScheduled += opsPerThread[tIdx++]) {
          // execute  in a separate thread
          opsPerThread[tIdx] = (numOpsRequired-opsScheduled)/(numThreads-tIdx);
          if(opsPerThread[tIdx] == 0)
            opsPerThread[tIdx] = 1;
        }
        // if numThreads > numOpsRequired then the remaining threads will do nothing
        for(; tIdx < numThreads; tIdx++)
          opsPerThread[tIdx] = 0;
        generateInputs(opsPerThread);
        setNameNodeLoggingLevel(logLevel);
        for(tIdx=0; tIdx < numThreads; tIdx++)
          daemons.add(new StatsDaemon(tIdx, opsPerThread[tIdx], this));
        start = Time.now();
        LOG.info("Starting " + numOpsRequired + " " + getOpName() + "(s).");
        for(StatsDaemon d : daemons)
          d.start();
      } finally {
        while(isInProgress()) {
          // try {Thread.sleep(500);} catch (InterruptedException e) {}
        }
        elapsedTime = Time.now() - start;
        for(StatsDaemon d : daemons) {
          incrementStats(d.localNumOpsExecuted, d.localCumulativeTime);
          // System.out.println(d.toString() + ": ops Exec = " + d.localNumOpsExecuted);
        }
      }
    }

    private boolean isInProgress() {
      for(StatsDaemon d : daemons)
        if(d.isInProgress())
          return true;
      return false;
    }

    void cleanUp() throws IOException {
      clientProto.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE,
          false);
      if(!keepResults)
        clientProto.delete(getBaseDir(), true);
    }

    int getNumOpsExecuted() {
      return numOpsExecuted;
    }

    long getCumulativeTime() {
      return cumulativeTime;
    }

    long getElapsedTime() {
      return elapsedTime;
    }

    long getAverageTime() {
      return numOpsExecuted == 0 ? 0 : cumulativeTime / numOpsExecuted;
    }

    double getOpsPerSecond() {
      return elapsedTime == 0 ? 0 : 1000*(double)numOpsExecuted / elapsedTime;
    }

    String getBaseDir() {
      return baseDir;
    }

    String getClientName(int idx) {
      return getOpName() + "-client-" + idx;
    }

    void incrementStats(int ops, long time) {
      numOpsExecuted += ops;
      cumulativeTime += time;
    }

    /**
     * Parse first 2 arguments, corresponding to the "-op" option.
     * 
     * @param args argument list
     * @return true if operation is all, which means that options not related
     * to this operation should be ignored, or false otherwise, meaning
     * that usage should be printed when an unrelated option is encountered.
     */
    protected boolean verifyOpArgument(List<String> args) {
      if(args.size() < 2 || ! args.get(0).startsWith("-op"))
        printUsage();

      // process common options
      int krIndex = args.indexOf("-keepResults");
      keepResults = (krIndex >= 0);
      if(keepResults) {
        args.remove(krIndex);
      }

      int llIndex = args.indexOf("-logLevel");
      if(llIndex >= 0) {
        if(args.size() <= llIndex + 1)
          printUsage();
        logLevel = Level.toLevel(args.get(llIndex+1), Level.ERROR);
        args.remove(llIndex+1);
        args.remove(llIndex);
      }

      int ugrcIndex = args.indexOf("-UGCacheRefreshCount");
      if(ugrcIndex >= 0) {
        if(args.size() <= ugrcIndex + 1)
          printUsage();
        int g = Integer.parseInt(args.get(ugrcIndex+1));
        if(g > 0) ugcRefreshCount = g;
        args.remove(ugrcIndex+1);
        args.remove(ugrcIndex);
      }

      String type = args.get(1);
      if(OP_ALL_NAME.equals(type)) {
        type = getOpName();
        return true;
      }
      if(!getOpName().equals(type))
        printUsage();
      return false;
    }

    void printStats() {
      LOG.info("--- " + getOpName() + " stats  ---");
      LOG.info("# operations: " + getNumOpsExecuted());
      LOG.info("Elapsed Time: " + getElapsedTime());
      LOG.info(" Ops per sec: " + getOpsPerSecond());
      LOG.info("Average Time: " + getAverageTime());
    }
  }

  /**
   * One of the threads that perform stats operations.
   */
  private class StatsDaemon extends Thread {
    private final int daemonId;
    private int opsPerThread;
    private String arg1;      // argument passed to executeOp()
    private volatile int  localNumOpsExecuted = 0;
    private volatile long localCumulativeTime = 0;
    private final OperationStatsBase statsOp;

    StatsDaemon(int daemonId, int nrOps, OperationStatsBase op) {
      this.daemonId = daemonId;
      this.opsPerThread = nrOps;
      this.statsOp = op;
      setName(toString());
    }

    @Override
    public void run() {
      localNumOpsExecuted = 0;
      localCumulativeTime = 0;
      arg1 = statsOp.getExecutionArgument(daemonId);
      try {
        benchmarkOne();
      } catch(IOException ex) {
        LOG.error("StatsDaemon " + daemonId + " failed: \n" 
            + StringUtils.stringifyException(ex));
      }
    }

    @Override
    public String toString() {
      return "StatsDaemon-" + daemonId;
    }

    void benchmarkOne() throws IOException {
      for(int idx = 0; idx < opsPerThread; idx++) {
        if((localNumOpsExecuted+1) % statsOp.ugcRefreshCount == 0)
          refreshUserMappingsProto.refreshUserToGroupsMappings();
        long stat = statsOp.executeOp(daemonId, idx, arg1);
        localNumOpsExecuted++;
        localCumulativeTime += stat;
      }
    }

    boolean isInProgress() {
      return localNumOpsExecuted < opsPerThread;
    }

    /**
     * Schedule to stop this daemon.
     */
    void terminate() {
      opsPerThread = localNumOpsExecuted;
    }
  }

  /**
   * Clean all benchmark result directories.
   */
  class CleanAllStats extends OperationStatsBase {
    // Operation types
    static final String OP_CLEAN_NAME = "clean";
    static final String OP_CLEAN_USAGE = "-op clean";

    CleanAllStats(List<String> args) {
      super();
      parseArguments(args);
      numOpsRequired = 1;
      numThreads = 1;
      keepResults = true;
    }

    @Override
    String getOpName() {
      return OP_CLEAN_NAME;
    }

    @Override
    void parseArguments(List<String> args) {
      boolean ignoreUnrelatedOptions = verifyOpArgument(args);
      if(args.size() > 2 && !ignoreUnrelatedOptions)
        printUsage();
    }

    @Override
    void generateInputs(int[] opsPerThread) throws IOException {
      // do nothing
    }

    /**
     * Does not require the argument
     */
    @Override
    String getExecutionArgument(int daemonId) {
      return null;
    }

    /**
     * Remove entire benchmark directory.
     */
    @Override
    long executeOp(int daemonId, int inputIdx, String ignore) 
    throws IOException {
      clientProto.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE,
          false);
      long start = Time.now();
      clientProto.delete(BASE_DIR_NAME, true);
      long end = Time.now();
      return end-start;
    }

    @Override
    void printResults() {
      LOG.info("--- " + getOpName() + " inputs ---");
      LOG.info("Remove directory " + BASE_DIR_NAME);
      printStats();
    }
  }

  /**
   * File creation statistics.
   * 
   * Each thread creates the same (+ or -1) number of files.
   * File names are pre-generated during initialization.
   * The created files do not have blocks.
   */
  class CreateFileStats extends OperationStatsBase {
    // Operation types
    static final String OP_CREATE_NAME = "create";
    static final String OP_CREATE_USAGE = 
      "-op create [-threads T] [-files N] [-filesPerDir P] [-close]";

    protected FileNameGenerator nameGenerator;
    protected String[][] fileNames;
    private boolean closeUponCreate;

    CreateFileStats(List<String> args) {
      super();
      parseArguments(args);
    }

    @Override
    String getOpName() {
      return OP_CREATE_NAME;
    }

    @Override
    void parseArguments(List<String> args) {
      boolean ignoreUnrelatedOptions = verifyOpArgument(args);
      int nrFilesPerDir = 4;
      closeUponCreate = false;
      for (int i = 2; i < args.size(); i++) {       // parse command line
        if(args.get(i).equals("-files")) {
          if(i+1 == args.size())  printUsage();
          numOpsRequired = Integer.parseInt(args.get(++i));
        } else if(args.get(i).equals("-threads")) {
          if(i+1 == args.size())  printUsage();
          numThreads = Integer.parseInt(args.get(++i));
        } else if(args.get(i).equals("-filesPerDir")) {
          if(i+1 == args.size())  printUsage();
          nrFilesPerDir = Integer.parseInt(args.get(++i));
        } else if(args.get(i).equals("-close")) {
          closeUponCreate = true;
        } else if(!ignoreUnrelatedOptions)
          printUsage();
      }
      nameGenerator = new FileNameGenerator(getBaseDir(), nrFilesPerDir);
    }

    @Override
    void generateInputs(int[] opsPerThread) throws IOException {
      assert opsPerThread.length == numThreads : "Error opsPerThread.length"; 
      clientProto.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE,
          false);
      // int generatedFileIdx = 0;
      LOG.info("Generate " + numOpsRequired + " intputs for " + getOpName());
      fileNames = new String[numThreads][];
      for(int idx=0; idx < numThreads; idx++) {
        int threadOps = opsPerThread[idx];
        fileNames[idx] = new String[threadOps];
        for(int jdx=0; jdx < threadOps; jdx++)
          fileNames[idx][jdx] = nameGenerator.
                                  getNextFileName("ThroughputBench");
      }
    }

    void dummyActionNoSynch(int daemonId, int fileIdx) {
      for(int i=0; i < 2000; i++)
        fileNames[daemonId][fileIdx].contains(""+i);
    }

    /**
     * returns client name
     */
    @Override
    String getExecutionArgument(int daemonId) {
      return getClientName(daemonId);
    }

    /**
     * Do file create.
     */
    @Override
    long executeOp(int daemonId, int inputIdx, String clientName) 
    throws IOException {
      long start = Time.now();
      // dummyActionNoSynch(fileIdx);
      clientProto.create(fileNames[daemonId][inputIdx],
          FsPermission.getDefault(), clientName,
          new EnumSetWritable<CreateFlag>(EnumSet
              .of(CreateFlag.CREATE, CreateFlag.OVERWRITE)), true,
          replication, BLOCK_SIZE, CryptoProtocolVersion.supported(), null);
      long end = Time.now();
      for (boolean written = !closeUponCreate; !written;
        written = clientProto.complete(fileNames[daemonId][inputIdx],
            clientName, null, HdfsConstants.GRANDFATHER_INODE_ID)) {
      };
      return end-start;
    }

    @Override
    void printResults() {
      LOG.info("--- " + getOpName() + " inputs ---");
      LOG.info("nrFiles = " + numOpsRequired);
      LOG.info("nrThreads = " + numThreads);
      LOG.info("nrFilesPerDir = " + nameGenerator.getFilesPerDirectory());
      printStats();
    }
  }

  /**
   * Directory creation statistics.
   *
   * Each thread creates the same (+ or -1) number of directories.
   * Directory names are pre-generated during initialization.
   */
  class MkdirsStats extends OperationStatsBase {
    // Operation types
    static final String OP_MKDIRS_NAME = "mkdirs";
    static final String OP_MKDIRS_USAGE = "-op mkdirs [-threads T] [-dirs N] " +
        "[-dirsPerDir P]";

    protected FileNameGenerator nameGenerator;
    protected String[][] dirPaths;

    MkdirsStats(List<String> args) {
      super();
      parseArguments(args);
    }

    @Override
    String getOpName() {
      return OP_MKDIRS_NAME;
    }

    @Override
    void parseArguments(List<String> args) {
      boolean ignoreUnrelatedOptions = verifyOpArgument(args);
      int nrDirsPerDir = 2;
      for (int i = 2; i < args.size(); i++) {       // parse command line
        if(args.get(i).equals("-dirs")) {
          if(i+1 == args.size())  printUsage();
          numOpsRequired = Integer.parseInt(args.get(++i));
        } else if(args.get(i).equals("-threads")) {
          if(i+1 == args.size())  printUsage();
          numThreads = Integer.parseInt(args.get(++i));
        } else if(args.get(i).equals("-dirsPerDir")) {
          if(i+1 == args.size())  printUsage();
          nrDirsPerDir = Integer.parseInt(args.get(++i));
        } else if(!ignoreUnrelatedOptions)
          printUsage();
      }
      nameGenerator = new FileNameGenerator(getBaseDir(), nrDirsPerDir);
    }

    @Override
    void generateInputs(int[] opsPerThread) throws IOException {
      assert opsPerThread.length == numThreads : "Error opsPerThread.length";
      clientProto.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE,
          false);
      LOG.info("Generate " + numOpsRequired + " inputs for " + getOpName());
      dirPaths = new String[numThreads][];
      for(int idx=0; idx < numThreads; idx++) {
        int threadOps = opsPerThread[idx];
        dirPaths[idx] = new String[threadOps];
        for(int jdx=0; jdx < threadOps; jdx++)
          dirPaths[idx][jdx] = nameGenerator.
              getNextFileName("ThroughputBench");
      }
    }

    /**
     * returns client name
     */
    @Override
    String getExecutionArgument(int daemonId) {
      return getClientName(daemonId);
    }

    /**
     * Do mkdirs operation.
     */
    @Override
    long executeOp(int daemonId, int inputIdx, String clientName)
        throws IOException {
      long start = Time.now();
      clientProto.mkdirs(dirPaths[daemonId][inputIdx],
          FsPermission.getDefault(), true);
      long end = Time.now();
      return end-start;
    }

    @Override
    void printResults() {
      LOG.info("--- " + getOpName() + " inputs ---");
      LOG.info("nrDirs = " + numOpsRequired);
      LOG.info("nrThreads = " + numThreads);
      LOG.info("nrDirsPerDir = " + nameGenerator.getFilesPerDirectory());
      printStats();
    }
  }

  /**
   * Open file statistics.
   * 
   * Measure how many open calls (getBlockLocations()) 
   * the name-node can handle per second.
   */
  class OpenFileStats extends CreateFileStats {
    // Operation types
    static final String OP_OPEN_NAME = "open";
    static final String OP_USAGE_ARGS = 
      " [-threads T] [-files N] [-filesPerDir P] [-useExisting]";
    static final String OP_OPEN_USAGE = 
      "-op " + OP_OPEN_NAME + OP_USAGE_ARGS;

    private boolean useExisting;  // do not generate files, use existing ones

    OpenFileStats(List<String> args) {
      super(args);
    }

    @Override
    String getOpName() {
      return OP_OPEN_NAME;
    }

    @Override
    void parseArguments(List<String> args) {
      int ueIndex = args.indexOf("-useExisting");
      useExisting = (ueIndex >= 0);
      if(useExisting) {
        args.remove(ueIndex);
      }
      super.parseArguments(args);
    }

    @Override
    void generateInputs(int[] opsPerThread) throws IOException {
      // create files using opsPerThread
      String[] createArgs = new String[] {
              "-op", "create", 
              "-threads", String.valueOf(this.numThreads), 
              "-files", String.valueOf(numOpsRequired),
              "-filesPerDir", 
              String.valueOf(nameGenerator.getFilesPerDirectory()),
              "-close"};
      CreateFileStats opCreate =  new CreateFileStats(Arrays.asList(createArgs));

      if(!useExisting) {  // create files if they were not created before
        opCreate.benchmark();
        LOG.info("Created " + numOpsRequired + " files.");
      } else {
        LOG.info("useExisting = true. Assuming " 
            + numOpsRequired + " files have been created before.");
      }
      // use the same files for open
      super.generateInputs(opsPerThread);
      if(clientProto.getFileInfo(opCreate.getBaseDir()) != null
          && clientProto.getFileInfo(getBaseDir()) == null) {
        clientProto.rename(opCreate.getBaseDir(), getBaseDir());
      }
      if(clientProto.getFileInfo(getBaseDir()) == null) {
        throw new IOException(getBaseDir() + " does not exist.");
      }
    }

    /**
     * Do file open.
     */
    @Override
    long executeOp(int daemonId, int inputIdx, String ignore) 
    throws IOException {
      long start = Time.now();
      clientProto.getBlockLocations(fileNames[daemonId][inputIdx], 0L, BLOCK_SIZE);
      long end = Time.now();
      return end-start;
    }
  }

  /**
   * Delete file statistics.
   * 
   * Measure how many delete calls the name-node can handle per second.
   */
  class DeleteFileStats extends OpenFileStats {
    // Operation types
    static final String OP_DELETE_NAME = "delete";
    static final String OP_DELETE_USAGE = 
      "-op " + OP_DELETE_NAME + OP_USAGE_ARGS;

    DeleteFileStats(List<String> args) {
      super(args);
    }

    @Override
    String getOpName() {
      return OP_DELETE_NAME;
    }

    @Override
    long executeOp(int daemonId, int inputIdx, String ignore) 
    throws IOException {
      long start = Time.now();
      clientProto.delete(fileNames[daemonId][inputIdx], false);
      long end = Time.now();
      return end-start;
    }
  }

  /**
   * List file status statistics.
   * 
   * Measure how many get-file-status calls the name-node can handle per second.
   */
  class FileStatusStats extends OpenFileStats {
    // Operation types
    static final String OP_FILE_STATUS_NAME = "fileStatus";
    static final String OP_FILE_STATUS_USAGE = 
      "-op " + OP_FILE_STATUS_NAME + OP_USAGE_ARGS;

    FileStatusStats(List<String> args) {
      super(args);
    }

    @Override
    String getOpName() {
      return OP_FILE_STATUS_NAME;
    }

    @Override
    long executeOp(int daemonId, int inputIdx, String ignore) 
    throws IOException {
      long start = Time.now();
      clientProto.getFileInfo(fileNames[daemonId][inputIdx]);
      long end = Time.now();
      return end-start;
    }
  }

  /**
   * Rename file statistics.
   * 
   * Measure how many rename calls the name-node can handle per second.
   */
  class RenameFileStats extends OpenFileStats {
    // Operation types
    static final String OP_RENAME_NAME = "rename";
    static final String OP_RENAME_USAGE = 
      "-op " + OP_RENAME_NAME + OP_USAGE_ARGS;

    protected String[][] destNames;

    RenameFileStats(List<String> args) {
      super(args);
    }

    @Override
    String getOpName() {
      return OP_RENAME_NAME;
    }

    @Override
    void generateInputs(int[] opsPerThread) throws IOException {
      super.generateInputs(opsPerThread);
      destNames = new String[fileNames.length][];
      for(int idx=0; idx < numThreads; idx++) {
        int nrNames = fileNames[idx].length;
        destNames[idx] = new String[nrNames];
        for(int jdx=0; jdx < nrNames; jdx++)
          destNames[idx][jdx] = fileNames[idx][jdx] + ".r";
      }
    }

    @Override
    long executeOp(int daemonId, int inputIdx, String ignore) 
    throws IOException {
      long start = Time.now();
      clientProto.rename(fileNames[daemonId][inputIdx],
                      destNames[daemonId][inputIdx]);
      long end = Time.now();
      return end-start;
    }
  }

  static void printUsage() {
    System.err.println("Usage: NNThroughputBenchmark"
        + "\n\t"    + OperationStatsBase.OP_ALL_USAGE
        + " | \n\t" + CreateFileStats.OP_CREATE_USAGE
        + " | \n\t" + MkdirsStats.OP_MKDIRS_USAGE
        + " | \n\t" + OpenFileStats.OP_OPEN_USAGE
        + " | \n\t" + DeleteFileStats.OP_DELETE_USAGE
        + " | \n\t" + FileStatusStats.OP_FILE_STATUS_USAGE
        + " | \n\t" + RenameFileStats.OP_RENAME_USAGE
        + " | \n\t" + CleanAllStats.OP_CLEAN_USAGE
        + " | \n\t" + GENERAL_OPTIONS_USAGE
    );
    System.err.println();
    GenericOptionsParser.printGenericCommandUsage(System.err);
    System.err.println("If connecting to a remote NameNode with -fs option, " +
        "dfs.namenode.fs-limits.min-block-size should be set to 16.");
    ExitUtil.terminate(-1);
  }

  public static void runBenchmark(Configuration conf, String[] args)
      throws Exception {
    NNThroughputBenchmark bench = null;
    try {
      bench = new NNThroughputBenchmark(conf);
      ToolRunner.run(bench, args);
    } finally {
      if(bench != null)
        bench.close();
    }
  }

  /**
   * Main method of the benchmark.
   * @param aArgs command line parameters
   */
  @Override // Tool
  public int run(String[] aArgs) throws Exception {
    List<String> args = new ArrayList<String>(Arrays.asList(aArgs));
    if(args.size() < 2 || ! args.get(0).startsWith("-op"))
      printUsage();

    String type = args.get(1);
    boolean runAll = OperationStatsBase.OP_ALL_NAME.equals(type);

    final URI nnUri = FileSystem.getDefaultUri(config);
    // Start the NameNode
    String[] argv = new String[] {};

    List<OperationStatsBase> ops = new ArrayList<OperationStatsBase>();
    OperationStatsBase opStat = null;
    try {
      if(runAll || CreateFileStats.OP_CREATE_NAME.equals(type)) {
        opStat = new CreateFileStats(args);
        ops.add(opStat);
      }
      if(runAll || MkdirsStats.OP_MKDIRS_NAME.equals(type)) {
        opStat = new MkdirsStats(args);
        ops.add(opStat);
      }
      if(runAll || OpenFileStats.OP_OPEN_NAME.equals(type)) {
        opStat = new OpenFileStats(args);
        ops.add(opStat);
      }
      if(runAll || DeleteFileStats.OP_DELETE_NAME.equals(type)) {
        opStat = new DeleteFileStats(args);
        ops.add(opStat);
      }
      if(runAll || FileStatusStats.OP_FILE_STATUS_NAME.equals(type)) {
        opStat = new FileStatusStats(args);
        ops.add(opStat);
      }
      if(runAll || RenameFileStats.OP_RENAME_NAME.equals(type)) {
        opStat = new RenameFileStats(args);
        ops.add(opStat);
      }
      if(runAll || CleanAllStats.OP_CLEAN_NAME.equals(type)) {
        opStat = new CleanAllStats(args);
        ops.add(opStat);
      }
      if (ops.isEmpty()) {
        printUsage();
      }

      if (nnUri.getScheme() == null || nnUri.getScheme().equals("file")) {
        LOG.info("Remote NameNode is not specified. Creating one.");
        FileSystem.setDefaultUri(config, "hdfs://localhost:0");
        config.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
        nameNode = NameNode.createNameNode(argv, config);
        NamenodeProtocols nnProtos = nameNode.getRpcServer();
        nameNodeProto = nnProtos;
        clientProto = nnProtos;
        refreshUserMappingsProto = nnProtos;
        bpid = nameNode.getNamesystem().getBlockPoolId();
      } else {
        DistributedFileSystem dfs = (DistributedFileSystem)
            FileSystem.get(getConf());
        nameNodeProto = DFSTestUtil.getNamenodeProtocolProxy(config, nnUri,
            UserGroupInformation.getCurrentUser());
        clientProto = dfs.getClient().getNamenode();
        refreshUserMappingsProto =
            DFSTestUtil.getRefreshUserMappingsProtocolProxy(config, nnUri);
        getBlockPoolId(dfs);
      }
      // run each benchmark
      for(OperationStatsBase op : ops) {
        LOG.info("Starting benchmark: " + op.getOpName());
        op.benchmark();
        op.cleanUp();
      }
      // print statistics
      for(OperationStatsBase op : ops) {
        LOG.info("");
        op.printResults();
      }
    } catch(Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw e;
    }
    return 0;
  }

  private void getBlockPoolId(DistributedFileSystem unused)
    throws IOException {
    final NamespaceInfo nsInfo = nameNodeProto.versionRequest();
    bpid = nsInfo.getBlockPoolID();
  }

  public static void main(String[] args) throws Exception {
    runBenchmark(new HdfsConfiguration(), args);
  }

  @Override // Configurable
  public void setConf(Configuration conf) {
    config = conf;
  }

  @Override // Configurable
  public Configuration getConf() {
    return config;
  }
}
