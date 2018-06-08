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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.http.HttpConfig;

/** 
 * This class contains constants for configuration keys and default values
 * used in hdfs.
 */
@InterfaceAudience.Private
public class DFSConfigKeys extends CommonConfigurationKeys {
  public static final String  DFS_BLOCK_SIZE_KEY =
      HdfsClientConfigKeys.DFS_BLOCK_SIZE_KEY;
  public static final long    DFS_BLOCK_SIZE_DEFAULT =
      HdfsClientConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
  public static final String  DFS_REPLICATION_KEY =
      HdfsClientConfigKeys.DFS_REPLICATION_KEY;
  public static final short   DFS_REPLICATION_DEFAULT =
      HdfsClientConfigKeys.DFS_REPLICATION_DEFAULT;

  public static final String  DFS_STREAM_BUFFER_SIZE_KEY = "dfs.stream-buffer-size";
  public static final int     DFS_STREAM_BUFFER_SIZE_DEFAULT = 4096;
  public static final String  DFS_BYTES_PER_CHECKSUM_KEY =
      HdfsClientConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY;
  public static final int     DFS_BYTES_PER_CHECKSUM_DEFAULT =
      HdfsClientConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT;
  @Deprecated
  public static final String  DFS_USER_HOME_DIR_PREFIX_KEY =
      HdfsClientConfigKeys.DFS_USER_HOME_DIR_PREFIX_KEY;
  @Deprecated
  public static final String  DFS_USER_HOME_DIR_PREFIX_DEFAULT =
      HdfsClientConfigKeys.DFS_USER_HOME_DIR_PREFIX_DEFAULT;
  public static final String  DFS_CHECKSUM_TYPE_KEY = HdfsClientConfigKeys
      .DFS_CHECKSUM_TYPE_KEY;
  public static final String  DFS_CHECKSUM_TYPE_DEFAULT =
      HdfsClientConfigKeys.DFS_CHECKSUM_TYPE_DEFAULT;
  public static final String DFS_WEBHDFS_USE_IPC_CALLQ =
      "dfs.webhdfs.use.ipc.callq";
  public static final boolean DFS_WEBHDFS_USE_IPC_CALLQ_DEFAULT = true;

  public static final String  DFS_NAMENODE_PATH_BASED_CACHE_BLOCK_MAP_ALLOCATION_PERCENT =
    "dfs.namenode.path.based.cache.block.map.allocation.percent";
  public static final float    DFS_NAMENODE_PATH_BASED_CACHE_BLOCK_MAP_ALLOCATION_PERCENT_DEFAULT = 0.25f;

  public static final int     DFS_NAMENODE_HTTP_PORT_DEFAULT =
      HdfsClientConfigKeys.DFS_NAMENODE_HTTP_PORT_DEFAULT;
  public static final String  DFS_NAMENODE_HTTP_ADDRESS_KEY =
      HdfsClientConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY;
  public static final String  DFS_NAMENODE_HTTP_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_NAMENODE_HTTP_PORT_DEFAULT;
  public static final String  DFS_NAMENODE_HTTP_BIND_HOST_KEY = "dfs.namenode.http-bind-host";
  public static final String  DFS_NAMENODE_RPC_ADDRESS_KEY =
      HdfsClientConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
  public static final String  DFS_NAMENODE_RPC_BIND_HOST_KEY = "dfs.namenode.rpc-bind-host";
  public static final String  DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY = "dfs.namenode.servicerpc-address";
  public static final String  DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY = "dfs.namenode.servicerpc-bind-host";
  public static final String  DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY =
      "dfs.namenode.lifeline.rpc-address";
  public static final String  DFS_NAMENODE_LIFELINE_RPC_BIND_HOST_KEY =
      "dfs.namenode.lifeline.rpc-bind-host";
  public static final String  DFS_NAMENODE_MAX_OBJECTS_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_MAX_OBJECTS_KEY;
  public static final long    DFS_NAMENODE_MAX_OBJECTS_DEFAULT = 0;
  public static final String  DFS_NAMENODE_SAFEMODE_EXTENSION_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_SAFEMODE_EXTENSION_KEY;
  public static final int     DFS_NAMENODE_SAFEMODE_EXTENSION_DEFAULT = 30000;
  public static final String  DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY;
  public static final float   DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT = 0.999f;
  // set this to a slightly smaller value than
  // DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT to populate
  // needed replication queues before exiting safe mode
  public static final String  DFS_NAMENODE_REPL_QUEUE_THRESHOLD_PCT_KEY =
    "dfs.namenode.replqueue.threshold-pct";
  public static final String  DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY = "dfs.namenode.safemode.min.datanodes";
  public static final int     DFS_NAMENODE_SAFEMODE_MIN_DATANODES_DEFAULT = 0;
  public static final String  DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY;
  public static final String  DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_DEFAULT =
      "0.0.0.0:9868";
  public static final String  DFS_NAMENODE_SECONDARY_HTTPS_ADDRESS_KEY = "dfs.namenode.secondary.https-address";
  public static final String  DFS_NAMENODE_SECONDARY_HTTPS_ADDRESS_DEFAULT =
      "0.0.0.0:9869";
  public static final String  DFS_NAMENODE_CHECKPOINT_QUIET_MULTIPLIER_KEY = "dfs.namenode.checkpoint.check.quiet-multiplier";
  public static final double  DFS_NAMENODE_CHECKPOINT_QUIET_MULTIPLIER_DEFAULT = 1.5;
  public static final String  DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY = "dfs.namenode.checkpoint.check.period";
  public static final long    DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_DEFAULT = 60;
  public static final String  DFS_NAMENODE_CHECKPOINT_PERIOD_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY;
  public static final long    DFS_NAMENODE_CHECKPOINT_PERIOD_DEFAULT = 3600;
  public static final String  DFS_NAMENODE_CHECKPOINT_TXNS_KEY = "dfs.namenode.checkpoint.txns";
  public static final long    DFS_NAMENODE_CHECKPOINT_TXNS_DEFAULT = 1000000;
  public static final String  DFS_NAMENODE_CHECKPOINT_MAX_RETRIES_KEY = "dfs.namenode.checkpoint.max-retries";
  public static final int     DFS_NAMENODE_CHECKPOINT_MAX_RETRIES_DEFAULT = 3;
  public static final String  DFS_NAMENODE_MISSING_CHECKPOINT_PERIODS_BEFORE_SHUTDOWN_KEY = "dfs.namenode.missing.checkpoint.periods.before.shutdown";
  public static final int     DFS_NAMENODE_MISSING_CHECKPOINT_PERIODS_BEFORE_SHUTDOWN_DEFAULT = 3;
  public static final String  DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY;
  public static final int     DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT = 5*60*1000;
  public static final String  DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_KEY = "dfs.namenode.tolerate.heartbeat.multiplier";
  public static final int     DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_DEFAULT = 4;
  public static final String  DFS_NAMENODE_ACCESSTIME_PRECISION_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY;
  public static final long    DFS_NAMENODE_ACCESSTIME_PRECISION_DEFAULT = 3600000;
  public static final String DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY;
  public static final boolean DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_DEFAULT =
      true;
  public static final String  DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_FACTOR =
      "dfs.namenode.redundancy.considerLoad.factor";
  public static final double
      DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_FACTOR_DEFAULT = 2.0;
  public static final String DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY;
  public static final int DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_DEFAULT = 3;
  public static final String  DFS_NAMENODE_REPLICATION_MIN_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_REPLICATION_MIN_KEY;
  public static final int     DFS_NAMENODE_REPLICATION_MIN_DEFAULT = 1;
  public static final String  DFS_NAMENODE_FILE_CLOSE_NUM_COMMITTED_ALLOWED_KEY
      = "dfs.namenode.file.close.num-committed-allowed";
  public static final int     DFS_NAMENODE_FILE_CLOSE_NUM_COMMITTED_ALLOWED_DEFAULT
      = 0;
  public static final String  DFS_NAMENODE_STRIPE_MIN_KEY = "dfs.namenode.stripe.min";
  public static final int     DFS_NAMENODE_STRIPE_MIN_DEFAULT = 1;
  public static final String  DFS_NAMENODE_SAFEMODE_REPLICATION_MIN_KEY =
      "dfs.namenode.safemode.replication.min";

  public static final String  DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_KEY =
      "dfs.namenode.reconstruction.pending.timeout-sec";
  public static final int
      DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_DEFAULT = 300;

  public static final String  DFS_NAMENODE_MAINTENANCE_REPLICATION_MIN_KEY =
      "dfs.namenode.maintenance.replication.min";
  public static final int     DFS_NAMENODE_MAINTENANCE_REPLICATION_MIN_DEFAULT
      = 1;

  public static final String  DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY;
  public static final int     DFS_NAMENODE_REPLICATION_MAX_STREAMS_DEFAULT = 2;
  public static final String  DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_KEY = "dfs.namenode.replication.max-streams-hard-limit";
  public static final int     DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_DEFAULT = 4;
  public static final String DFS_NAMENODE_STORAGEINFO_DEFRAGMENT_INTERVAL_MS_KEY
      = "dfs.namenode.storageinfo.defragment.interval.ms";
  public static final int
      DFS_NAMENODE_STORAGEINFO_DEFRAGMENT_INTERVAL_MS_DEFAULT = 10 * 60 * 1000;
  public static final String DFS_NAMENODE_STORAGEINFO_DEFRAGMENT_TIMEOUT_MS_KEY
      = "dfs.namenode.storageinfo.defragment.timeout.ms";
  public static final int
      DFS_NAMENODE_STORAGEINFO_DEFRAGMENT_TIMEOUT_MS_DEFAULT = 4;
  public static final String DFS_NAMENODE_STORAGEINFO_DEFRAGMENT_RATIO_KEY
      = "dfs.namenode.storageinfo.defragment.ratio";
  public static final double
      DFS_NAMENODE_STORAGEINFO_DEFRAGMENT_RATIO_DEFAULT = 0.75;
  public static final String  DFS_WEBHDFS_AUTHENTICATION_FILTER_KEY = "dfs.web.authentication.filter";
  /* Phrased as below to avoid javac inlining as a constant, to match the behavior when
     this was AuthFilter.class.getName(). Note that if you change the import for AuthFilter, you
     need to update the literal here as well as TestDFSConfigKeys.
   */
  public static final String  DFS_WEBHDFS_AUTHENTICATION_FILTER_DEFAULT =
      "org.apache.hadoop.hdfs.web.AuthFilter";
  @Deprecated
  public static final String  DFS_WEBHDFS_USER_PATTERN_KEY =
      HdfsClientConfigKeys.DFS_WEBHDFS_USER_PATTERN_KEY;
  @Deprecated
  public static final String  DFS_WEBHDFS_USER_PATTERN_DEFAULT =
      HdfsClientConfigKeys.DFS_WEBHDFS_USER_PATTERN_DEFAULT;
  public static final String  DFS_PERMISSIONS_ENABLED_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_PERMISSIONS_ENABLED_KEY;
  public static final boolean DFS_PERMISSIONS_ENABLED_DEFAULT = true;
  public static final String  DFS_PERMISSIONS_SUPERUSERGROUP_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY;
  public static final String  DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT = "supergroup";
  public static final String  DFS_NAMENODE_ACLS_ENABLED_KEY = "dfs.namenode.acls.enabled";
  public static final boolean DFS_NAMENODE_ACLS_ENABLED_DEFAULT = false;
  public static final String DFS_NAMENODE_POSIX_ACL_INHERITANCE_ENABLED_KEY =
      "dfs.namenode.posix.acl.inheritance.enabled";
  public static final boolean
      DFS_NAMENODE_POSIX_ACL_INHERITANCE_ENABLED_DEFAULT = true;
  public static final String DFS_REFORMAT_DISABLED = "dfs.reformat.disabled";
  public static final boolean DFS_REFORMAT_DISABLED_DEFAULT = false;

  public static final String  DFS_NAMENODE_XATTRS_ENABLED_KEY = "dfs.namenode.xattrs.enabled";
  public static final boolean DFS_NAMENODE_XATTRS_ENABLED_DEFAULT = true;
  public static final String  DFS_ADMIN = "dfs.cluster.administrators";
  public static final String  DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY = "dfs.https.server.keystore.resource";
  public static final String  DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_DEFAULT = "ssl-server.xml";
  public static final String  DFS_SERVER_HTTPS_KEYPASSWORD_KEY = "ssl.server.keystore.keypassword";
  public static final String  DFS_SERVER_HTTPS_KEYSTORE_PASSWORD_KEY = "ssl.server.keystore.password";
  public static final String  DFS_SERVER_HTTPS_KEYSTORE_LOCATION_KEY = "ssl.server.keystore.location";
  public static final String  DFS_SERVER_HTTPS_TRUSTSTORE_LOCATION_KEY = "ssl.server.truststore.location";
  public static final String  DFS_SERVER_HTTPS_TRUSTSTORE_PASSWORD_KEY = "ssl.server.truststore.password";
  public static final String  DFS_NAMENODE_NAME_DIR_RESTORE_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_NAME_DIR_RESTORE_KEY;
  public static final boolean DFS_NAMENODE_NAME_DIR_RESTORE_DEFAULT = false;
  public static final String  DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_KEY = "dfs.namenode.support.allow.format";
  public static final boolean DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_DEFAULT = true;
  public static final String  DFS_NAMENODE_NUM_CHECKPOINTS_RETAINED_KEY = "dfs.namenode.num.checkpoints.retained";
  public static final int     DFS_NAMENODE_NUM_CHECKPOINTS_RETAINED_DEFAULT = 2;
  public static final String  DFS_NAMENODE_NUM_EXTRA_EDITS_RETAINED_KEY = "dfs.namenode.num.extra.edits.retained";
  public static final int     DFS_NAMENODE_NUM_EXTRA_EDITS_RETAINED_DEFAULT = 1000000; //1M
  public static final String  DFS_NAMENODE_MAX_EXTRA_EDITS_SEGMENTS_RETAINED_KEY = "dfs.namenode.max.extra.edits.segments.retained";
  public static final int     DFS_NAMENODE_MAX_EXTRA_EDITS_SEGMENTS_RETAINED_DEFAULT = 10000; // 10k
  public static final String  DFS_NAMENODE_MIN_SUPPORTED_DATANODE_VERSION_KEY = "dfs.namenode.min.supported.datanode.version";
  public static final String  DFS_NAMENODE_MIN_SUPPORTED_DATANODE_VERSION_DEFAULT = "2.1.0-beta";

  public static final String  DFS_NAMENODE_EDITS_DIR_MINIMUM_KEY = "dfs.namenode.edits.dir.minimum";
  public static final int     DFS_NAMENODE_EDITS_DIR_MINIMUM_DEFAULT = 1;
  public static final String  DFS_NAMENODE_QUOTA_INIT_THREADS_KEY = "dfs.namenode.quota.init-threads";
  public static final int     DFS_NAMENODE_QUOTA_INIT_THREADS_DEFAULT = 4;

  public static final String  DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD = "dfs.namenode.edit.log.autoroll.multiplier.threshold";
  public static final float   DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD_DEFAULT = 2.0f;
  public static final String  DFS_NAMENODE_EDIT_LOG_AUTOROLL_CHECK_INTERVAL_MS = "dfs.namenode.edit.log.autoroll.check.interval.ms";
  public static final int     DFS_NAMENODE_EDIT_LOG_AUTOROLL_CHECK_INTERVAL_MS_DEFAULT = 5*60*1000;

  public static final String  DFS_NAMENODE_EDITS_NOEDITLOGCHANNELFLUSH = "dfs.namenode.edits.noeditlogchannelflush";
  public static final boolean DFS_NAMENODE_EDITS_NOEDITLOGCHANNELFLUSH_DEFAULT = false;

  public static final String  DFS_NAMENODE_EDITS_ASYNC_LOGGING =
      "dfs.namenode.edits.asynclogging";
  public static final boolean DFS_NAMENODE_EDITS_ASYNC_LOGGING_DEFAULT = true;

  public static final String  DFS_LIST_LIMIT = "dfs.ls.limit";
  public static final int     DFS_LIST_LIMIT_DEFAULT = 1000;
  public static final String  DFS_CONTENT_SUMMARY_LIMIT_KEY = "dfs.content-summary.limit";
  public static final int     DFS_CONTENT_SUMMARY_LIMIT_DEFAULT = 5000;
  public static final String  DFS_CONTENT_SUMMARY_SLEEP_MICROSEC_KEY = "dfs.content-summary.sleep-microsec";
  public static final long    DFS_CONTENT_SUMMARY_SLEEP_MICROSEC_DEFAULT = 500;

  public static final String  DFS_NAMENODE_LIST_CACHE_POOLS_NUM_RESPONSES =
      "dfs.namenode.list.cache.pools.num.responses";
  public static final int     DFS_NAMENODE_LIST_CACHE_POOLS_NUM_RESPONSES_DEFAULT = 100;
  public static final String  DFS_NAMENODE_LIST_CACHE_DIRECTIVES_NUM_RESPONSES =
      "dfs.namenode.list.cache.directives.num.responses";
  public static final int     DFS_NAMENODE_LIST_CACHE_DIRECTIVES_NUM_RESPONSES_DEFAULT = 100;
  public static final String  DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS =
      "dfs.namenode.path.based.cache.refresh.interval.ms";
  public static final long    DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS_DEFAULT = 30000L;

  /** Pending period of block deletion since NameNode startup */
  public static final String  DFS_NAMENODE_STARTUP_DELAY_BLOCK_DELETION_SEC_KEY = "dfs.namenode.startup.delay.block.deletion.sec";
  public static final long    DFS_NAMENODE_STARTUP_DELAY_BLOCK_DELETION_SEC_DEFAULT = 0L;

  public static final String DFS_NAMENODE_SNAPSHOT_CAPTURE_OPENFILES =
      HdfsClientConfigKeys.DFS_NAMENODE_SNAPSHOT_CAPTURE_OPENFILES;
  public static final boolean DFS_NAMENODE_SNAPSHOT_CAPTURE_OPENFILES_DEFAULT =
      HdfsClientConfigKeys.DFS_NAMENODE_SNAPSHOT_CAPTURE_OPENFILES_DEFAULT;

  public static final String DFS_NAMENODE_SNAPSHOT_SKIP_CAPTURE_ACCESSTIME_ONLY_CHANGE = "dfs.namenode.snapshot.skip.capture.accesstime-only-change";
  public static final boolean DFS_NAMENODE_SNAPSHOT_SKIP_CAPTURE_ACCESSTIME_ONLY_CHANGE_DEFAULT = false;

  public static final String
      DFS_NAMENODE_SNAPSHOT_DIFF_ALLOW_SNAP_ROOT_DESCENDANT =
      "dfs.namenode.snapshotdiff.allow.snap-root-descendant";
  public static final boolean
      DFS_NAMENODE_SNAPSHOT_DIFF_ALLOW_SNAP_ROOT_DESCENDANT_DEFAULT =
      true;

  public static final String
      DFS_NAMENODE_SNAPSHOT_DIFF_LISTING_LIMIT  =
      "dfs.namenode.snapshotdiff.listing.limit";
  public static final int
      DFS_NAMENODE_SNAPSHOT_DIFF_LISTING_LIMIT_DEFAULT = 1000;

  public static final String DFS_NAMENODE_SNAPSHOT_MAX_LIMIT =
      "dfs.namenode.snapshot.max.limit";

  public static final int DFS_NAMENODE_SNAPSHOT_MAX_LIMIT_DEFAULT = 65536;
  public static final String DFS_NAMENODE_SNAPSHOT_SKIPLIST_SKIP_INTERVAL =
      "dfs.namenode.snapshot.skiplist.interval";
  public static final int DFS_NAMENODE_SNAPSHOT_SKIPLIST_SKIP_INTERVAL_DEFAULT =
      10;
  public static final String DFS_NAMENODE_SNAPSHOT_SKIPLIST_MAX_LEVELS =
      "dfs.namenode.snapshot.skiplist.max.levels";
  public static final int
      DFS_NAMENODE_SNAPSHOT_SKIPLIST_MAX_SKIP_LEVELS_DEFAULT = 0;

  // Replication monitoring related keys
  public static final String DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION =
      "dfs.namenode.invalidate.work.pct.per.iteration";
  public static final float DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION_DEFAULT = 0.32f;
  public static final String DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION =
      "dfs.namenode.replication.work.multiplier.per.iteration";
  public static final int DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION_DEFAULT = 2;

  //Delegation token related keys
  public static final String  DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_KEY = "dfs.namenode.delegation.key.update-interval";
  public static final long    DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT = 24*60*60*1000; // 1 day
  public static final String  DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_KEY = "dfs.namenode.delegation.token.renew-interval";
  public static final long    DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT = 24*60*60*1000;  // 1 day
  public static final String  DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_KEY = "dfs.namenode.delegation.token.max-lifetime";
  public static final long    DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT = 7*24*60*60*1000; // 7 days
  public static final String  DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY = "dfs.namenode.delegation.token.always-use"; // for tests
  public static final boolean DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_DEFAULT = false;

  //Filesystem limit keys
  public static final String  DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY = "dfs.namenode.fs-limits.max-component-length";
  public static final int     DFS_NAMENODE_MAX_COMPONENT_LENGTH_DEFAULT = 255;
  public static final String  DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY = "dfs.namenode.fs-limits.max-directory-items";
  public static final int     DFS_NAMENODE_MAX_DIRECTORY_ITEMS_DEFAULT = 1024*1024;
  public static final String  DFS_NAMENODE_MIN_BLOCK_SIZE_KEY = "dfs.namenode.fs-limits.min-block-size";
  public static final long    DFS_NAMENODE_MIN_BLOCK_SIZE_DEFAULT = 1024*1024;
  public static final String  DFS_NAMENODE_MAX_BLOCKS_PER_FILE_KEY = "dfs.namenode.fs-limits.max-blocks-per-file";
  public static final long    DFS_NAMENODE_MAX_BLOCKS_PER_FILE_DEFAULT = 10*1000;
  public static final String  DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY = "dfs.namenode.fs-limits.max-xattrs-per-inode";
  public static final int     DFS_NAMENODE_MAX_XATTRS_PER_INODE_DEFAULT = 32;
  public static final String  DFS_NAMENODE_MAX_XATTR_SIZE_KEY = "dfs.namenode.fs-limits.max-xattr-size";
  public static final int     DFS_NAMENODE_MAX_XATTR_SIZE_DEFAULT = 16384;
  public static final int     DFS_NAMENODE_MAX_XATTR_SIZE_HARD_LIMIT = 32768;

  public static final String  DFS_NAMENODE_LEASE_RECHECK_INTERVAL_MS_KEY =
      "dfs.namenode.lease-recheck-interval-ms";
  public static final long    DFS_NAMENODE_LEASE_RECHECK_INTERVAL_MS_DEFAULT =
      2000;
  public static final String
      DFS_NAMENODE_MAX_LOCK_HOLD_TO_RELEASE_LEASE_MS_KEY =
      "dfs.namenode.max-lock-hold-to-release-lease-ms";
  public static final long
      DFS_NAMENODE_MAX_LOCK_HOLD_TO_RELEASE_LEASE_MS_DEFAULT = 25;

  public static final String DFS_NAMENODE_FSLOCK_FAIR_KEY =
      "dfs.namenode.fslock.fair";
  public static final boolean DFS_NAMENODE_FSLOCK_FAIR_DEFAULT = true;

  public static final String  DFS_NAMENODE_LOCK_DETAILED_METRICS_KEY =
      "dfs.namenode.lock.detailed-metrics.enabled";
  public static final boolean DFS_NAMENODE_LOCK_DETAILED_METRICS_DEFAULT =
      false;
  // Threshold for how long namenode locks must be held for the
  // event to be logged
  public static final String  DFS_NAMENODE_WRITE_LOCK_REPORTING_THRESHOLD_MS_KEY =
      "dfs.namenode.write-lock-reporting-threshold-ms";
  public static final long    DFS_NAMENODE_WRITE_LOCK_REPORTING_THRESHOLD_MS_DEFAULT = 5000L;
  public static final String  DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_KEY =
      "dfs.namenode.read-lock-reporting-threshold-ms";
  public static final long    DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_DEFAULT = 5000L;
  // Threshold for how long the lock warnings must be suppressed
  public static final String DFS_LOCK_SUPPRESS_WARNING_INTERVAL_KEY =
      "dfs.lock.suppress.warning.interval";
  public static final long DFS_LOCK_SUPPRESS_WARNING_INTERVAL_DEFAULT =
      10000; //ms

  public static final String  DFS_UPGRADE_DOMAIN_FACTOR = "dfs.namenode.upgrade.domain.factor";
  public static final int DFS_UPGRADE_DOMAIN_FACTOR_DEFAULT = DFS_REPLICATION_DEFAULT;

  public static final int     DFS_NAMENODE_HTTPS_PORT_DEFAULT =
      HdfsClientConfigKeys.DFS_NAMENODE_HTTPS_PORT_DEFAULT;
  public static final String  DFS_NAMENODE_HTTPS_ADDRESS_KEY =
      HdfsClientConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY;
  public static final String  DFS_NAMENODE_HTTPS_BIND_HOST_KEY = "dfs.namenode.https-bind-host";
  public static final String  DFS_NAMENODE_HTTPS_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_NAMENODE_HTTPS_PORT_DEFAULT;
  public static final String  DFS_NAMENODE_NAME_DIR_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_NAME_DIR_KEY;
  public static final String  DFS_NAMENODE_EDITS_DIR_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_EDITS_DIR_KEY;
  public static final String  DFS_NAMENODE_SHARED_EDITS_DIR_KEY = "dfs.namenode.shared.edits.dir";
  public static final String  DFS_NAMENODE_EDITS_PLUGIN_PREFIX = "dfs.namenode.edits.journal-plugin";
  public static final String  DFS_NAMENODE_EDITS_DIR_REQUIRED_KEY = "dfs.namenode.edits.dir.required";
  public static final String  DFS_NAMENODE_EDITS_DIR_DEFAULT = "file:///tmp/hadoop/dfs/name";
  public static final String  DFS_METRICS_SESSION_ID_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_METRICS_SESSION_ID_KEY;
  public static final String  DFS_METRICS_PERCENTILES_INTERVALS_KEY = "dfs.metrics.percentiles.intervals";

  public static final String  DFS_NAMENODE_CHECKPOINT_DIR_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY;
  public static final String  DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY;
  public static final String  DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY =
      "dfs.namenode.hosts.provider.classname";
  public static final String  DFS_HOSTS = "dfs.hosts";
  public static final String  DFS_HOSTS_EXCLUDE = "dfs.hosts.exclude";
  public static final String  DFS_NAMENODE_AUDIT_LOGGERS_KEY = "dfs.namenode.audit.loggers";
  public static final String  DFS_NAMENODE_DEFAULT_AUDIT_LOGGER_NAME = "default";
  public static final String  DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_KEY = "dfs.namenode.audit.log.token.tracking.id";
  public static final boolean DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_DEFAULT = false;
  public static final String  DFS_NAMENODE_AUDIT_LOG_ASYNC_KEY = "dfs.namenode.audit.log.async";
  public static final boolean DFS_NAMENODE_AUDIT_LOG_ASYNC_DEFAULT = false;
  public static final String  DFS_NAMENODE_AUDIT_LOG_DEBUG_CMDLIST = "dfs.namenode.audit.log.debug.cmdlist";
  public static final String  DFS_NAMENODE_METRICS_LOGGER_PERIOD_SECONDS_KEY =
      "dfs.namenode.metrics.logger.period.seconds";
  public static final int     DFS_NAMENODE_METRICS_LOGGER_PERIOD_SECONDS_DEFAULT =
      600;

  public static final String  DFS_NAMENODE_EC_POLICIES_MAX_CELLSIZE_KEY = "dfs.namenode.ec.policies.max.cellsize";
  public static final int     DFS_NAMENODE_EC_POLICIES_MAX_CELLSIZE_DEFAULT = 4 * 1024 * 1024;
  public static final String  DFS_NAMENODE_EC_SYSTEM_DEFAULT_POLICY =
      "dfs.namenode.ec.system.default.policy";
  public static final String  DFS_NAMENODE_EC_SYSTEM_DEFAULT_POLICY_DEFAULT =
      "RS-6-3-1024k";
  public static final String  DFS_HEARTBEAT_INTERVAL_KEY = "dfs.heartbeat.interval";
  public static final long    DFS_HEARTBEAT_INTERVAL_DEFAULT = 3;
  public static final String  DFS_NAMENODE_HANDLER_COUNT_KEY = "dfs.namenode.handler.count";
  public static final int     DFS_NAMENODE_HANDLER_COUNT_DEFAULT = 10;
  public static final String  DFS_NAMENODE_LIFELINE_HANDLER_RATIO_KEY =
      "dfs.namenode.lifeline.handler.ratio";
  public static final float   DFS_NAMENODE_LIFELINE_HANDLER_RATIO_DEFAULT =
      0.1f;
  public static final String  DFS_NAMENODE_LIFELINE_HANDLER_COUNT_KEY =
      "dfs.namenode.lifeline.handler.count";
  public static final String  DFS_NAMENODE_SERVICE_HANDLER_COUNT_KEY = "dfs.namenode.service.handler.count";
  public static final int     DFS_NAMENODE_SERVICE_HANDLER_COUNT_DEFAULT = 10;
  public static final String  DFS_HTTP_POLICY_KEY = "dfs.http.policy";
  public static final String  DFS_HTTP_POLICY_DEFAULT =  HttpConfig.Policy.HTTP_ONLY.name();
  public static final String  DFS_DEFAULT_CHUNK_VIEW_SIZE_KEY = "dfs.default.chunk.view.size";
  public static final int     DFS_DEFAULT_CHUNK_VIEW_SIZE_DEFAULT = 32*1024;
  public static final String  DFS_NAMENODE_INODE_ATTRIBUTES_PROVIDER_KEY = "dfs.namenode.inode.attributes.provider.class";
  public static final String  DFS_NAMENODE_INODE_ATTRIBUTES_PROVIDER_BYPASS_USERS_KEY = "dfs.namenode.inode.attributes.provider.bypass.users";
  public static final String  DFS_NAMENODE_INODE_ATTRIBUTES_PROVIDER_BYPASS_USERS_DEFAULT = "";
  public static final String  DFS_REPLICATION_MAX_KEY = "dfs.replication.max";
  public static final int     DFS_REPLICATION_MAX_DEFAULT = 512;
  public static final String  DFS_DF_INTERVAL_KEY = "dfs.df.interval";
  public static final int     DFS_DF_INTERVAL_DEFAULT = 60000;
  public static final String  DFS_BLOCK_INVALIDATE_LIMIT_KEY = "dfs.block.invalidate.limit";
  public static final int     DFS_BLOCK_INVALIDATE_LIMIT_DEFAULT = 1000;

  // property for fsimage compression
  public static final String DFS_IMAGE_COMPRESS_KEY = "dfs.image.compress";
  public static final boolean DFS_IMAGE_COMPRESS_DEFAULT = false;
  public static final String DFS_IMAGE_COMPRESSION_CODEC_KEY =
                                   "dfs.image.compression.codec";
  public static final String DFS_IMAGE_COMPRESSION_CODEC_DEFAULT =
                                   "org.apache.hadoop.io.compress.DefaultCodec";

  public static final String DFS_IMAGE_TRANSFER_RATE_KEY =
                                           "dfs.image.transfer.bandwidthPerSec";
  public static final long DFS_IMAGE_TRANSFER_RATE_DEFAULT = 0;  //no throttling

  public static final String DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY =
      "dfs.image.transfer-bootstrap-standby.bandwidthPerSec";
  public static final long DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_DEFAULT =
      0;  //no throttling

  // Image transfer timeout
  public static final String DFS_IMAGE_TRANSFER_TIMEOUT_KEY = "dfs.image.transfer.timeout";
  public static final int DFS_IMAGE_TRANSFER_TIMEOUT_DEFAULT = 60 * 1000;

  // Image transfer chunksize
  public static final String DFS_IMAGE_TRANSFER_CHUNKSIZE_KEY = "dfs.image.transfer.chunksize";
  public static final int DFS_IMAGE_TRANSFER_CHUNKSIZE_DEFAULT = 64 * 1024;

  // Edit Log segment transfer timeout
  public static final String DFS_EDIT_LOG_TRANSFER_TIMEOUT_KEY =
      "dfs.edit.log.transfer.timeout";
  public static final int DFS_EDIT_LOG_TRANSFER_TIMEOUT_DEFAULT = 30 * 1000;

  // Throttling Edit Log Segment transfer for Journal Sync
  public static final String DFS_EDIT_LOG_TRANSFER_RATE_KEY =
      "dfs.edit.log.transfer.bandwidthPerSec";
  public static final long DFS_EDIT_LOG_TRANSFER_RATE_DEFAULT = 0; //no throttling

  public static final String DFS_QJM_OPERATIONS_TIMEOUT =
      "dfs.qjm.operations.timeout";
  public static final long DFS_QJM_OPERATIONS_TIMEOUT_DEFAULT = 60000;
  public static final String  DFS_NAMENODE_PLUGINS_KEY = "dfs.namenode.plugins";
  public static final String  DFS_WEB_UGI_KEY = "dfs.web.ugi";
  public static final String  DFS_NAMENODE_STARTUP_KEY = "dfs.namenode.startup";
  public static final String
      DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS =
      HdfsClientConfigKeys
          .DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS;
  public static final int
      DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS_DEFAULT =
      HdfsClientConfigKeys
          .DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS_DEFAULT;
  public static final String  DFS_NAMENODE_KEYTAB_FILE_KEY = "dfs.namenode.keytab.file";
  public static final String  DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY =
      HdfsClientConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
  @Deprecated
  public static final String  DFS_NAMENODE_USER_NAME_KEY = DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
  public static final String  DFS_NAMENODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY = "dfs.namenode.kerberos.internal.spnego.principal";
  @Deprecated
  public static final String  DFS_NAMENODE_INTERNAL_SPNEGO_USER_NAME_KEY = DFS_NAMENODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY;
  public static final String  DFS_SECONDARY_NAMENODE_KEYTAB_FILE_KEY = "dfs.secondary.namenode.keytab.file";
  public static final String  DFS_SECONDARY_NAMENODE_KERBEROS_PRINCIPAL_KEY = "dfs.secondary.namenode.kerberos.principal";
  @Deprecated
  public static final String  DFS_SECONDARY_NAMENODE_USER_NAME_KEY = DFS_SECONDARY_NAMENODE_KERBEROS_PRINCIPAL_KEY;
  public static final String  DFS_SECONDARY_NAMENODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY = "dfs.secondary.namenode.kerberos.internal.spnego.principal";
  @Deprecated
  public static final String  DFS_SECONDARY_NAMENODE_INTERNAL_SPNEGO_USER_NAME_KEY = DFS_SECONDARY_NAMENODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY;
  public static final String  DFS_NAMENODE_NAME_CACHE_THRESHOLD_KEY = "dfs.namenode.name.cache.threshold";
  public static final int     DFS_NAMENODE_NAME_CACHE_THRESHOLD_DEFAULT = 10;
  public static final String  DFS_NAMENODE_LEGACY_OIV_IMAGE_DIR_KEY = "dfs.namenode.legacy-oiv-image.dir";

  public static final String  DFS_NAMESERVICES =
      HdfsClientConfigKeys.DFS_NAMESERVICES;
  public static final String  DFS_NAMESERVICE_ID =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_NAMESERVICE_ID;
  public static final String  DFS_INTERNAL_NAMESERVICES_KEY = "dfs.internal.nameservices";
  public static final String  DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY = "dfs.namenode.resource.check.interval";
  public static final int     DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_DEFAULT = 5000;
  public static final String  DFS_NAMENODE_DU_RESERVED_KEY = "dfs.namenode.resource.du.reserved";
  public static final long    DFS_NAMENODE_DU_RESERVED_DEFAULT = 1024 * 1024 * 100; // 100 MB
  public static final String  DFS_NAMENODE_CHECKED_VOLUMES_KEY = "dfs.namenode.resource.checked.volumes";
  public static final String  DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_KEY = "dfs.namenode.resource.checked.volumes.minimum";
  public static final int     DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_DEFAULT = 1;
  public static final String  DFS_WEB_AUTHENTICATION_SIMPLE_ANONYMOUS_ALLOWED = "dfs.web.authentication.simple.anonymous.allowed";
  public static final String  DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY = "dfs.web.authentication.kerberos.principal";
  public static final String  DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY = "dfs.web.authentication.kerberos.keytab";
  public static final String  DFS_NAMENODE_MAX_OP_SIZE_KEY = "dfs.namenode.max.op.size";
  public static final int     DFS_NAMENODE_MAX_OP_SIZE_DEFAULT = 50 * 1024 * 1024;

  public static final String DFS_BLOCK_LOCAL_PATH_ACCESS_USER_KEY = "dfs.block.local-path-access.user";
  public static final String DFS_DOMAIN_SOCKET_PATH_KEY =
      HdfsClientConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY;
  public static final String DFS_DOMAIN_SOCKET_PATH_DEFAULT =
      HdfsClientConfigKeys.DFS_DOMAIN_SOCKET_PATH_DEFAULT;

  public static final String  DFS_STORAGE_POLICY_ENABLED_KEY = "dfs.storage.policy.enabled";
  public static final boolean DFS_STORAGE_POLICY_ENABLED_DEFAULT = true;

  public static final String  DFS_QUOTA_BY_STORAGETYPE_ENABLED_KEY = "dfs.quota.by.storage.type.enabled";
  public static final boolean DFS_QUOTA_BY_STORAGETYPE_ENABLED_DEFAULT = true;

  // HA related configuration
  public static final String DFS_HA_NAMENODES_KEY_PREFIX =
      HdfsClientConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
  public static final String DFS_HA_NAMENODE_ID_KEY = "dfs.ha.namenode.id";
  public static final String  DFS_HA_STANDBY_CHECKPOINTS_KEY = "dfs.ha.standby.checkpoints";
  public static final boolean DFS_HA_STANDBY_CHECKPOINTS_DEFAULT = true;
  public static final String DFS_HA_LOGROLL_PERIOD_KEY = "dfs.ha.log-roll.period";
  public static final int DFS_HA_LOGROLL_PERIOD_DEFAULT = 2 * 60; // 2m
  public static final String DFS_HA_TAILEDITS_PERIOD_KEY = "dfs.ha.tail-edits.period";
  public static final int DFS_HA_TAILEDITS_PERIOD_DEFAULT = 60; // 1m
  public static final String DFS_HA_TAILEDITS_ALL_NAMESNODES_RETRY_KEY = "dfs.ha.tail-edits.namenode-retries";
  public static final int DFS_HA_TAILEDITS_ALL_NAMESNODES_RETRY_DEFAULT = 3;
  public static final String  DFS_HA_TAILEDITS_INPROGRESS_KEY =
          "dfs.ha.tail-edits.in-progress";
  public static final boolean DFS_HA_TAILEDITS_INPROGRESS_DEFAULT = false;
  public static final String DFS_HA_TAILEDITS_ROLLEDITS_TIMEOUT_KEY =
      "dfs.ha.tail-edits.rolledits.timeout";
  public static final int DFS_HA_TAILEDITS_ROLLEDITS_TIMEOUT_DEFAULT = 60; // 1m
  public static final String DFS_HA_LOGROLL_RPC_TIMEOUT_KEY = "dfs.ha.log-roll.rpc.timeout";
  public static final int DFS_HA_LOGROLL_RPC_TIMEOUT_DEFAULT = 20000; // 20s
  public static final String DFS_HA_FENCE_METHODS_KEY = "dfs.ha.fencing.methods";
  public static final String DFS_HA_AUTO_FAILOVER_ENABLED_KEY = "dfs.ha.automatic-failover.enabled";
  public static final boolean DFS_HA_AUTO_FAILOVER_ENABLED_DEFAULT = false;
  public static final String DFS_HA_ZKFC_PORT_KEY = "dfs.ha.zkfc.port";
  public static final int DFS_HA_ZKFC_PORT_DEFAULT = 8019;
  public static final String DFS_HA_ZKFC_NN_HTTP_TIMEOUT_KEY = "dfs.ha.zkfc.nn.http.timeout.ms";
  public static final int DFS_HA_ZKFC_NN_HTTP_TIMEOUT_KEY_DEFAULT = 20000;

  // Security-related configs
  public static final String DFS_ENCRYPT_DATA_TRANSFER_KEY = "dfs.encrypt.data.transfer";
  public static final boolean DFS_ENCRYPT_DATA_TRANSFER_DEFAULT = false;
  public static final String DFS_XFRAME_OPTION_ENABLED = "dfs.xframe.enabled";
  public static final boolean DFS_XFRAME_OPTION_ENABLED_DEFAULT = true;

  public static final String DFS_XFRAME_OPTION_VALUE = "dfs.xframe.value";
  public static final String DFS_XFRAME_OPTION_VALUE_DEFAULT = "SAMEORIGIN";
  public static final int    DFS_NAMENODE_LIST_ENCRYPTION_ZONES_NUM_RESPONSES_DEFAULT = 100;
  public static final String DFS_NAMENODE_LIST_ENCRYPTION_ZONES_NUM_RESPONSES = "dfs.namenode.list.encryption.zones.num.responses";
  public static final int    DFS_NAMENODE_LIST_REENCRYPTION_STATUS_NUM_RESPONSES_DEFAULT = 100;
  public static final String DFS_NAMENODE_LIST_REENCRYPTION_STATUS_NUM_RESPONSES_KEY = "dfs.namenode.list.reencryption.status.num.responses";
  public static final String DFS_NAMENODE_LIST_OPENFILES_NUM_RESPONSES =
      "dfs.namenode.list.openfiles.num.responses";
  public static final int    DFS_NAMENODE_LIST_OPENFILES_NUM_RESPONSES_DEFAULT =
      1000;
  public static final String DFS_NAMENODE_EDEKCACHELOADER_INTERVAL_MS_KEY = "dfs.namenode.edekcacheloader.interval.ms";
  public static final int DFS_NAMENODE_EDEKCACHELOADER_INTERVAL_MS_DEFAULT = 1000;
  public static final String DFS_NAMENODE_EDEKCACHELOADER_INITIAL_DELAY_MS_KEY = "dfs.namenode.edekcacheloader.initial.delay.ms";
  public static final int DFS_NAMENODE_EDEKCACHELOADER_INITIAL_DELAY_MS_DEFAULT = 3000;
  public static final String DFS_NAMENODE_REENCRYPT_SLEEP_INTERVAL_KEY = "dfs.namenode.reencrypt.sleep.interval";
  public static final String DFS_NAMENODE_REENCRYPT_SLEEP_INTERVAL_DEFAULT = "1m";
  public static final String DFS_NAMENODE_REENCRYPT_BATCH_SIZE_KEY = "dfs.namenode.reencrypt.batch.size";
  public static final int DFS_NAMENODE_REENCRYPT_BATCH_SIZE_DEFAULT = 1000;
  public static final String DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_HANDLER_RATIO_KEY = "dfs.namenode.reencrypt.throttle.limit.handler.ratio";
  public static final double DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_HANDLER_RATIO_DEFAULT = 1.0;
  public static final String DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_UPDATER_RATIO_KEY = "dfs.namenode.reencrypt.throttle.limit.updater.ratio";
  public static final double DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_UPDATER_RATIO_DEFAULT = 1.0;
  public static final String DFS_NAMENODE_REENCRYPT_EDEK_THREADS_KEY = "dfs.namenode.reencrypt.edek.threads";
  public static final int DFS_NAMENODE_REENCRYPT_EDEK_THREADS_DEFAULT = 10;

  // Journal-node related configs. These are read on the JN side.
  public static final String  DFS_JOURNALNODE_EDITS_DIR_KEY = "dfs.journalnode.edits.dir";
  public static final String  DFS_JOURNALNODE_EDITS_DIR_DEFAULT = "/tmp/hadoop/dfs/journalnode/";
  public static final String  DFS_JOURNALNODE_RPC_ADDRESS_KEY = "dfs.journalnode.rpc-address";
  public static final int     DFS_JOURNALNODE_RPC_PORT_DEFAULT = 8485;
  public static final String  DFS_JOURNALNODE_RPC_BIND_HOST_KEY = "dfs.journalnode.rpc-bind-host";
  public static final String  DFS_JOURNALNODE_RPC_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_JOURNALNODE_RPC_PORT_DEFAULT;

  public static final String  DFS_JOURNALNODE_HTTP_ADDRESS_KEY = "dfs.journalnode.http-address";
  public static final int     DFS_JOURNALNODE_HTTP_PORT_DEFAULT = 8480;
  public static final String  DFS_JOURNALNODE_HTTP_BIND_HOST_KEY = "dfs.journalnode.http-bind-host";
  public static final String  DFS_JOURNALNODE_HTTP_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_JOURNALNODE_HTTP_PORT_DEFAULT;
  public static final String  DFS_JOURNALNODE_HTTPS_ADDRESS_KEY = "dfs.journalnode.https-address";
  public static final int     DFS_JOURNALNODE_HTTPS_PORT_DEFAULT = 8481;
  public static final String  DFS_JOURNALNODE_HTTPS_BIND_HOST_KEY = "dfs.journalnode.https-bind-host";
  public static final String  DFS_JOURNALNODE_HTTPS_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_JOURNALNODE_HTTPS_PORT_DEFAULT;


  public static final String  DFS_JOURNALNODE_KEYTAB_FILE_KEY = "dfs.journalnode.keytab.file";
  public static final String  DFS_JOURNALNODE_KERBEROS_PRINCIPAL_KEY = "dfs.journalnode.kerberos.principal";
  public static final String  DFS_JOURNALNODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY = "dfs.journalnode.kerberos.internal.spnego.principal";
  public static final String DFS_JOURNALNODE_ENABLE_SYNC_KEY =
      "dfs.journalnode.enable.sync";
  public static final boolean DFS_JOURNALNODE_ENABLE_SYNC_DEFAULT = true;
  public static final String DFS_JOURNALNODE_SYNC_INTERVAL_KEY =
      "dfs.journalnode.sync.interval";
  public static final long DFS_JOURNALNODE_SYNC_INTERVAL_DEFAULT = 2*60*1000L;

  // Journal-node related configs for the client side.
  public static final String  DFS_QJOURNAL_QUEUE_SIZE_LIMIT_KEY = "dfs.qjournal.queued-edits.limit.mb";
  public static final int     DFS_QJOURNAL_QUEUE_SIZE_LIMIT_DEFAULT = 10;
  
  // Quorum-journal timeouts for various operations. Unlikely to need
  // to be tweaked, but configurable just in case.
  public static final String  DFS_QJOURNAL_START_SEGMENT_TIMEOUT_KEY = "dfs.qjournal.start-segment.timeout.ms";
  public static final String  DFS_QJOURNAL_PREPARE_RECOVERY_TIMEOUT_KEY = "dfs.qjournal.prepare-recovery.timeout.ms";
  public static final String  DFS_QJOURNAL_ACCEPT_RECOVERY_TIMEOUT_KEY = "dfs.qjournal.accept-recovery.timeout.ms";
  public static final String  DFS_QJOURNAL_FINALIZE_SEGMENT_TIMEOUT_KEY = "dfs.qjournal.finalize-segment.timeout.ms";
  public static final String  DFS_QJOURNAL_SELECT_INPUT_STREAMS_TIMEOUT_KEY = "dfs.qjournal.select-input-streams.timeout.ms";
  public static final String  DFS_QJOURNAL_GET_JOURNAL_STATE_TIMEOUT_KEY = "dfs.qjournal.get-journal-state.timeout.ms";
  public static final String  DFS_QJOURNAL_NEW_EPOCH_TIMEOUT_KEY = "dfs.qjournal.new-epoch.timeout.ms";
  public static final String  DFS_QJOURNAL_WRITE_TXNS_TIMEOUT_KEY = "dfs.qjournal.write-txns.timeout.ms";
  public static final int     DFS_QJOURNAL_START_SEGMENT_TIMEOUT_DEFAULT = 20000;
  public static final int     DFS_QJOURNAL_PREPARE_RECOVERY_TIMEOUT_DEFAULT = 120000;
  public static final int     DFS_QJOURNAL_ACCEPT_RECOVERY_TIMEOUT_DEFAULT = 120000;
  public static final int     DFS_QJOURNAL_FINALIZE_SEGMENT_TIMEOUT_DEFAULT = 120000;
  public static final int     DFS_QJOURNAL_SELECT_INPUT_STREAMS_TIMEOUT_DEFAULT = 20000;
  public static final int     DFS_QJOURNAL_GET_JOURNAL_STATE_TIMEOUT_DEFAULT = 120000;
  public static final int     DFS_QJOURNAL_NEW_EPOCH_TIMEOUT_DEFAULT = 120000;
  public static final int     DFS_QJOURNAL_WRITE_TXNS_TIMEOUT_DEFAULT = 20000;
  
  public static final String DFS_NAMENODE_ENABLE_RETRY_CACHE_KEY = "dfs.namenode.enable.retrycache";
  public static final boolean DFS_NAMENODE_ENABLE_RETRY_CACHE_DEFAULT = true;
  public static final String DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_KEY = "dfs.namenode.retrycache.expirytime.millis";
  public static final long DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_DEFAULT = 600000; // 10 minutes
  public static final String DFS_NAMENODE_RETRY_CACHE_HEAP_PERCENT_KEY = "dfs.namenode.retrycache.heap.percent";
  public static final float DFS_NAMENODE_RETRY_CACHE_HEAP_PERCENT_DEFAULT = 0.03f;

  public static final String DFS_NAMENODE_INOTIFY_MAX_EVENTS_PER_RPC_KEY =
      "dfs.namenode.inotify.max.events.per.rpc";
  public static final int DFS_NAMENODE_INOTIFY_MAX_EVENTS_PER_RPC_DEFAULT =
      1000;

  public static final String IGNORE_SECURE_PORTS_FOR_TESTING_KEY =
      "ignore.secure.ports.for.testing";
  public static final boolean IGNORE_SECURE_PORTS_FOR_TESTING_DEFAULT = false;

  // nntop Configurations
  public static final String NNTOP_ENABLED_KEY =
      "dfs.namenode.top.enabled";
  public static final boolean NNTOP_ENABLED_DEFAULT = true;
  public static final String NNTOP_BUCKETS_PER_WINDOW_KEY =
      "dfs.namenode.top.window.num.buckets";
  public static final int NNTOP_BUCKETS_PER_WINDOW_DEFAULT = 10;
  public static final String NNTOP_NUM_USERS_KEY =
      "dfs.namenode.top.num.users";
  public static final int NNTOP_NUM_USERS_DEFAULT = 10;
  // comma separated list of nntop reporting periods in minutes
  public static final String NNTOP_WINDOWS_MINUTES_KEY =
      "dfs.namenode.top.windows.minutes";
  public static final String[] NNTOP_WINDOWS_MINUTES_DEFAULT = {"1","5","25"};
  public static final String DFS_PIPELINE_ECN_ENABLED = "dfs.pipeline.ecn";
  public static final boolean DFS_PIPELINE_ECN_ENABLED_DEFAULT = false;


  public static final String  DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY;
  public static final String  DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_DEFAULT = "ssl-client.xml";
  public static final String  DFS_CLIENT_HTTPS_NEED_AUTH_KEY =
      HdfsClientConfigKeys.DeprecatedKeys.DFS_CLIENT_HTTPS_NEED_AUTH_KEY;
  public static final boolean DFS_CLIENT_HTTPS_NEED_AUTH_DEFAULT = false;

}
