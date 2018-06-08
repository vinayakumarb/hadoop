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
package org.apache.hadoop.hdfs.protocolPB;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.StorageTypeProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.BlockWithLocationsProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.BlocksWithLocationsProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.CheckpointCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.CheckpointSignatureProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.NNHAStatusHeartbeatProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.NamenodeCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.NamenodeRegistrationProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.NamenodeRegistrationProto.NamenodeRoleProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.NamespaceInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.RemoteEditLogManifestProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.RemoteEditLogProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.ReplicaStateProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.StorageInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.JournalProtocolProtos.JournalInfoProto;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.StripedBlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.CheckpointCommand;
import org.apache.hadoop.hdfs.server.protocol.JournalInfo;
import org.apache.hadoop.hdfs.server.protocol.NNHAStatusHeartbeat;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;

/**
 * Utilities for converting protobuf classes to and from implementation classes
 * and other helper utilities to help in dealing with protobuf.
 * 
 * Note that when converting from an internal type to protobuf type, the
 * converter never return null for protobuf type. The check for internal type
 * being null must be done before calling the convert() method.
 *
 * For those helper methods that convert HDFS client-side data structures from
 * and to protobuf, see {@link PBHelperClient}.
 */
public class PBHelper {
  private PBHelper() {
    /** Hidden constructor */
  }

  public static NamenodeRole convert(NamenodeRoleProto role) {
    switch (role) {
    case NAMENODE:
      return NamenodeRole.NAMENODE;
    }
    return null;
  }

  public static NamenodeRoleProto convert(NamenodeRole role) {
    switch (role) {
    case NAMENODE:
      return NamenodeRoleProto.NAMENODE;
    }
    return null;
  }

  public static StorageInfoProto convert(StorageInfo info) {
    return StorageInfoProto.newBuilder().setClusterID(info.getClusterID())
        .setCTime(info.getCTime()).setLayoutVersion(info.getLayoutVersion())
        .setNamespceID(info.getNamespaceID()).build();
  }

  public static StorageInfo convert(StorageInfoProto info, NodeType type) {
    return new StorageInfo(info.getLayoutVersion(), info.getNamespceID(),
        info.getClusterID(), info.getCTime(), type);
  }

  public static NamenodeRegistrationProto convert(NamenodeRegistration reg) {
    return NamenodeRegistrationProto.newBuilder()
        .setHttpAddress(reg.getHttpAddress()).setRole(convert(reg.getRole()))
        .setRpcAddress(reg.getAddress())
        .setStorageInfo(convert((StorageInfo) reg)).build();
  }

  public static NamenodeRegistration convert(NamenodeRegistrationProto reg) {
    StorageInfo si = convert(reg.getStorageInfo(), NodeType.NAME_NODE);
    return new NamenodeRegistration(reg.getRpcAddress(), reg.getHttpAddress(),
        si, convert(reg.getRole()));
  }

  public static BlockWithLocationsProto convert(BlockWithLocations blk) {
    BlockWithLocationsProto.Builder builder = BlockWithLocationsProto
        .newBuilder().setBlock(PBHelperClient.convert(blk.getBlock()))
        .addAllDatanodeUuids(Arrays.asList(blk.getDatanodeUuids()))
        .addAllStorageUuids(Arrays.asList(blk.getStorageIDs()))
        .addAllStorageTypes(PBHelperClient.convertStorageTypes(blk.getStorageTypes()));
    if (blk instanceof StripedBlockWithLocations) {
      StripedBlockWithLocations sblk = (StripedBlockWithLocations) blk;
      builder.setIndices(PBHelperClient.getByteString(sblk.getIndices()));
      builder.setDataBlockNum(sblk.getDataBlockNum());
      builder.setCellSize(sblk.getCellSize());
    }
    return builder.build();
  }

  public static BlockWithLocations convert(BlockWithLocationsProto b) {
    final List<String> datanodeUuids = b.getDatanodeUuidsList();
    final List<String> storageUuids = b.getStorageUuidsList();
    final List<StorageTypeProto> storageTypes = b.getStorageTypesList();
    BlockWithLocations blk = new BlockWithLocations(PBHelperClient.
        convert(b.getBlock()),
        datanodeUuids.toArray(new String[datanodeUuids.size()]),
        storageUuids.toArray(new String[storageUuids.size()]),
        PBHelperClient.convertStorageTypes(storageTypes, storageUuids.size()));
    if (b.hasIndices()) {
      blk = new StripedBlockWithLocations(blk, b.getIndices().toByteArray(),
          (short) b.getDataBlockNum(), b.getCellSize());
    }
    return blk;
  }

  public static BlocksWithLocationsProto convert(BlocksWithLocations blks) {
    BlocksWithLocationsProto.Builder builder = BlocksWithLocationsProto
        .newBuilder();
    for (BlockWithLocations b : blks.getBlocks()) {
      builder.addBlocks(convert(b));
    }
    return builder.build();
  }

  public static BlocksWithLocations convert(BlocksWithLocationsProto blocks) {
    List<BlockWithLocationsProto> b = blocks.getBlocksList();
    BlockWithLocations[] ret = new BlockWithLocations[b.size()];
    int i = 0;
    for (BlockWithLocationsProto entry : b) {
      ret[i++] = convert(entry);
    }
    return new BlocksWithLocations(ret);
  }

  public static CheckpointSignatureProto convert(CheckpointSignature s) {
    return CheckpointSignatureProto.newBuilder()
        .setBlockPoolId(s.getBlockpoolID())
        .setCurSegmentTxId(s.getCurSegmentTxId())
        .setMostRecentCheckpointTxId(s.getMostRecentCheckpointTxId())
        .setStorageInfo(PBHelper.convert((StorageInfo) s)).build();
  }

  public static CheckpointSignature convert(CheckpointSignatureProto s) {
    StorageInfo si = PBHelper.convert(s.getStorageInfo(), NodeType.NAME_NODE);
    return new CheckpointSignature(si, s.getBlockPoolId(),
        s.getMostRecentCheckpointTxId(), s.getCurSegmentTxId());
  }

  public static RemoteEditLogProto convert(RemoteEditLog log) {
    return RemoteEditLogProto.newBuilder()
        .setStartTxId(log.getStartTxId())
        .setEndTxId(log.getEndTxId())
        .setIsInProgress(log.isInProgress()).build();
  }

  public static RemoteEditLog convert(RemoteEditLogProto l) {
    return new RemoteEditLog(l.getStartTxId(), l.getEndTxId(),
        l.getIsInProgress());
  }

  public static RemoteEditLogManifestProto convert(
      RemoteEditLogManifest manifest) {
    RemoteEditLogManifestProto.Builder builder = RemoteEditLogManifestProto
        .newBuilder()
        .setCommittedTxnId(manifest.getCommittedTxnId());
    for (RemoteEditLog log : manifest.getLogs()) {
      builder.addLogs(convert(log));
    }
    return builder.build();
  }

  public static RemoteEditLogManifest convert(
      RemoteEditLogManifestProto manifest) {
    List<RemoteEditLog> logs = new ArrayList<RemoteEditLog>(manifest
        .getLogsList().size());
    for (RemoteEditLogProto l : manifest.getLogsList()) {
      logs.add(convert(l));
    }
    return new RemoteEditLogManifest(logs,
            manifest.getCommittedTxnId());
  }

  public static CheckpointCommandProto convert(CheckpointCommand cmd) {
    return CheckpointCommandProto.newBuilder()
        .setSignature(convert(cmd.getSignature()))
        .setNeedToReturnImage(cmd.needToReturnImage()).build();
  }

  public static NamenodeCommandProto convert(NamenodeCommand cmd) {
    if (cmd instanceof CheckpointCommand) {
      return NamenodeCommandProto.newBuilder().setAction(cmd.getAction())
          .setType(NamenodeCommandProto.Type.CheckPointCommand)
          .setCheckpointCmd(convert((CheckpointCommand) cmd)).build();
    }
    return NamenodeCommandProto.newBuilder()
        .setType(NamenodeCommandProto.Type.NamenodeCommand)
        .setAction(cmd.getAction()).build();
  }

  public static NamespaceInfo convert(NamespaceInfoProto info) {
    StorageInfoProto storage = info.getStorageInfo();
    return new NamespaceInfo(storage.getNamespceID(), storage.getClusterID(),
        info.getBlockPoolID(), storage.getCTime(), info.getBuildVersion(),
        info.getSoftwareVersion(), info.getCapabilities(),
        convert(info.getState()));
  }

  public static NamenodeCommand convert(NamenodeCommandProto cmd) {
    if (cmd == null) return null;
    switch (cmd.getType()) {
    case CheckPointCommand:
      CheckpointCommandProto chkPt = cmd.getCheckpointCmd();
      return new CheckpointCommand(PBHelper.convert(chkPt.getSignature()),
          chkPt.getNeedToReturnImage());
    default:
      return new NamenodeCommand(cmd.getAction());
    }
  }

  public static ReplicaState convert(ReplicaStateProto state) {
    switch (state) {
    case RBW:
      return ReplicaState.RBW;
    case RUR:
      return ReplicaState.RUR;
    case RWR:
      return ReplicaState.RWR;
    case TEMPORARY:
      return ReplicaState.TEMPORARY;
    case FINALIZED:
    default:
      return ReplicaState.FINALIZED;
    }
  }

  public static ReplicaStateProto convert(ReplicaState state) {
    switch (state) {
    case RBW:
      return ReplicaStateProto.RBW;
    case RUR:
      return ReplicaStateProto.RUR;
    case RWR:
      return ReplicaStateProto.RWR;
    case TEMPORARY:
      return ReplicaStateProto.TEMPORARY;
    case FINALIZED:
    default:
      return ReplicaStateProto.FINALIZED;
    }
  }
  
  public static NamespaceInfoProto convert(NamespaceInfo info) {
    NamespaceInfoProto.Builder builder = NamespaceInfoProto.newBuilder();
    builder.setBlockPoolID(info.getBlockPoolID())
        .setBuildVersion(info.getBuildVersion())
        .setUnused(0)
        .setStorageInfo(PBHelper.convert((StorageInfo)info))
        .setSoftwareVersion(info.getSoftwareVersion())
        .setCapabilities(info.getCapabilities());
    HAServiceState state = info.getState();
    if(state != null) {
      builder.setState(convert(info.getState()));
    }
    return builder.build();
  }

  public static HAServiceState convert(NNHAStatusHeartbeatProto.State s) {
    if (s == null) {
      return null;
    }
    switch (s) {
    case ACTIVE:
      return HAServiceState.ACTIVE;
    case STANDBY:
      return HAServiceState.STANDBY;
    default:
      throw new IllegalArgumentException("Unexpected HAServiceStateProto:"
          + s);
    }
  }

  public static NNHAStatusHeartbeatProto.State convert(HAServiceState s) {
    if (s == null) {
      return null;
    }
    switch (s) {
    case ACTIVE:
      return NNHAStatusHeartbeatProto.State.ACTIVE;
    case STANDBY:
      return NNHAStatusHeartbeatProto.State.STANDBY;
    default:
      throw new IllegalArgumentException("Unexpected HAServiceState:"
          + s);
    }
  }

  public static NNHAStatusHeartbeat convert(NNHAStatusHeartbeatProto s) {
    if (s == null) {
      return null;
    }
    return new NNHAStatusHeartbeat(convert(s.getState()), s.getTxid());
  }

  public static NNHAStatusHeartbeatProto convert(NNHAStatusHeartbeat hb) {
    if (hb == null) {
      return null;
    }
    NNHAStatusHeartbeatProto.Builder builder =
        NNHAStatusHeartbeatProto.newBuilder();
    builder.setState(convert(hb.getState()));
    builder.setTxid(hb.getTxId());
    return builder.build();
  }

  public static JournalInfo convert(JournalInfoProto info) {
    int lv = info.hasLayoutVersion() ? info.getLayoutVersion() : 0;
    int nsID = info.hasNamespaceID() ? info.getNamespaceID() : 0;
    return new JournalInfo(lv, info.getClusterID(), nsID);
  }

  /**
   * Method used for converting {@link JournalInfoProto} sent from Namenode
   * to Journal receivers to {@link NamenodeRegistration}.
   */
  public static JournalInfoProto convert(JournalInfo j) {
    return JournalInfoProto.newBuilder().setClusterID(j.getClusterId())
        .setLayoutVersion(j.getLayoutVersion())
        .setNamespaceID(j.getNamespaceId()).build();
  }
}
