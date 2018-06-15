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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ExtendedBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.LocatedBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.CheckpointSignatureProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.NamenodeRegistrationProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.NamenodeRegistrationProto.NamenodeRoleProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.NamespaceInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.RemoteEditLogManifestProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.RemoteEditLogProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.StorageInfoProto;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

/**
 * Tests for {@link PBHelper}
 */
public class TestPBHelper {

  /**
   * Used for asserting equality on doubles.
   */
  private static final double DELTA = 0.000001;

  @Test
  public void testGetByteString() {
    assertSame(ByteString.EMPTY, PBHelperClient.getByteString(new byte[0]));
  }

  @Test
  public void testConvertNamenodeRole() {
    assertEquals(NamenodeRoleProto.NAMENODE,
        PBHelper.convert(NamenodeRole.NAMENODE));
    assertEquals(NamenodeRole.NAMENODE,
        PBHelper.convert(NamenodeRoleProto.NAMENODE));
  }

  private static StorageInfo getStorageInfo(NodeType type) {
    return new StorageInfo(1, 2, "cid", 3, type);
  }

  @Test
  public void testConvertStoragInfo() {
    StorageInfo info = getStorageInfo(NodeType.NAME_NODE);
    StorageInfoProto infoProto = PBHelper.convert(info);
    StorageInfo info2 = PBHelper.convert(infoProto, NodeType.NAME_NODE);
    assertEquals(info.getClusterID(), info2.getClusterID());
    assertEquals(info.getCTime(), info2.getCTime());
    assertEquals(info.getLayoutVersion(), info2.getLayoutVersion());
    assertEquals(info.getNamespaceID(), info2.getNamespaceID());
  }

  @Test
  public void testConvertNamenodeRegistration() {
    StorageInfo info = getStorageInfo(NodeType.NAME_NODE);
    NamenodeRegistration reg = new NamenodeRegistration("address:999",
        "http:1000", info, NamenodeRole.NAMENODE);
    NamenodeRegistrationProto regProto = PBHelper.convert(reg);
    NamenodeRegistration reg2 = PBHelper.convert(regProto);
    assertEquals(reg.getAddress(), reg2.getAddress());
    assertEquals(reg.getClusterID(), reg2.getClusterID());
    assertEquals(reg.getCTime(), reg2.getCTime());
    assertEquals(reg.getHttpAddress(), reg2.getHttpAddress());
    assertEquals(reg.getLayoutVersion(), reg2.getLayoutVersion());
    assertEquals(reg.getNamespaceID(), reg2.getNamespaceID());
    assertEquals(reg.getRegistrationID(), reg2.getRegistrationID());
    assertEquals(reg.getRole(), reg2.getRole());
    assertEquals(reg.getVersion(), reg2.getVersion());

  }

  @Test
  public void testConvertBlock() {
    Block b = new Block(Block.generateBlockId(1), 100);
    BlockProto bProto = PBHelperClient.convert(b);
    Block b2 = PBHelperClient.convert(bProto);
    assertEquals(b, b2);
  }

  @Test
  public void testConvertCheckpointSignature() {
    CheckpointSignature s = new CheckpointSignature(
        getStorageInfo(NodeType.NAME_NODE), "bpid", 100, 1);
    CheckpointSignatureProto sProto = PBHelper.convert(s);
    CheckpointSignature s1 = PBHelper.convert(sProto);
    assertEquals(s.getBlockpoolID(), s1.getBlockpoolID());
    assertEquals(s.getClusterID(), s1.getClusterID());
    assertEquals(s.getCTime(), s1.getCTime());
    assertEquals(s.getCurSegmentTxId(), s1.getCurSegmentTxId());
    assertEquals(s.getLayoutVersion(), s1.getLayoutVersion());
    assertEquals(s.getMostRecentCheckpointTxId(),
        s1.getMostRecentCheckpointTxId());
    assertEquals(s.getNamespaceID(), s1.getNamespaceID());
  }
  
  private static void compare(RemoteEditLog l1, RemoteEditLog l2) {
    assertEquals(l1.getEndTxId(), l2.getEndTxId());
    assertEquals(l1.getStartTxId(), l2.getStartTxId());
  }
  
  @Test
  public void testConvertRemoteEditLog() {
    RemoteEditLog l = new RemoteEditLog(1, 100);
    RemoteEditLogProto lProto = PBHelper.convert(l);
    RemoteEditLog l1 = PBHelper.convert(lProto);
    compare(l, l1);
  }

  private void convertAndCheckRemoteEditLogManifest(RemoteEditLogManifest m,
                                                    List<RemoteEditLog> logs,
                                                    long committedTxnId) {
    RemoteEditLogManifestProto mProto = PBHelper.convert(m);
    RemoteEditLogManifest m1 = PBHelper.convert(mProto);

    List<RemoteEditLog> logs1 = m1.getLogs();
    assertEquals(logs.size(), logs1.size());
    for (int i = 0; i < logs.size(); i++) {
      compare(logs.get(i), logs1.get(i));
    }
    assertEquals(committedTxnId, m.getCommittedTxnId());
  }

  @Test
  public void testConvertRemoteEditLogManifest() {
    List<RemoteEditLog> logs = new ArrayList<RemoteEditLog>();
    logs.add(new RemoteEditLog(1, 10));
    logs.add(new RemoteEditLog(11, 20));

    convertAndCheckRemoteEditLogManifest(new RemoteEditLogManifest(logs, 20),
        logs, 20);
    convertAndCheckRemoteEditLogManifest(new RemoteEditLogManifest(logs),
        logs, HdfsServerConstants.INVALID_TXID);
  }

  public ExtendedBlock getExtendedBlock() {
    return getExtendedBlock(Block.generateBlockId(1));
  }

  public ExtendedBlock getExtendedBlock(byte[] blkid) {
    return new ExtendedBlock("bpid", blkid, 100);
  }
  
  private void compare(DatanodeInfo dn1, DatanodeInfo dn2) {
      assertEquals(dn1.getAdminState(), dn2.getAdminState());
      assertEquals(dn1.getBlockPoolUsed(), dn2.getBlockPoolUsed());
      assertEquals(dn1.getBlockPoolUsedPercent(), 
          dn2.getBlockPoolUsedPercent(), DELTA);
      assertEquals(dn1.getCapacity(), dn2.getCapacity());
      assertEquals(dn1.getDatanodeReport(), dn2.getDatanodeReport());
      assertEquals(dn1.getDfsUsed(), dn1.getDfsUsed());
      assertEquals(dn1.getDfsUsedPercent(), dn1.getDfsUsedPercent(), DELTA);
      assertEquals(dn1.getIpAddr(), dn2.getIpAddr());
      assertEquals(dn1.getHostName(), dn2.getHostName());
      assertEquals(dn1.getInfoPort(), dn2.getInfoPort());
      assertEquals(dn1.getIpcPort(), dn2.getIpcPort());
      assertEquals(dn1.getLastUpdate(), dn2.getLastUpdate());
      assertEquals(dn1.getLevel(), dn2.getLevel());
      assertEquals(dn1.getNetworkLocation(), dn2.getNetworkLocation());
  }
  
  @Test
  public void testConvertExtendedBlock() {
    ExtendedBlock b = getExtendedBlock();
    ExtendedBlockProto bProto = PBHelperClient.convert(b);
    ExtendedBlock b1 = PBHelperClient.convert(bProto);
    assertEquals(b, b1);
    
    b.setBlockId(Block.generateBlockId(-1));
    bProto = PBHelperClient.convert(b);
    b1 = PBHelperClient.convert(bProto);
    assertEquals(b, b1);
  }

  @Test
  public void testConvertText() {
    Text t = new Text("abc".getBytes());
    String s = t.toString();
    Text t1 = new Text(s);
    assertEquals(t, t1);
  }
  
  @Test
  public void testConvertBlockToken() {
    Token<BlockTokenIdentifier> token = new Token<BlockTokenIdentifier>(
        "identifier".getBytes(), "password".getBytes(), new Text("kind"),
        new Text("service"));
    TokenProto tokenProto = PBHelperClient.convert(token);
    Token<BlockTokenIdentifier> token2 = PBHelperClient.convert(tokenProto);
    compare(token, token2);
  }
  
  @Test
  public void testConvertNamespaceInfo() {
    NamespaceInfo info = new NamespaceInfo(37, "clusterID", "bpID", 2300);
    NamespaceInfoProto proto = PBHelper.convert(info);
    NamespaceInfo info2 = PBHelper.convert(proto);
    compare(info, info2); //Compare the StorageInfo
    assertEquals(info.getBlockPoolID(), info2.getBlockPoolID());
    assertEquals(info.getBuildVersion(), info2.getBuildVersion());
  }

  private void compare(StorageInfo expected, StorageInfo actual) {
    assertEquals(expected.clusterID, actual.clusterID);
    assertEquals(expected.namespaceID, actual.namespaceID);
    assertEquals(expected.cTime, actual.cTime);
    assertEquals(expected.layoutVersion, actual.layoutVersion);
  }

  private void compare(Token<BlockTokenIdentifier> expected,
      Token<BlockTokenIdentifier> actual) {
    assertTrue(Arrays.equals(expected.getIdentifier(), actual.getIdentifier()));
    assertTrue(Arrays.equals(expected.getPassword(), actual.getPassword()));
    assertEquals(expected.getKind(), actual.getKind());
    assertEquals(expected.getService(), actual.getService());
  }

  private void compare(LocatedBlock expected, LocatedBlock actual) {
    assertEquals(expected.getBlock(), actual.getBlock());
    assertEquals(expected.getStartOffset(), actual.getStartOffset());
  }

  private LocatedBlock createLocatedBlock() {
    LocatedBlock lb = new LocatedBlock(
        new ExtendedBlock("bp12", Block.generateBlockId(12345), 10), 5);
    return lb;
  }

  @Test
  public void testConvertLocatedBlock() {
    LocatedBlock lb = createLocatedBlock();
    LocatedBlockProto lbProto = PBHelperClient.convertLocatedBlock(lb);
    LocatedBlock lb2 = PBHelperClient.convertLocatedBlockProto(lbProto);
    compare(lb,lb2);
  }

  @Test
  public void testConvertLocatedBlockList() {
    ArrayList<LocatedBlock> lbl = new ArrayList<LocatedBlock>();
    for (int i=0;i<3;i++) {
      lbl.add(createLocatedBlock());
    }
    List<LocatedBlockProto> lbpl = PBHelperClient.convertLocatedBlocks2(lbl);
    List<LocatedBlock> lbl2 = PBHelperClient.convertLocatedBlocks(lbpl);
    assertEquals(lbl.size(), lbl2.size());
    for (int i=0;i<lbl.size();i++) {
      compare(lbl.get(i), lbl2.get(2));
    }
  }
  
  @Test
  public void testConvertLocatedBlockArray() {
    LocatedBlock [] lbl = new LocatedBlock[3];
    for (int i=0;i<3;i++) {
      lbl[i] = createLocatedBlock();
    }
    LocatedBlockProto [] lbpl = PBHelperClient.convertLocatedBlocks(lbl);
    LocatedBlock [] lbl2 = PBHelperClient.convertLocatedBlocks(lbpl);
    assertEquals(lbl.length, lbl2.length);
    for (int i=0;i<lbl.length;i++) {
      compare(lbl[i], lbl2[i]);
    }
  }

  @Test
  public void testChecksumTypeProto() {
    assertEquals(DataChecksum.Type.NULL,
        PBHelperClient.convert(HdfsProtos.ChecksumTypeProto.CHECKSUM_NULL));
    assertEquals(DataChecksum.Type.CRC32,
        PBHelperClient.convert(HdfsProtos.ChecksumTypeProto.CHECKSUM_CRC32));
    assertEquals(DataChecksum.Type.CRC32C,
        PBHelperClient.convert(HdfsProtos.ChecksumTypeProto.CHECKSUM_CRC32C));
    assertEquals(PBHelperClient.convert(DataChecksum.Type.NULL),
        HdfsProtos.ChecksumTypeProto.CHECKSUM_NULL);
    assertEquals(PBHelperClient.convert(DataChecksum.Type.CRC32),
        HdfsProtos.ChecksumTypeProto.CHECKSUM_CRC32);
    assertEquals(PBHelperClient.convert(DataChecksum.Type.CRC32C),
        HdfsProtos.ChecksumTypeProto.CHECKSUM_CRC32C);
  }

  @Test
  public void testAclEntryProto() {
    // All fields populated.
    AclEntry e1 = new AclEntry.Builder().setName("test")
        .setPermission(FsAction.READ_EXECUTE).setScope(AclEntryScope.DEFAULT)
        .setType(AclEntryType.OTHER).build();
    // No name.
    AclEntry e2 = new AclEntry.Builder().setScope(AclEntryScope.ACCESS)
        .setType(AclEntryType.USER).setPermission(FsAction.ALL).build();
    // No permission, which will default to the 0'th enum element.
    AclEntry e3 = new AclEntry.Builder().setScope(AclEntryScope.ACCESS)
        .setType(AclEntryType.USER).setName("test").build();
    AclEntry[] expected = new AclEntry[] { e1, e2,
        new AclEntry.Builder()
            .setScope(e3.getScope())
            .setType(e3.getType())
            .setName(e3.getName())
            .setPermission(FsAction.NONE)
            .build() };
    AclEntry[] actual = Lists.newArrayList(
        PBHelperClient.convertAclEntry(PBHelperClient.convertAclEntryProto(Lists
            .newArrayList(e1, e2, e3)))).toArray(new AclEntry[0]);
    Assert.assertArrayEquals(expected, actual);
  }

  @Test
  public void testAclStatusProto() {
    AclEntry e = new AclEntry.Builder().setName("test")
        .setPermission(FsAction.READ_EXECUTE).setScope(AclEntryScope.DEFAULT)
        .setType(AclEntryType.OTHER).build();
    AclStatus s = new AclStatus.Builder().owner("foo").group("bar").addEntry(e)
        .build();
    Assert.assertEquals(s, PBHelperClient.convert(PBHelperClient.convert(s)));
  }
  
  /**
   * Test case for old namenode where the namenode doesn't support returning
   * keyProviderUri.
   */
  @Test
  public void testFSServerDefaultsHelper() {
    HdfsProtos.FsServerDefaultsProto.Builder b =
        HdfsProtos.FsServerDefaultsProto
        .newBuilder();
    b.setBlockSize(DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
    b.setBytesPerChecksum(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT);
    b.setWritePacketSize(
        HdfsClientConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT);
    b.setReplication(DFSConfigKeys.DFS_REPLICATION_DEFAULT);
    b.setFileBufferSize(DFSConfigKeys.IO_FILE_BUFFER_SIZE_DEFAULT);
    b.setEncryptDataTransfer(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_DEFAULT);
    b.setTrashInterval(DFSConfigKeys.FS_TRASH_INTERVAL_DEFAULT);
    b.setChecksumType(HdfsProtos.ChecksumTypeProto.valueOf(
        DataChecksum.Type.valueOf(DFSConfigKeys.DFS_CHECKSUM_TYPE_DEFAULT).id));
    HdfsProtos.FsServerDefaultsProto proto = b.build();

    assertFalse("KeyProvider uri is not supported",
        proto.hasKeyProviderUri());
    FsServerDefaults fsServerDefaults = PBHelperClient.convert(proto);
    Assert.assertNotNull("FsServerDefaults is null", fsServerDefaults);
    Assert.assertNull("KeyProviderUri should be null",
        fsServerDefaults.getKeyProviderUri());
  }
}
