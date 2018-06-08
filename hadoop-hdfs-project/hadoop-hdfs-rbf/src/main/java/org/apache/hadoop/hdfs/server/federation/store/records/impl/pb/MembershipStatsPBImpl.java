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
package org.apache.hadoop.hdfs.server.federation.store.records.impl.pb;

import java.io.IOException;

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.NamenodeMembershipStatsRecordProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.NamenodeMembershipStatsRecordProto.Builder;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.NamenodeMembershipStatsRecordProtoOrBuilder;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.FederationProtocolPBTranslator;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipStats;

import com.google.protobuf.Message;

/**
 * Protobuf implementation of the MembershipStats record.
 */
public class MembershipStatsPBImpl extends MembershipStats
    implements PBRecord {

  private FederationProtocolPBTranslator<NamenodeMembershipStatsRecordProto,
      Builder, NamenodeMembershipStatsRecordProtoOrBuilder> translator =
          new FederationProtocolPBTranslator<NamenodeMembershipStatsRecordProto,
          Builder, NamenodeMembershipStatsRecordProtoOrBuilder>(
              NamenodeMembershipStatsRecordProto.class);

  public MembershipStatsPBImpl() {
  }

  @Override
  public NamenodeMembershipStatsRecordProto getProto() {
    return this.translator.build();
  }

  @Override
  public void setProto(Message proto) {
    this.translator.setProto(proto);
  }

  @Override
  public void readInstance(String base64String) throws IOException {
    this.translator.readInstance(base64String);
  }

  @Override
  public void setTotalSpace(long space) {
    this.translator.getBuilder().setTotalSpace(space);
  }

  @Override
  public long getTotalSpace() {
    return this.translator.getProtoOrBuilder().getTotalSpace();
  }

  @Override
  public void setAvailableSpace(long space) {
    this.translator.getBuilder().setAvailableSpace(space);
  }

  @Override
  public long getAvailableSpace() {
    return this.translator.getProtoOrBuilder().getAvailableSpace();
  }

  @Override
  public void setNumOfFiles(long files) {
    this.translator.getBuilder().setNumOfFiles(files);
  }

  @Override
  public long getNumOfFiles() {
    return this.translator.getProtoOrBuilder().getNumOfFiles();
  }

  @Override
  public void setNumOfBlocks(long blocks) {
    this.translator.getBuilder().setNumOfBlocks(blocks);
  }

  @Override
  public long getNumOfBlocks() {
    return this.translator.getProtoOrBuilder().getNumOfBlocks();
  }

  @Override
  public void setNumOfBlocksPendingDeletion(long blocks) {
    this.translator.getBuilder().setNumOfBlocksPendingDeletion(blocks);
  }

  @Override
  public long getNumOfBlocksPendingDeletion() {
    return this.translator.getProtoOrBuilder().getNumOfBlocksPendingDeletion();
  }
}