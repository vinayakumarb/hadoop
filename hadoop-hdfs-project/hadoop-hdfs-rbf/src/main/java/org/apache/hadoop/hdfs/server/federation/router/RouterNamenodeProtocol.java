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
package org.apache.hadoop.hdfs.server.federation.router;

import java.io.IOException;

import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;

/**
 * Module that implements all the RPC calls in {@link NamenodeProtocol} in the
 * {@link RouterRpcServer}.
 */
public class RouterNamenodeProtocol implements NamenodeProtocol {

  /** RPC server to receive client calls. */
  private final RouterRpcServer rpcServer;
  /** RPC clients to connect to the Namenodes. */
  private final RouterRpcClient rpcClient;
  /** Interface to map global name space to HDFS subcluster name spaces. */
  private final FileSubclusterResolver subclusterResolver;


  public RouterNamenodeProtocol(RouterRpcServer server) {
    this.rpcServer = server;
    this.rpcClient =  this.rpcServer.getRPCClient();
    this.subclusterResolver = this.rpcServer.getSubclusterResolver();
  }

  @Override
  public long getTransactionID() throws IOException {
    rpcServer.checkOperation(OperationCategory.READ);

    // We return the information from the default name space
    String defaultNsId = subclusterResolver.getDefaultNamespace();
    RemoteMethod method =
        new RemoteMethod(NamenodeProtocol.class, "getTransactionID");
    return rpcClient.invokeSingle(defaultNsId, method, long.class);
  }

  @Override
  public long getMostRecentCheckpointTxId() throws IOException {
    rpcServer.checkOperation(OperationCategory.READ);

    // We return the information from the default name space
    String defaultNsId = subclusterResolver.getDefaultNamespace();
    RemoteMethod method =
        new RemoteMethod(NamenodeProtocol.class, "getMostRecentCheckpointTxId");
    return rpcClient.invokeSingle(defaultNsId, method, long.class);
  }

  @Override
  public CheckpointSignature rollEditLog() throws IOException {
    rpcServer.checkOperation(OperationCategory.WRITE, false);
    return null;
  }

  @Override
  public NamespaceInfo versionRequest() throws IOException {
    rpcServer.checkOperation(OperationCategory.READ);

    // We return the information from the default name space
    String defaultNsId = subclusterResolver.getDefaultNamespace();
    RemoteMethod method =
        new RemoteMethod(NamenodeProtocol.class, "versionRequest");
    return rpcClient.invokeSingle(defaultNsId, method, NamespaceInfo.class);
  }

  @Override
  public NamenodeCommand startCheckpoint(NamenodeRegistration registration)
      throws IOException {
    rpcServer.checkOperation(OperationCategory.WRITE, false);
    return null;
  }

  @Override
  public void endCheckpoint(NamenodeRegistration registration,
      CheckpointSignature sig) throws IOException {
    rpcServer.checkOperation(OperationCategory.WRITE, false);
  }

  @Override
  public RemoteEditLogManifest getEditLogManifest(long sinceTxId)
      throws IOException {
    rpcServer.checkOperation(OperationCategory.READ, false);
    return null;
  }

  @Override
  public boolean isUpgradeFinalized() throws IOException {
    rpcServer.checkOperation(OperationCategory.READ, false);
    return false;
  }

  @Override
  public boolean isRollingUpgrade() throws IOException {
    rpcServer.checkOperation(OperationCategory.READ, false);
    return false;
  }
}
