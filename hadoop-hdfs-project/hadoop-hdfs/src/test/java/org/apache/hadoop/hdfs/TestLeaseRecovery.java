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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.After;
import org.junit.Test;

public class TestLeaseRecovery {
  static final int BLOCK_SIZE = 1024;
  static final short REPLICATION_NUM = (short)3;
  private static final long LEASE_PERIOD = 300L;

  private MiniDFSCluster cluster;

  @After
  public void shutdown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  static int min(Integer... x) {
    int m = x[0];
    for(int i = 1; i < x.length; i++) {
      if (x[i] < m) {
        m = x[i];
      }
    }
    return m;
  }

  void waitLeaseRecovery(MiniDFSCluster cluster) {
    cluster.setLeasePeriod(LEASE_PERIOD, LEASE_PERIOD);
    // wait for the lease to expire
    try {
      Thread.sleep(2 * 3000);  // 2 heartbeat intervals
    } catch (InterruptedException e) {
    }
  }

  /**
   * Block/lease recovery should be retried with failed nodes from the second
   * stage removed to avoid perpetual recovery failures.
   */
  @Test
  public void testBlockRecoveryRetryAfterFailedRecovery() throws Exception {
    Configuration conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).build();
    Path file = new Path("/testBlockRecoveryRetryAfterFailedRecovery");
    DistributedFileSystem dfs = cluster.getFileSystem();

    // Create a file.
    FSDataOutputStream out = dfs.create(file);
    final int FILE_SIZE = 128 * 1024;
    int count = 0;
    while (count < FILE_SIZE) {
      out.writeBytes("DE K9SUL");
      count += 8;
    }
    out.hsync();

    // Abort the original stream.
    ((DFSOutputStream) out.getWrappedStream()).abort();

    LocatedBlocks locations = cluster.getNameNodeRpc().getBlockLocations(
        file.toString(), 0, count);
    ExtendedBlock block = locations.get(0).getBlock();

    // Try to recover the lease.
    DistributedFileSystem newDfs = (DistributedFileSystem) FileSystem
        .newInstance(cluster.getConfiguration(0));
    count = 0;
    while (count++ < 15 && !newDfs.recoverLease(file)) {
      Thread.sleep(1000);
    }
    // The lease should have been recovered.
    assertTrue("File should be closed", newDfs.recoverLease(file));
  }

  /**
   * Recover the lease on a file and append file from another client.
   */
  @Test
  public void testLeaseRecoveryAndAppend() throws Exception {
    Configuration conf = new Configuration();
    try{
    cluster = new MiniDFSCluster.Builder(conf).build();
    Path file = new Path("/testLeaseRecovery");
    DistributedFileSystem dfs = cluster.getFileSystem();

    // create a file with 0 bytes
    FSDataOutputStream out = dfs.create(file);
    out.hflush();
    out.hsync();

    // abort the original stream
    ((DFSOutputStream) out.getWrappedStream()).abort();
    DistributedFileSystem newdfs =
        (DistributedFileSystem) FileSystem.newInstance
        (cluster.getConfiguration(0));

    // Append to a file , whose lease is held by another client should fail
    try {
        newdfs.append(file);
        fail("Append to a file(lease is held by another client) should fail");
    } catch (RemoteException e) {
      assertTrue(e.getMessage().contains("file lease is currently owned"));
    }

    // Lease recovery on first try should be successful
    boolean recoverLease = newdfs.recoverLease(file);
    assertTrue(recoverLease);
    FSDataOutputStream append = newdfs.append(file);
    append.write("test".getBytes());
    append.close();
    }finally{
      if (cluster != null) {
        cluster.shutdown();
        cluster = null;
      }
    }
  }
}
