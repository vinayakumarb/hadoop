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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDebugAdmin {

  static private final String TEST_ROOT_DIR =
      new File(System.getProperty("test.build.data", "/tmp"),
          TestDebugAdmin.class.getSimpleName()).getAbsolutePath();

  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private DebugAdmin admin;

  @Before
  public void setUp() throws Exception {
    final File testRoot = new File(TEST_ROOT_DIR);
    testRoot.delete();
    testRoot.mkdirs();
    Configuration conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    admin = new DebugAdmin(conf);
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private String runCmd(String[] cmd) throws Exception {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final PrintStream out = new PrintStream(bytes);
    final PrintStream oldErr = System.err;
    final PrintStream oldOut = System.out;
    System.setErr(out);
    System.setOut(out);
    int ret;
    try {
      ret = admin.run(cmd);
    } finally {
      System.setErr(oldErr);
      System.setOut(oldOut);
      IOUtils.closeStream(out);
    }
    return "ret: " + ret + ", " +
        bytes.toString().replaceAll(System.lineSeparator(), "");
  }

  @Test(timeout = 60000)
  public void testRecoverLease() throws Exception {
    assertEquals("ret: 1, You must supply a -path argument to recoverLease.",
        runCmd(new String[]{"recoverLease", "-retries", "1"}));
    FSDataOutputStream out = fs.create(new Path("/foo"));
    out.write(123);
    out.close();
    assertEquals("ret: 0, recoverLease SUCCEEDED on /foo",
        runCmd(new String[]{"recoverLease", "-path", "/foo"}));
  }

  @Test(timeout = 60000)
  public void testRecoverLeaseforFileNotFound() throws Exception {
    assertTrue(runCmd(new String[] {
        "recoverLease", "-path", "/foo", "-retries", "2" }).contains(
        "Giving up on recoverLease for /foo after 1 try"));
  }
}
