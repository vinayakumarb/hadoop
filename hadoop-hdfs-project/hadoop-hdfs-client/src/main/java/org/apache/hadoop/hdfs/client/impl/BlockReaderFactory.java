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
package org.apache.hadoop.hdfs.client.impl;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.ClientContext;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.RemotePeerFactory;
import org.apache.hadoop.hdfs.ReplicaAccessor;
import org.apache.hadoop.hdfs.ReplicaAccessorBuilder;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf.ShortCircuitConf;
import org.apache.hadoop.hdfs.net.DomainPeer;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.shortcircuit.DomainSocketFactory;
import org.apache.hadoop.hdfs.util.IOUtilsClient;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.PerformanceAdvisory;
import org.apache.htrace.core.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;


/**
 * Utility class to create BlockReader implementations.
 */
@InterfaceAudience.Private
public class BlockReaderFactory {
  static final Logger LOG = LoggerFactory.getLogger(BlockReaderFactory.class);

  public static class FailureInjector {
    public void injectRequestFileDescriptorsFailure() throws IOException {
      // do nothing
    }
    public boolean getSupportsReceiptVerification() {
      return true;
    }
  }

  private final DfsClientConf conf;

  /**
   * Injects failures into specific operations during unit tests.
   */
  private static FailureInjector failureInjector = new FailureInjector();

  /**
   * The file name, for logging and debugging purposes.
   */
  private String fileName;

  /**
   * The block ID and block pool ID to use.
   */
  private ExtendedBlock block;

  /**
   * The block token to use for security purposes.
   */
  private Token<BlockTokenIdentifier> token;

  /**
   * The offset within the block to start reading at.
   */
  private long startOffset;

  /**
   * If false, we won't try to verify the block checksum.
   */
  private boolean verifyChecksum;

  /**
   * The name of this client.
   */
  private String clientName;

  /**
   * The DataNode we're talking to.
   */
  private DatanodeInfo datanode;

  /**
   * StorageType of replica on DataNode.
   */
  private StorageType storageType;

  /**
   * If false, we won't try short-circuit local reads.
   */
  private boolean allowShortCircuitLocalReads;

  /**
   * The ClientContext to use for things like the PeerCache.
   */
  private ClientContext clientContext;

  /**
   * Number of bytes to read. Must be set to a non-negative value.
   */
  private long length = -1;

  /**
   * Socket address to use to connect to peer.
   */
  private InetSocketAddress inetSocketAddress;

  /**
   * Remote peer factory to use to create a peer, if needed.
   */
  private RemotePeerFactory remotePeerFactory;

  /**
   * UserGroupInformation to use for legacy block reader local objects,
   * if needed.
   */
  private UserGroupInformation userGroupInformation;

  /**
   * Configuration to use for legacy block reader local objects, if needed.
   */
  private Configuration configuration;

  /**
   * The HTrace tracer to use.
   */
  private Tracer tracer;
  /**
   * Information about the domain socket path we should use to connect to the
   * local peer-- or null if we haven't examined the local domain socket.
   */
  private DomainSocketFactory.PathInfo pathInfo;

  /**
   * The remaining number of times that we'll try to pull a socket out of the
   * cache.
   */
  private int remainingCacheTries;

  public BlockReaderFactory(DfsClientConf conf) {
    this.conf = conf;
    this.remainingCacheTries = conf.getNumCachedConnRetry();
  }

  public BlockReaderFactory setFileName(String fileName) {
    this.fileName = fileName;
    return this;
  }

  public BlockReaderFactory setBlock(ExtendedBlock block) {
    this.block = block;
    return this;
  }

  public BlockReaderFactory setBlockToken(Token<BlockTokenIdentifier> token) {
    this.token = token;
    return this;
  }

  public BlockReaderFactory setStartOffset(long startOffset) {
    this.startOffset = startOffset;
    return this;
  }

  public BlockReaderFactory setVerifyChecksum(boolean verifyChecksum) {
    this.verifyChecksum = verifyChecksum;
    return this;
  }

  public BlockReaderFactory setClientName(String clientName) {
    this.clientName = clientName;
    return this;
  }

  public BlockReaderFactory setDatanodeInfo(DatanodeInfo datanode) {
    this.datanode = datanode;
    return this;
  }

  public BlockReaderFactory setStorageType(StorageType storageType) {
    this.storageType = storageType;
    return this;
  }

  public BlockReaderFactory setAllowShortCircuitLocalReads(
      boolean allowShortCircuitLocalReads) {
    this.allowShortCircuitLocalReads = allowShortCircuitLocalReads;
    return this;
  }

  public BlockReaderFactory setClientCacheContext(
      ClientContext clientContext) {
    this.clientContext = clientContext;
    return this;
  }

  public BlockReaderFactory setLength(long length) {
    this.length = length;
    return this;
  }

  public BlockReaderFactory setInetSocketAddress (
      InetSocketAddress inetSocketAddress) {
    this.inetSocketAddress = inetSocketAddress;
    return this;
  }

  public BlockReaderFactory setUserGroupInformation(
      UserGroupInformation userGroupInformation) {
    this.userGroupInformation = userGroupInformation;
    return this;
  }

  public BlockReaderFactory setRemotePeerFactory(
      RemotePeerFactory remotePeerFactory) {
    this.remotePeerFactory = remotePeerFactory;
    return this;
  }

  public BlockReaderFactory setConfiguration(
      Configuration configuration) {
    this.configuration = configuration;
    return this;
  }

  public BlockReaderFactory setTracer(Tracer tracer) {
    this.tracer = tracer;
    return this;
  }

  @VisibleForTesting
  public static void setFailureInjectorForTesting(FailureInjector injector) {
    failureInjector = injector;
  }

  /**
   * Build a BlockReader with the given options.
   *
   * This function will do the best it can to create a block reader that meets
   * all of our requirements.  We prefer short-circuit block readers
   * (BlockReaderLocal and BlockReaderLocalLegacy) over remote ones, since the
   * former avoid the overhead of socket communication.  If short-circuit is
   * unavailable, our next fallback is data transfer over UNIX domain sockets,
   * if dfs.client.domain.socket.data.traffic has been enabled.  If that doesn't
   * work, we will try to create a remote block reader that operates over TCP
   * sockets.
   *
   * There are a few caches that are important here.
   *
   * The ShortCircuitCache stores file descriptor objects which have been passed
   * from the DataNode.
   *
   * The DomainSocketFactory stores information about UNIX domain socket paths
   * that we not been able to use in the past, so that we don't waste time
   * retrying them over and over.  (Like all the caches, it does have a timeout,
   * though.)
   *
   * The PeerCache stores peers that we have used in the past.  If we can reuse
   * one of these peers, we avoid the overhead of re-opening a socket.  However,
   * if the socket has been timed out on the remote end, our attempt to reuse
   * the socket may end with an IOException.  For that reason, we limit our
   * attempts at socket reuse to dfs.client.cached.conn.retry times.  After
   * that, we create new sockets.  This avoids the problem where a thread tries
   * to talk to a peer that it hasn't talked to in a while, and has to clean out
   * every entry in a socket cache full of stale entries.
   *
   * @return The new BlockReader.  We will not return null.
   *
   * @throws InvalidToken
   *             If the block token was invalid.
   *         InvalidEncryptionKeyException
   *             If the encryption key was invalid.
   *         Other IOException
   *             If there was another problem.
   */
  public BlockReader build() throws IOException {
    Preconditions.checkNotNull(configuration);
    Preconditions
        .checkState(length >= 0, "Length must be set to a non-negative value");
    BlockReader reader = tryToCreateExternalBlockReader();
    if (reader != null) {
      return reader;
    }
    final ShortCircuitConf scConf = conf.getShortCircuitConf();
    try {
      if (scConf.isDomainSocketDataTraffic()) {
        reader = getRemoteBlockReaderFromDomain();
        if (reader != null) {
          LOG.trace("{}: returning new remote block reader using UNIX domain "
              + "socket on {}", this, pathInfo.getPath());
          return reader;
        }
      }
    } catch (IOException e) {
      LOG.debug("Block read failed. Getting remote block reader using TCP", e);
    }
    Preconditions.checkState(!DFSInputStream.tcpReadsDisabledForTesting,
        "TCP reads were disabled for testing, but we failed to " +
        "do a non-TCP read.");
    return getRemoteBlockReaderFromTcp();
  }

  private BlockReader tryToCreateExternalBlockReader() {
    List<Class<? extends ReplicaAccessorBuilder>> clses =
        conf.getReplicaAccessorBuilderClasses();
    for (Class<? extends ReplicaAccessorBuilder> cls : clses) {
      try {
        ByteArrayDataOutput bado = ByteStreams.newDataOutput();
        token.write(bado);
        byte tokenBytes[] = bado.toByteArray();

        Constructor<? extends ReplicaAccessorBuilder> ctor =
            cls.getConstructor();
        ReplicaAccessorBuilder builder = ctor.newInstance();
        long visibleLength = startOffset + length;
        ReplicaAccessor accessor = builder.
            setBlock(block.getBlockId(), block.getBlockPoolId()).
            setBlockAccessToken(tokenBytes).
            setClientName(clientName).
            setConfiguration(configuration).
            setFileName(fileName).
            setVerifyChecksum(verifyChecksum).
            setVisibleLength(visibleLength).
            build();
        if (accessor == null) {
          LOG.trace("{}: No ReplicaAccessor created by {}",
              this, cls.getName());
        } else {
          return new ExternalBlockReader(accessor, visibleLength, startOffset);
        }
      } catch (Throwable t) {
        LOG.warn("Failed to construct new object of type " +
            cls.getName(), t);
      }
    }
    return null;
  }

  /**
   * Get a BlockReaderRemote that communicates over a UNIX domain socket.
   *
   * @return The new BlockReader, or null if we failed to create the block
   * reader.
   *
   * @throws InvalidToken    If the block token was invalid.
   * Potentially other security-related execptions.
   */
  private BlockReader getRemoteBlockReaderFromDomain() throws IOException {
    if (pathInfo == null) {
      pathInfo = clientContext.getDomainSocketFactory()
          .getPathInfo(inetSocketAddress, conf.getShortCircuitConf());
    }
    if (!pathInfo.getPathState().getUsableForDataTransfer()) {
      PerformanceAdvisory.LOG.debug("{}: not trying to create a " +
          "remote block reader because the UNIX domain socket at {}" +
           " is not usable.", this, pathInfo);
      return null;
    }
    LOG.trace("{}: trying to create a remote block reader from the UNIX domain "
        + "socket at {}", this, pathInfo.getPath());

    while (true) {
      BlockReaderPeer curPeer = nextDomainPeer();
      if (curPeer == null) break;
      if (curPeer.fromCache) remainingCacheTries--;
      DomainPeer peer = (DomainPeer)curPeer.peer;
      BlockReader blockReader = null;
      try {
        blockReader = getRemoteBlockReader(peer);
        return blockReader;
      } catch (IOException ioe) {
        IOUtilsClient.cleanup(LOG, peer);
        if (isSecurityException(ioe)) {
          LOG.trace("{}: got security exception while constructing a remote "
                  + " block reader from the unix domain socket at {}",
              this, pathInfo.getPath(), ioe);
          throw ioe;
        }
        if (curPeer.fromCache) {
          // Handle an I/O error we got when using a cached peer.  These are
          // considered less serious because the underlying socket may be stale.
          LOG.debug("Closed potentially stale domain peer {}", peer, ioe);
        } else {
          // Handle an I/O error we got when using a newly created domain peer.
          // We temporarily disable the domain socket path for a few minutes in
          // this case, to prevent wasting more time on it.
          LOG.warn("I/O error constructing remote block reader.  Disabling " +
              "domain socket " + peer.getDomainSocket(), ioe);
          clientContext.getDomainSocketFactory()
              .disableDomainSocketPath(pathInfo.getPath());
          return null;
        }
      } finally {
        if (blockReader == null) {
          IOUtilsClient.cleanup(LOG, peer);
        }
      }
    }
    return null;
  }

  /**
   * Get a BlockReaderRemote that communicates over a TCP socket.
   *
   * @return The new BlockReader.  We will not return null, but instead throw
   *         an exception if this fails.
   *
   * @throws InvalidToken
   *             If the block token was invalid.
   *         InvalidEncryptionKeyException
   *             If the encryption key was invalid.
   *         Other IOException
   *             If there was another problem.
   */
  private BlockReader getRemoteBlockReaderFromTcp() throws IOException {
    LOG.trace("{}: trying to create a remote block reader from a TCP socket",
        this);
    BlockReader blockReader = null;
    while (true) {
      BlockReaderPeer curPeer = null;
      Peer peer = null;
      try {
        curPeer = nextTcpPeer();
        if (curPeer.fromCache) remainingCacheTries--;
        peer = curPeer.peer;
        blockReader = getRemoteBlockReader(peer);
        return blockReader;
      } catch (IOException ioe) {
        if (isSecurityException(ioe)) {
          LOG.trace("{}: got security exception while constructing a remote "
              + "block reader from {}", this, peer, ioe);
          throw ioe;
        }
        if ((curPeer != null) && curPeer.fromCache) {
          // Handle an I/O error we got when using a cached peer.  These are
          // considered less serious, because the underlying socket may be
          // stale.
          LOG.debug("Closed potentially stale remote peer {}", peer, ioe);
        } else {
          // Handle an I/O error we got when using a newly created peer.
          LOG.warn("I/O error constructing remote block reader.", ioe);
          throw ioe;
        }
      } finally {
        if (blockReader == null) {
          IOUtilsClient.cleanup(LOG, peer);
        }
      }
    }
  }

  public static class BlockReaderPeer {
    final Peer peer;
    final boolean fromCache;

    BlockReaderPeer(Peer peer, boolean fromCache) {
      this.peer = peer;
      this.fromCache = fromCache;
    }
  }

  /**
   * Get the next DomainPeer-- either from the cache or by creating it.
   *
   * @return the next DomainPeer, or null if we could not construct one.
   */
  private BlockReaderPeer nextDomainPeer() {
    if (remainingCacheTries > 0) {
      Peer peer = clientContext.getPeerCache().get(datanode, true);
      if (peer != null) {
        LOG.trace("nextDomainPeer: reusing existing peer {}", peer);
        return new BlockReaderPeer(peer, true);
      }
    }
    DomainSocket sock = clientContext.getDomainSocketFactory().
        createSocket(pathInfo, conf.getSocketTimeout());
    if (sock == null) return null;
    return new BlockReaderPeer(new DomainPeer(sock), false);
  }

  /**
   * Get the next TCP-based peer-- either from the cache or by creating it.
   *
   * @return the next Peer, or null if we could not construct one.
   *
   * @throws IOException  If there was an error while constructing the peer
   *                      (such as an InvalidEncryptionKeyException)
   */
  private BlockReaderPeer nextTcpPeer() throws IOException {
    if (remainingCacheTries > 0) {
      Peer peer = clientContext.getPeerCache().get(datanode, false);
      if (peer != null) {
        LOG.trace("nextTcpPeer: reusing existing peer {}", peer);
        return new BlockReaderPeer(peer, true);
      }
    }
    try {
      Peer peer = remotePeerFactory.newConnectedPeer(inetSocketAddress, token,
          datanode);
      LOG.trace("nextTcpPeer: created newConnectedPeer {}", peer);
      return new BlockReaderPeer(peer, false);
    } catch (IOException e) {
      LOG.trace("nextTcpPeer: failed to create newConnectedPeer connected to"
          + "{}", datanode);
      throw e;
    }
  }

  /**
   * Determine if an exception is security-related.
   *
   * We need to handle these exceptions differently than other IOExceptions.
   * They don't indicate a communication problem.  Instead, they mean that there
   * is some action the client needs to take, such as refetching block tokens,
   * renewing encryption keys, etc.
   *
   * @param ioe    The exception
   * @return       True only if the exception is security-related.
   */
  private static boolean isSecurityException(IOException ioe) {
    return (ioe instanceof InvalidToken) ||
            (ioe instanceof InvalidEncryptionKeyException) ||
            (ioe instanceof InvalidBlockTokenException) ||
            (ioe instanceof AccessControlException);
  }

  @SuppressWarnings("deprecation")
  private BlockReader getRemoteBlockReader(Peer peer) throws IOException {
    return null;
  }

  @Override
  public String toString() {
    return "BlockReaderFactory(fileName=" + fileName + ", block=" + block + ")";
  }

  /**
   * File name to print when accessing a block directly (from servlets)
   * @param s Address of the block location
   * @param poolId Block pool ID of the block
   * @param blockId Block ID of the block
   * @return string that has a file name for debug purposes
   */
  public static String getFileName(final InetSocketAddress s,
      final String poolId, final long blockId) {
    return s.toString() + ":" + poolId + ":" + blockId;
  }
}
