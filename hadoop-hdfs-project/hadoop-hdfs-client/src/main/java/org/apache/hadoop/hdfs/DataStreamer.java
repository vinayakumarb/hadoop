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

import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.SUCCESS;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.BlockWrite;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.util.ByteArrayManager;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Time;
import org.apache.htrace.core.Span;
import org.apache.htrace.core.SpanId;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

/*********************************************************************
 *
 * The DataStreamer class is responsible for sending data packets to the
 * datanodes in the pipeline. It retrieves a new blockid and block locations
 * from the namenode, and starts streaming packets to the pipeline of
 * Datanodes. Every packet has a sequence number associated with
 * it. When all the packets for a block are sent out and acks for each
 * if them are received, the DataStreamer closes the current block.
 *
 * The DataStreamer thread picks up packets from the dataQueue, sends it to
 * the first datanode in the pipeline and moves it from the dataQueue to the
 * ackQueue. The ResponseProcessor receives acks from the datanodes. When an
 * successful ack for a packet is received from all datanodes, the
 * ResponseProcessor removes the corresponding packet from the ackQueue.
 *
 * In case of error, all outstanding packets are moved from ackQueue. A new
 * pipeline is setup by eliminating the bad datanode from the original
 * pipeline. The DataStreamer now starts sending packets from the dataQueue.
 *
 *********************************************************************/

@InterfaceAudience.Private
class DataStreamer extends Daemon {
  static final Logger LOG = LoggerFactory.getLogger(DataStreamer.class);

  private class RefetchEncryptionKeyPolicy {
    private int fetchEncryptionKeyTimes = 0;
    private InvalidEncryptionKeyException lastException;
    private final DatanodeInfo src;

    RefetchEncryptionKeyPolicy(DatanodeInfo src) {
      this.src = src;
    }
    boolean continueRetryingOrThrow() throws InvalidEncryptionKeyException {
      if (fetchEncryptionKeyTimes >= 2) {
        // hit the same exception twice connecting to the node, so
        // throw the exception and exclude the node.
        throw lastException;
      }
      // Don't exclude this node just yet.
      // Try again with a new encryption key.
      LOG.info("Will fetch a new encryption key and retry, "
          + "encryption key was invalid when connecting to "
          + this.src + ": ", lastException);
      // The encryption key used is invalid.
      dfsClient.clearDataEncryptionKey();
      return true;
    }

    /**
     * Record a connection exception.
     */
    void recordFailure(final InvalidEncryptionKeyException e)
        throws InvalidEncryptionKeyException {
      fetchEncryptionKeyTimes++;
      lastException = e;
    }
  }

  private class StreamerStreams implements java.io.Closeable {
    private Socket sock = null;
    private DataOutputStream out = null;
    private DataInputStream in = null;

    StreamerStreams(final DatanodeInfo src,
        final long writeTimeout, final long readTimeout,
        final Token<BlockTokenIdentifier> blockToken)
        throws IOException {
      sock = createSocketForPipeline(src, 2, dfsClient);

      OutputStream unbufOut = NetUtils.getOutputStream(sock, writeTimeout);
      InputStream unbufIn = NetUtils.getInputStream(sock, readTimeout);
      IOStreamPair saslStreams = dfsClient.saslClient
          .socketSend(sock, unbufOut, unbufIn, dfsClient, blockToken, src);
      unbufOut = saslStreams.out;
      unbufIn = saslStreams.in;
      out = new DataOutputStream(new BufferedOutputStream(unbufOut,
          DFSUtilClient.getSmallBufferSize(dfsClient.getConfiguration())));
      in = new DataInputStream(unbufIn);
    }

    void sendTransferBlock(final DatanodeInfo[] targets,
        final StorageType[] targetStorageTypes,
        final String[] targetStorageIDs,
        final Token<BlockTokenIdentifier> blockToken) throws IOException {
      //send the TRANSFER_BLOCK request
      new Sender(out).transferBlock(block.getCurrentBlock(), blockToken,
          dfsClient.clientName, targets, targetStorageTypes,
          targetStorageIDs);
      out.flush();
      //ack
      BlockOpResponseProto transferResponse = BlockOpResponseProto
          .parseFrom(PBHelperClient.vintPrefixed(in));
      if (SUCCESS != transferResponse.getStatus()) {
        throw new IOException("Failed to add a datanode. Response status: "
            + transferResponse.getStatus());
      }
    }

    @Override
    public void close() throws IOException {
      IOUtils.closeStream(in);
      IOUtils.closeStream(out);
      IOUtils.closeSocket(sock);
    }
  }

  static class BlockToWrite {
    private ExtendedBlock currentBlock;

    BlockToWrite(ExtendedBlock block) {
      setCurrentBlock(block);
    }

    synchronized ExtendedBlock getCurrentBlock() {
      return currentBlock == null ? null : new ExtendedBlock(currentBlock);
    }

    synchronized long getNumBytes() {
      return currentBlock == null ? 0 : currentBlock.getNumBytes();
    }

    synchronized void setCurrentBlock(ExtendedBlock block) {
      currentBlock = (block == null || block.getLocalBlock() == null) ?
          null : new ExtendedBlock(block);
    }

    synchronized void setNumBytes(long numBytes) {
      assert currentBlock != null;
      currentBlock.setNumBytes(numBytes);
    }

    synchronized void setGenerationStamp(long generationStamp) {
      assert currentBlock != null;
      currentBlock.setGenerationStamp(generationStamp);
    }

    @Override
    public synchronized String toString() {
      return currentBlock == null ? "null" : currentBlock.toString();
    }
  }

  /**
   * Create a socket for a write pipeline
   *
   * @param first the first datanode
   * @param length the pipeline length
   * @param client client
   * @return the socket connected to the first datanode
   */
  static Socket createSocketForPipeline(final DatanodeInfo first,
      final int length, final DFSClient client) throws IOException {
    final DfsClientConf conf = client.getConf();
    final String dnAddr = first.getXferAddr(conf.isConnectToDnViaHostname());
    LOG.debug("Connecting to datanode {}", dnAddr);
    final InetSocketAddress isa = NetUtils.createSocketAddr(dnAddr);
    final Socket sock = client.socketFactory.createSocket();
    final int timeout = client.getDatanodeReadTimeout(length);
    NetUtils.connect(sock, isa, client.getRandomLocalInterfaceAddr(),
        conf.getSocketTimeout());
    sock.setTcpNoDelay(conf.getDataTransferTcpNoDelay());
    sock.setSoTimeout(timeout);
    sock.setKeepAlive(true);
    if (conf.getSocketSendBufferSize() > 0) {
      sock.setSendBufferSize(conf.getSocketSendBufferSize());
    }
    LOG.debug("Send buf size {}", sock.getSendBufferSize());
    return sock;
  }

  /**
   * if this file is lazy persist
   *
   * @param stat the HdfsFileStatus of a file
   * @return if this file is lazy persist
   */
  static boolean isLazyPersist(HdfsFileStatus stat) {
    return stat.getStoragePolicy() == HdfsConstants.MEMORY_STORAGE_POLICY_ID;
  }

  /**
   * release a list of packets to ByteArrayManager
   *
   * @param packets packets to be release
   * @param bam ByteArrayManager
   */
  private static void releaseBuffer(List<DFSPacket> packets, ByteArrayManager bam) {
    for(DFSPacket p : packets) {
      p.releaseBuffer(bam);
    }
    packets.clear();
  }

  class LastExceptionInStreamer extends ExceptionLastSeen {
    /**
     * Check if there already is an exception.
     */
    @Override
    synchronized void check(boolean resetToNull) throws IOException {
      final IOException thrown = get();
      if (thrown != null) {
        if (LOG.isTraceEnabled()) {
          // wrap and print the exception to know when the check is called
          LOG.trace("Got Exception while checking, " + DataStreamer.this,
              new Throwable(thrown));
        }
        super.check(resetToNull);
      }
    }
  }

  enum ErrorType {
    NONE, INTERNAL, EXTERNAL
  }

  static class ErrorState {
    ErrorType error = ErrorType.NONE;
    private int badNodeIndex = -1;
    private boolean waitForRestart = true;
    private int restartingNodeIndex = -1;
    private long restartingNodeDeadline = 0;
    private final long datanodeRestartTimeout;

    ErrorState(long datanodeRestartTimeout) {
      this.datanodeRestartTimeout = datanodeRestartTimeout;
    }

    synchronized void resetInternalError() {
      if (hasInternalError()) {
        error = ErrorType.NONE;
      }
      badNodeIndex = -1;
      restartingNodeIndex = -1;
      restartingNodeDeadline = 0;
      waitForRestart = true;
    }

    synchronized void reset() {
      error = ErrorType.NONE;
      badNodeIndex = -1;
      restartingNodeIndex = -1;
      restartingNodeDeadline = 0;
      waitForRestart = true;
    }

    synchronized boolean hasInternalError() {
      return error == ErrorType.INTERNAL;
    }

    synchronized boolean hasExternalError() {
      return error == ErrorType.EXTERNAL;
    }

    synchronized boolean hasError() {
      return error != ErrorType.NONE;
    }

    synchronized boolean hasDatanodeError() {
      return error == ErrorType.INTERNAL && isNodeMarked();
    }

    synchronized void setInternalError() {
      this.error = ErrorType.INTERNAL;
    }

    synchronized void setExternalError() {
      if (!hasInternalError()) {
        this.error = ErrorType.EXTERNAL;
      }
    }

    synchronized void setBadNodeIndex(int index) {
      this.badNodeIndex = index;
    }

    synchronized int getBadNodeIndex() {
      return badNodeIndex;
    }

    synchronized int getRestartingNodeIndex() {
      return restartingNodeIndex;
    }

    synchronized void initRestartingNode(int i, String message,
        boolean shouldWait) {
      restartingNodeIndex = i;
      if (shouldWait) {
        restartingNodeDeadline = Time.monotonicNow() + datanodeRestartTimeout;
        // If the data streamer has already set the primary node
        // bad, clear it. It is likely that the write failed due to
        // the DN shutdown. Even if it was a real failure, the pipeline
        // recovery will take care of it.
        badNodeIndex = -1;
      } else {
        this.waitForRestart = false;
      }
      LOG.info(message);
    }

    synchronized boolean isRestartingNode() {
      return restartingNodeIndex >= 0;
    }

    synchronized boolean isNodeMarked() {
      return badNodeIndex >= 0 || (isRestartingNode() && doWaitForRestart());
    }

    /**
     * This method is used when no explicit error report was received, but
     * something failed. The first node is a suspect or unsure about the cause
     * so that it is marked as failed.
     */
    synchronized void markFirstNodeIfNotMarked() {
      // There should be no existing error and no ongoing restart.
      if (!isNodeMarked()) {
        badNodeIndex = 0;
      }
    }

    synchronized void adjustState4RestartingNode() {
      // Just took care of a node error while waiting for a node restart
      if (restartingNodeIndex >= 0) {
        // If the error came from a node further away than the restarting
        // node, the restart must have been complete.
        if (badNodeIndex > restartingNodeIndex) {
          restartingNodeIndex = -1;
        } else if (badNodeIndex < restartingNodeIndex) {
          // the node index has shifted.
          restartingNodeIndex--;
        } else if (waitForRestart) {
          throw new IllegalStateException("badNodeIndex = " + badNodeIndex
              + " = restartingNodeIndex = " + restartingNodeIndex);
        }
      }

      if (!isRestartingNode()) {
        error = ErrorType.NONE;
      }
      badNodeIndex = -1;
    }

    synchronized void checkRestartingNodeDeadline(DatanodeInfo[] nodes) {
      if (restartingNodeIndex >= 0) {
        if (error == ErrorType.NONE) {
          throw new IllegalStateException("error=false while checking" +
              " restarting node deadline");
        }

        // check badNodeIndex
        if (badNodeIndex == restartingNodeIndex) {
          // ignore, if came from the restarting node
          badNodeIndex = -1;
        }
        // not within the deadline
        if (Time.monotonicNow() >= restartingNodeDeadline) {
          // expired. declare the restarting node dead
          restartingNodeDeadline = 0;
          final int i = restartingNodeIndex;
          restartingNodeIndex = -1;
          LOG.warn("Datanode " + i + " did not restart within "
              + datanodeRestartTimeout + "ms: " + nodes[i]);
          // Mark the restarting node as failed. If there is any other failed
          // node during the last pipeline construction attempt, it will not be
          // overwritten/dropped. In this case, the restarting node will get
          // excluded in the following attempt, if it still does not come up.
          if (badNodeIndex == -1) {
            badNodeIndex = i;
          }
        }
      }
    }

    boolean doWaitForRestart() {
      return waitForRestart;
    }
  }

  private volatile boolean streamerClosed = false;
  protected final BlockToWrite block; // its length is number of bytes acked
  private DataOutputStream blockStream;
  private final ErrorState errorState;

  private volatile BlockConstructionStage stage;  // block construction stage
  protected long bytesSent = 0; // number of bytes that've been sent
  private final boolean isLazyPersistFile;

  /** Has the current block been hflushed? */
  private boolean isHflushed = false;
  /** Append on an existing block? */
  private final boolean isAppend;

  private long currentSeqno = 0;
  private long lastQueuedSeqno = -1;
  private long lastAckedSeqno = -1;
  private long bytesCurBlock = 0; // bytes written in current block
  private final LastExceptionInStreamer lastException = new LastExceptionInStreamer();
  private Socket s;

  protected final DFSClient dfsClient;
  protected final String src;
  /** Only for DataTransferProtocol.writeBlock(..) */
  final DataChecksum checksum4WriteBlock;
  final Progressable progress;
  protected final HdfsFileStatus stat;
  // appending to existing partial block
  private volatile boolean appendChunk = false;
  // both dataQueue and ackQueue are protected by dataQueue lock
  protected final LinkedList<DFSPacket> dataQueue = new LinkedList<>();
  private final Map<Long, Long> packetSendTime = new HashMap<>();
  private final LinkedList<DFSPacket> ackQueue = new LinkedList<>();
  private final AtomicReference<CachingStrategy> cachingStrategy;
  private final ByteArrayManager byteArrayManager;
  //persist blocks on namenode
  private final AtomicBoolean persistBlocks = new AtomicBoolean(false);
  private boolean failPacket = false;
  private final long dfsclientSlowLogThresholdMs;
  private long artificialSlowdown = 0;
  private static final int CONGESTION_BACKOFF_MEAN_TIME_IN_MS = 5000;
  private static final int CONGESTION_BACK_OFF_MAX_TIME_IN_MS =
      CONGESTION_BACKOFF_MEAN_TIME_IN_MS * 10;
  private int lastCongestionBackoffTime;
  private final String[] favoredNodes;
  private final EnumSet<AddBlockFlag> addBlockFlags;

  private DataStreamer(HdfsFileStatus stat, ExtendedBlock block,
                       DFSClient dfsClient, String src,
                       Progressable progress, DataChecksum checksum,
                       AtomicReference<CachingStrategy> cachingStrategy,
                       ByteArrayManager byteArrayManage,
                       boolean isAppend, String[] favoredNodes,
                       EnumSet<AddBlockFlag> flags) {
    this.block = new BlockToWrite(block);
    this.dfsClient = dfsClient;
    this.src = src;
    this.progress = progress;
    this.stat = stat;
    this.checksum4WriteBlock = checksum;
    this.cachingStrategy = cachingStrategy;
    this.byteArrayManager = byteArrayManage;
    this.isLazyPersistFile = isLazyPersist(stat);
    this.isAppend = isAppend;
    this.favoredNodes = favoredNodes;
    final DfsClientConf conf = dfsClient.getConf();
    this.dfsclientSlowLogThresholdMs = conf.getSlowIoWarningThresholdMs();
    this.errorState = new ErrorState(conf.getDatanodeRestartTimeout());
    this.addBlockFlags = flags;
  }

  /**
   * construction with tracing info
   */
  DataStreamer(HdfsFileStatus stat, ExtendedBlock block, DFSClient dfsClient,
               String src, Progressable progress, DataChecksum checksum,
               AtomicReference<CachingStrategy> cachingStrategy,
               ByteArrayManager byteArrayManage, String[] favoredNodes,
               EnumSet<AddBlockFlag> flags) {
    this(stat, block, dfsClient, src, progress, checksum, cachingStrategy,
        byteArrayManage, false, favoredNodes, flags);
    stage = BlockConstructionStage.PIPELINE_SETUP_CREATE;
  }

  /**
   * Construct a data streamer for appending to the last partial block
   * @param lastBlock last block of the file to be appended
   * @param stat status of the file to be appended
   */
  DataStreamer(LocatedBlock lastBlock, HdfsFileStatus stat, DFSClient dfsClient,
               String src, Progressable progress, DataChecksum checksum,
               AtomicReference<CachingStrategy> cachingStrategy,
               ByteArrayManager byteArrayManage) {
    this(stat, lastBlock.getBlock(), dfsClient, src, progress, checksum, cachingStrategy,
        byteArrayManage, true, null, null);
    stage = BlockConstructionStage.PIPELINE_SETUP_APPEND;
    bytesSent = block.getNumBytes();
  }

  /**
   * Initialize for data streaming
   */
  private void initDataStreaming() {
    this.setName("DataStreamer for file " + src +
        " block " + block);
    stage = BlockConstructionStage.DATA_STREAMING;
  }

  protected void endBlock() {
    LOG.debug("Closing old block {}", block);
    this.setName("DataStreamer for file " + src);
    closeStream();
    stage = BlockConstructionStage.PIPELINE_SETUP_CREATE;
  }

  private boolean shouldStop() {
    return streamerClosed || errorState.hasError() || !dfsClient.clientRunning;
  }

  /*
   * streamer thread is the only thread that opens streams to datanode,
   * and closes them. Any error recovery is also done by this thread.
   */
  @Override
  public void run() {
    long lastPacket = Time.monotonicNow();
    TraceScope scope = null;
    while (!streamerClosed && dfsClient.clientRunning) {
      DFSPacket one;
      try {
        // process datanode IO errors if any
        boolean doSleep = processDatanodeOrExternalError();

        final int halfSocketTimeout = dfsClient.getConf().getSocketTimeout()/2;
        synchronized (dataQueue) {
          // wait for a packet to be sent.
          long now = Time.monotonicNow();
          while ((!shouldStop() && dataQueue.size() == 0 &&
              (stage != BlockConstructionStage.DATA_STREAMING ||
                  now - lastPacket < halfSocketTimeout)) || doSleep) {
            long timeout = halfSocketTimeout - (now-lastPacket);
            timeout = timeout <= 0 ? 1000 : timeout;
            timeout = (stage == BlockConstructionStage.DATA_STREAMING)?
                timeout : 1000;
            try {
              dataQueue.wait(timeout);
            } catch (InterruptedException  e) {
              LOG.warn("Caught exception", e);
            }
            doSleep = false;
            now = Time.monotonicNow();
          }
          if (shouldStop()) {
            continue;
          }
          // get packet to be sent.
          if (dataQueue.isEmpty()) {
            one = createHeartbeatPacket();
          } else {
            one = dataQueue.getFirst(); // regular data packet
            SpanId[] parents = one.getTraceParents();
            if (parents.length > 0) {
              scope = dfsClient.getTracer().
                  newScope("dataStreamer", parents[0]);
              scope.getSpan().setParents(parents);
            }
          }
        }

        // get new block from namenode.
        if (LOG.isDebugEnabled()) {
          LOG.debug("stage=" + stage + ", " + this);
        }
        if (stage == BlockConstructionStage.PIPELINE_SETUP_CREATE) {
          LOG.debug("Allocating new block: {}", this);
          nextBlockOutputStream();
          initDataStreaming();
        } else if (stage == BlockConstructionStage.PIPELINE_SETUP_APPEND) {
          LOG.debug("Append to block {}", block);
          setupPipelineForAppendOrRecovery();
          if (streamerClosed) {
            continue;
          }
          initDataStreaming();
        }

        long lastByteOffsetInBlock = one.getLastByteOffsetBlock();
        if (lastByteOffsetInBlock > stat.getBlockSize()) {
          throw new IOException("BlockSize " + stat.getBlockSize() +
              " < lastByteOffsetInBlock, " + this + ", " + one);
        }

        if (one.isLastPacketInBlock()) {
          // wait for all data packets have been successfully acked
          synchronized (dataQueue) {
            while (!shouldStop() && ackQueue.size() != 0) {
              try {
                // wait for acks to arrive from datanodes
                dataQueue.wait(1000);
              } catch (InterruptedException  e) {
                LOG.warn("Caught exception", e);
              }
            }
          }
          if (shouldStop()) {
            continue;
          }
          stage = BlockConstructionStage.PIPELINE_CLOSE;
        }

        // send the packet
        SpanId spanId = SpanId.INVALID;
        synchronized (dataQueue) {
          // move packet from dataQueue to ackQueue
          if (!one.isHeartbeatPacket()) {
            if (scope != null) {
              spanId = scope.getSpanId();
              scope.detach();
              one.setTraceScope(scope);
            }
            scope = null;
            dataQueue.removeFirst();
            ackQueue.addLast(one);
            packetSendTime.put(one.getSeqno(), Time.monotonicNow());
            dataQueue.notifyAll();
          }
        }

        LOG.debug("{} sending {}", this, one);

        // write out data to remote datanode
        try (TraceScope ignored = dfsClient.getTracer().
            newScope("DataStreamer#writeTo", spanId)) {
          one.writeTo(blockStream);
          blockStream.flush();
        } catch (IOException e) {
          // HDFS-3398 treat primary DN is down since client is unable to
          // write to primary DN. If a failed or restarting node has already
          // been recorded by the responder, the following call will have no
          // effect. Pipeline recovery can handle only one node error at a
          // time. If the primary node fails again during the recovery, it
          // will be taken out then.
          errorState.markFirstNodeIfNotMarked();
          throw e;
        }
        lastPacket = Time.monotonicNow();

        // update bytesSent
        long tmpBytesSent = one.getLastByteOffsetBlock();
        if (bytesSent < tmpBytesSent) {
          bytesSent = tmpBytesSent;
        }

        if (shouldStop()) {
          continue;
        }

        // Is this block full?
        if (one.isLastPacketInBlock()) {
          // wait for the close packet has been acked
          synchronized (dataQueue) {
            while (!shouldStop() && ackQueue.size() != 0) {
              dataQueue.wait(1000);// wait for acks to arrive from datanodes
            }
          }
          if (shouldStop()) {
            continue;
          }

          endBlock();
        }
        if (progress != null) { progress.progress(); }

        // This is used by unit test to trigger race conditions.
        if (artificialSlowdown != 0 && dfsClient.clientRunning) {
          Thread.sleep(artificialSlowdown);
        }
      } catch (Throwable e) {
        // Log warning if there was a real error.
        if (!errorState.isRestartingNode()) {
          // Since their messages are descriptive enough, do not always
          // log a verbose stack-trace WARN for quota exceptions.
          if (e instanceof QuotaExceededException) {
            LOG.debug("DataStreamer Quota Exception", e);
          } else {
            LOG.warn("DataStreamer Exception", e);
          }
        }
        lastException.set(e);
        assert !(e instanceof NullPointerException);
        errorState.setInternalError();
        if (!errorState.isNodeMarked()) {
          // Not a datanode issue
          streamerClosed = true;
        }
      } finally {
        if (scope != null) {
          scope.close();
          scope = null;
        }
      }
    }
    closeInternal();
  }

  private void closeInternal() {
    closeStream();
    streamerClosed = true;
    release();
    synchronized (dataQueue) {
      dataQueue.notifyAll();
    }
  }

  /**
   * release the DFSPackets in the two queues
   *
   */
  void release() {
    synchronized (dataQueue) {
      releaseBuffer(dataQueue, byteArrayManager);
      releaseBuffer(ackQueue, byteArrayManager);
    }
  }

  /**
   * wait for the ack of seqno
   *
   * @param seqno the sequence number to be acked
   * @throws IOException
   */
  void waitForAckedSeqno(long seqno) throws IOException {
    try (TraceScope ignored = dfsClient.getTracer().
        newScope("waitForAckedSeqno")) {
      LOG.debug("{} waiting for ack for: {}", this, seqno);
      long begin = Time.monotonicNow();
      try {
        synchronized (dataQueue) {
          while (!streamerClosed) {
            checkClosed();
            if (lastAckedSeqno >= seqno) {
              break;
            }
            try {
              dataQueue.wait(1000); // when we receive an ack, we notify on
              // dataQueue
            } catch (InterruptedException ie) {
              throw new InterruptedIOException(
                  "Interrupted while waiting for data to be acknowledged by pipeline");
            }
          }
        }
        checkClosed();
      } catch (ClosedChannelException cce) {
      }
      long duration = Time.monotonicNow() - begin;
      if (duration > dfsclientSlowLogThresholdMs) {
        LOG.warn("Slow waitForAckedSeqno took {}ms (threshold={}ms). File being"
                + " written: {}, block: {}",
            duration, dfsclientSlowLogThresholdMs, src, block);
      }
    }
  }

  /**
   * wait for space of dataQueue and queue the packet
   *
   * @param packet  the DFSPacket to be queued
   * @throws IOException
   */
  void waitAndQueuePacket(DFSPacket packet) throws IOException {
    synchronized (dataQueue) {
      try {
        // If queue is full, then wait till we have enough space
        boolean firstWait = true;
        try {
          while (!streamerClosed && dataQueue.size() + ackQueue.size() >
              dfsClient.getConf().getWriteMaxPackets()) {
            if (firstWait) {
              Span span = Tracer.getCurrentSpan();
              if (span != null) {
                span.addTimelineAnnotation("dataQueue.wait");
              }
              firstWait = false;
            }
            try {
              dataQueue.wait();
            } catch (InterruptedException e) {
              // If we get interrupted while waiting to queue data, we still need to get rid
              // of the current packet. This is because we have an invariant that if
              // currentPacket gets full, it will get queued before the next writeChunk.
              //
              // Rather than wait around for space in the queue, we should instead try to
              // return to the caller as soon as possible, even though we slightly overrun
              // the MAX_PACKETS length.
              Thread.currentThread().interrupt();
              break;
            }
          }
        } finally {
          Span span = Tracer.getCurrentSpan();
          if ((span != null) && (!firstWait)) {
            span.addTimelineAnnotation("end.wait");
          }
        }
        checkClosed();
        queuePacket(packet);
      } catch (ClosedChannelException ignored) {
      }
    }
  }

  /*
   * close the streamer, should be called only by an external thread
   * and only after all data to be sent has been flushed to datanode.
   *
   * Interrupt this data streamer if force is true
   *
   * @param force if this data stream is forced to be closed
   */
  void close(boolean force) {
    streamerClosed = true;
    synchronized (dataQueue) {
      dataQueue.notifyAll();
    }
    if (force) {
      this.interrupt();
    }
  }

  void setStreamerAsClosed() {
    streamerClosed = true;
  }

  private void checkClosed() throws IOException {
    if (streamerClosed) {
      lastException.throwException4Close();
    }
  }

  void closeStream() {
    final MultipleIOException.Builder b = new MultipleIOException.Builder();

    if (blockStream != null) {
      try {
        blockStream.close();
      } catch (IOException e) {
        b.add(e);
      } finally {
        blockStream = null;
      }
    }
    if (null != s) {
      try {
        s.close();
      } catch (IOException e) {
        b.add(e);
      } finally {
        s = null;
      }
    }

    final IOException ioe = b.build();
    if (ioe != null) {
      lastException.set(ioe);
    }
  }

  private boolean shouldHandleExternalError(){
    return errorState.hasExternalError() && blockStream != null;
  }

  /**
   * If this stream has encountered any errors, shutdown threads
   * and mark the stream as closed.
   *
   * @return true if it should sleep for a while after returning.
   */
  private boolean processDatanodeOrExternalError() throws IOException {
    if (!errorState.hasDatanodeError() && !shouldHandleExternalError()) {
      return false;
    }
    LOG.debug("start process datanode/external error, {}", this);
    closeStream();

    // move packets from ack queue to front of the data queue
    synchronized (dataQueue) {
      dataQueue.addAll(0, ackQueue);
      ackQueue.clear();
      packetSendTime.clear();
    }

    setupPipelineForAppendOrRecovery();

    if (!streamerClosed && dfsClient.clientRunning) {
      if (stage == BlockConstructionStage.PIPELINE_CLOSE) {

        // If we had an error while closing the pipeline, we go through a fast-path
        // where the BlockReceiver does not run. Instead, the DataNode just finalizes
        // the block immediately during the 'connect ack' process. So, we want to pull
        // the end-of-block packet from the dataQueue, since we don't actually have
        // a true pipeline to send it over.
        //
        // We also need to set lastAckedSeqno to the end-of-block Packet's seqno, so that
        // a client waiting on close() will be aware that the flush finished.
        synchronized (dataQueue) {
          DFSPacket endOfBlockPacket = dataQueue.remove();  // remove the end of block packet
          // Close any trace span associated with this Packet
          TraceScope scope = endOfBlockPacket.getTraceScope();
          if (scope != null) {
            scope.reattach();
            scope.close();
            endOfBlockPacket.setTraceScope(null);
          }
          assert endOfBlockPacket.isLastPacketInBlock();
          assert lastAckedSeqno == endOfBlockPacket.getSeqno() - 1;
          lastAckedSeqno = endOfBlockPacket.getSeqno();
          dataQueue.notifyAll();
        }
        endBlock();
      } else {
        initDataStreaming();
      }
    }

    return false;
  }

  void setHflush() {
    isHflushed = true;
  }

  private long computeTransferWriteTimeout() {
    return dfsClient.getDatanodeWriteTimeout(2);
  }
  private long computeTransferReadTimeout() {
    // transfer timeout multiplier based on the transfer size
    // One per 200 packets = 12.8MB. Minimum is 2.
    int multi = 2
        + (int) (bytesSent / dfsClient.getConf().getWritePacketSize()) / 200;
    return dfsClient.getDatanodeReadTimeout(multi);
  }

  private void transfer(final DatanodeInfo src, final DatanodeInfo[] targets,
                        final StorageType[] targetStorageTypes,
                        final String[] targetStorageIDs,
                        final Token<BlockTokenIdentifier> blockToken)
      throws IOException {
    //transfer replica to the new datanode
    RefetchEncryptionKeyPolicy policy = new RefetchEncryptionKeyPolicy(src);
    do {
      StreamerStreams streams = null;
      try {
        final long writeTimeout = computeTransferWriteTimeout();
        final long readTimeout = computeTransferReadTimeout();

        streams = new StreamerStreams(src, writeTimeout, readTimeout,
            blockToken);
        streams.sendTransferBlock(targets, targetStorageTypes,
            targetStorageIDs, blockToken);
        return;
      } catch (InvalidEncryptionKeyException e) {
        policy.recordFailure(e);
      } finally {
        IOUtils.closeStream(streams);
      }
    } while (policy.continueRetryingOrThrow());
  }

  /**
   * Open a DataStreamer to a DataNode pipeline so that
   * it can be written to.
   * This happens when a file is appended or data streaming fails
   * It keeps on trying until a pipeline is setup
   */
  private void setupPipelineForAppendOrRecovery() throws IOException {
    // Check number of datanodes. Note that if there is no healthy datanode,
    // this must be internal error because we mark external error in striped
    // outputstream only when all the streamers are in the DATA_STREAMING stage
    setupPipelineInternal();
  }

  protected void setupPipelineInternal() throws IOException {
    boolean success = false;
    long newGS = 0L;
    while (!success && !streamerClosed && dfsClient.clientRunning) {
      final boolean isRecovery = errorState.hasInternalError();
      // set up the pipeline again with the remaining nodes
      success = createBlockOutputStream(isRecovery);

      failPacket4Testing();
    } // while
  }

  void failPacket4Testing() {
    if (failPacket) { // for testing
      failPacket = false;
      try {
        // Give DNs time to send in bad reports. In real situations,
        // good reports should follow bad ones, if client committed
        // with those nodes.
        Thread.sleep(2000);
      } catch (InterruptedException ignored) {
      }
    }
  }

  void updateBlockGS(final long newGS) {
    block.setGenerationStamp(newGS);
  }

  /**
   * Open a DataStreamer to a DataNode so that it can be written to.
   * This happens when a file is created and each time a new block is allocated.
   * Must get block ID and the IDs of the destinations from the namenode.
   * Returns the list of target datanodes.
   */
  protected LocatedBlock nextBlockOutputStream() throws IOException {
    LocatedBlock lb;
    DatanodeInfo[] nodes;
    StorageType[] nextStorageTypes;
    String[] nextStorageIDs;
    int count = dfsClient.getConf().getNumBlockWriteRetry();
    boolean success;
    final ExtendedBlock oldBlock = block.getCurrentBlock();
    do {
      errorState.resetInternalError();
      lastException.clear();

      lb = locateFollowingBlock(oldBlock);
      block.setCurrentBlock(lb.getBlock());
      block.setNumBytes(0);
      bytesSent = 0;
      // Connect to first DataNode in the list.
      success = createBlockOutputStream(false);

      if (!success) {
        LOG.warn("Abandoning " + block);
        dfsClient.namenode.abandonBlock(block.getCurrentBlock(),
            stat.getFileId(), src, dfsClient.clientName);
        block.setCurrentBlock(null);
      }
    } while (!success && --count >= 0);

    if (!success) {
      throw new IOException("Unable to create new block.");
    }
    return lb;
  }

  // connects to the first datanode in the pipeline
  // Returns true if success, otherwise return failure.
  //
  boolean createBlockOutputStream(boolean recoveryFlag) {
     //TODO : create PlogWriter as a stream.
    return false;
  }

  private boolean[] getPinnings(DatanodeInfo[] nodes) {
    if (favoredNodes == null) {
      return null;
    } else {
      boolean[] pinnings = new boolean[nodes.length];
      HashSet<String> favoredSet = new HashSet<>(Arrays.asList(favoredNodes));
      for (int i = 0; i < nodes.length; i++) {
        pinnings[i] = favoredSet.remove(nodes[i].getXferAddrWithHostname());
        LOG.debug("{} was chosen by name node (favored={}).",
            nodes[i].getXferAddrWithHostname(), pinnings[i]);
      }
      if (!favoredSet.isEmpty()) {
        // There is one or more favored nodes that were not allocated.
        LOG.warn("These favored nodes were specified but not chosen: "
            + favoredSet + " Specified favored nodes: "
            + Arrays.toString(favoredNodes));

      }
      return pinnings;
    }
  }

  private LocatedBlock locateFollowingBlock(ExtendedBlock oldBlock) throws IOException {
    return DFSOutputStream.addBlock(dfsClient, src, oldBlock,
        stat.getFileId(), favoredNodes, addBlockFlags);
  }

  /**
   * get the block this streamer is writing to
   *
   * @return the block this streamer is writing to
   */
  ExtendedBlock getBlock() {
    return block.getCurrentBlock();
  }

  BlockConstructionStage getStage() {
    return stage;
  }

  ErrorState getErrorState() {
    return errorState;
  }

  /**
   * Put a packet to the data queue
   *
   * @param packet the packet to be put into the data queued
   */
  void queuePacket(DFSPacket packet) {
    synchronized (dataQueue) {
      if (packet == null) return;
      packet.addTraceParent(Tracer.getCurrentSpanId());
      dataQueue.addLast(packet);
      lastQueuedSeqno = packet.getSeqno();
      LOG.debug("Queued {}, {}", packet, this);
      dataQueue.notifyAll();
    }
  }

  /**
   * For heartbeat packets, create buffer directly by new byte[]
   * since heartbeats should not be blocked.
   */
  private DFSPacket createHeartbeatPacket() {
    final byte[] buf = new byte[PacketHeader.PKT_MAX_HEADER_LEN];
    return new DFSPacket(buf, 0, 0, DFSPacket.HEART_BEAT_SEQNO, 0, false);
  }

  private static LoadingCache<DatanodeInfo, DatanodeInfo> initExcludedNodes(
      long excludedNodesCacheExpiry) {
    return CacheBuilder.newBuilder()
        .expireAfterWrite(excludedNodesCacheExpiry, TimeUnit.MILLISECONDS)
        .removalListener(new RemovalListener<DatanodeInfo, DatanodeInfo>() {
          @Override
          public void onRemoval(
              @Nonnull RemovalNotification<DatanodeInfo, DatanodeInfo>
                  notification) {
            LOG.info("Removing node " + notification.getKey()
                + " from the excluded nodes list");
          }
        }).build(new CacheLoader<DatanodeInfo, DatanodeInfo>() {
          @Override
          public DatanodeInfo load(DatanodeInfo key) throws Exception {
            return key;
          }
        });
  }

  private static <T> void arraycopy(T[] srcs, T[] dsts, int skipIndex) {
    System.arraycopy(srcs, 0, dsts, 0, skipIndex);
    System.arraycopy(srcs, skipIndex+1, dsts, skipIndex, dsts.length-skipIndex);
  }

  /**
   * check if to persist blocks on namenode
   *
   * @return if to persist blocks on namenode
   */
  AtomicBoolean getPersistBlocks(){
    return persistBlocks;
  }

  /**
   * check if to append a chunk
   *
   * @param appendChunk if to append a chunk
   */
  void setAppendChunk(boolean appendChunk){
    this.appendChunk = appendChunk;
  }

  /**
   * get if to append a chunk
   *
   * @return if to append a chunk
   */
  boolean getAppendChunk(){
    return appendChunk;
  }

  /**
   * @return the last exception
   */
  LastExceptionInStreamer getLastException(){
    return lastException;
  }

  /**
   * set socket to null
   */
  void setSocketToNull() {
    this.s = null;
  }

  /**
   * return current sequence number and then increase it by 1
   *
   * @return current sequence number before increasing
   */
  long getAndIncCurrentSeqno() {
    long old = this.currentSeqno;
    this.currentSeqno++;
    return old;
  }

  /**
   * get last queued sequence number
   *
   * @return last queued sequence number
   */
  long getLastQueuedSeqno() {
    return lastQueuedSeqno;
  }

  /**
   * get the number of bytes of current block
   *
   * @return the number of bytes of current block
   */
  long getBytesCurBlock() {
    return bytesCurBlock;
  }

  /**
   * set the bytes of current block that have been written
   *
   * @param bytesCurBlock bytes of current block that have been written
   */
  void setBytesCurBlock(long bytesCurBlock) {
    this.bytesCurBlock = bytesCurBlock;
  }

  /**
   * increase bytes of current block by len.
   *
   * @param len how many bytes to increase to current block
   */
  void incBytesCurBlock(long len) {
    this.bytesCurBlock += len;
  }

  /**
   * set artificial slow down for unit test
   *
   * @param period artificial slow down
   */
  void setArtificialSlowdown(long period) {
    this.artificialSlowdown = period;
  }

  /**
   * if this streamer is to terminate
   *
   * @return if this streamer is to terminate
   */
  boolean streamerClosed(){
    return streamerClosed;
  }

  void closeSocket() throws IOException {
    if (s != null) {
      s.close();
    }
  }

  HdfsFileStatus getStat() {
    return stat;
  }

  @Override
  public String toString() {
    final ExtendedBlock extendedBlock = block.getCurrentBlock();
    return extendedBlock == null ?
        "block==null" : "" + extendedBlock.getLocalBlock();
  }
}
