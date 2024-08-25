package org.elasticsearch.indices.segmentscopy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.*;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.RecoveryEngineException;
import org.elasticsearch.index.engine.SegmentsCopyInfo;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.indices.recovery.*;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

public class RemoteTargetShardCopyState implements TargetShardCopyState{
    private static final Logger logger = LogManager.getLogger(RemoteTargetShardCopyState.class);
    public final ShardId shardId;
    public  IndexId indexId;
    public final TransportService transportService;
    public final ThreadPool threadPool;
    public ShardRouting replicaRouting;
    private  int chunkSizeInBytes;
    private  int maxConcurrentFileChunks = 4;
    private  long timeout;
    private  long internalActionTimeout;
    private final DiscoveryNode localNode;
    private final DiscoveryNode targetNode;
    private final Map<Object, RetryableAction<?>> onGoingRetryableActions = ConcurrentCollections.newConcurrentMap();
    private final AtomicLong bytesSinceLastPause = new AtomicLong();
    private final TransportRequestOptions fileChunkRequestOptions;
    private final Consumer<Long> onSourceThrottle;
    private final CancellableThreads cancellableThreads = new CancellableThreads();
    private final boolean retriesSupported;
    private volatile boolean isCancelled = false;
    private final AtomicBoolean finished = new AtomicBoolean();
    private final AtomicLong requestSeqNoGenerator;

    private Set<String> currFileNames;
    // last time this status was accessed
    private volatile long lastAccessTime = System.nanoTime();
    public RemoteTargetShardCopyState(ShardId shardId, ThreadPool threadPool, TransportService transportService, DiscoveryNode localNode,
                                      DiscoveryNode targetNode, AtomicLong requestSeqNoGenerator, Long internalActionTimeout, Consumer<Long> onSourceThrottle) {
        this.shardId = shardId;
        this.threadPool = threadPool;
//        this.threadPool = transportService.getThreadPool();
        this.transportService = transportService;
        this.localNode = localNode;
        this.targetNode = targetNode;
        this.onSourceThrottle = onSourceThrottle;
        this.internalActionTimeout = internalActionTimeout;
        this.requestSeqNoGenerator = requestSeqNoGenerator;
        this.fileChunkRequestOptions = TransportRequestOptions.builder()
            .withType(TransportRequestOptions.Type.COPY)
            .withTimeout(internalActionTimeout)//超时时间
            .build();
        this.retriesSupported = targetNode.getVersion().onOrAfter(Version.V_7_9_0);
    }

    void sendSegmentsInfo(SegmentsCopyInfo segmentsCopyInfo, Long internalActionTimeout, ActionListener<SegmentsInfoResponse> listener){
        SegmentsInfoRequest infoRequest = createSegmentsInfoRequest(shardId, segmentsCopyInfo);
        final TransportRequestOptions options =
            TransportRequestOptions.builder().withTimeout(120000).build();
//            TransportRequestOptions.builder().withTimeout(internalActionTimeout).build();
        // 没有重试
//        SegmentsInfoResponseHandler handler = new SegmentsInfoResponseHandler(infoRequest, listener);
//        transportService.sendRequest(targetNode, SegmentsCopyTargetService.Actions.SEGMENTS_INFO, infoRequest, options, handler);
        // 有重试
        final Writeable.Reader<SegmentsInfoResponse> reader = SegmentsInfoResponse::new;
        executeRetryableAction(SegmentsCopyTargetService.Actions.SEGMENTS_INFO, infoRequest, options, listener, reader);
    }

    private SegmentsInfoRequest createSegmentsInfoRequest(ShardId shardId, SegmentsCopyInfo scs){
        return new SegmentsInfoRequest(
            requestSeqNoGenerator.getAndIncrement(),
            shardId,
            localNode,
            scs.version,
            scs.gen,
            scs.primaryTerm,
            scs.refreshedCheckpoint,
            scs.infosBytes,
            scs.files,
            new ArrayList<>()
        );
    }


    void sendFiles(Store store, StoreFileMetadata[] files, ActionListener<Void> listener) {
        ArrayUtil.timSort(files, (a, b) ->  Math.toIntExact(a.length() - b.length())); // send smallest first

        final CopyMultiChunkTransfer<StoreFileMetadata, FileChunk> multiFileSender = new CopyMultiChunkTransfer<StoreFileMetadata, FileChunk>(
            logger, threadPool.getThreadContext(), listener, maxConcurrentFileChunks, Arrays.asList(files)) {

            final Deque<byte[]> buffers = new ConcurrentLinkedDeque<>();
            InputStreamIndexInput currentInput = null;
            long offset = 0;

            @Override
            protected void onNewResource(StoreFileMetadata md) throws IOException {
                offset = 0;
                IOUtils.close(currentInput, () -> currentInput = null);
                final IndexInput indexInput = store.directory().openInput(md.name(), IOContext.READONCE);
                currentInput = new InputStreamIndexInput(indexInput, md.length()) {
                    @Override
                    public void close() throws IOException {
                        IOUtils.close(indexInput, super::close); // InputStreamIndexInput's close is a noop
                    }
                };
            }

            private byte[] acquireBuffer() {
                final byte[] buffer = buffers.pollFirst();
                if (buffer != null) {
                    return buffer;
                }
                return new byte[chunkSizeInBytes];
            }

            @Override
            protected FileChunk nextChunkRequest(StoreFileMetadata md) throws IOException {
                assert Transports.assertNotTransportThread("read file chunk");
                cancellableThreads.checkForCancel();
                final byte[] buffer = acquireBuffer();
                final int bytesRead = currentInput.read(buffer);
                if (bytesRead == -1) {
                    throw new CorruptIndexException("file truncated; length=" + md.length() + " offset=" + offset, md.name());
                }
                final boolean lastChunk = offset + bytesRead == md.length();
                final FileChunk chunk = new FileChunk(md, new BytesArray(buffer, 0, bytesRead), offset, lastChunk,
                    () -> buffers.addFirst(buffer));
                offset += bytesRead;
                return chunk;
            }

            @Override
            protected void executeChunkRequest(FileChunk request, ActionListener<Void> listener) {
                cancellableThreads.checkForCancel();
                writeFileChunk(
                    request.md, request.position, request.content, request.lastChunk, 0,
                    ActionListener.runBefore(listener, request::close));
            }

            @Override
            protected void handleError(StoreFileMetadata md, Exception e) throws Exception {
                logger.error("send send send send send send send files error {} mata {}", e, md.name());
                handleErrorOnSendFiles(store, e, new StoreFileMetadata[]{md});
            }

            @Override
            public void close() throws IOException {
                IOUtils.close(currentInput, () -> currentInput = null);
            }
        };
//        resources.add(multiFileSender);
        multiFileSender.start();
    }


    private void handleErrorOnSendFiles(Store store, Exception e, StoreFileMetadata[] mds) throws Exception {
        final IOException corruptIndexException = ExceptionsHelper.unwrapCorruption(e);
        assert Transports.assertNotTransportThread(this + "[handle error on send/clean files]");
        if (corruptIndexException != null) {
            Exception localException = null;
            for (StoreFileMetadata md : mds) {
                cancellableThreads.checkForCancel();
                logger.debug("checking integrity for file {} after remove corruption exception", md);
                if (store.checkIntegrityNoException(md) == false) { // we are corrupted on the primary -- fail!
                    logger.warn("{} Corrupted file detected {} checksum mismatch", shardId, md);
                    if (localException == null) {
                        localException = corruptIndexException;
                    }
                    // TODO: 需要做何处理
//                    failEngine(corruptIndexException);
                }
            }
            if (localException != null) {
                throw localException;
            } else { // corruption has happened on the way to replica
                RemoteTransportException remoteException = new RemoteTransportException(
                    "File corruption occurred on copy but checksums are ok", null);
                remoteException.addSuppressed(e);
                logger.warn(() -> new ParameterizedMessage("{} Remote file corruption on node {}, copying {}. local checksum OK",
                    shardId, targetNode, mds), corruptIndexException);
                throw remoteException;
            }
        }
        throw e;
    }

    @Override
    public void writeFileChunk(StoreFileMetadata fileMetadata, long position, BytesReference content,
                               boolean lastChunk, int totalTranslogOps, ActionListener<Void> listener) {
        // Pause using the rate limiter, if desired, to throttle the copy
        final long throttleTimeInNanos;
        // always fetch the ratelimiter - it might be updated in real-time on the copy settings
        final RateLimiter rl = new RateLimiter.SimpleRateLimiter(1000L);
        if (rl != null) {
            long bytes = bytesSinceLastPause.addAndGet(content.length());
            if (bytes > rl.getMinPauseCheckBytes()) {
                // Time to pause
                bytesSinceLastPause.addAndGet(-bytes);
                try {
                    throttleTimeInNanos = rl.pause(bytes);
                    onSourceThrottle.accept(throttleTimeInNanos);
                } catch (IOException e) {
                    throw new ElasticsearchException("failed to pause copy", e);
                }
            } else {
                throttleTimeInNanos = 0;
            }
        } else {
            throttleTimeInNanos = 0;
        }

        final String action = SegmentsCopyTargetService.Actions.FILE_CHUNK;
        final long requestSeqNo = requestSeqNoGenerator.getAndIncrement();
        /* we send estimateTotalOperations with every request since we collect stats on the target and that way we can
         * see how many translog ops we accumulate while copying files across the network. A future optimization
         * would be in to restart file copy again (new deltas) if we have too many translog ops are piling up.
         */
        final CopyFileChunkRequest request = new CopyFileChunkRequest(
            requestSeqNo, shardId, fileMetadata, position, content, lastChunk, totalTranslogOps, throttleTimeInNanos);
        final Writeable.Reader<TransportResponse.Empty> reader = in -> TransportResponse.Empty.INSTANCE;
        executeRetryableAction(action, request, fileChunkRequestOptions, ActionListener.map(listener, r -> null), reader);
    }

    @Override
    public void cleanFiles(long globalCheckpoint, Map<String, StoreFileMetadata> sourceMetadata, ActionListener<Void> listener) {
        final String action = SegmentsCopyTargetService.Actions.CLEAN_FILES;
        final long requestSeqNo = requestSeqNoGenerator.getAndIncrement();
        final CopyCleanFilesRequest request =
            new CopyCleanFilesRequest(requestSeqNo, shardId, sourceMetadata, globalCheckpoint);
        final TransportRequestOptions options =
            //TODO:这里需要添加配置
//            TransportRequestOptions.builder().withTimeout(recoverySettings.internalActionTimeout()).build();
            TransportRequestOptions.builder().withTimeout(timeValueMillis(10000)).build();
        final Writeable.Reader<TransportResponse.Empty> reader = in -> TransportResponse.Empty.INSTANCE;
        final ActionListener<TransportResponse.Empty> responseListener = ActionListener.map(listener, r -> null);
        executeRetryableAction(action, request, options, responseListener, reader);
    }

    private <T extends TransportResponse> void executeRetryableAction(String action, CopyTransportRequest request,
                                                                      TransportRequestOptions options, ActionListener<T> actionListener,
                                                                      Writeable.Reader<T> reader) {
        final Object key = new Object();
        final ActionListener<T> removeListener = ActionListener.runBefore(actionListener, () -> onGoingRetryableActions.remove(key));
        final TimeValue initialDelay = TimeValue.timeValueMillis(120000);
        // TODO 改成从配置中读取，并且有专门的配置
//        final TimeValue timeout = new TimeValue(internalActionTimeout);
        final TimeValue timeout = new TimeValue(120000);
//        final TimeValue timeout = recoverySettings.internalActionRetryTimeout();
        final RetryableAction<T> retryableAction = new RetryableAction<T>(logger, threadPool, initialDelay, timeout, removeListener) {

            @Override
            public void tryAction(ActionListener<T> listener) {
                transportService.sendRequest(targetNode, action, request, options,
                    new ActionListenerResponseHandler<>(listener, reader, ThreadPool.Names.GENERIC));
            }

            @Override
            public boolean shouldRetry(Exception e) {
                logger.error("[{}] [{}] failed to copy segments", action, shardId,e);
                return retriesSupported && retryableException(e);
            }
        };
        onGoingRetryableActions.put(key, retryableAction);
        retryableAction.run();
        if (isCancelled) {
            retryableAction.cancel(new CancellableThreads.ExecutionCancelledException("segments copy was cancelled"));
        }
    }

    private static boolean retryableException(Exception e) {
        if (e instanceof ConnectTransportException) {
            return true;
        } else if (e instanceof SendRequestTransportException) {
            final Throwable cause = ExceptionsHelper.unwrapCause(e);
            return cause instanceof ConnectTransportException;
        } else if (e instanceof RemoteTransportException) {
            final Throwable cause = ExceptionsHelper.unwrapCause(e);
            return cause instanceof CircuitBreakingException ||
                cause instanceof EsRejectedExecutionException;
        }
        return false;
    }

    private static class FileChunk implements CopyMultiChunkTransfer.ChunkRequest, Releasable {
        final StoreFileMetadata md;
        final BytesReference content;
        final long position;
        final boolean lastChunk;
        final Releasable onClose;

        FileChunk(StoreFileMetadata md, BytesReference content, long position, boolean lastChunk, Releasable onClose) {
            this.md = md;
            this.content = content;
            this.position = position;
            this.lastChunk = lastChunk;
            this.onClose = onClose;
        }

        @Override
        public boolean lastChunk() {
            return lastChunk;
        }

        @Override
        public void close() {
            onClose.close();
        }
    }

    private ChecksumIndexInput toIndexInput(byte[] input) {
        return new BufferedChecksumIndexInput(
            new ByteBuffersIndexInput(
                new ByteBuffersDataInput(Arrays.asList(ByteBuffer.wrap(input))), "SegmentInfos"));
    }



    private class SegmentsInfoResponseHandler implements TransportResponseHandler<SegmentsInfoResponse> {

        private final SegmentsInfoRequest request;
        private final ActionListener<SegmentsInfoResponse> listener;

        private SegmentsInfoResponseHandler(final SegmentsInfoRequest request, ActionListener<SegmentsInfoResponse> listener) {
            this.request = request;
            this.listener = listener;
        }

        @Override
        public void handleResponse(SegmentsInfoResponse response) {
            currFileNames = response.fileNames;
            listener.onResponse(response);
        }

        @Override
        public void handleException(TransportException e) {
            onException(e);
        }

        private void onException(Exception e) {
            if (logger.isTraceEnabled()) {
                logger.trace(() -> new ParameterizedMessage(
                    "[{}][{}] Got exception on segments copy", request.shardId().getIndex().getName(),
                    request.shardId().id()), e);
            }
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause instanceof CancellableThreads.ExecutionCancelledException) {
                // this can also come from the source wrapped in a RemoteTransportException
//                onGoingRecoveries.failRecovery(recoveryId, new RecoveryFailedException(request,
//                    "source has canceled the recovery", cause), false);
                return;
            }
            if (cause instanceof RecoveryEngineException) {
                // unwrap an exception that was thrown as part of the recovery
                cause = cause.getCause();
            }
            // do it twice, in case we have double transport exception
            cause = ExceptionsHelper.unwrapCause(cause);
            if (cause instanceof RecoveryEngineException) {
                // unwrap an exception that was thrown as part of the recovery
                cause = cause.getCause();
            }

            // here, we would add checks against exception that need to be retried (and not removeAndClean in this case)

            if (cause instanceof IllegalIndexShardStateException || cause instanceof IndexNotFoundException ||
                cause instanceof ShardNotFoundException) {
                // if the target is not ready yet, retry
//                retryRecovery(
//                    recoveryId,
//                    "remote shard not ready",
//                    recoverySettings.retryDelayStateSync(),
//                    recoverySettings.activityTimeout());
                return;
            }

            // PeerRecoveryNotFound is returned when the source node cannot find the recovery requested by
            // the REESTABLISH_RECOVERY request. In this case, we delay and then attempt to restart.
            if (cause instanceof DelayRecoveryException || cause instanceof PeerRecoveryNotFound) {
//                retryRecovery(recoveryId, cause, recoverySettings.retryDelayStateSync(),
//                    recoverySettings.activityTimeout());
                return;
            }

            if (cause instanceof ConnectTransportException) {
//                logger.info("recovery of {} from [{}] interrupted by network disconnect, will retry in [{}]; cause: [{}]",
//                    request.shardId(), request.sourceNode(), recoverySettings.retryDelayNetwork(), cause.getMessage());
//                if (request.sourceNode().getVersion().onOrAfter(Version.V_7_9_0)) {
//                    reestablishRecovery(request, cause.getMessage(), recoverySettings.retryDelayNetwork());
//                } else {
//                    retryRecovery(recoveryId, cause.getMessage(), recoverySettings.retryDelayNetwork(),
//                        recoverySettings.activityTimeout());
//                }
                return;
            }

            if (cause instanceof AlreadyClosedException) {
//                onGoingRecoveries.failRecovery(recoveryId,
//                    new RecoveryFailedException(request, "source shard is closed", cause), false);
                return;
            }

//            onGoingRecoveries.failRecovery(recoveryId, new RecoveryFailedException(request, e), true);
        }

        @Override
        public String executor() {
            // we do some heavy work like refreshes in the response so fork off to the generic threadpool
            return ThreadPool.Names.GENERIC;
        }

        @Override
        public <Q extends TransportResponse> TransportResponseHandler<Q> wrap(Function<Q, SegmentsInfoResponse> converter, Writeable.Reader<Q> reader) {
            return TransportResponseHandler.super.wrap(converter, reader);
        }

        @Override
        public SegmentsInfoResponse read(StreamInput in) throws IOException {
            return new SegmentsInfoResponse(in);
        }
    }

    public Set<String> getCurrFileNames() {
        return currFileNames;
    }

    public void setChunkSizeInBytes(int chunkSizeInBytes) {
        this.chunkSizeInBytes = chunkSizeInBytes;
    }

    public void setMaxConcurrentFileChunks(int maxConcurrentFileChunks) {
        this.maxConcurrentFileChunks = maxConcurrentFileChunks;
    }

    public DiscoveryNode getTargetNode() {
        return targetNode;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    @Override
    public void  cancel() {
        isCancelled = true;
        if (onGoingRetryableActions.isEmpty()) {
            return;
        }
        final RuntimeException exception = new CancellableThreads.ExecutionCancelledException("recovery was cancelled");
        // Dispatch to generic as cancellation calls can come on the cluster state applier thread
        threadPool.generic().execute(() -> {
            for (RetryableAction<?> action : onGoingRetryableActions.values()) {
                action.cancel(exception);
            }
            onGoingRetryableActions.clear();
        });
    }

    /**
     * fail the recovery and call listener
     *
     * @param e                exception that encapsulating the failure
     * @param sendShardFailure indicates whether to notify the master of the shard failure
     */
    public void fail(RecoveryFailedException e, boolean sendShardFailure) {
        if (finished.compareAndSet(false, true)) {
            try {
                notifyListener(e, sendShardFailure);
            } finally {
                try {
                    cancellableThreads.cancel("failed recovery [" + ExceptionsHelper.stackTrace(e) + "]");
                } finally {
                }
            }
        }
    }

    public void notifyListener(RecoveryFailedException e, boolean sendShardFailure) {
//        listener.onRecoveryFailure(state(), e, sendShardFailure);
    }

}
