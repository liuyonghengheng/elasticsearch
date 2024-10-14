package org.elasticsearch.indices.segmentscopy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.RateLimiter;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.DataCopyEngine;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.SegmentsCopyInfo;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.*;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class SegmentsCopyTargetService implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(SegmentsCopyTargetService.class);
    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(logger.getName());

    public static class Actions {
        public static final String SEGMENTS_INFO = "internal:index/shard/segments/copy/segments_info";
        public static final String FILE_CHUNK = "internal:index/shard/segments/copy/file_chunk";
        public static final String CLEAN_FILES = "internal:index/shard/segments/copy/clean_files";
    }

    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final SegmentsCopySettings segmentsCopySettings;

    private static final Map<ShardId, LocalTargetShardCopyState> onGoingShards = new HashMap<>();

    @Inject
    public SegmentsCopyTargetService(IndicesService indicesService, ClusterService clusterService, TransportService transportService,
                                     SegmentsCopySettings segmentsCopySettings) {
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.segmentsCopySettings =  segmentsCopySettings;

        transportService.registerRequestHandler(SegmentsCopyTargetService.Actions.SEGMENTS_INFO, ThreadPool.Names.GENERIC, SegmentsInfoRequest::new,
            new SegmentsInfoRequestHandler());

        transportService.registerRequestHandler(SegmentsCopyTargetService.Actions.FILE_CHUNK, ThreadPool.Names.GENERIC, CopyFileChunkRequest::new,
            new FileChunkTransportRequestHandler());

        transportService.registerRequestHandler(Actions.CLEAN_FILES, ThreadPool.Names.GENERIC, CopyCleanFilesRequest::new,
            new CopyCleanFilesRequestHandler());
    }

    public LocalTargetShardCopyState getTargetShardCopyState(SegmentsInfoRequest request, ActionListener<TransportResponse> listener) {
        LocalTargetShardCopyState localTargetShardCopyState = onGoingShards.get(request.shardId());
        if (localTargetShardCopyState == null) {
            IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
            IndexShard indexShard = indexService.getShard(request.shardId().id());
            if (indexShard == null) {
                // 异常情况要处理，比如relocalting 状态
                listener.onResponse(new LocalTargetShardCopyState.ErrorResponse(LocalTargetShardCopyState.ErrorType.SHARD_ERROR));
                return null;
            }
            Engine engine = indexShard.getEngineOrNull();

            if (engine != null && "segment".equals(indexService.getIndexSettings().getSettings()
                .get("index.datasycn.type","operation"))) { //((DataCopyEngine) engine).getIsPrimary()
                localTargetShardCopyState = new LocalTargetShardCopyState(indexShard, request.sourceNode,
                    indexService.getIndexSettings().getSettings()
                        .getAsLong("index.datasycn.segment.shard.internal_action_timeout", 500L));
                onGoingShards.put(request.shardId(), localTargetShardCopyState);
                return localTargetShardCopyState;
            }else{
                // 异常情况要处理
                listener.onFailure(new IOException("#####################################target can not start a new copy task {}"+request.shardId()));
//                listener.onResponse(new LocalTargetShardCopyState.ErrorResponse(LocalTargetShardCopyState.ErrorType.ENGINE_ERROR));
                return null;
            }
        }else{
            //如果已经存在，需要先判断状态是否正常
            if(localTargetShardCopyState.isRunning.get()){
                // 已经在运行了，不能再接收新的请求！
//                listener.onResponse(new LocalTargetShardCopyState.ErrorResponse(LocalTargetShardCopyState.ErrorType.IS_RUNNING_ERROR));
                listener.onFailure(new IOException("##################################### segments copy task is running can not start a new seagments copy task! {}"+request.shardId()));
                return null;
            }else{
                return localTargetShardCopyState;
            }
        }
    }

    class SegmentsInfoRequestHandler implements TransportRequestHandler<SegmentsInfoRequest> {
        @Override
        public void messageReceived(SegmentsInfoRequest request, TransportChannel channel, Task task) throws Exception {
            logger.error("SegmentCopyTargetService [{}] receive segments info request", request.shardId());
            ChannelActionListener<TransportResponse, SegmentsInfoRequest> listener =
                new ChannelActionListener<>(channel, Actions.SEGMENTS_INFO, request);
            LocalTargetShardCopyState localTargetShardCopyState = getTargetShardCopyState(request, listener);
            logger.error("SegmentCopyTargetService [{}] receive segments info request", request.shardId());
            logger.error("SegmentCopyTargetService receive SegmentsInfo request, task status [{}]", localTargetShardCopyState.isRunning.get());
            if(localTargetShardCopyState == null){
                // 异常情况直接返回
//                channel.sendResponse(new LocalTargetShardCopyState.ErrorResponse(LocalTargetShardCopyState.ErrorType.PRIMARY_TERM_ERROR));
                logger.error("##################################### segments copy task is running can not start a new seagments copy task! {}",request.shardId());
                return;
            }
            logger.error("SegmentCopyTargetService [{}] receive segments info request", request.shardId());
//            localTargetShardCopyState.isRunning.set(true);
            if(!localTargetShardCopyState.isRunning.get()) {
                localTargetShardCopyState.isRunning.set(true);
                SegmentsCopyInfo segmentsCopyInfo = new SegmentsCopyInfo(request.fileNames, null, request.segmentInfoVersion,
                    request.segmentInfoGen, request.infosBytes, request.primaryTerm, null, null, request.refreshedCheckpoint);
                localTargetShardCopyState.receiveSegmentsInfo(segmentsCopyInfo, listener);
            }
        }
    }


    class FileChunkTransportRequestHandler implements TransportRequestHandler<CopyFileChunkRequest> {
        final AtomicLong bytesSinceLastPause = new AtomicLong();
        @Override
        public void messageReceived(CopyFileChunkRequest request, TransportChannel channel, Task task) throws Exception {
            logger.error("SegmentCopyTargetService [{}] receive FileChunk request file [{}]", request.shardId(), request.name());
            LocalTargetShardCopyState targetShardCopyState = onGoingShards.get(request.shardId());
            if(targetShardCopyState == null){
                TransportChannel.sendErrorResponse(channel, SegmentsCopyTargetService.Actions.FILE_CHUNK, request,
                    new IOException("SegmentCopyTargetService receive FileChunk request, but have no onGoing task"));
                return;
            }else if(!targetShardCopyState.isRunning.get()){
                TransportChannel.sendErrorResponse(channel, SegmentsCopyTargetService.Actions.FILE_CHUNK, request,
                    new IOException("SegmentCopyTargetService receive FileChunk request, but task is not running"));
                return;
            }
            logger.error("SegmentCopyTargetService receive FileChunk request, task status [{}]", targetShardCopyState.isRunning.get());
            final ActionListener<Void> listener = createOrFinishListener(targetShardCopyState, channel, SegmentsCopyTargetService.Actions.FILE_CHUNK, request);
            if (listener == null) {
                return;
            }

            if(!targetShardCopyState.isRunning.get()){
                logger.error("file trunk stype EEEEEEEEEEEEEEEEEE  errorrrrrrrrrrrrrrrrrrrrrrrrrrrr 已经停止");
//                listener.onFailure(new IOException("本次拷贝 已经停止不能再发送！"));
            }

// TODO 设置限速等, 这种是主要业务功能，能不能限速？难道不应该越快越好吗？
//            final RecoveryState.Index indexState = recoveryTarget.state().getIndex();
//            if (request.sourceThrottleTimeInNanos() != RecoveryState.Index.UNKNOWN) {
//                indexState.addSourceThrottling(request.sourceThrottleTimeInNanos());
//            }

            RateLimiter rateLimiter = segmentsCopySettings.rateLimiter();
            if (rateLimiter != null) {
                long bytes = bytesSinceLastPause.addAndGet(request.content().length());
                if (bytes > rateLimiter.getMinPauseCheckBytes()) {
                    // Time to pause
                    bytesSinceLastPause.addAndGet(-bytes);
                    long throttleTimeInNanos = rateLimiter.pause(bytes);
// TODO 设置限速等
//                    indexState.addTargetThrottling(throttleTimeInNanos);
//                    recoveryTarget.indexShard().recoveryStats().addThrottleTime(throttleTimeInNanos);
                }
            }
            targetShardCopyState.writeFileChunk(request.metadata(), request.position(), request.content(), request.lastChunk(),
                request.totalTranslogOps(), listener);
        }
    }

    class CopyCleanFilesRequestHandler implements TransportRequestHandler<CopyCleanFilesRequest> {

        @Override
        public void messageReceived(CopyCleanFilesRequest request, TransportChannel channel, Task task) throws Exception {
            LocalTargetShardCopyState targetShardCopyState = onGoingShards.get(request.shardId());
            if(targetShardCopyState == null){
                TransportChannel.sendErrorResponse(channel, SegmentsCopyTargetService.Actions.FILE_CHUNK, request,
                    new IOException("SegmentCopyTargetService receive CleanFiles request, but have no onGoing task"));
                return;
            }else if(!targetShardCopyState.isRunning.get()){
                TransportChannel.sendErrorResponse(channel, SegmentsCopyTargetService.Actions.FILE_CHUNK, request,
                    new IOException("SegmentCopyTargetService receive CleanFiles request, but task is not running"));
                return;
            }
            logger.error("SegmentCopyTargetService receive CleanFiles request, task status [{}]", targetShardCopyState.isRunning.get());
            try {
                final ActionListener<Void> listener = createOrFinishListener(targetShardCopyState, channel, Actions.CLEAN_FILES, request);
                if (listener == null) {
                    return;
                }
                if(!targetShardCopyState.isRunning.get()){
                    logger.error("clean stype EEEEEEEEEEEEEEEEEE  errorrrrrrrrrrrrrrrrrrrrrrrrrrrr 已经停止");
//                listener.onFailure(new IOException("本次拷贝 已经停止不能再发送！"));
                }
                targetShardCopyState.cleanFiles(request.getGlobalCheckpoint(), request.sourceMetaSnapshot(),
                        listener);
            } finally {

            }
        }
    }

    private ActionListener<Void> createOrFinishListener(final LocalTargetShardCopyState targetShardCopyState, final TransportChannel channel,
                                                        final String action, final CopyTransportRequest request) {
        return createOrFinishListener(targetShardCopyState, channel, action, request, nullVal -> TransportResponse.Empty.INSTANCE);
    }

    private ActionListener<Void> createOrFinishListener(final LocalTargetShardCopyState targetShardCopyState, final TransportChannel channel,
                                                        final String action, final CopyTransportRequest request,
                                                        final CheckedFunction<Void, TransportResponse, Exception> responseFn) {
        final ActionListener<TransportResponse> channelListener = new ChannelActionListener<>(channel, action, request);
        final ActionListener<Void> voidListener = ActionListener.map(channelListener, responseFn);

        final long requestSeqNo = request.requestSeqNo();
        final ActionListener<Void> listener;
        if (requestSeqNo != SequenceNumbers.UNASSIGNED_SEQ_NO) {
            listener = targetShardCopyState.markRequestReceivedAndCreateListener(requestSeqNo, voidListener);
        } else {
            listener = voidListener;
        }

        return listener;
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        if (indexShard != null) {
            cancelSegmentsCOpyForShard(shardId, "shard closed");
        }
    }

    public static boolean cancelSegmentsCOpyForShard(ShardId shardId, String reason) {
        boolean cancelled = false;
        List<LocalTargetShardCopyState> matchedRecoveries = new ArrayList<>();
        synchronized (onGoingShards) {
            for (Iterator<LocalTargetShardCopyState> it = onGoingShards.values().iterator(); it.hasNext(); ) {
                LocalTargetShardCopyState status = it.next();
                if (status.shardId.equals(shardId)) {
                    matchedRecoveries.add(status);
                    it.remove();
                }
            }
        }
        for (LocalTargetShardCopyState removed : matchedRecoveries) {
            logger.trace("{} canceled segments copy, (reason [{}])",
                removed.shardId, reason);
            removed.cancel(reason);
            cancelled = true;
        }
        return cancelled;
    }

}

