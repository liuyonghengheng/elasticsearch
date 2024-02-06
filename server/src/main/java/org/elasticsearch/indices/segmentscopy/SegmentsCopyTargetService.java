package org.elasticsearch.indices.segmentscopy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.SegmentsCopyInfo;
import org.elasticsearch.index.seqno.SequenceNumbers;
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

import java.util.HashMap;
import java.util.Map;

public class SegmentsCopyTargetService {

    private static final Logger logger = LogManager.getLogger(SegmentsCopyTargetService.class);
    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(logger.getName());

    public static class Actions {
        public static final String SEGMENTS_INFO = "internal:index/shard/segments/copy/segmentsInfo";
        public static final String FILES_INFO = "internal:index/shard/segments/copy/filesInfo";
        public static final String FILE_CHUNK = "internal:index/shard/segments/copy/file_chunk";
        public static final String CLEAN_FILES = "internal:index/shard/segments/copy/clean_files";
    }

    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    private final Map<ShardId, LocalTargetShardCopyState> onGoingShards = new HashMap<>();

    @Inject
    public SegmentsCopyTargetService(IndicesService indicesService, ClusterService clusterService, TransportService transportService, IndexNameExpressionResolver indexNameExpressionResolver) {
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;

        transportService.registerRequestHandler(SegmentsCopyTargetService.Actions.SEGMENTS_INFO, ThreadPool.Names.GENERIC, SegmentsInfoRequest::new,
            new SegmentsInfoRequestHandler());

        transportService.registerRequestHandler(SegmentsCopyTargetService.Actions.FILES_INFO, ThreadPool.Names.GENERIC, CopyFilesInfoRequest::new,
            new FilesInfoRequestHandler());

        transportService.registerRequestHandler(SegmentsCopyTargetService.Actions.FILE_CHUNK, ThreadPool.Names.GENERIC, CopyFileChunkRequest::new,
            new FileChunkTransportRequestHandler());
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
                .get("index.datasycn.type","operation"))) {
                localTargetShardCopyState = new LocalTargetShardCopyState(request.shardId(), indexShard,
                    transportService, request.sourceNode,
                    indexService.getIndexSettings().getSettings()
                        .getAsLong("index.datasycn.segment.shard.internal_action_timeout", 500L));
                onGoingShards.put(request.shardId(), localTargetShardCopyState);
                return localTargetShardCopyState;
            }else{
                // 异常情况要处理
                listener.onResponse(new LocalTargetShardCopyState.ErrorResponse(LocalTargetShardCopyState.ErrorType.ENGINE_ERROR));
                return null;
            }
        }else{
            //如果已经存在，需要先判断状态是否正常
            if(localTargetShardCopyState.isRunning.get()){
                // 已经在运行了，不能再接收新的请求！
                listener.onResponse(new LocalTargetShardCopyState.ErrorResponse(LocalTargetShardCopyState.ErrorType.IS_RUNNING_ERROR));
                return null;
            }else{
                return localTargetShardCopyState;
            }
        }
    }

    class SegmentsInfoRequestHandler implements TransportRequestHandler<SegmentsInfoRequest> {
        @Override
        public void messageReceived(SegmentsInfoRequest request, TransportChannel channel, Task task) throws Exception {
            ChannelActionListener<TransportResponse, SegmentsInfoRequest> listener =
                new ChannelActionListener<>(channel, Actions.SEGMENTS_INFO, request);
            LocalTargetShardCopyState localTargetShardCopyState = getTargetShardCopyState(request, listener);
            if(localTargetShardCopyState == null){
                // 异常情况直接返回
//                channel.sendResponse(new LocalTargetShardCopyState.ErrorResponse(LocalTargetShardCopyState.ErrorType.PRIMARY_TERM_ERROR));
                return;
            }
            localTargetShardCopyState.isRunning.set(true);
            SegmentsCopyInfo segmentsCopyInfo = new SegmentsCopyInfo(request.fileNames, request.segmentInfoVersion,
                request.segmentInfoGen, request.infosBytes, request.primaryTerm, null, null);
            localTargetShardCopyState.receiveSegmentsInfo(segmentsCopyInfo, listener);
        }
    }

    class FilesInfoRequestHandler implements TransportRequestHandler<CopyFilesInfoRequest> {

        @Override
        public void messageReceived(CopyFilesInfoRequest request, TransportChannel channel, Task task) throws Exception {
//            try (RecoveriesCollection.RecoveryRef recoveryRef = onGoingShards.getRecoverySafe(request.recoveryId(), request.shardId())) {
//                final ActionListener<Void> listener = createOrFinishListener(recoveryRef, channel, PeerRecoveryTargetService.Actions.FILES_INFO, request);
//                if (listener == null) {
//                    return;
//                }
//
//                recoveryRef.target().receiveFileInfo(
//                    request.phase1FileNames, request.phase1FileSizes, request.phase1ExistingFileNames, request.phase1ExistingFileSizes,
//                    request.totalTranslogOps, listener);
//            }
        }
    }

    class FileChunkTransportRequestHandler implements TransportRequestHandler<CopyFileChunkRequest> {

        @Override
        public void messageReceived(CopyFileChunkRequest request, TransportChannel channel, Task task) throws Exception {

        }
    }

    private ActionListener<Void> createOrFinishListener(final RecoveriesCollection.RecoveryRef recoveryRef, final TransportChannel channel,
                                                        final String action, final RecoveryTransportRequest request) {
        return createOrFinishListener(recoveryRef, channel, action, request, nullVal -> TransportResponse.Empty.INSTANCE);
    }

    private ActionListener<Void> createOrFinishListener(final RecoveriesCollection.RecoveryRef recoveryRef, final TransportChannel channel,
                                                        final String action, final RecoveryTransportRequest request,
                                                        final CheckedFunction<Void, TransportResponse, Exception> responseFn) {
        final RecoveryTarget recoveryTarget = recoveryRef.target();
        final ActionListener<TransportResponse> channelListener = new ChannelActionListener<>(channel, action, request);
        final ActionListener<Void> voidListener = ActionListener.map(channelListener, responseFn);

        final long requestSeqNo = request.requestSeqNo();
        final ActionListener<Void> listener;
        if (requestSeqNo != SequenceNumbers.UNASSIGNED_SEQ_NO) {
            listener = recoveryTarget.markRequestReceivedAndCreateListener(requestSeqNo, voidListener);
        } else {
            listener = voidListener;
        }

        return listener;
    }

}

