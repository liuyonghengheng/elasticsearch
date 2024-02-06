package org.elasticsearch.indices.segmentscopy;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.RecoveryEngineException;
import org.elasticsearch.index.engine.SegmentsCopyInfo;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.recovery.*;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

public class SourceShardCopyState {
    private Logger logger;
    private ClusterService clusterService;
    private TransportService transportService;
    private final ShardId shardId;
    private IndexShard indexShard;
    private final ConcurrentLinkedQueue<SegmentsCopyInfo> segmentsCopyStateQueue = new ConcurrentLinkedQueue<>();
    private final Set<SegmentsCopyInfo> finishSet = new HashSet<>();
    public AtomicBoolean isCopying = new AtomicBoolean(false);
    private SegmentsCopyInfo curSegment = null;
    private  int chunkSizeInBytes;
    private  int maxConcurrentFileChunks;
    private  long internalActionTimeout;
    private  long timeout;
    private  ThreadPool threadPool;
    private final CancellableThreads cancellableThreads = new CancellableThreads();
    private final Map<ShardRouting, TargetShardCopyState>  replicas = new HashMap<>();
    private final AtomicLong requestSeqNoGenerator = new AtomicLong(0);

    // TODO：挪到shard stat中
    private final AtomicLong throttleTimeInNanos = new AtomicLong();

    public SourceShardCopyState(ShardId shardId){
        this.shardId = shardId;
    }

    public void add(SegmentsCopyInfo state){
        segmentsCopyStateQueue.add(state);
    }

    synchronized public SegmentsCopyInfo  get(){
        SegmentsCopyInfo last = null;
        while(!segmentsCopyStateQueue.isEmpty()){
            last = segmentsCopyStateQueue.poll();
            if(!segmentsCopyStateQueue.isEmpty()){
                last.decRefDeleter();
            }
        }
        // 注意：copy 完成之后需要执行decRefDeleter()
        curSegment = last;
        return last;
    }

    public void initSourceShardCopyState(Logger logger, ClusterService clusterService,
                                         TransportService transportService, IndexShard indexShard, int chunkSizeInBytes,
                                         int maxConcurrentFileChunks, Long internalActionTimeout,
                                         long timeout, ThreadPool threadPool) {
        this.logger = logger;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.indexShard = indexShard;
        this.chunkSizeInBytes = chunkSizeInBytes;
        this.maxConcurrentFileChunks = maxConcurrentFileChunks;
        this.internalActionTimeout = internalActionTimeout;
        this.timeout = timeout;
        this.threadPool = threadPool;
    }

    public void addThrottleTime(long nanos) {
        throttleTimeInNanos.addAndGet(nanos);
    }

    // TODO: 需要一个listener，最终收集任务状态。
    public void copyToOneReplica(ShardId shardId, ShardRouting replicaRouting, SegmentsCopyInfo sci){
        if(Objects.nonNull(sci)){
            curSegment = sci;
        }
        String nodeId = replicaRouting.currentNodeId();
        final DiscoveryNode replicaNode = clusterService.state().nodes().get(nodeId);
        // replica 不存在，直接返回
        if (replicaNode == null) {
//                    失败处理,还是直接退出？
//                    listener.onFailure(new NoNodeAvailableException("unknown node [" + nodeId + "]"));
            return;
        }
        // create target node processor
        final RemoteTargetShardCopyState remoteTargetShardCopyState = new RemoteTargetShardCopyState(shardId,
            transportService, clusterService.localNode(),replicaNode, internalActionTimeout,
            throttleTime -> addThrottleTime(throttleTime));

        // 发送 segments info
        remoteTargetShardCopyState.sendSegmentsInfo(curSegment, internalActionTimeout, null);

        // 根据replca返回的文件列表，先验证文件是否存在

        // 向replica发送文件
        final StepListener<Void> sendFilesStep = new StepListener<>();

        // 向replica发送 Done， 客户端 应用segments，并更新 local checkpoint
        // 本地更新 global checkpoint

        // 本地对应文件 delete def -1
    }

    public ShardId getShardId() {
        return shardId;
    }

    public void setChunkSizeInBytes(int chunkSizeInBytes) {
        this.chunkSizeInBytes = chunkSizeInBytes;
    }

    public void setMaxConcurrentFileChunks(int maxConcurrentFileChunks) {
        this.maxConcurrentFileChunks = maxConcurrentFileChunks;
    }


    public void setThreadPool(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    public void setIndexShard(IndexShard indexShard) {
        this.indexShard = indexShard;
    }

    public IndexShard getIndexShard() {
        return this.indexShard;
    }

    public void setTimeout(long timeout){
        this.timeout = timeout;
    }

    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    public void setClusterService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public void setTransportService(TransportService transportService) {
        this.transportService = transportService;
    }

}
