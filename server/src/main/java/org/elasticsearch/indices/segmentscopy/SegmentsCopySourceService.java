package org.elasticsearch.indices.segmentscopy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.DataCopyEngine;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.RecoveryEngineException;
import org.elasticsearch.index.engine.SegmentsCopyInfo;
import org.elasticsearch.index.shard.*;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.*;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

public class SegmentsCopySourceService {

    private static final Logger logger = LogManager.getLogger(SegmentsCopySourceService.class);
    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(logger.getName());

    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final List<SourceShardCopyState> shardCopyStates  = new LinkedList<SourceShardCopyState>();

    private final ConcurrentLinkedDeque<ShardId> newShardIds = new ConcurrentLinkedDeque<>();
    @Inject
    public SegmentsCopySourceService(IndicesService indicesService, ClusterService clusterService, TransportService transportService) {
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.transportService = transportService;
    }

    /**
     * 如果相同的shard 已经存在，需要把老的任务都停掉，然后直接覆盖新的状态！
     * @param shardId
     * @param sourceShardCopyState
     */
    public void addSourceShardCopyState(ShardId shardId, SourceShardCopyState sourceShardCopyState){
        this.shardCopyStates.add(sourceShardCopyState);
    }

    public void addNewShardId(ShardId shardId){
        newShardIds.add(shardId);
    }

    /**
     * 先停止任务，然后删除
     * @param shardId
     * @param sourceShardCopyState
     */
    public void removeSourceShardCopyState(ShardId shardId, SourceShardCopyState sourceShardCopyState){

    }

    public void stopShardCopy(){

    }

    private IndexShard getIndexShard(ShardId shardId) {
        IndexService indexService = null;
        try{
            indexService = indicesService.indexServiceSafe(shardId.getIndex());
        }catch (IndexNotFoundException e){
            logger.info("Index Not Found", e);
            return null;
        }
        return indexService.getShard(shardId.id());
    }

    public boolean initIndexShard(ShardId shardId) {
        try{
            IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
            IndexShard indexShard = indexService.getShard(shardId.id());
            Engine engine = indexShard.getEngineOrNull();

            if(engine != null && engine instanceof DataCopyEngine){
                SourceShardCopyState ss  = ((DataCopyEngine) engine).getSourceShardCopyState();
                ss.initSourceShardCopyState(logger, clusterService, transportService, indexShard,
                    indexService.getIndexSettings()
                        .getSettings().getAsMemory("index.datasycn.segment.chunk_size","10M").bytesAsInt(),
                    indexService.getIndexSettings()
                        .getSettings().getAsInt("index.datasycn.segment.shard.max_concurrent_file_chunks",1),
                    indexService.getIndexSettings()
                        .getSettings().getAsLong("index.datasycn.segment.shard.internal_action_timeout",500L),
                    indexService.getIndexSettings()
                        .getSettings().getAsLong("index.datasycn.segment.timeout",500L),
                    indexShard.getThreadPool());
                shardCopyStates.add(ss);
                return true;
            }else if(engine == null){
                // 如果目前engine是null 还要添加回队列，等待下次检查
                return false;
            }
        }catch (IndexNotFoundException e){
            logger.info("Index Not Found", e);
            return false;
        }
        return true;
    }

    public boolean initIndexShard(IndexShard indexShard, int chunkSize, int concurrentChunks, long  interTimeout, long timeout) {
        try{
            Engine engine = indexShard.getEngineOrNull();

            if(engine != null && engine instanceof DataCopyEngine){
                SourceShardCopyState ss  = ((DataCopyEngine) engine).getSourceShardCopyState();
                ss.initSourceShardCopyState(logger, clusterService, transportService, indexShard,
                    chunkSize, concurrentChunks,interTimeout,timeout,
                    indexShard.getThreadPool());
                shardCopyStates.add(ss);
                return true;
            }else if(engine == null){
                // 如果目前engine是null 还要添加回队列，等待下次检查
                return false;
            }
        }catch (IndexNotFoundException e){
            logger.info("Index Not Found", e);
            return false;
        }
        return true;
    }

    public void intNewShardIds(){
        for(int i=newShardIds.size(); i>0 ; i--){
            ShardId shardId = newShardIds.poll();
            if(!initIndexShard(shardId)){
                newShardIds.add(shardId);
            }
        }
    }

    public void copyOneShard(SourceShardCopyState sourceShardCopyState){
        IndexShard indexShard = sourceShardCopyState.getIndexShard();
        ShardId shardId = sourceShardCopyState.getShardId();
        if(!checkShardState(shardId, indexShard, sourceShardCopyState)){
            // primary shard状态不对，停止已有的任务，删除, 并返回！
            return;
        }
        if(sourceShardCopyState.isCopying.get()){
            //正在运行直接跳过
            return;
        }
        ShardRouting primaryRouting = indexShard.routingEntry();
        ReplicationGroup replicationGroup = indexShard.getReplicationGroup();
        SegmentsCopyInfo sci = sourceShardCopyState.get();
        for (final ShardRouting replicaRouting : replicationGroup.getReplicationTargets()) {
            if (replicaRouting.isSameAllocation(primaryRouting) == false) {
                sourceShardCopyState.copyToOneReplica(shardId, replicaRouting, sci);
            }
        }
    }

    public boolean checkShardState(ShardId shardId, IndexShard indexShard, SourceShardCopyState sourceShardCopyState){
        if(!indexShard.routingEntry().primary() || indexShard.state() != IndexShardState.STARTED){
            removeSourceShardCopyState(shardId, sourceShardCopyState);
            return false;
        }
        return true;
    }

    public void doShardsCopy(){
        if (!shardCopyStates.isEmpty()) {
            for(SourceShardCopyState ss: shardCopyStates){
                copyOneShard(ss);
            }
        }
    }

    /**
     * 处理shards 的copy，主要就是分发任务
     */
    public void doCopy(){
        // TODO: 是否改成定时调度线程？
        while(true) {
            intNewShardIds();
            doShardsCopy();
        }
    }

    /**
     * 清理掉不再是primary 的shard！
     * */
    public void pruneList(){

    }
    /**
     * for test
    * */
    protected List<SourceShardCopyState> getShardCopyStates() {
        return shardCopyStates;
    }

}
