package org.elasticsearch.indices.segmentscopy;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.Version;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
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
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.indices.recovery.*;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.index.store.Store.digestToString;

public class SourceShardCopyState {
    public static BlockingQueue<ShardId> blockingQueue = new LinkedBlockingQueue<>();
    private Logger logger;
    private ClusterService clusterService;
    private TransportService transportService;
    private final ShardId shardId;
    private IndexShard indexShard;
    private final ConcurrentLinkedQueue<SegmentsCopyInfo> segmentsCopyStateQueue = new ConcurrentLinkedQueue<>();
    private final Set<SegmentsCopyInfo> finishSet = new HashSet<>();
    public AtomicBoolean isCopying = new AtomicBoolean(false);
    private SegmentsCopyInfo curSegment = null;
    private SegmentsCopyInfo lastSegment = null;
    private  int chunkSizeInBytes;
    private  int maxConcurrentFileChunks;
    private  long internalActionTimeout;
    private  long timeout;
    private  ThreadPool threadPool;
    private final CancellableThreads cancellableThreads = new CancellableThreads();
    private final Map<DiscoveryNode, RemoteTargetShardCopyState>  replicas = new HashMap<>();
    private final AtomicLong requestSeqNoGenerator = new AtomicLong(0);

    // TODO：挪到shard stat中
    private final AtomicLong throttleTimeInNanos = new AtomicLong();

    Map<String, StoreFileMetadata> lastFileMetaData;

    public SourceShardCopyState(ShardId shardId){
        this.shardId = shardId;
    }

    public void add(SegmentsCopyInfo state){
        segmentsCopyStateQueue.add(state);
    }

    synchronized public SegmentsCopyInfo  getNextOne(){
        SegmentsCopyInfo last = null;
        if(!segmentsCopyStateQueue.isEmpty()){
            last = segmentsCopyStateQueue.poll();
        }
        // 注意：copy 完成之后需要执行decRefDeleter()
        curSegment = last;
        return last;
    }

    // TODO:需要判断是否在运行中
    synchronized public SegmentsCopyInfo  getLatest(){
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
    public void copyToOneReplica(RemoteTargetShardCopyState remoteTargetShardCopyState, SegmentsCopyInfo sci, ActionListener<Void> listener){
        final Store store = indexShard.store();
        if(Objects.nonNull(sci)){
            curSegment = sci;
        }

        final StepListener<SegmentsInfoResponse> sendSegmentsInfoStep = new StepListener<>();
        final StepListener<Void> sendFilesStep = new StepListener<>();
        final StepListener<Void> sendCheckpointStep = new StepListener<>();

        // 1.发送 segments info
        remoteTargetShardCopyState.sendSegmentsInfo(curSegment, internalActionTimeout, sendSegmentsInfoStep);

        // SegmentsInfo发送完成，处理结果
        sendSegmentsInfoStep.whenComplete(sir -> {
            Set<String> currFileNames = sir.fileNames;
            // 根据replca返回的文件列表，先验证文件是否存在
            if(!checkFileList(currFileNames)){
                finish();
            }
            Map<String, StoreFileMetadata> fileMetaDatas;
            // 拿到对应的文件元数据，用于验证
            fileMetaDatas = readFilesMetaData(currFileNames, sci);
        // 2.向replica发送文件
            remoteTargetShardCopyState.sendFiles(store, fileMetaDatas.values().toArray(new StoreFileMetadata[0]), sendFilesStep);
            },r -> {logger.error("send segments info failed", r);});

        // 文件发送完成，处理结果，并进行下一步
        // 3. 更新checkpoint
        // 向replica发送 global check point， 客户端 应用segments，并更新 local checkpoint 和 global check point，并返回local checkpoint
        // 根据各个副本返回的checkpoint，本地更新 global checkpoint
        sendFilesStep.whenComplete(r -> {},r -> {logger.error("send segments files failed2 ", r);});

        indexShard.getLocalCheckpoint();

        // 本地对应文件 delete def -1 （所有的replica执行完才能进行-1 操作！）
        finish();
    }

    public RemoteTargetShardCopyState initRemoteTargetShardCopyState(ShardId shardId, ShardRouting replicaRouting){
        String nodeId = replicaRouting.currentNodeId();
        final DiscoveryNode replicaNode = clusterService.state().nodes().get(nodeId);
        // replica 不存在，直接返回
        if (replicaNode == null) {
//                    失败处理,还是直接退出？
//                    listener.onFailure(new NoNodeAvailableException("unknown node [" + nodeId + "]"));
            return null;
        }

        // create target node processor
        final RemoteTargetShardCopyState remoteTargetShardCopyState = new RemoteTargetShardCopyState(shardId,
            transportService.getThreadPool(), transportService, clusterService.localNode(),replicaNode, internalActionTimeout,
            throttleTime -> addThrottleTime(throttleTime));
        replicas.put(replicaNode, remoteTargetShardCopyState);
        return remoteTargetShardCopyState;
    }

    public boolean checkFileList(Set<String> currFileNames){
        return true;
    }

    public void finish(){
        lastSegment = curSegment;
        curSegment = null;
    }

    public Map<String, StoreFileMetadata> readFilesMetaData(Set<String> fileNames, SegmentsCopyInfo sci) {
        Map<String, StoreFileMetadata> fileMetaDatas = new HashMap<>();
        fileNames.forEach(f -> fileMetaDatas.put(f, sci.filesMetadata.get(f)));
        return fileMetaDatas;
    }

    public StoreFileMetadata readLocalFileMetaData(String fileName, Directory dir, Version version) throws IOException {

        Map<String, StoreFileMetadata> cache = lastFileMetaData;
        StoreFileMetadata result;
        if (cache != null) {
            // We may already have this file cached from the last NRT point:
            result = cache.get(fileName);
        } else {
            result = null;
        }

        if (result == null) {
            // Pull from the filesystem
            String checksum;
            final BytesRefBuilder fileHash = new BytesRefBuilder();
            long length;
            try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
                try {
                    length = in.length();
                    if (length < CodecUtil.footerLength()) {
                        // truncated files trigger IAE if we seek negative... these files are really corrupted though
                        throw new CorruptIndexException("Can't retrieve checksum from file: " + fileName + " file length must be >= " +
                            CodecUtil.footerLength() + " but was: " + in.length(), in);
                    }
//                    if (readFileAsHash) {
//                        // additional safety we checksum the entire file we read the hash for...
//                        final Store.VerifyingIndexInput verifyingIndexInput = new Store.VerifyingIndexInput(in);
//                        hashFile(fileHash, new InputStreamIndexInput(verifyingIndexInput, length), length);
//                        checksum = digestToString(verifyingIndexInput.verify());
//                    } else {
//                        checksum = digestToString(CodecUtil.retrieveChecksum(in));
//                    }
                    // 不检测hash
                    checksum = digestToString(CodecUtil.retrieveChecksum(in));
                } catch (Exception ex) {
                    logger.debug(() -> new ParameterizedMessage("Can retrieve checksum from file [{}]", fileName), ex);
                    throw ex;
                }
            } catch (@SuppressWarnings("unused") FileNotFoundException | NoSuchFileException e) {
//                if (VERBOSE_FILES) {
//                    message("file " + fileName + ": will copy [file does not exist]");
//                }
                return null;
            }

            // NOTE: checksum is redundant w/ footer, but we break it out separately because when the bits
            // cross the wire we need direct access to
            // checksum when copying to catch bit flips:
//            result = new CopyFileMetaData(header, footer, length, checksum);
            result = new StoreFileMetadata(fileName, length, checksum, version);
        }

        return result;
    }

    /**
     * Cancels the recovery and interrupts all eligible threads.
     */
    public void cancel(String reason) {
        cancellableThreads.cancel(reason);
        replicas.values().forEach(TargetShardCopyState::cancel);
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

    public Map<DiscoveryNode, RemoteTargetShardCopyState> getReplicas() {
        return replicas;
    }
}
