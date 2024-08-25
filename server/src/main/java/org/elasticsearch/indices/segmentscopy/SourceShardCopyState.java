package org.elasticsearch.indices.segmentscopy;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.Version;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.core.internal.io.IOUtils;
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

import java.io.Closeable;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.stream.StreamSupport;

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
    private AtomicInteger runningReplicas = new AtomicInteger(0);
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
// TODO:这里应该是每个router（replica）对应一个 seqno?
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

    public boolean getIsCopying() {
        return isCopying.get();
    }

    public void setIsCopying(boolean val) {
        isCopying.set(val);
    }

    public SegmentsCopyInfo getCurSegment() {
        return curSegment;
    }

    public AtomicLong getRequestSeqNoGenerator() {
        return requestSeqNoGenerator;
    }

    public void incrRnningReplicas(){
        runningReplicas.incrementAndGet();
    }

    public void decrRnningReplicas(){
        if(runningReplicas.decrementAndGet() == 0){
            isCopying.set(false);
            curSegment.decRefDeleter();
        }
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
    synchronized public SegmentsCopyInfo  pollLatestSci(){
        SegmentsCopyInfo latest = null;
        while(!segmentsCopyStateQueue.isEmpty()){
            latest = segmentsCopyStateQueue.poll();
            if(!segmentsCopyStateQueue.isEmpty()){
                latest.decRefDeleter();
            }
        }
        // 注意：copy 完成之后需要执行decRefDeleter()
        lastSegment = curSegment;
        curSegment = latest;
        return latest;
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
    public void copyToOneReplica(RemoteTargetShardCopyState remoteTargetShardCopyState, ActionListener<Void> listener){

        final ListenableFuture<Void> future = new ListenableFuture<>();
        future.addListener(listener, EsExecutors.newDirectExecutorService());
        final List<Closeable> resources = new CopyOnWriteArrayList<>();
        final Closeable releaseResources = () -> IOUtils.close(resources);
        final Consumer<Exception> onFailure = e -> {
            assert Transports.assertNotTransportThread(SourceShardCopyState.this + "[onFailure]");
            IOUtils.closeWhileHandlingException(releaseResources, () -> future.onFailure(e));
        };

        final StepListener<SegmentsInfoResponse> sendSegmentsInfoStep = new StepListener<>();
        final StepListener<Void> sendFilesStep = new StepListener<>();
        final StepListener<Void> cleanFilesStep = new StepListener<>();
        // checkpoint 在index同步过程中已经更新了
//        final StepListener<Void> sendCheckpointStep = new StepListener<>();

        // TODO:liuyongheng 这里，我们是shard正常运行过程中执行的 segments copy，这意味着
        // 如果store被关闭，那么indexshard的状态一定是不对的！，因此是不是没有必要执行这个操作？
        final Releasable releaseStore = acquireStore(indexShard.store());
        resources.add(releaseStore);
        sendFilesStep.whenComplete(r -> IOUtils.close(releaseStore), e -> {
            try {
                IOUtils.close(releaseStore);
            } catch (final IOException ex) {
                logger.warn("releasing store caused exception", ex);
            }
        });

        final Store store = indexShard.store();
        // 1.发送 segments info
        remoteTargetShardCopyState.sendSegmentsInfo(curSegment, internalActionTimeout, sendSegmentsInfoStep);

        // SegmentsInfo发送完成，处理结果
        sendSegmentsInfoStep.whenComplete(sir -> {
            Set<String> currFileNames = sir.fileNames;
            // 根据replca返回的文件列表，先验证文件是否存在
            if(!checkFileList(currFileNames)){
                decrRnningReplicas();
            }
            final Map<String, StoreFileMetadata> fileMetaDatas;
            // 拿到对应的文件元数据，用于验证
            fileMetaDatas = readFilesMetaData(currFileNames, curSegment);
        // 2.向replica发送文件
            remoteTargetShardCopyState.sendFiles(store, fileMetaDatas.values().toArray(new StoreFileMetadata[0]), sendFilesStep);
            },
            onFailure);
//            r -> {logger.error("send segments info failed", r);});

        // 文件发送完成，处理结果，并进行下一步
        // 3. 更新checkpoint
        // 向replica发送 global check point， 客户端 应用segments，并更新 local checkpoint 和 global check point，并返回local checkpoint
        // 根据各个副本返回的checkpoint，本地更新 global checkpoint
        sendFilesStep.whenComplete(
            r -> {
                final long lastKnownGlobalCheckpoint = indexShard.getLastKnownGlobalCheckpoint();
//                final Store.MetadataSnapshot recoverySourceMetadata;
//                try {
//                    recoverySourceMetadata = store.getMetadata(indexShard.acquireSafeIndexCommit().getIndexCommit());
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
                cleanFiles(remoteTargetShardCopyState, store, curSegment.filesMetadata, lastKnownGlobalCheckpoint, cleanFilesStep);
            },
            onFailure);
//            r -> {logger.error("send segments files failed2", r);});

        cleanFilesStep.whenComplete(
            r -> {
                // 本地对应文件 delete def -1 （所有的replica执行完才能进行-1 操作！）
                decrRnningReplicas();
                IOUtils.close(resources);
            },
            onFailure
//            e -> {logger.error("send segments clean files failed ", e);}
        );
    }

    private Releasable acquireStore(Store store) {
        store.incRef();
        return Releasables.releaseOnce(() -> runWithGenericThreadPool(store::decRef));
    }

    private void runWithGenericThreadPool(CheckedRunnable<Exception> task) {
        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        assert threadPool.generic().isShutdown() == false;
        // TODO: We shouldn't use the generic thread pool here as we already execute this from the generic pool.
        //       While practically unlikely at a min pool size of 128 we could technically block the whole pool by waiting on futures
        //       below and thus make it impossible for the store release to execute which in turn would block the futures forever
        threadPool.generic().execute(ActionRunnable.run(future, task));
        FutureUtils.get(future);
    }

    private void cleanFiles(TargetShardCopyState remoteTargetShardCopyState, Store store, Map<String, StoreFileMetadata> sourceMetadata,
                            long globalCheckpoint, ActionListener<Void> listener) {
        // Send the CLEAN_FILES request, which takes all of the files that
        // were transferred and renames them from their temporary file
        // names to the actual file names. It also writes checksums for
        // the files after they have been renamed.
        //
        // Once the files have been renamed, any other files that are not
        // related to this recovery (out of date segments, for example)
        // are deleted
        cancellableThreads.checkForCancel();
        //TODO:liuyongheng 删掉Store.MetadataSnapshot类型的sourceMetadata！因为已经在segmentsinfo中传过去了！
        //或者包含更全的sourceMetadata，而不是仅仅只是lastcommit
        remoteTargetShardCopyState.cleanFiles(globalCheckpoint, sourceMetadata,
            ActionListener.delegateResponse(listener, (l, e) -> ActionListener.completeWith(l, () -> {
                StoreFileMetadata[] mds = curSegment.filesMetadata.values().toArray(new StoreFileMetadata[0]);
                ArrayUtil.timSort(mds, Comparator.comparingLong(StoreFileMetadata::length)); // check small files first
                handleErrorOnSendFiles(store, e, mds);
                throw e;
            })));
    }

    private void handleErrorOnSendFiles(Store store, Exception e, StoreFileMetadata[] mds) throws Exception {
        final IOException corruptIndexException = ExceptionsHelper.unwrapCorruption(e);
        assert Transports.assertNotTransportThread(SourceShardCopyState.this + "[handle error on send/clean files]");
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
//                    failEngine(corruptIndexException);
                }
            }
            if (localException != null) {
                throw localException;
            } else { // corruption has happened on the way to replica
                RemoteTransportException remoteException = new RemoteTransportException(
                    "File corruption occurred on recovery but checksums are ok", null);
                remoteException.addSuppressed(e);
//                logger.warn(() -> new ParameterizedMessage("{} Remote file corruption on node {}, recovering {}. local checksum OK",
//                    shardId, request.targetNode(), mds), corruptIndexException);
                logger.warn(() -> new ParameterizedMessage("{} Remote file corruption on node {}. local checksum OK",
                    shardId, mds), corruptIndexException);
                logger.warn(() -> new ParameterizedMessage("{} Remote file corruption on node, recovering {}. local checksum OK",
                    shardId, mds), corruptIndexException);
                throw remoteException;
            }
        }
        throw e;
    }


    public RemoteTargetShardCopyState initRemoteTargetShardCopyState(ShardId shardId, ShardRouting replicaRouting, AtomicLong  requestSeqNoGenerator){
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
            transportService.getThreadPool(), transportService, clusterService.localNode(),replicaNode, requestSeqNoGenerator, internalActionTimeout,
            throttleTime -> addThrottleTime(throttleTime));
        // TODO: bug，之前忘了在初始化时配置了，临时在这里配置，后续需要统一的settings进行配置！
        remoteTargetShardCopyState.setChunkSizeInBytes(1024 * 1024 * 10 - 16);
        replicas.put(replicaNode, remoteTargetShardCopyState);
        return remoteTargetShardCopyState;
    }

    public boolean checkFileList(Set<String> currFileNames){
        return true;
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
