package org.elasticsearch.indices.segmentscopy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.*;
import org.apache.lucene.store.*;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.engine.*;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.index.translog.ChannelFactory;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.recovery.*;
import org.elasticsearch.transport.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.elasticsearch.index.store.Store.CORRUPTED_MARKER_NAME_PREFIX;

/**
 * TargetShardCopyState 在接收文件过程中，会并发的接收，所以需要处理并发场景，但是其他阶段不允许并发！
 */
public class LocalTargetShardCopyState extends AbstractRefCounted implements TargetShardCopyState{
    private static final Logger logger = LogManager.getLogger(RemoteRecoveryTargetHandler.class);
    public final ShardId shardId;
    public final IndexShard indexShard;
    private  long timeout;
    private final DiscoveryNode sourceNode;
    private final Map<Object, RetryableAction<?>> onGoingRetryableActions = ConcurrentCollections.newConcurrentMap();
    private final AtomicLong bytesSinceLastPause = new AtomicLong();
    private final TransportRequestOptions fileChunkRequestOptions;
    private final CancellableThreads cancellableThreads = new CancellableThreads();
    private final boolean retriesSupported;
    private volatile boolean isCancelled = false;
    private final AtomicBoolean finished = new AtomicBoolean();
    private  AtomicLong requestSeqNo;
    AtomicBoolean isRunning = new AtomicBoolean(false);
    private SegmentsCopyInfo currSegmentsCopyInfo;
    private SegmentsCopyInfo lastSegmentsCopyInfo;
    private Map<String, String> copiedFiles = new HashMap<>();

    private final CopyMultiFileWriter multiFileWriter;
    private final CopyRequestTracker requestTracker = new CopyRequestTracker();
    public CopyReadElasticsearchReaderManager mgr;
    private static final String COPY_PREFIX = "copy.";


    public LocalTargetShardCopyState(IndexShard indexShard, DiscoveryNode sourceNode, Long internalActionTimeout) {
        super("copy_status");
        this.shardId = indexShard.shardId();
        this.indexShard =  indexShard;
        this.sourceNode = sourceNode;
        this.fileChunkRequestOptions = TransportRequestOptions.builder()
            .withType(TransportRequestOptions.Type.COPY)
            .withTimeout(internalActionTimeout)//超时时间
            .build();
        String tempFilePrefix = COPY_PREFIX + UUIDs.randomBase64UUID() + ".";
        this.multiFileWriter = new CopyMultiFileWriter(indexShard.store(), indexShard.recoveryState().getIndex(), tempFilePrefix, logger,
            ()->{});
//        this.retriesSupported = sourceNode.getVersion().onOrAfter(Version.V_7_9_0);
        this.retriesSupported = true;
        mgr = new CopyReadElasticsearchReaderManager((ElasticsearchDirectoryReader)(indexShard.acquireSearcher("123").getDirectoryReader()));
    }

    void receiveSegmentsInfo(SegmentsCopyInfo segmentsCopyInfo, ActionListener<TransportResponse> listener){
        // primaryTerm 是否正确
//        if(segmentsCopyInfo.primaryTerm != indexShard.getOperationPrimaryTerm()){
//            // 异常处理
//            listener.onResponse(new ErrorResponse(ErrorType.PRIMARY_TERM_ERROR));
//            return;
//        }
        // 设置当前同步的segments
        lastSegmentsCopyInfo = currSegmentsCopyInfo;
        currSegmentsCopyInfo = segmentsCopyInfo;
        // 判断version和
        SegmentInfos oldInfos = null;
        if(lastSegmentsCopyInfo !=null && lastSegmentsCopyInfo.infos != null){
            oldInfos = lastSegmentsCopyInfo.infos;
        }else{
            try (DirectoryReader reader = indexShard.getEngineOrNull().acquireSearcher("segmentCopy").getDirectoryReader()) {
                logger.error("SegmentCopyTargetService :get old infos");
                oldInfos = ((StandardDirectoryReader)((ElasticsearchDirectoryReader) reader).getDelegate()).getSegmentInfos();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        long version = oldInfos.getVersion();
        if(segmentsCopyInfo.version <= version){
            // 异常处理
            isRunning.set(false);
            listener.onResponse(new ErrorResponse(ErrorType.SEGMENTS_INFO_VERSION_ERROR));
            logger.error("SegmentCopyTargetService :version {} grater than old version {}", segmentsCopyInfo.version, version);
            return;
        }
        // 需要同步的文件列表
        Set<String> diffFiles = new HashSet<>(segmentsCopyInfo.files);
        Set<String> oldFiles = null;
        try {
            oldFiles = new HashSet<>(oldInfos.files(false));
        } catch (IOException e) {
            // TODO:异常处理！
            listener.onResponse(new ErrorResponse(ErrorType.GET_OLD_FILES_ERROR));
            isRunning.set(false);
            return;
        }
        diffFiles.removeAll(oldFiles);
        if(diffFiles.isEmpty()){
            //
            listener.onResponse(new ErrorResponse(ErrorType.NO_FILES_NEED_COPY_ERROR));
            isRunning.set(false);
            return;
        }
        // 正常返回
        logger.error("SegmentCopyTargetService : diff files: {}", diffFiles);
        listener.onResponse(new SegmentsInfoResponse(diffFiles));
    }

    /**
     * for test
     */
    void receiveFilesLocal(Path sourcePath, Path targetPath, List<String> files) {
        FileChannel inChannel = null;
        FileChannel outChannel = null;
        try {
            //获取通道
            for(String fileName: files){
                String tmpFileName = "tmp_"+fileName;
                copiedFiles.put(fileName,tmpFileName);
                inChannel = FileChannel.open(sourcePath.resolve(fileName), StandardOpenOption.READ);
                outChannel = FileChannel.open(targetPath.resolve(tmpFileName),StandardOpenOption.WRITE,StandardOpenOption.READ,StandardOpenOption.CREATE);
                inChannel.transferTo(0,inChannel.size(),outChannel);
                System.out.println("fileName:"+fileName+" copied to: "+tmpFileName);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            //关闭流
            try {
                if (outChannel != null) {
                    outChannel.close();
                }
                if (inChannel != null) {
                    inChannel.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    void receiveFilesLocal2(Directory sourceDirectory, Directory targetDirectory, List<String> files) {
        FileChannel inChannel = null;
        FileChannel outChannel = null;
        try {
            //获取通道
            for(String fileName: files){
                String tmpFileName = fileName+".copytmp";
                copiedFiles.put(fileName,tmpFileName);
                targetDirectory.copyFrom(sourceDirectory, fileName, tmpFileName, IOContext.DEFAULT);
                System.out.println("fileName:"+fileName+" copied to: "+tmpFileName);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            //关闭流
            try {
                if (outChannel != null) {
                    outChannel.close();
                }
                if (inChannel != null) {
                    inChannel.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void renameFiles(Directory dir) throws IOException {
        for (Map.Entry<String, String> ent : copiedFiles.entrySet()) {
            String tmpFileName = ent.getValue();
            String fileName = ent.getKey();

            // 如果部分文件重命名失败，这种问题不需要处理，最多也只会导致有一些无用的文件，不会造成其他问题，因为此时还没更新segment元数据
            dir.rename(tmpFileName, fileName);
        }
        copiedFiles.clear();
    }

    public SegmentInfos updateSegmentsInfo(Store store) throws IOException {
        // Turn byte[] back to SegmentInfos:
//        SegmentInfos lastInfos = null;
//        try{
//            lastInfos = store.readLastCommittedSegmentsInfo();
//        }catch(IOException e){
//            logger.error("readLastCommittedSegmentsInfo  readLastCommittedSegmentsInfo error:", e);
//        }

        SegmentInfos infos =
            SegmentInfos.readCommit(store.directory(), toIndexInput(currSegmentsCopyInfo.infosBytes), currSegmentsCopyInfo.gen);
        // for test
        // TODO 处理
//        mgr.setCurrentInfos(infos);
        Engine engine = indexShard.getEngineOrNull();
//        if(engine !=null && engine instanceof DataCopyReadEngine){
//            ((DataCopyReadEngine) engine).setCurrentInfos(infos);
//        }
        if(engine !=null && engine instanceof DataCopyEngine){
//            try{
//                if(lastInfos != null){
//                    if(((DataCopyEngine) engine).getLastCommittedSegmentInfosGen()<lastInfos.getGeneration()){
//                        ((DataCopyEngine) engine).setLastCommittedSegmentInfos(lastInfos);
//                    }
//                }
//            }catch (Exception e){
//                logger.error("readLastCommittedSegmentsInfo  readLastCommittedSegmentsInfo  update update error :", e);
//            }
            ((DataCopyEngine) engine).setCurrentInfos(infos);
            ((DataCopyEngine) engine).setLastRefreshedCheckpointCopy(currSegmentsCopyInfo.refreshedCheckpoint);
        }
        engine.refresh("test");
//        if(){
//        infos.commit();
//        }
        return infos;
    }

    private ChecksumIndexInput toIndexInput(byte[] input) {
        return new BufferedChecksumIndexInput(
            new ByteBuffersIndexInput(
                new ByteBuffersDataInput(Arrays.asList(ByteBuffer.wrap(input))), "SegmentInfos"));
    }
    private static void traverseFolder(File folder) {
        File[] files = folder.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    traverseFolder(file);  // 递归调用遍历子文件夹
                } else {
                    // 处理文件
                    System.out.println(file.getAbsolutePath());
                }
            }
        }
    }

    void finish(ArrayList<String> files, ActionListener<Void> listener) {

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

    private static class FileChunk implements MultiChunkTransfer.ChunkRequest, Releasable {
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

    public static class ErrorResponse extends TransportResponse {
        ErrorType errorType;

        ErrorResponse(ErrorType errorType){
            this.errorType = errorType;
        }

        ErrorResponse(StreamInput in) throws IOException {
            super(in);
            this.errorType = in.readEnum(ErrorType.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(errorType);
        }
    }

    public enum ErrorType{

        IS_RUNNING_ERROR(1,"copy is running"),
        PRIMARY_TERM_ERROR(2,"primary term not match"),
        SEGMENTS_INFO_VERSION_ERROR(3,"segments info version not match"),
        NO_FILES_NEED_COPY_ERROR(4,"no files need copy"),
        SHARD_ERROR(5,"shard status is error"),
        ENGINE_ERROR(6,"engine status is error"),
        GET_OLD_FILES_ERROR(7,"no files need copy"),

        ;
        int number;
        String message;
        ErrorType(int number, String message){
            this.number=number;
            this.message=message;
        }

        public int getNumber(){
            return this.number;
        }

        public String getMessage(){
            return this.message;
        }
    }

    @Override
    public void writeFileChunk(StoreFileMetadata fileMetadata, long position, BytesReference content,
                               boolean lastChunk, int totalTranslogOps, ActionListener<Void> listener) {
        try {
            multiFileWriter.writeFileChunk(fileMetadata, position, content, lastChunk);
            listener.onResponse(null);
        } catch (Exception e) {
            logger.error("trunk trunk trunk trunk trunk trunk write error: {} {}", e, fileMetadata.name());
            // TODO:liuyongheng 需要定位一下为什么会出现重复获取的问题，是不是segments信息没有同步？
            if(e.getMessage().contains("has already been created")){
                listener.onResponse(null);
            }else{
//                multiFileWriter.closeInternal();// TODO:liuyongheng 这里失败是有重试的，索引不能直接关闭所有正在同步的文件！
                // TODO:liuyongheng 在这里输出错误到日志中，看一下具体的文件，是不是没写进去，
                listener.onFailure(e);
                // TODO:liuyongheng 只有一个trunk失败的情况下不能直接停止，这里还会重试的！
//                isRunning.set(false);
            }
        }
    }

    @Override
    public void cleanFiles(long globalCheckpoint, Map<String, StoreFileMetadata> sourceMetadata,
                           ActionListener<Void> listener) {
        ActionListener.completeWith(listener, () -> {
//            state().getTranslog().totalOperations(totalTranslogOps);
            // first, we go and move files that were created with the recovery id suffix to
            // the actual names, its ok if we have a corrupted index here, since we have replicas
            // to recover from in case of a full cluster shutdown just when this code executes...
            multiFileWriter.renameAllTempFiles();
//            final Store store = store();
            //TODO:liuyongheng 这里需要确认是否可以这么写，有可能涉及到资源的引用和锁等
            final Store store = indexShard.store();
            store.incRef();
            try {
                // TODO: 清理的和验证要做，但是逻辑跟老的不一样，
                // 需要验证 某个segmnets info 对应的文件，而不是commit对应的文件，可能还没commit
                // 或者 commit 之前得，保持一致
//                store.cleanupAndVerify("recovery CleanFilesRequestHandler", sourceMetadata);
                checkFilesIntegrity(store, sourceMetadata);
//                deleteFiles(store, currSegmentsCopyInfo);
                if (indexShard.indexSettings().getIndexVersionCreated().before(Version.V_6_0_0_rc1)) {
                    store.ensureIndexHasHistoryUUID();
                }
//                if("segment".equals(indexShard.indexSettings().getSettings().get("index.datasycn.type", ""))){
//                    final ChannelFactory channelFactory = FileChannel::open;
//                    Translog.createEmptyTranslog(indexShard.shardPath().resolveTranslog(), shardId, globalCheckpoint,
//                        indexShard.getPendingPrimaryTerm(), store.getTranslogUUID(), channelFactory);
//                }else{
//                    final String translogUUID = Translog.createEmptyTranslog(
//                        indexShard.shardPath().resolveTranslog(), globalCheckpoint, shardId, indexShard.getPendingPrimaryTerm());
//                    store.associateIndexWithNewTranslog(translogUUID);
//                }

                if (indexShard.getRetentionLeases().leases().isEmpty()) {
                    // if empty, may be a fresh IndexShard, so write an empty leases file to disk
                    indexShard.persistRetentionLeases();
                    assert indexShard.loadRetentionLeases().leases().isEmpty();
                } else {
                    assert indexShard.assertRetentionLeasesPersisted();
                }
                // TODO:liuyongheng 确认需不需要check，不需要全检查，只需要检查增量copy过来的文件
//                indexShard.maybeCheckIndex();
                updateSegmentsInfo(store);
                deleteFiles(store, currSegmentsCopyInfo);
            } catch (CorruptIndexException | IndexFormatTooNewException | IndexFormatTooOldException ex) {
                // this is a fatal exception at this stage.
                // this means we transferred files from the remote that have not be checksummed and they are
                // broken. We have to clean up this shard entirely, remove all files and bubble it up to the
                // source shard since this index might be broken there as well? The Source can handle this and checks
                // its content on disk if possible.
                logger.error("clean clean clean clean clean clean files error", ex);
                try {
                    try {
                        store.removeCorruptionMarker();
                    } finally {
                        // clean up and delete all files
//                        Lucene.cleanLuceneIndex(store.directory());
                        // TODO: 失败的情况下，删除最新同步的segments文件
                    }
                } catch (Exception e) {
                    logger.debug("Failed to clean lucene index", e);
                    ex.addSuppressed(e);
                }
                // TODO：异常处理
//                RecoveryFailedException rfe = new RecoveryFailedException(state(), "failed to clean after recovery", ex);
//                fail(rfe, true);
//                throw rfe;
            } catch (Exception ex) {
//                throw ex;
                throw new RuntimeException(ex);
//                RecoveryFailedException rfe = new RecoveryFailedException(state(), "failed to clean after recovery", ex);
//                fail(rfe, true);
//                throw rfe;
            } finally {
                multiFileWriter.closeInternal();// TODO:liuyongheng multiFileWriter的清理非常重要
                store.decRef();
                isRunning.set(false);
            }
            return null;
        });
    }

    public void checkFilesIntegrity(Store store, Map<String, StoreFileMetadata> sourceMetadata){
        for(StoreFileMetadata md: sourceMetadata.values()){
            boolean res = store.checkIntegrityNoException(md);
            if(!res){
                logger.error("check Integrity check Integrity check Integrity check Integrity check Integrity {}", md.name());
            }
        }
        try {
            store.directory().syncMetaData();
        } catch (IOException e) {
            logger.error(e);
            throw new RuntimeException(e);
        }
    }

    // 不安全的删除方式！ 把历史的全删了，如果正好有查询在使用老的segments，会导致错误！
    // TODO:liuyongheng 这里是不是应该由 IndexDeletionPolicy 来删除？
    public boolean deleteFiles(Store store, SegmentsCopyInfo segmentsCopyInfo){
        boolean result = false;
        List<String> files = new LinkedList<>();
        try {
            Directory directory=  store.directory();
            for (String existingFile : directory.listAll()) {
                if (Store.isAutogenerated(existingFile) || segmentsCopyInfo.files.contains(existingFile)) {
                    // don't delete snapshot file, or the checksums file (note, this is extra protection since the Store won't delete
                    // checksum)
                    continue;
                }
                if(existingFile.startsWith(IndexFileNames.SEGMENTS)
                    || existingFile.equals(IndexFileNames.OLD_SEGMENTS_GEN)
                    || existingFile.startsWith(CORRUPTED_MARKER_NAME_PREFIX)){
                    continue;
                }
                files.add(existingFile);
            }
            directory.syncMetaData();
//            verifyAfterCleanup(sourceMetadata, metadataOrEmpty);
            store.deleteQuiet(files.toArray(new String [0]));
            result = true;
        }catch (Exception e){
            e.printStackTrace();
        }
        return result;
    }

    public void cancel(String reason) {
        if (finished.compareAndSet(false, true)) {
            try {
                logger.debug("recovery canceled (reason: [{}])", reason);
                cancellableThreads.cancel(reason);
            } finally {
            }
        }
    }

    public void  close() {
        try {
            multiFileWriter.close();
        } finally {

        }
    }


    @Override
    protected void closeInternal() {

    }

    public ActionListener<Void> markRequestReceivedAndCreateListener(long requestSeqNo, ActionListener<Void> listener) {
        return requestTracker.markReceivedAndCreateListener(requestSeqNo, listener);
    }

    public void setCurrSegmentsCopyInfo(SegmentsCopyInfo currSegmentsCopyInfo) {
        this.currSegmentsCopyInfo = currSegmentsCopyInfo;
    }

}
