package org.elasticsearch.indices.segmentscopy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.*;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.engine.SegmentsCopyInfo;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.indices.recovery.*;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;

public class LocalTargetShardCopyState implements TargetShardCopyState{
    private static final Logger logger = LogManager.getLogger(RemoteRecoveryTargetHandler.class);
    public final ShardId shardId;
    public final IndexShard indexShard;
    public final TransportService transportService;
    public final ThreadPool threadPool;
    private  long timeout;
    private final DiscoveryNode sourceNode;
    private final Map<Object, RetryableAction<?>> onGoingRetryableActions = ConcurrentCollections.newConcurrentMap();
    private final AtomicLong bytesSinceLastPause = new AtomicLong();
    private final TransportRequestOptions fileChunkRequestOptions;
    private final CancellableThreads cancellableThreads = new CancellableThreads();
    private final boolean retriesSupported;
    private volatile boolean isCancelled = false;
    private  AtomicLong requestSeqNo;
    AtomicBoolean isRunning = new AtomicBoolean(false);

    private SegmentsCopyInfo currSegmentsCopyInfo;
    private SegmentsCopyInfo lastSegmentsCopyInfo;
    private Map<String, String> copiedFiles = new HashMap<>();


    public LocalTargetShardCopyState(ShardId shardId, IndexShard indexShard, TransportService transportService,
                                     DiscoveryNode sourceNode, Long internalActionTimeout) {
        this.transportService = transportService;
        this.threadPool = transportService.getThreadPool();
        this.shardId = shardId;
        this.indexShard =  indexShard;
        this.sourceNode = sourceNode;
        this.fileChunkRequestOptions = TransportRequestOptions.builder()
            .withType(TransportRequestOptions.Type.COPY)
            .withTimeout(internalActionTimeout)//超时时间
            .build();
        this.retriesSupported = sourceNode.getVersion().onOrAfter(Version.V_7_9_0);
    }

    void receiveSegmentsInfo(SegmentsCopyInfo segmentsCopyInfo, ActionListener<TransportResponse> listener){
        // primaryTerm 是否正确
//        if(segmentsCopyInfo.primaryTerm != indexShard.getOperationPrimaryTerm()){
//            // 异常处理
//            listener.onResponse(new ErrorResponse(ErrorType.PRIMARY_TERM_ERROR));
//            return;
//        }
        // 判断version和
        SegmentInfos oldInfos = null;
        if(lastSegmentsCopyInfo !=null && lastSegmentsCopyInfo.infos != null){
            oldInfos = lastSegmentsCopyInfo.infos;
        }else{
            try (DirectoryReader reader = indexShard.getEngineOrNull().acquireSearcher("segmentCopy").getDirectoryReader()) {
//                oldInfos = ((StandardDirectoryReader) reader).getSegmentInfos();
                oldInfos = ((StandardDirectoryReader)((ElasticsearchDirectoryReader) reader).getDelegate()).getSegmentInfos();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        long version = oldInfos.getVersion();
        if(segmentsCopyInfo.version <= version){
            // 异常处理
            listener.onResponse(new ErrorResponse(ErrorType.SEGMENTS_INFO_VERSION_ERROR));
            return;
        }
        // 需要同步的文件列表
        Set<String> diffFiles = new HashSet<>(segmentsCopyInfo.files);
        Set<String> oldFiles = new HashSet<>();

        for (SegmentCommitInfo info : oldInfos) {
            try {
                for (String fileName : info.files()) {
                    oldFiles.add(fileName);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        diffFiles.removeAll(oldFiles);
        if(diffFiles.isEmpty()){
            //
            listener.onResponse(new ErrorResponse(ErrorType.NO_FILES_NEED_COPY_ERROR));
            return;
        }
        // 设置当前同步的segments
        currSegmentsCopyInfo = segmentsCopyInfo;
        // 正常返回
        listener.onResponse(new SegmentsInfoResponse(diffFiles));
    }

    void receiveFiles(Store store, StoreFileMetadata[] files, ActionListener<Void> listener) {

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
        ENGINE_ERROR(5,"engine status is error"),

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
}
