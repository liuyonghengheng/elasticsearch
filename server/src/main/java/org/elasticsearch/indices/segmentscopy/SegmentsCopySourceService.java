package org.elasticsearch.indices.segmentscopy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.FutureUtils;
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

public class SegmentsCopySourceService extends AbstractLifecycleComponent implements IndexEventListener, ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(SegmentsCopySourceService.class);
    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(logger.getName());

    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final ThreadPool threadPool;
    private final OngoingCopies ongoingCopies = new OngoingCopies();
    private final SegmentsCopySettings segmentsCopySettings;

    @Inject
    public SegmentsCopySourceService(IndicesService indicesService, ClusterService clusterService, TransportService transportService,
                                     SegmentsCopySettings segmentsCopySettings) {
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.threadPool = transportService.getThreadPool();
        this.segmentsCopySettings = segmentsCopySettings;
    }

    public boolean initNewShardCopy(ShardId shardId) {
        return ongoingCopies.initNewShardCopy(shardId);
    }

    public void copyOneShard(SourceShardCopyState sourceShardCopyState) {
        IndexShard indexShard = sourceShardCopyState.getIndexShard();
        ShardId shardId = sourceShardCopyState.getShardId();
        if (!checkShardState(shardId, indexShard, sourceShardCopyState)) {
            // primary shard状态不对，停止已有的任务，删除, 并返回！
            return;
        }
        if (sourceShardCopyState.isCopying.get()) {
            //正在运行直接跳过
            return;
        }
        ShardRouting primaryRouting = indexShard.routingEntry();
        ReplicationGroup replicationGroup = indexShard.getReplicationGroup();
        SegmentsCopyInfo sci = sourceShardCopyState.getLatest();
        for (final ShardRouting replicaRouting : replicationGroup.getReplicationTargets()) {
            if (replicaRouting.isSameAllocation(primaryRouting) == false) {
                RemoteTargetShardCopyState target =
                    sourceShardCopyState.initRemoteTargetShardCopyState(shardId, replicaRouting);
                if(target == null){
                    //TODO 需要处理吗？
                    continue;
                }
                ongoingCopies.addTargetShardCopyState(target);
                sourceShardCopyState.copyToOneReplica(target, sci, ActionListener.wrap(() -> ongoingCopies.removeTarget(target)));
            }
        }
    }

    /**
     * 先停止任务，然后删除
     */
    public void removeSourceShardCopyState(ShardId shardId, SourceShardCopyState sourceShardCopyState) {

    }

    public void stopShardCopy() {

    }

    /**
     * 当队列有数据时，进行分发任务
     */
    public void doCopy() {
        while (true) {
            ShardId shardId = null;
            logger.error("SegmentsCopySource:doCopy get shardId");
            try {
                shardId = SourceShardCopyState.blockingQueue.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            logger.error("SegmentsCopySource:doCopy shardId:{}",shardId);
            if (ongoingCopies.initNewShardCopy(shardId)) {
                copyOneShard(ongoingCopies.shardCopyStates.get(shardId));
            }
        }
    }

    public boolean checkShardState(ShardId shardId, IndexShard indexShard, SourceShardCopyState sourceShardCopyState) {
        if (!indexShard.routingEntry().primary() || indexShard.state() != IndexShardState.STARTED) {
            removeSourceShardCopyState(shardId, sourceShardCopyState);
            return false;
        }
        return true;
    }

    /**
     * 清理掉不再是primary 的shard！
     */
    public void pruneList() {

    }

    /**
     * for test
     */
    protected List<SourceShardCopyState> getShardCopyStates() {
        return new ArrayList<>(ongoingCopies.shardCopyStates.values());
    }

    public OngoingCopies getOngoingCopies() {
        return ongoingCopies;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.nodesRemoved()) {
            for (DiscoveryNode removedNode : event.nodesDelta().removedNodes()) {
                ongoingCopies.cancelOnNodeLeft(removedNode);
            }
        }
    }

    @Override
    protected void doStart() {
        final ClusterService clusterService = indicesService.clusterService();
        if (DiscoveryNode.isDataNode(clusterService.getSettings())) {
            clusterService.addListener(this);
        }
        threadPool.executor(ThreadPool.Names.SAME).execute(this::doCopy);
    }

    @Override
    protected void doStop() {
        final ClusterService clusterService = indicesService.clusterService();
        if (DiscoveryNode.isDataNode(clusterService.getSettings())) {
            ongoingCopies.awaitEmpty();
            indicesService.clusterService().removeListener(this);
        }
    }

    @Override
    protected void doClose() throws IOException {

    }

    /**
     * TODO： 这里有个问题，当shard被关闭，但是队列中还有数据时，下次被消费到可能会再次启动！！！！ 这个需要处理
     * 重新启动之前增加判断 shard状态，以及这里需要清空 内部的segments info 队列，即便阻塞队列中有数据，但是实际
     * 没数据也不需要处理！
     * 如果再shard内部管理文件复制任务可能就不会有这些问题，生命周期可以更好的和shard对齐。
     */
    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard,
                                       Settings indexSettings) {
        if (indexShard != null) {
            ongoingCopies.cancel(indexShard, "shard is closed");
        }
    }

    //
    final public class OngoingCopies {
        private final Map<ShardId, SourceShardCopyState> shardCopyStates = new HashMap<>();
        private final Map<DiscoveryNode, Collection<RemoteTargetShardCopyState>> nodeToHandlers = new HashMap<>();

        @Nullable
        private List<ActionListener<Void>> emptyListeners;

        /**
         * 如果相同的shard 已经存在，需要把老的任务都停掉，然后直接覆盖新的状态！
         */
        public void addSourceShardCopyState(ShardId shardId, SourceShardCopyState sourceShardCopyState) {
            this.shardCopyStates.put(shardId, sourceShardCopyState);
        }

        public void addTargetShardCopyState(RemoteTargetShardCopyState targetShardCopyState){
            nodeToHandlers.computeIfAbsent(targetShardCopyState.getTargetNode(), k -> new HashSet<>()).add(targetShardCopyState);
        }

        public boolean initNewShardCopy(ShardId shardId) {
            if (shardCopyStates.containsKey(shardId)) {
                return true;
            }
            if (initIndexShard(shardId)) {
                return true;
            }
            return false;
        }

        public boolean initIndexShard(ShardId shardId) {
            try {
                IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
                IndexShard indexShard = indexService.getShard(shardId.id());
                Engine engine = indexShard.getEngineOrNull();

                if (engine != null && engine instanceof DataCopyEngine) {
                    SourceShardCopyState ss = ((DataCopyEngine) engine).getSourceShardCopyState();
                    ss.initSourceShardCopyState(logger, clusterService, transportService, indexShard,
                        indexService.getIndexSettings()
                            .getSettings().getAsMemory("index.datasycn.segment.chunk_size", "10M").bytesAsInt(),
                        indexService.getIndexSettings()
                            .getSettings().getAsInt("index.datasycn.segment.shard.max_concurrent_file_chunks", 1),
                        indexService.getIndexSettings()
                            .getSettings().getAsLong("index.datasycn.segment.shard.internal_action_timeout", 500L),
                        indexService.getIndexSettings()
                            .getSettings().getAsLong("index.datasycn.segment.timeout", 500L),
                        indexShard.getThreadPool());
                    shardCopyStates.put(shardId, ss);
                    return true;
                } else if (engine == null) {
                    // 如果目前engine是null 还要添加回队列，等待下次检查
                    return false;
                }
            } catch (IndexNotFoundException e) {
                logger.info("Index Not Found", e);
                return false;
            }
            return true;
        }

        public boolean initIndexShard(IndexShard indexShard, int chunkSize, int concurrentChunks, long interTimeout, long timeout) {
            try {
                Engine engine = indexShard.getEngineOrNull();

                if (engine != null && engine instanceof DataCopyEngine) {
                    SourceShardCopyState ss = ((DataCopyEngine) engine).getSourceShardCopyState();
                    ss.initSourceShardCopyState(logger, clusterService, transportService, indexShard,
                        chunkSize, concurrentChunks, interTimeout, timeout,
                        indexShard.getThreadPool());
                    shardCopyStates.put(indexShard.shardId(), ss);
                    return true;
                } else if (engine == null) {
                    // 如果目前engine是null 还要添加回队列，等待下次检查
                    return false;
                }
            } catch (IndexNotFoundException e) {
                logger.info("Index Not Found", e);
                return false;
            }
            return true;
        }

        synchronized void cancelOnNodeLeft(DiscoveryNode node) {
            final Collection<RemoteTargetShardCopyState> handlers = nodeToHandlers.get(node);
            if (handlers != null) {
                for (RemoteTargetShardCopyState handler : handlers) {
                    handler.cancel();
                }
            }
        }

        /**
         * 将指定shard从任务中移除，一般为shard被删除，或者shard从主变成副本。
         */
        synchronized void remove(IndexShard shard) {
            SourceShardCopyState shardCopyState = shardCopyStates.get(shard.shardId());
            assert shardCopyState != null : "Segments copy Shard was not registered [" + shard + "]";
            if (shardCopyState != null) {
                shardCopyState.cancel("remove primary shard");
//                assert nodeToHandlers.getOrDefault(removed.targetNode(), Collections.emptySet()).contains(removed)
//                    : "Remote recovery was not properly tracked [" + removed + "]";
                shardCopyState.getReplicas().forEach((k,v) -> {
                    nodeToHandlers.computeIfPresent(k, (k2, handlersForNode) -> {
                        handlersForNode.remove(v);
                        if (handlersForNode.isEmpty()) {
                            return null;
                        }
                        return handlersForNode;
                    });
                });
            }
            // 删除
            shardCopyStates.remove(shard.shardId());
            if (shardCopyStates.isEmpty()) {
                if (emptyListeners != null) {
                    final List<ActionListener<Void>> onEmptyListeners = emptyListeners;
                    emptyListeners = null;
                    ActionListener.onResponse(onEmptyListeners, null);
                }
            }
        }

        /**
         * 复制完成删除对应的运行中副本信息！
         */
        synchronized void removeTarget(RemoteTargetShardCopyState target) {
            nodeToHandlers.computeIfPresent(target.getTargetNode(), (k, handlersForNode) -> {
                handlersForNode.remove(target);
                if (handlersForNode.isEmpty()) {
                    return null;
                }
                return handlersForNode;
            });
        }
        /**
         * 移除指定shard ，关闭相关的所有任务
         */
        synchronized void removeSource(IndexShard shard, String reason) {
            SourceShardCopyState shardCopyState = shardCopyStates.remove(shard.shardId());
            if (shardCopyState != null) {
                final List<Exception> failures = new ArrayList<>();
                try {
                    shardCopyState.cancel(reason);
                } catch (Exception ex) {
                    failures.add(ex);
                } finally {
                    // TODO 这里可以根据需要添加一些处理
//                    shard.recoveryStats().decCurrentAsSource();
                }
                ExceptionsHelper.maybeThrowRuntimeAndSuppress(failures);
            }
        }
        /**
         * 取消 制定shard 相关的所有任务
         */
        synchronized void cancel(IndexShard shard, String reason) {
            SourceShardCopyState shardCopyState = shardCopyStates.get(shard.shardId());
            if (shardCopyState != null) {
                final List<Exception> failures = new ArrayList<>();
                try {
                    shardCopyState.cancel(reason);
                } catch (Exception ex) {
                    failures.add(ex);
                } finally {
                    // TODO 这里可以根据需要添加一些处理
//                    shard.recoveryStats().decCurrentAsSource();
                }
                ExceptionsHelper.maybeThrowRuntimeAndSuppress(failures);
            }
        }

        void awaitEmpty() {
            assert lifecycle.stoppedOrClosed();
            final PlainActionFuture<Void> future;
            synchronized (this) {
                if (shardCopyStates.isEmpty()) {
                    return;
                }
                future = new PlainActionFuture<>();
                if (emptyListeners == null) {
                    emptyListeners = new ArrayList<>();
                }
                emptyListeners.add(future);
            }
            FutureUtils.get(future);
        }

    }

    private class CopyMonitor extends AbstractRunnable {
        private final ShardId shardId;
        private final DiscoveryNode node;
        private final TimeValue checkInterval;

        private volatile long lastSeenAccessTime;

        private CopyMonitor(ShardId shardId, DiscoveryNode node, long lastSeenAccessTime, TimeValue checkInterval) {
            this.shardId = shardId;
            this.node = node;
            this.checkInterval = checkInterval;
            this.lastSeenAccessTime = lastSeenAccessTime;
        }

        @Override
        public void onFailure(Exception e) {
            logger.error(() -> new ParameterizedMessage("unexpected error while monitoring copy [{} replica {}]", shardId, node), e);
        }

        @Override
        protected void doRun() throws Exception {
            RemoteTargetShardCopyState status = ongoingCopies.shardCopyStates.get(shardId).getReplicas().get(node);
            if (status == null) {
                logger.trace("[monitor] no status found for [{} replica {}], shutting down", shardId, node);
                return;
            }
            long accessTime = status.getLastAccessTime();
            if (accessTime == lastSeenAccessTime) {
                String message = "no activity after [" + checkInterval + "]";
//                failRecovery(recoveryId,
//                    new RecoveryFailedException(status.state(), message, new ElasticsearchTimeoutException(message)),
//                    true // to be safe, we don't know what go stuck
//                );
                return;
            }
            lastSeenAccessTime = accessTime;
            logger.trace("[monitor] rescheduling check for [{} replica {}]. last access time is [{}]", shardId, node,lastSeenAccessTime);
            threadPool.schedule(this, checkInterval, ThreadPool.Names.GENERIC);
        }
    }

}
