package org.elasticsearch.indices.segmentscopy;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.DataCopyEngine;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.engine.SegmentsCopyInfo;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.PeerRecoverySourceService;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.recovery.StartRecoveryRequest;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.mockito.internal.matchers.Any;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class SegmentsCopySourceServiceTests extends IndexShardTestCase {

    public void testApplyCopiedSegmentsInfoFiles() throws IOException {
        // service
        final IndicesService indicesService = mock(IndicesService.class);
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(NodeRoles.dataNode());
        when(indicesService.clusterService()).thenReturn(clusterService);
        SegmentsCopySourceService segmentsCopySourceService = new SegmentsCopySourceService(
            indicesService, clusterService, mock(TransportService.class));
        // shard
        ShardId shardId = new ShardId("index", "_na_", 0);
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder("index")
            .settings(Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING.getKey(), Long.MAX_VALUE, TimeUnit.NANOSECONDS)
                .put("index.datasycn.type", "segment"))
            .state(IndexMetadata.State.CLOSE).primaryTerm(0, 75);
        ShardRouting shardRouting = TestShardRouting.newShardRouting(shardId, randomAlphaOfLength(8), true,
            ShardRoutingState.INITIALIZING, RecoverySource.EmptyStoreRecoverySource.INSTANCE);
        AtomicBoolean synced = new AtomicBoolean();
        IndexShard primaryShard = newShard(shardRouting, indexMetadata.build(), null, DataCopyEngine::new,
            () -> synced.set(true), RetentionLeaseSyncer.EMPTY);
        recoverShardFromStore(primaryShard);

        IndexShard replicaShard = newShard(true);
        recoverShardFromStore(replicaShard);

        int cnt = primaryShard.getEngineOrNull().acquireSearcher("test").getDirectoryReader().getDocCount("_id");
        System.out.println("primary cnt:"+cnt);

        cnt = replicaShard.getEngineOrNull().acquireSearcher("test").getDirectoryReader().getDocCount("_id");
        System.out.println("replica cnt:"+cnt);
        // write doc，refresh
        indexDoc(primaryShard, "_doc", "1");
        indexDoc(primaryShard, "_doc", "2");
        indexDoc(primaryShard, "_doc", "3");
        primaryShard.refresh("test");
        cnt = primaryShard.getEngineOrNull().acquireSearcher("test").getDirectoryReader().getDocCount("_id");
        System.out.println("primary cnt:"+cnt);
        indexDoc(primaryShard, "_doc", "4");
        indexDoc(primaryShard, "_doc", "5");
        indexDoc(primaryShard, "_doc", "6");
        primaryShard.refresh("test");
        cnt = primaryShard.getEngineOrNull().acquireSearcher("test").getDirectoryReader().getDocCount("_id");
        System.out.println("primary cnt:"+cnt);

        cnt = replicaShard.getEngineOrNull().acquireSearcher("test").getDirectoryReader().getDocCount("_id");
        System.out.println("replica cnt:"+cnt);

        final DiscoveryNode pNode = getFakeDiscoNode(primaryShard.routingEntry().currentNodeId());
        final DiscoveryNode rNode = getFakeDiscoNode(replicaShard.routingEntry().currentNodeId());
        //复制
        cnt = replicaShard.getEngineOrNull().acquireSearcher("test").getDirectoryReader().getDocCount("_id");
        System.out.println("replica cnt:"+cnt);
        // 添加shardid
//        segmentsCopySourceService.addNewShardId(sourceShard.shardId());
        // 处理shardid
        segmentsCopySourceService.initIndexShard(primaryShard, 10240, 1, 10000,20000);
        // 执行拷贝，没有副本不会真正执行
//        segmentsCopySourceService.doShardsCopy();
        // 执行
        SourceShardCopyState sourceShardCopyState = segmentsCopySourceService.getShardCopyStates().get(0);

        SegmentsCopyInfo sci = sourceShardCopyState.get();

        sci.files.stream().forEach(name -> System.out.println(name));


        final ShardPath primaryShardPath = primaryShard.shardPath();
        final ShardPath replicaShardPath = replicaShard.shardPath();

        System.out.println(primaryShardPath.getDataPath());
        System.out.println(replicaShardPath.getDataPath());

        File folder = new File(primaryShardPath.getDataPath().toUri());
        if (folder.exists() && folder.isDirectory()) {
            traverseFolder(folder);
        } else {
            System.out.println("文件夹不存在");
        }

        Path primaryShardPathIndex = primaryShardPath.resolveIndex();
        Path replicaShardPathIndex = replicaShardPath.resolveIndex();

        System.out.println(primaryShardPathIndex);
        System.out.println(replicaShardPathIndex);

        final TransportService transportService = mock(TransportService.class);
        LocalTargetShardCopyState localTargetShardCopyState = new LocalTargetShardCopyState(
            replicaShard.shardId(), replicaShard, transportService,
            pNode, 10000L);


//        replicaShard.store().directory().copyFrom();
        localTargetShardCopyState.receiveFilesLocal(primaryShardPathIndex, replicaShardPathIndex, sci.files);

        folder = new File(replicaShardPath.getDataPath().toUri());
        if (folder.exists() && folder.isDirectory()) {
            traverseFolder(folder);
        } else {
            System.out.println("文件夹不存在");
        }

        sci.decRefDeleter();

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

    public void test4() throws IOException {
        // service
        final IndicesService indicesService = mock(IndicesService.class);
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(NodeRoles.dataNode());
        when(indicesService.clusterService()).thenReturn(clusterService);
        SegmentsCopySourceService segmentsCopySourceService = new SegmentsCopySourceService(
            indicesService, clusterService, mock(TransportService.class));
        // shard
        ShardId shardId = new ShardId("index", "_na_", 0);
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder("index")
            .settings(Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING.getKey(), Long.MAX_VALUE, TimeUnit.NANOSECONDS)
                .put("index.datasycn.type", "segment"))
            .state(IndexMetadata.State.CLOSE).primaryTerm(0, 75);
        ShardRouting shardRouting = TestShardRouting.newShardRouting(shardId, randomAlphaOfLength(8), true,
            ShardRoutingState.INITIALIZING, RecoverySource.EmptyStoreRecoverySource.INSTANCE);
        AtomicBoolean synced = new AtomicBoolean();
        IndexShard primaryShard = newShard(shardRouting, indexMetadata.build(), null, DataCopyEngine::new,
            () -> synced.set(true), RetentionLeaseSyncer.EMPTY);
        recoverShardFromStore(primaryShard);

        IndexShard replicaShard = newShard(true);
        recoverShardFromStore(replicaShard);

        int cnt = primaryShard.getEngineOrNull().acquireSearcher("test").getDirectoryReader().getDocCount("_id");
        System.out.println("primary cnt:"+cnt);

        cnt = replicaShard.getEngineOrNull().acquireSearcher("test").getDirectoryReader().getDocCount("_id");
        System.out.println("replica cnt:"+cnt);
        // write doc，refresh
        indexDoc(primaryShard, "_doc", "1");
        indexDoc(primaryShard, "_doc", "2");
        indexDoc(primaryShard, "_doc", "3");
        primaryShard.refresh("test");
        cnt = primaryShard.getEngineOrNull().acquireSearcher("test").getDirectoryReader().getDocCount("_id");
        System.out.println("primary cnt:"+cnt);
        indexDoc(primaryShard, "_doc", "4");
        indexDoc(primaryShard, "_doc", "5");
        indexDoc(primaryShard, "_doc", "6");
        primaryShard.refresh("test");
        cnt = primaryShard.getEngineOrNull().acquireSearcher("test").getDirectoryReader().getDocCount("_id");
        System.out.println("primary cnt:"+cnt);

        cnt = replicaShard.getEngineOrNull().acquireSearcher("test").getDirectoryReader().getDocCount("_id");
        System.out.println("replica cnt:"+cnt);


        final DiscoveryNode pNode = getFakeDiscoNode(primaryShard.routingEntry().currentNodeId());
        final DiscoveryNode rNode = getFakeDiscoNode(replicaShard.routingEntry().currentNodeId());
        //复制
        cnt = replicaShard.getEngineOrNull().acquireSearcher("test").getDirectoryReader().getDocCount("_id");
        System.out.println("replica cnt:"+cnt);
        // 添加shardid
//        segmentsCopySourceService.addNewShardId(sourceShard.shardId());

        // 处理shardid
        segmentsCopySourceService.initIndexShard(primaryShard, 10240, 1, 10000,20000);
        // 执行拷贝，没有副本不会真正执行
//        segmentsCopySourceService.doShardsCopy();
        // 执行
        SourceShardCopyState sourceShardCopyState = segmentsCopySourceService.getShardCopyStates().get(0);

        SegmentsCopyInfo sci = sourceShardCopyState.get();
//        sourceShardCopyState.copyToOneReplica(sourceShard.shardId(), targetShard.routingEntry(), sci);

        final TransportService transportService = mock(TransportService.class);
//        when(transportService.sendRequest(any(DiscoveryNode.class), anyString(), any(), any(),any())).thenReturn(null);
//        when(transportService).thenReturn(null);

        final RemoteTargetShardCopyState remoteTargetShardCopyState = new RemoteTargetShardCopyState(
            primaryShard.shardId(), transportService, pNode, rNode, 10000L,null);

        // 发送 segments info
//        remoteTargetShardCopyState.sendSegmentsInfo(sci, 10000L, null);

        LocalTargetShardCopyState localTargetShardCopyState = new LocalTargetShardCopyState(
            replicaShard.shardId(), replicaShard, transportService,
            pNode, 10000L);

        localTargetShardCopyState.receiveSegmentsInfo(sci,null);
    }

    public void test3() throws IOException {
        IndexShard replicaShard = newShard(true);
        recoverShardFromStore(replicaShard);
        int cnt = replicaShard.getEngineOrNull().acquireSearcher("test").getDirectoryReader().getDocCount("_id");
        System.out.println("replica cnt:"+cnt);
        // write doc，refresh
        indexDoc(replicaShard, "_doc", "1");
        indexDoc(replicaShard, "_doc", "2");
        indexDoc(replicaShard, "_doc", "3");
        replicaShard.refresh("test");

        cnt = replicaShard.getEngineOrNull().acquireSearcher("test").getDirectoryReader().getDocCount("_id");
        System.out.println("replica cnt:"+cnt);
    }

}
