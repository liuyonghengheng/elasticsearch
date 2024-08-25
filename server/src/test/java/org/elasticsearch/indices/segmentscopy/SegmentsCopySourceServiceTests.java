package org.elasticsearch.indices.segmentscopy;

import org.apache.lucene.store.Directory;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.DataCopyEngine;
import org.elasticsearch.index.engine.SegmentsCopyInfo;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.transport.TransportService;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.*;

public class SegmentsCopySourceServiceTests extends IndexShardTestCase {

    // 本地测试复制segments 数据文件，应用segmentsInfo
    public void testApplyCopiedSegmentsInfoFilesLocal() throws IOException {
        // service
        final IndicesService indicesService = mock(IndicesService.class);
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(NodeRoles.dataNode());
        when(indicesService.clusterService()).thenReturn(clusterService);
        SegmentsCopySourceService segmentsCopySourceService = new SegmentsCopySourceService(
            indicesService, clusterService, mock(TransportService.class),
            new SegmentsCopySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)));
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

        // 没有数据写入之前，数据量都是0
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
        // 确认primary 数据量已经变化
        cnt = primaryShard.getEngineOrNull().acquireSearcher("test").getDirectoryReader().getDocCount("_id");
        System.out.println("primary cnt:"+cnt);
        // 确认replica 数据量不变
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
        segmentsCopySourceService.getOngoingCopies().initIndexShard(primaryShard, 10240, 1, 10000,20000);
        // 执行拷贝，没有副本不会真正执行
//        segmentsCopySourceService.doShardsCopy();
        // 执行
        SourceShardCopyState sourceShardCopyState = segmentsCopySourceService.getShardCopyStates().get(0);

        SegmentsCopyInfo sci = sourceShardCopyState.pollLatestSci();

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
        // 创建目标节点
        LocalTargetShardCopyState localTargetShardCopyState = new LocalTargetShardCopyState(
            replicaShard, pNode, 10000L);

//        localTargetShardCopyState.receiveFilesLocal(primaryShardPathIndex, replicaShardPathIndex, sci.files);

        Directory replicaDirectory = replicaShard.store().directory();
        Directory primaryDirectory = primaryShard.store().directory();
        // 拷贝文件
        localTargetShardCopyState.receiveFilesLocal2(primaryDirectory, replicaDirectory, sci.files);
        // 验证文件是否拷贝成功
        folder = new File(replicaShardPath.getDataPath().toUri());
        if (folder.exists() && folder.isDirectory()) {
            traverseFolder(folder);
        } else {
            System.out.println("文件夹不存在");
        }
        // 所有的临时文件都拷贝完成
        // 重命名临时文件名
        localTargetShardCopyState.renameFiles(replicaDirectory);
        // 验证文件重命名成功
        if (folder.exists() && folder.isDirectory()) {
            traverseFolder(folder);
        } else {
            System.out.println("文件夹不存在");
        }
        // 首先确认数据量没有变化
        ElasticsearchDirectoryReader searcher1 = localTargetShardCopyState.mgr.acquire();
        cnt = searcher1.getDelegate().getDocCount("_id");
        searcher1.decRef();
        System.out.println("before update segments info:"+"replica cnt:"+cnt);
        // 更新segments info 和index reader
        localTargetShardCopyState.setCurrSegmentsCopyInfo(sci);
//        localTargetShardCopyState.updateSegmentsInfo(replicaDirectory);
        localTargetShardCopyState.updateSegmentsInfo(replicaShard.store());
        // 验证数据量已经变化，说明拷贝成功
        ElasticsearchDirectoryReader searcher = localTargetShardCopyState.mgr.acquire();
        cnt = searcher.getDelegate().getDocCount("_id");
        searcher.decRef();
        System.out.println("after update segments info:"+"replica cnt:"+cnt);
        //
        sci.decRefDeleter();

        // 再次写入验证
        System.out.println("==============================================");
        System.out.println("==============================================");
        // write doc，refresh
        indexDoc(primaryShard, "_doc", "7");
        indexDoc(primaryShard, "_doc", "8");
        indexDoc(primaryShard, "_doc", "9");
        primaryShard.refresh("test");
        cnt = primaryShard.getEngineOrNull().acquireSearcher("test").getDirectoryReader().getDocCount("_id");
        System.out.println("primary cnt:"+cnt);
        indexDoc(primaryShard, "_doc", "10");
        indexDoc(primaryShard, "_doc", "11");
        indexDoc(primaryShard, "_doc", "12");
        primaryShard.refresh("test");
        // 确认primary 数据量已经变化
        cnt = primaryShard.getEngineOrNull().acquireSearcher("test").getDirectoryReader().getDocCount("_id");
        System.out.println("primary cnt:"+cnt);
        // 确认replica 数据量不变,这里是直接使用的engine中的searcher，并没有更新过segmentsInfo，所以还是0
        cnt = replicaShard.getEngineOrNull().acquireSearcher("test").getDirectoryReader().getDocCount("_id");
        System.out.println("replica cnt:"+cnt);

        // 获取更新的segments 信息
        sourceShardCopyState = segmentsCopySourceService.getShardCopyStates().get(0);
        sci = sourceShardCopyState.pollLatestSci();

        sci.files.stream().forEach(name -> System.out.println(name));

        // 拷贝文件
        localTargetShardCopyState.receiveFilesLocal2(primaryDirectory, replicaDirectory, sci.files);
        // 验证文件是否拷贝成功
        folder = new File(replicaShardPath.getDataPath().toUri());
        if (folder.exists() && folder.isDirectory()) {
            traverseFolder(folder);
        } else {
            System.out.println("文件夹不存在");
        }
        // 所有的临时文件都拷贝完成
        // 重命名临时文件名
        localTargetShardCopyState.renameFiles(replicaDirectory);
        // 验证文件重命名成功
        if (folder.exists() && folder.isDirectory()) {
            traverseFolder(folder);
        } else {
            System.out.println("文件夹不存在");
        }
        // 首先确认数据量没有变化
        searcher1 = localTargetShardCopyState.mgr.acquire();
        cnt = searcher1.getDelegate().getDocCount("_id");
        searcher1.decRef();
        System.out.println("before update segments info:"+"replica cnt:"+cnt);
        // 更新segments info 和index reader
        localTargetShardCopyState.setCurrSegmentsCopyInfo(sci);
//        localTargetShardCopyState.updateSegmentsInfo(replicaDirectory);
        localTargetShardCopyState.updateSegmentsInfo(replicaShard.store());
        // 验证数据量已经变化，说明拷贝成功
        searcher = localTargetShardCopyState.mgr.acquire();
        cnt = searcher.getDelegate().getDocCount("_id");
        searcher.decRef();
        System.out.println("after update segments info:"+"replica cnt:"+cnt);
        //
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
            indicesService, clusterService, mock(TransportService.class),
            new SegmentsCopySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)));
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
        segmentsCopySourceService.getOngoingCopies().initIndexShard(primaryShard, 10240, 1, 10000,20000);
        // 执行拷贝，没有副本不会真正执行
//        segmentsCopySourceService.doShardsCopy();
        // 执行
        SourceShardCopyState sourceShardCopyState = segmentsCopySourceService.getShardCopyStates().get(0);

        SegmentsCopyInfo sci = sourceShardCopyState.pollLatestSci();
//        sourceShardCopyState.copyToOneReplica(sourceShard.shardId(), targetShard.routingEntry(), sci);

        final TransportService transportService = mock(TransportService.class);
//        when(transportService.sendRequest(any(DiscoveryNode.class), anyString(), any(), any(),any())).thenReturn(null);
//        when(transportService).thenReturn(null);

        final RemoteTargetShardCopyState remoteTargetShardCopyState = new RemoteTargetShardCopyState(
            primaryShard.shardId(), transportService.getThreadPool(), transportService, pNode, rNode, new AtomicLong(0),10000L,null);

        // 发送 segments info
//        remoteTargetShardCopyState.sendSegmentsInfo(sci, 10000L, null);

        LocalTargetShardCopyState localTargetShardCopyState = new LocalTargetShardCopyState(
            replicaShard, pNode, 10000L);

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
