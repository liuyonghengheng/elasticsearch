package org.elasticsearch.indices.segmentscopy;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.SegmentsCopyInfo;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.indices.recovery.*;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class SourceShardCopyStateTests extends ESTestCase {

    private static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings("index",
        Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, org.elasticsearch.Version.CURRENT).build());
    private final ShardId shardId = new ShardId(INDEX_SETTINGS.getIndex(), 1);
    private final ClusterSettings service = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

    private ThreadPool threadPool;
    private Executor recoveryExecutor;


    @Before
    public void setUpThreadPool() {
        if (randomBoolean()) {
            threadPool = new TestThreadPool(getTestName());
            recoveryExecutor = threadPool.generic();
        } else {
            // verify that both sending and receiving files can be completed with a single thread
            threadPool = new TestThreadPool(getTestName(),
                new FixedExecutorBuilder(Settings.EMPTY, "recovery_executor", between(1, 16), between(16, 128), "recovery_executor"));
            recoveryExecutor = threadPool.executor("recovery_executor");
        }
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

//
//    public void testSendSegmentsInfo() throws Throwable {
//        final RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, service);
//        final StartRecoveryRequest request = getStartRecoveryRequest();
//        Store store = newStore(createTempDir());
//        Directory dir = store.directory();
//        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, newIndexWriterConfig());
//        int numDocs = randomIntBetween(10, 100);
//        for (int i = 0; i < numDocs; i++) {
//            Document document = new Document();
//            document.add(new StringField("id", Integer.toString(i), Field.Store.YES));
//            document.add(newField("field", randomUnicodeOfCodepointLengthBetween(1, 10), TextField.TYPE_STORED));
//            writer.addDocument(document);
//        }
//        writer.commit(); //writer.flush();
//
//
//
//        Store.MetadataSnapshot metadata = store.getMetadata(null);
//
//        List<StoreFileMetadata> metas = new ArrayList<>();
//        for (StoreFileMetadata md : metadata) {
//            metas.add(md);
//        }
//        Store targetStore = newStore(createTempDir());
//        MultiFileWriter multiFileWriter = new MultiFileWriter(targetStore, mock(RecoveryState.Index.class), "", logger, () -> {});
//        RecoveryTargetHandler target = new RecoverySourceHandlerTests.TestRecoveryTargetHandler() {
//            @Override
//            public void writeFileChunk(StoreFileMetadata md, long position, BytesReference content, boolean lastChunk,
//                                       int totalTranslogOps, ActionListener<Void> listener) {
//                ActionListener.completeWith(listener, () -> {
//                    multiFileWriter.writeFileChunk(md, position, content, lastChunk);
//                    return null;
//                });
//            }
//        };
//        RecoverySourceHandler handler = new RecoverySourceHandler(null, new AsyncRecoveryTarget(target, recoveryExecutor),
//            threadPool, request, Math.toIntExact(recoverySettings.getChunkSize().getBytes()), between(1, 5), between(1, 5));
//        PlainActionFuture<Void> sendFilesFuture = new PlainActionFuture<>();
//        handler.sendFiles(store, metas.toArray(new StoreFileMetadata[0]), () -> 0, sendFilesFuture);
//        sendFilesFuture.actionGet();
//        Store.MetadataSnapshot targetStoreMetadata = targetStore.getMetadata(null);
//        Store.RecoveryDiff recoveryDiff = targetStoreMetadata.recoveryDiff(metadata);
//        assertEquals(metas.size(), recoveryDiff.identical.size());
//        assertEquals(0, recoveryDiff.different.size());
//        assertEquals(0, recoveryDiff.missing.size());
//        IndexReader reader = DirectoryReader.open(targetStore.directory());
//        assertEquals(numDocs, reader.maxDoc());
//        IOUtils.close(reader, store, multiFileWriter, targetStore);
//    }
//
//    private SegmentsInfoRequest createSegmentsInfoRequest(SegmentsCopyInfo scs){
//        Store.MetadataSnapshot metadataSnapshot = randomBoolean() ? Store.MetadataSnapshot.EMPTY :
//            new Store.MetadataSnapshot(Collections.emptyMap(),
//                Collections.singletonMap(Engine.HISTORY_UUID_KEY, UUIDs.randomBase64UUID()), randomIntBetween(0, 100));
//        return new SegmentsInfoRequest(
//            randomBoolean() || metadataSnapshot.getHistoryUUID() == null ?
//                SequenceNumbers.UNASSIGNED_SEQ_NO : randomNonNegativeLong(),
//            shardId,
//            new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
//            scs.version,
//            scs.gen,
//            scs.primaryTerm,
//            scs.infosBytes,
//            scs.files,
//            new ArrayList<>()
//        );
//    }
//
//    private Store newStore(Path path) throws IOException {
//        return newStore(path, true);
//    }
//
//    private Store newStore(Path path, boolean checkIndex) throws IOException {
//        BaseDirectoryWrapper baseDirectoryWrapper = RecoverySourceHandlerTests.newFSDirectory(path);
//        if (checkIndex == false) {
//            baseDirectoryWrapper.setCheckIndexOnClose(false); // don't run checkindex we might corrupt the index in these tests
//        }
//        return new Store(shardId,  INDEX_SETTINGS, baseDirectoryWrapper, new DummyShardLock(shardId));
//    }
//
//
//
//    /////////////////////////////////////////////**
//
//    /**
//     *
//     * @return
//     * @throws IOException
//     */
//    public StartRecoveryRequest getStartRecoveryRequest() throws IOException {
//        Store.MetadataSnapshot metadataSnapshot = randomBoolean() ? Store.MetadataSnapshot.EMPTY :
//            new Store.MetadataSnapshot(Collections.emptyMap(),
//                Collections.singletonMap(Engine.HISTORY_UUID_KEY, UUIDs.randomBase64UUID()), randomIntBetween(0, 100));
//        return new StartRecoveryRequest(
//            shardId,
//            null,
//            new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
//            new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
//            metadataSnapshot,
//            randomBoolean(),
//            randomNonNegativeLong(),
//            randomBoolean() || metadataSnapshot.getHistoryUUID() == null ?
//                SequenceNumbers.UNASSIGNED_SEQ_NO : randomNonNegativeLong());
//    }
//
//    public void testSendFiles() throws Throwable {
//        final RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, service);
//        final StartRecoveryRequest request = getStartRecoveryRequest();
//        Store store = newStore(createTempDir());
//        Directory dir = store.directory();
//        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, newIndexWriterConfig());
//        int numDocs = randomIntBetween(10, 100);
//        for (int i = 0; i < numDocs; i++) {
//            Document document = new Document();
//            document.add(new StringField("id", Integer.toString(i), Field.Store.YES));
//            document.add(newField("field", randomUnicodeOfCodepointLengthBetween(1, 10), TextField.TYPE_STORED));
//            writer.addDocument(document);
//        }
//        writer.commit(); //writer.flush();
//        writer.close();
////        SegmentInfos infos = ((StandardDirectoryReader) writer.getReader()).getSegmentInfos();
//
//        Store.MetadataSnapshot metadata = store.getMetadata(null);
//        List<StoreFileMetadata> metas = new ArrayList<>();
//        for (StoreFileMetadata md : metadata) {
//            metas.add(md);
//        }
//        Store targetStore = newStore(createTempDir());
//        MultiFileWriter multiFileWriter = new MultiFileWriter(targetStore, mock(RecoveryState.Index.class), "", logger, () -> {});
//        RecoveryTargetHandler target = new RecoverySourceHandlerTests.TestRecoveryTargetHandler() {
//            @Override
//            public void writeFileChunk(StoreFileMetadata md, long position, BytesReference content, boolean lastChunk,
//                                       int totalTranslogOps, ActionListener<Void> listener) {
//                ActionListener.completeWith(listener, () -> {
//                    multiFileWriter.writeFileChunk(md, position, content, lastChunk);
//                    return null;
//                });
//            }
//        };
//        RecoverySourceHandler handler = new RecoverySourceHandler(null, new AsyncRecoveryTarget(target, recoveryExecutor),
//            threadPool, request, Math.toIntExact(recoverySettings.getChunkSize().getBytes()), between(1, 5), between(1, 5));
//        PlainActionFuture<Void> sendFilesFuture = new PlainActionFuture<>();
//        handler.sendFiles(store, metas.toArray(new StoreFileMetadata[0]), () -> 0, sendFilesFuture);
//        sendFilesFuture.actionGet();
//        Store.MetadataSnapshot targetStoreMetadata = targetStore.getMetadata(null);
//        Store.RecoveryDiff recoveryDiff = targetStoreMetadata.recoveryDiff(metadata);
//        assertEquals(metas.size(), recoveryDiff.identical.size());
//        assertEquals(0, recoveryDiff.different.size());
//        assertEquals(0, recoveryDiff.missing.size());
//        IndexReader reader = DirectoryReader.open(targetStore.directory());
//        assertEquals(numDocs, reader.maxDoc());
//        IOUtils.close(reader, store, multiFileWriter, targetStore);
//    }

}
