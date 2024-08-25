package org.elasticsearch.indices.segmentscopy;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.BytesRefBuilder;
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
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
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
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.index.store.Store.digestToString;
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


    public void testSendSegmentsInfo() throws Throwable {

    }

    private SegmentsInfoRequest createSegmentsInfoRequest(SegmentsCopyInfo scs){
        Store.MetadataSnapshot metadataSnapshot = randomBoolean() ? Store.MetadataSnapshot.EMPTY :
            new Store.MetadataSnapshot(Collections.emptyMap(),
                Collections.singletonMap(Engine.HISTORY_UUID_KEY, UUIDs.randomBase64UUID()), randomIntBetween(0, 100));
        return new SegmentsInfoRequest(
            randomBoolean() || metadataSnapshot.getHistoryUUID() == null ?
                SequenceNumbers.UNASSIGNED_SEQ_NO : randomNonNegativeLong(),
            shardId,
            new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
            scs.version,
            scs.gen,
            scs.primaryTerm,
            scs.refreshedCheckpoint,
            scs.infosBytes,
            scs.files,
            new ArrayList<>()
        );
    }

    public void testSegmentsInfoRequest() throws Throwable {
        final RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, service);
        final StartRecoveryRequest request = getStartRecoveryRequest();
        Store store = newStore(createTempDir());
        Directory dir = store.directory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, newIndexWriterConfig());
        int numDocs = randomIntBetween(10, 100);
        for (int i = 0; i < numDocs; i++) {
            Document document = new Document();
            document.add(new StringField("id", Integer.toString(i), Field.Store.YES));
            document.add(newField("field", randomUnicodeOfCodepointLengthBetween(1, 10), TextField.TYPE_STORED));
            writer.addDocument(document);
        }
        // 落盘
        writer.commit();
        SegmentInfos segmentInfos = ((StandardDirectoryReader) writer.getReader()).getSegmentInfos();

        List<String> files = new ArrayList<>();
        Map<String, StoreFileMetadata> filesMetadata = new HashMap<>();
        for (SegmentCommitInfo info : segmentInfos) {
            for (String fileName : info.files()) {
                files.add(fileName);
                StoreFileMetadata metadata = readLocalFileMetaData(fileName, dir, info.info.getVersion());
                filesMetadata.put(fileName, metadata);
            }
        }
        // Serialize the SegmentInfos.
        ByteBuffersDataOutput buffer = new ByteBuffersDataOutput();
        try (ByteBuffersIndexOutput tmpIndexOutput =
                 new ByteBuffersIndexOutput(buffer, "temporary", "temporary")) {
            segmentInfos.write(tmpIndexOutput);
        }
        byte[] infosBytes = buffer.toArrayCopy();
        SegmentsCopyInfo segmentsCopyInfo = new SegmentsCopyInfo(
            files,
            filesMetadata,
            segmentInfos.getVersion(),
            segmentInfos.getGeneration(),
            infosBytes,
            1L,
            segmentInfos,
            null,
            0L);
        SegmentsInfoRequest  sir = createSegmentsInfoRequest(segmentsCopyInfo);
        System.out.println("sir = " + sir);
        try (BytesStreamOutput buffer2 = new BytesStreamOutput()) {
            sir.writeTo(buffer2);
            SegmentsInfoRequest sir2 = new SegmentsInfoRequest(buffer2.bytes().streamInput());
            System.out.println("sir2 = " + sir2);
        }
    }


    private Store newStore(Path path) throws IOException {
        return newStore(path, true);
    }

    private Store newStore(Path path, boolean checkIndex) throws IOException {
        BaseDirectoryWrapper baseDirectoryWrapper = RecoverySourceHandlerTests.newFSDirectory(path);
        if (checkIndex == false) {
            baseDirectoryWrapper.setCheckIndexOnClose(false); // don't run checkindex we might corrupt the index in these tests
        }
        return new Store(shardId,  INDEX_SETTINGS, baseDirectoryWrapper, new DummyShardLock(shardId));
    }



    /////////////////////////////////////////////**

    /**
     *
     * @return
     * @throws IOException
     */
    public StartRecoveryRequest getStartRecoveryRequest() throws IOException {
        Store.MetadataSnapshot metadataSnapshot = randomBoolean() ? Store.MetadataSnapshot.EMPTY :
            new Store.MetadataSnapshot(Collections.emptyMap(),
                Collections.singletonMap(Engine.HISTORY_UUID_KEY, UUIDs.randomBase64UUID()), randomIntBetween(0, 100));
        return new StartRecoveryRequest(
            shardId,
            null,
            new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
            new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
            metadataSnapshot,
            randomBoolean(),
            randomNonNegativeLong(),
            randomBoolean() || metadataSnapshot.getHistoryUUID() == null ?
                SequenceNumbers.UNASSIGNED_SEQ_NO : randomNonNegativeLong());
    }

    public void testSendFiles() throws Throwable {
        final RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, service);
        final StartRecoveryRequest request = getStartRecoveryRequest();
        Store store = newStore(createTempDir());
        Directory dir = store.directory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, newIndexWriterConfig());
        int numDocs = randomIntBetween(10, 100);
        for (int i = 0; i < numDocs; i++) {
            Document document = new Document();
            document.add(new StringField("id", Integer.toString(i), Field.Store.YES));
            document.add(newField("field", randomUnicodeOfCodepointLengthBetween(1, 10), TextField.TYPE_STORED));
            writer.addDocument(document);
        }
        // 落盘
        writer.commit();
        // 再写一批
        for (int i = numDocs; i < numDocs+5; i++) {
            Document document = new Document();
            document.add(new StringField("id", Integer.toString(i), Field.Store.YES));
            document.add(newField("field", randomUnicodeOfCodepointLengthBetween(1, 10), TextField.TYPE_STORED));
            writer.addDocument(document);
        }

        int sendNumDocs = numDocs+5;
        System.out.println("source write:" + sendNumDocs);
        // 这一批生成新的segments
        writer.flush();
//        writer.close();
        // 读取segments info
        SegmentInfos segmentInfos = ((StandardDirectoryReader) writer.getReader()).getSegmentInfos();
        Map<String, StoreFileMetadata> metaDatas = readLocalFilesMetaDatas(segmentInfos, dir);

        Store targetStore = newStore(createTempDir());
        CopyMultiFileWriter multiFileWriter = new CopyMultiFileWriter(targetStore, mock(RecoveryState.Index.class), "", logger, () -> {});

        TestTargetShardCopyState target = new TestTargetShardCopyState(shardId, mock(TransportService.class),
            new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
            new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
            1000L,
            (v) -> {});

        target.setMaxConcurrentFileChunks(1);
        target.setChunkSizeInBytes(1024*1024);
        target.setMultiFileWriter(multiFileWriter);

        PlainActionFuture<Void> sendFilesFuture = new PlainActionFuture<>();
        target.sendFiles(store, metaDatas.values().toArray(new StoreFileMetadata[0]), sendFilesFuture);

        sendFilesFuture.actionGet();

//      没有拷贝segment info 文件所以不能直接打开
//      IndexReader reader = DirectoryReader.open(targetStore.directory());
        IndexReader reader = StandardDirectoryReader.open(targetStore.directory(), segmentInfos, null);
        System.out.println("target receive: "+ reader.maxDoc());
        assertEquals(sendNumDocs, reader.maxDoc());

        System.out.println("test end end");

    }

    public Map<String, StoreFileMetadata> readLocalFilesMetaDatas(SegmentInfos segmentInfos, Directory dir){
        // Serialize the SegmentInfos.
        ByteBuffersDataOutput buffer = new ByteBuffersDataOutput();
        try (ByteBuffersIndexOutput tmpIndexOutput =
                 new ByteBuffersIndexOutput(buffer, "temporary", "temporary")) {
            segmentInfos.write(tmpIndexOutput);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        byte[] infosBytes = buffer.toArrayCopy();

        List<String> files = new ArrayList<>();
        Map<String, StoreFileMetadata> filesMetadata = new HashMap<>();
        for (SegmentCommitInfo info : segmentInfos) {
            try {
                for (String fileName : info.files()) {
                    files.add(fileName);
                    StoreFileMetadata metadata = readLocalFileMetaData(fileName, dir, info.info.getVersion());
                    filesMetadata.put(fileName, metadata);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        SegmentsCopyInfo segmentsCopyInfo = new SegmentsCopyInfo(
            files,
            filesMetadata,
            segmentInfos.getVersion(),
            segmentInfos.getGeneration(),
            infosBytes,
            1L,
            segmentInfos,
            null,
            0L);

        return filesMetadata;
    }


    public StoreFileMetadata readLocalFileMetaData(String fileName, Directory dir, org.apache.lucene.util.Version version) throws IOException {
        StoreFileMetadata result;
        // Pull from the filesystem
        String checksum;
        long length;
        try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
            try {
                length = in.length();
                if (length < CodecUtil.footerLength()) {
                    // truncated files trigger IAE if we seek negative... these files are really corrupted though
                    throw new CorruptIndexException("Can't retrieve checksum from file: " + fileName + " file length must be >= " +
                        CodecUtil.footerLength() + " but was: " + in.length(), in);
                }
                // 不检测hash
                checksum = digestToString(CodecUtil.retrieveChecksum(in));
            } catch (Exception ex) {
//                        logger.debug(() -> new ParameterizedMessage("Can retrieve checksum from file [{}]", fileName), ex);
                throw ex;
            }
        } catch (@SuppressWarnings("unused") FileNotFoundException | NoSuchFileException e) {
            return null;
        }
        result = new StoreFileMetadata(fileName, length, checksum, version);
        return result;
    }

    class TestTargetShardCopyState extends RemoteTargetShardCopyState{
        CopyMultiFileWriter multiFileWriter;

        public TestTargetShardCopyState(ShardId shardId, TransportService transportService, DiscoveryNode localNode, DiscoveryNode targetNode, Long internalActionTimeout, Consumer<Long> onSourceThrottle) {
            super(shardId, SourceShardCopyStateTests.this.threadPool, transportService, localNode, targetNode, new AtomicLong(0), internalActionTimeout, onSourceThrottle);
        }

        @Override
        public void writeFileChunk(StoreFileMetadata fileMetadata, long position, BytesReference content, boolean lastChunk, int totalTranslogOps, ActionListener<Void> listener) {
                ActionListener.completeWith(listener, () -> {
                    multiFileWriter.writeFileChunk(fileMetadata, position, content, lastChunk);
                    return null;
                });
            }

        public void setMultiFileWriter(CopyMultiFileWriter multiFileWriter) {
            this.multiFileWriter = multiFileWriter;
        }
    }

}
