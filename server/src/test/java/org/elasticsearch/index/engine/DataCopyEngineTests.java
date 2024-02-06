/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.engine;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.filter.RegexFilter;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.*;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndSeqNo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.Mapper.BuilderContext;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.seqno.*;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.SnapshotMatchers;
import org.elasticsearch.index.translog.TestTranslog;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.shuffle;
import static org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader.getElasticsearchDirectoryReader;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.*;
import static org.elasticsearch.index.seqno.SequenceNumbers.*;
import static org.elasticsearch.index.translog.TranslogDeletionPolicies.createTranslogDeletionPolicy;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.*;

public class DataCopyEngineTests extends EngineTestCase {
    // from InternalEngine swap to DataCopyEngine primary
    public void testWrite1() throws IOException {
        IOUtils.close(engine, store);
        Engine dataCopyEngine = null;
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            int numDocs = scaledRandomIntBetween(10, 1000);
            try (InternalEngine engine = createEngine(config)) {
                for (int i = 0; i < numDocs; i++) {
                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
                    engine.index(new Engine.Index(newUid(doc), doc, i, primaryTerm.get(), 1, null, Engine.Operation.Origin.REPLICA,
                        System.nanoTime(), -1, false, SequenceNumbers.UNASSIGNED_SEQ_NO, 0));
                    if (rarely()) {
                        engine.flush();
                    }
                    engine.syncTranslog(); // advance persisted local checkpoint
                    globalCheckpoint.set(engine.getPersistedLocalCheckpoint());
                }
                globalCheckpoint.set(engine.getPersistedLocalCheckpoint());
                engine.syncTranslog();
                engine.flushAndClose();
                dataCopyEngine = new DataCopyEngine(engine.engineConfig);
                for (int i = numDocs; i < numDocs+5; i++) {
                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
                    dataCopyEngine.index(new Engine.Index(newUid(doc), doc, UNASSIGNED_SEQ_NO, primaryTerm.get(), 1,
                        VersionType.EXTERNAL,
                        Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1, false, UNASSIGNED_SEQ_NO, 0));
                    dataCopyEngine.refresh("test");
                    if (rarely()) {
                        dataCopyEngine.flush();
                    }
                    dataCopyEngine.syncTranslog(); //把translog写入底层， advance persisted local checkpoint,这里会重新检查和设置LocalCheckpoint
                    globalCheckpoint.set(dataCopyEngine.getPersistedLocalCheckpoint());
                }
            } finally {
                IOUtils.close(dataCopyEngine);
            }
        }
    }
    // from InternalEngine swap to DataCopyEngine replica
    public void testWrite2() throws IOException {
        IOUtils.close(engine, store);
        Engine dataCopyEngine = null;
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            int numDocs = scaledRandomIntBetween(10, 1000);
            try (InternalEngine engine = createEngine(config)) {
                dataCopyEngine = new DataCopyEngine(engine.engineConfig);
                for (int i = 0; i < numDocs; i++) {
                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
                    dataCopyEngine.index(new Engine.Index(newUid(doc), doc, i, primaryTerm.get(), 1, null, Engine.Operation.Origin.REPLICA,
                        System.nanoTime(), -1, false, SequenceNumbers.UNASSIGNED_SEQ_NO, 0));
                    if (rarely()) {
                        dataCopyEngine.flush();
                    }
                    dataCopyEngine.syncTranslog(); // advance persisted local checkpoint
                    globalCheckpoint.set(dataCopyEngine.getPersistedLocalCheckpoint());
                }
                globalCheckpoint.set(dataCopyEngine.getPersistedLocalCheckpoint());
                dataCopyEngine.syncTranslog();
                dataCopyEngine.flushAndClose();

                Engine.CommitId flush = dataCopyEngine.flush(randomBoolean(), true);
                assertEquals(flush, dataCopyEngine.flush(randomBoolean(), true));
            } finally {
                IOUtils.close(dataCopyEngine);
            }
        }
    }
    // from InternalEngine swap to InternalEngine
    public void testWrite3() throws IOException {
        IOUtils.close(engine, store);
        Engine dataCopyEngine = null;
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            int numDocs = scaledRandomIntBetween(10, 1000);
            try (InternalEngine engine = createEngine(config)) {
                for (int i = 0; i < numDocs; i++) {
                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
                    engine.index(new Engine.Index(newUid(doc), doc, i, primaryTerm.get(), 1, null, Engine.Operation.Origin.REPLICA,
                        System.nanoTime(), -1, false, SequenceNumbers.UNASSIGNED_SEQ_NO, 0));
                    if (rarely()) {
                        engine.flush();
                    }
                    engine.syncTranslog(); // advance persisted local checkpoint
                    globalCheckpoint.set(engine.getPersistedLocalCheckpoint());
                }
                globalCheckpoint.set(engine.getPersistedLocalCheckpoint());
                engine.syncTranslog();
                engine.flushAndClose();
                dataCopyEngine = new InternalEngine(engine.engineConfig);
                for (int i = numDocs; i < numDocs+5; i++) {
                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
                    dataCopyEngine.index(new Engine.Index(newUid(doc), doc, i, primaryTerm.get(), 1, null, Engine.Operation.Origin.REPLICA,
                        System.nanoTime(), -1, false, SequenceNumbers.UNASSIGNED_SEQ_NO, 0));
                    dataCopyEngine.refresh("test");
                    if (rarely()) {
                        dataCopyEngine.flush();
                    }
                    dataCopyEngine.syncTranslog(); //把translog写入底层， advance persisted local checkpoint,这里会重新检查和设置LocalCheckpoint
                    globalCheckpoint.set(dataCopyEngine.getPersistedLocalCheckpoint());
                }
            } finally {
                IOUtils.close(dataCopyEngine);
            }
        }
    }


    public void testReadOnlyEngine() throws Exception {
        IOUtils.close(engine, store);
        Engine readOnlyEngine = null;
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            int numDocs = scaledRandomIntBetween(10, 1000);
            final SeqNoStats lastSeqNoStats;
            final List<DocIdSeqNoAndSource> lastDocIds;
            try (InternalEngine engine = createEngine(config)) {
                Engine.Get get = null;
                for (int i = 0; i < numDocs; i++) {
                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
                    engine.index(new Engine.Index(newUid(doc), doc, i, primaryTerm.get(), 1, null, Engine.Operation.Origin.REPLICA,
                        System.nanoTime(), -1, false, SequenceNumbers.UNASSIGNED_SEQ_NO, 0));
                    if (get == null || rarely()) {
                        get = newGet(randomBoolean(), doc);
                    }
                    if (rarely()) {
                        engine.flush();
                    }
                    globalCheckpoint.set(randomLongBetween(globalCheckpoint.get(), engine.getPersistedLocalCheckpoint()));
                }
                engine.syncTranslog();
                globalCheckpoint.set(randomLongBetween(globalCheckpoint.get(), engine.getPersistedLocalCheckpoint()));
                engine.flush();
                readOnlyEngine = new ReadOnlyEngine(engine.engineConfig, engine.getSeqNoStats(globalCheckpoint.get()),
                    engine.getTranslogStats(), false, Function.identity(), true);
                lastSeqNoStats = engine.getSeqNoStats(globalCheckpoint.get());
                lastDocIds = getDocIds(engine, true);
                assertThat(readOnlyEngine.getPersistedLocalCheckpoint(), equalTo(lastSeqNoStats.getLocalCheckpoint()));
                assertThat(readOnlyEngine.getSeqNoStats(globalCheckpoint.get()).getMaxSeqNo(), equalTo(lastSeqNoStats.getMaxSeqNo()));
                assertThat(getDocIds(readOnlyEngine, false), equalTo(lastDocIds));
                for (int i = 0; i < numDocs; i++) {
                    if (randomBoolean()) {
                        String delId = Integer.toString(i);
                        engine.delete(new Engine.Delete("test", delId, newUid(delId), primaryTerm.get()));
                    }
                    if (rarely()) {
                        engine.flush();
                    }
                }
                Engine.Searcher external = readOnlyEngine.acquireSearcher("test", Engine.SearcherScope.EXTERNAL);
                Engine.Searcher internal = readOnlyEngine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
                assertSame(external.getIndexReader(), internal.getIndexReader());
                assertThat(external.getIndexReader(), Matchers.instanceOf(DirectoryReader.class));
                DirectoryReader dirReader = external.getDirectoryReader();
                ElasticsearchDirectoryReader esReader = getElasticsearchDirectoryReader(dirReader);
                IndexReader.CacheHelper helper = esReader.getReaderCacheHelper();
                assertNotNull(helper);
                assertEquals(helper.getKey(), dirReader.getReaderCacheHelper().getKey());

                IOUtils.close(external, internal);
                // the locked down engine should still point to the previous commit
                assertThat(readOnlyEngine.getPersistedLocalCheckpoint(), equalTo(lastSeqNoStats.getLocalCheckpoint()));
                assertThat(readOnlyEngine.getSeqNoStats(globalCheckpoint.get()).getMaxSeqNo(), equalTo(lastSeqNoStats.getMaxSeqNo()));
                assertThat(getDocIds(readOnlyEngine, false), equalTo(lastDocIds));
                try (Engine.GetResult getResult = readOnlyEngine.get(get, readOnlyEngine::acquireSearcher)) {
                    assertTrue(getResult.exists());
                }
            }
            // Close and reopen the main engine
            try (InternalEngine recoveringEngine = new InternalEngine(config)) {
                recoveringEngine.recoverFromTranslog(translogHandler, Long.MAX_VALUE);
                // the locked down engine should still point to the previous commit
                assertThat(readOnlyEngine.getPersistedLocalCheckpoint(), equalTo(lastSeqNoStats.getLocalCheckpoint()));
                assertThat(readOnlyEngine.getSeqNoStats(globalCheckpoint.get()).getMaxSeqNo(), equalTo(lastSeqNoStats.getMaxSeqNo()));
                assertThat(getDocIds(readOnlyEngine, false), equalTo(lastDocIds));
            }
        } finally {
            IOUtils.close(readOnlyEngine);
        }
    }

}
