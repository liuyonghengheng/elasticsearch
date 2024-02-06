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

import org.apache.lucene.index.*;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

@SuppressForbidden(reason = "reference counting is required here")
class CopyReadElasticsearchReaderManager extends ReferenceManager<ElasticsearchDirectoryReader> {
    private final BiConsumer<ElasticsearchDirectoryReader, ElasticsearchDirectoryReader> refreshListener;
    private volatile SegmentInfos currentInfos;
    /**
     * Creates and returns a new ElasticsearchReaderManager from the given
     * already-opened {@link ElasticsearchDirectoryReader}, stealing
     * the incoming reference.
     *
     * @param reader            the directoryReader to use for future reopens
     * @param refreshListener   A consumer that is called every time a new reader is opened
     */
    CopyReadElasticsearchReaderManager(ElasticsearchDirectoryReader reader,
                                       BiConsumer<ElasticsearchDirectoryReader, ElasticsearchDirectoryReader> refreshListener) {
        this.current = reader;
        this.refreshListener = refreshListener;
        refreshListener.accept(current, null);
    }

    @Override
    protected void decRef(ElasticsearchDirectoryReader reference) throws IOException {
        reference.decRef();
    }

    @Override
    protected ElasticsearchDirectoryReader refreshIfNeeded(ElasticsearchDirectoryReader old) throws IOException {
        List<LeafReader> subs;
        if (old == null) {
            subs = null;
        } else {
            subs = new ArrayList<>();
            for (LeafReaderContext ctx : old.getDelegate().leaves()) {
                subs.add(ctx.reader());
            }
        }

        // Open a new reader, sharing any common segment readers with the old one:
        DirectoryReader r = StandardDirectoryReader.open(this.current.directory(), currentInfos, subs);
        final ElasticsearchDirectoryReader reader = ElasticsearchDirectoryReader.wrap(r, old.shardId());
        if (reader != null) {
            refreshListener.accept(reader, old);
        }
        return reader;
    }

    @Override
    protected boolean tryIncRef(ElasticsearchDirectoryReader reference) {
        return reference.tryIncRef();
    }

    @Override
    protected int getRefCount(ElasticsearchDirectoryReader reference) {
        return reference.getRefCount();
    }

    public void setCurrentInfos(SegmentInfos infos) throws IOException {
        if (currentInfos != null) {
            // So that if we commit, we will go to the next
            // (unwritten so far) generation:
            infos.updateGeneration(currentInfos);
        }
        currentInfos = infos;
        maybeRefresh();
    }
}
