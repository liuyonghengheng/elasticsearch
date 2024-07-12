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

package org.elasticsearch.action.search;

import org.apache.lucene.store.ByteArrayDataInput;
//import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.BytesRef;
//import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.transport.RemoteClusterAware;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

final class TransportSearchHelper {

    private static final String INCLUDE_CONTEXT_UUID = "include_context_uuid";

    static InternalScrollSearchRequest internalScrollSearchRequest(ShardSearchContextId id, SearchScrollRequest request) {
        return new InternalScrollSearchRequest(request, id);
    }
// RAMOutputStream 在lucene8.x已经不推荐使用，lucene9.x已经移除
//    static String buildScrollId(AtomicArray<? extends SearchPhaseResult> searchPhaseResults, Version version) {
//        boolean includeContextUUID = version.onOrAfter(Version.V_7_7_0);
//        try (RAMOutputStream out = new RAMOutputStream()) {
//            if (includeContextUUID) {
//                out.writeString(INCLUDE_CONTEXT_UUID);
//            }
//            out.writeString(searchPhaseResults.length() == 1 ? ParsedScrollId.QUERY_AND_FETCH_TYPE : ParsedScrollId.QUERY_THEN_FETCH_TYPE);
//            out.writeVInt(searchPhaseResults.asList().size());
//            for (SearchPhaseResult searchPhaseResult : searchPhaseResults.asList()) {
//                if (includeContextUUID) {
//                    out.writeString(searchPhaseResult.getContextId().getSessionId());
//                }
//                out.writeLong(searchPhaseResult.getContextId().getId());
//                SearchShardTarget searchShardTarget = searchPhaseResult.getSearchShardTarget();
//                if (searchShardTarget.getClusterAlias() != null) {
//                    out.writeString(
//                        RemoteClusterAware.buildRemoteIndexName(searchShardTarget.getClusterAlias(), searchShardTarget.getNodeId()));
//                } else {
//                    out.writeString(searchShardTarget.getNodeId());
//                }
//            }
//            byte[] bytes = new byte[(int) out.getFilePointer()];
//            out.writeTo(bytes, 0);
//            return Base64.getUrlEncoder().encodeToString(bytes);
//        } catch (IOException e) {
//            throw new UncheckedIOException(e);
//        }
//    }
//
    // TODO:liuyongheng 确认这里的逻辑
    static String buildScrollId(AtomicArray<? extends SearchPhaseResult> searchPhaseResults) {
        final BytesReference bytesReference;
        try (BytesStreamOutput encodedStreamOutput = new BytesStreamOutput()) {
            try (OutputStreamStreamOutput out = new OutputStreamStreamOutput(Base64.getUrlEncoder().wrap(encodedStreamOutput))) {
                out.writeString(INCLUDE_CONTEXT_UUID);
                out.writeString(
                    searchPhaseResults.length() == 1 ? ParsedScrollId.QUERY_AND_FETCH_TYPE : ParsedScrollId.QUERY_THEN_FETCH_TYPE
                );
                out.writeCollection(searchPhaseResults.asList(), (o, searchPhaseResult) -> {
                    o.writeString(searchPhaseResult.getContextId().getSessionId());
                    o.writeLong(searchPhaseResult.getContextId().getId());
                    SearchShardTarget searchShardTarget = searchPhaseResult.getSearchShardTarget();
                    if (searchShardTarget.getClusterAlias() != null) {
                        o.writeString(
                            RemoteClusterAware.buildRemoteIndexName(searchShardTarget.getClusterAlias(), searchShardTarget.getNodeId())
                        );
                    } else {
                        o.writeString(searchShardTarget.getNodeId());
                    }
                });
            }
            bytesReference = encodedStreamOutput.bytes();
        } catch (IOException e) {
            assert false : e;
            throw new UncheckedIOException(e);
        }
        final BytesRef bytesRef = bytesReference.toBytesRef();
        return new String(bytesRef.bytes, bytesRef.offset, bytesRef.length, StandardCharsets.ISO_8859_1);
    }

    static ParsedScrollId parseScrollId(String scrollId) {
        try {
//            byte[] bytes = Base64.getUrlDecoder().decode(scrollId);
//            ByteArrayDataInput in = new ByteArrayDataInput(bytes);
            // TODO:liuyongheng 又一处编码问题
            var decodedInputStream = Base64.getUrlDecoder().wrap(new ByteArrayInputStream(scrollId.getBytes(StandardCharsets.ISO_8859_1)));
            var in = new InputStreamStreamInput(decodedInputStream);
            final boolean includeContextUUID;
            final String type;
            final String firstChunk = in.readString();
            if (INCLUDE_CONTEXT_UUID.equals(firstChunk)) {
                includeContextUUID = true;
                type = in.readString();
            } else {
                includeContextUUID = false;
                type = firstChunk;
            }
            SearchContextIdForNode[] context = new SearchContextIdForNode[in.readVInt()];
            for (int i = 0; i < context.length; ++i) {
                final String contextUUID = includeContextUUID ? in.readString() : "";
                long id = in.readLong();
                String target = in.readString();
                String clusterAlias;
                final int index = target.indexOf(RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR);
                if (index == -1) {
                    clusterAlias = null;
                } else {
                    clusterAlias = target.substring(0, index);
                    target = target.substring(index+1);
                }
                context[i] = new SearchContextIdForNode(clusterAlias, target, new ShardSearchContextId(contextUUID, id));
            }
//            if (in.getPosition() != bytes.length) {
//                throw new IllegalArgumentException("Not all bytes were read");
//            }
            if (in.available() > 0) {
                throw new IllegalArgumentException("Not all bytes were read");
            }
            return new ParsedScrollId(scrollId, type, context);
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse scroll id", e);
        }
    }

    private TransportSearchHelper() {

    }
}
