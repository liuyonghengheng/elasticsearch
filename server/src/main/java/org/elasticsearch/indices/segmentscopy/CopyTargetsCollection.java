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

package org.elasticsearch.indices.segmentscopy;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class holds a collection of all on going recoveries on the current node (i.e., the node is the target node
 * of those recoveries). The class is used to guarantee concurrent semantics such that once a recoveries was done/cancelled/failed
 * no other thread will be able to find it. Last, the {@link LocalTargetShardCopyState} inner class verifies that recovery temporary files
 * and store will only be cleared once on going usage is finished.
 */
public class CopyTargetsCollection {

    /** This is the single source of truth for ongoing recoveries. If it's not here, it was canceled or done */
    private final ConcurrentMap<ShardId, LocalTargetShardCopyState> onGoingCopies = ConcurrentCollections.newConcurrentMap();

    private final Logger logger;
    private final ThreadPool threadPool;
    private final TransportService transportService;

    public CopyTargetsCollection(Logger logger, ThreadPool threadPool, TransportService transportService) {
        this.logger = logger;
        this.threadPool = threadPool;
        this.transportService = transportService;
    }

    /**
     * Starts are new copy for the given shard, source node and state
     *
     * @return the id of the new recovery.
     */
    public LocalTargetShardCopyState createCopyTarget(IndexShard indexShard, DiscoveryNode sourceNode, Long internalActionTimeout) {
        LocalTargetShardCopyState targetCopy = new LocalTargetShardCopyState(indexShard, sourceNode, internalActionTimeout);
        LocalTargetShardCopyState existingTarget = onGoingCopies.putIfAbsent(indexShard.shardId(), targetCopy);
        assert existingTarget == null : "found two RecoveryStatus instances with the same id";
        return targetCopy;
    }

    public LocalTargetShardCopyState getRecoveryTarget(ShardId shardId) {
        return onGoingCopies.get(shardId);
    }

    /**
     * a reference to {@link LocalTargetShardCopyState}, which implements {@link AutoCloseable}. closing the reference
     * causes {@link LocalTargetShardCopyState#decRef()} to be called. This makes sure that the underlying resources
     * will not be freed until {@link CopyRef#close()} is called.
     */
    public static class CopyRef implements AutoCloseable {

        private final LocalTargetShardCopyState status;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        /**
         * Important: {@link LocalTargetShardCopyState#tryIncRef()} should
         * be *successfully* called on status before
         */
        public CopyRef(LocalTargetShardCopyState status) {
            this.status = status;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                status.decRef();
            }
        }

        public LocalTargetShardCopyState target() {
            return status;
        }
    }

}

