package org.elasticsearch.indices.segmentscopy;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.IndexId;

import java.util.concurrent.atomic.AtomicLong;

public interface TargetShardCopyState {
    void writeFileChunk(StoreFileMetadata fileMetadata, long position, BytesReference content,
                        boolean lastChunk, int totalTranslogOps, ActionListener<Void> listener);

    default void cancel() {}
}
