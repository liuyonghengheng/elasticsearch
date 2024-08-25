package org.elasticsearch.indices.segmentscopy;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.IndexId;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public interface TargetShardCopyState {
    void writeFileChunk(StoreFileMetadata fileMetadata, long position, BytesReference content,
                        boolean lastChunk, int totalTranslogOps, ActionListener<Void> listener);
    /**
     * After all source files has been sent over, this command is sent to the target so it can clean any local
     * files that are not part of the source store
     *
     * @param globalCheckpoint the global checkpoint on the primary
     * @param sourceMetadata   meta data of the source store
     */
    void cleanFiles(long globalCheckpoint, Map<String, StoreFileMetadata> sourceMetadata, ActionListener<Void> listener);
    default void cancel() {}
}
