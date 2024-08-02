package org.elasticsearch.indices.segmentscopy;

import org.apache.lucene.util.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.StoreFileMetadata;

import java.io.IOException;

public final class CopyFileChunkRequest extends CopyTransportRequest {
    private final boolean lastChunk;
    private final ShardId shardId;
    private final long position;
    private final BytesReference content;
    private final StoreFileMetadata metadata;
    private final long sourceThrottleTimeInNanos;

    private final int totalTranslogOps;

    public CopyFileChunkRequest(StreamInput in) throws IOException {
        super(in);
        shardId = new ShardId(in);
        String name = in.readString();
        position = in.readVLong();
        long length = in.readVLong();
        String checksum = in.readString();
        content = in.readBytesReference();
        Version writtenBy = Lucene.parseVersionLenient(in.readString(), null);
        assert writtenBy != null;
        metadata = new StoreFileMetadata(name, length, checksum, writtenBy);
        lastChunk = in.readBoolean();
        totalTranslogOps = in.readVInt();
        sourceThrottleTimeInNanos = in.readLong();
    }

    public CopyFileChunkRequest(final long requestSeqNo, ShardId shardId, StoreFileMetadata metadata, long position,
                                BytesReference content, boolean lastChunk, int totalTranslogOps, long sourceThrottleTimeInNanos) {
        super(requestSeqNo);
        this.shardId = shardId;
        this.metadata = metadata;
        this.position = position;
        this.content = content;
        this.lastChunk = lastChunk;
        this.totalTranslogOps = totalTranslogOps;
        this.sourceThrottleTimeInNanos = sourceThrottleTimeInNanos;
    }

    public ShardId shardId() {
        return shardId;
    }

    public String name() {
        return metadata.name();
    }

    public long position() {
        return position;
    }

    public long length() {
        return metadata.length();
    }

    public BytesReference content() {
        return content;
    }

    public int totalTranslogOps() {
        return totalTranslogOps;
    }

    public long sourceThrottleTimeInNanos() {
        return sourceThrottleTimeInNanos;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        out.writeString(metadata.name());
        out.writeVLong(position);
        out.writeVLong(metadata.length());
        out.writeString(metadata.checksum());
        out.writeBytesReference(content);
        out.writeString(metadata.writtenBy().toString());
        out.writeBoolean(lastChunk);
        out.writeVInt(totalTranslogOps);
        out.writeLong(sourceThrottleTimeInNanos);
    }

    @Override
    public String toString() {
        return shardId + ": name='" + name() + '\'' +
                ", position=" + position +
                ", length=" + length();
    }

    public StoreFileMetadata metadata() {
        return metadata;
    }

    /**
     * Returns <code>true</code> if this chunk is the last chunk in the stream.
     */
    public boolean lastChunk() {
        return lastChunk;
    }
}
