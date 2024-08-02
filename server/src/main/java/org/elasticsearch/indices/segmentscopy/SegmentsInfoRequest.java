package org.elasticsearch.indices.segmentscopy;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SegmentsInfoRequest extends CopyTransportRequest {
    private ShardId shardId;
    DiscoveryNode sourceNode;
    Long segmentInfoVersion;
    Long segmentInfoGen;
    Long primaryTerm;
    byte [] infosBytes;
    List<String> fileNames;
    List<Long> fileSizes;

    public SegmentsInfoRequest(StreamInput in) throws IOException {
        super(in);
        shardId = new ShardId(in);
        sourceNode = new DiscoveryNode(in);
        segmentInfoVersion = in.readVLong();
        segmentInfoGen = in.readVLong();
        primaryTerm = in.readVLong();
//        int size = in.readVInt();
//        infosBytes = new byte[size];
//        in.readBytes(infosBytes, 0, size);
        infosBytes = in.readByteArray();

        int size = in.readVInt();
        fileNames = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            fileNames.add(in.readString());
        }

        size = in.readVInt();
        fileSizes = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            fileSizes.add(in.readVLong());
        }
    }

    public SegmentsInfoRequest(long requestSeqNo, ShardId shardId, DiscoveryNode sourceNode, Long segmentInfoVersion, Long segmentInfoGen,
                               Long primaryTerm, byte [] infosBytes, List<String> fileNames, List<Long> fileSizes) {
        super(requestSeqNo);
        this.shardId = shardId;
        this.sourceNode =  sourceNode;
        this.segmentInfoVersion = segmentInfoVersion;
        this.segmentInfoGen = segmentInfoGen;
        this.primaryTerm = primaryTerm;
        this.infosBytes=infosBytes;
        this.fileNames = fileNames;
        this.fileSizes = fileSizes;
    }

    public ShardId shardId() {
        return shardId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        sourceNode.writeTo(out);
        out.writeVLong(segmentInfoVersion);
        out.writeVLong(segmentInfoGen);
        out.writeVLong(primaryTerm);
//        out.writeVInt(infosBytes.length);
//        out.writeBytes(infosBytes, 0, infosBytes.length);
        out.writeByteArray(infosBytes);

        out.writeVInt(fileNames.size());
        for (String fileName : fileNames) {
            out.writeString(fileName);
        }

        out.writeVInt(fileSizes.size());
        for (Long fileSize : fileSizes) {
            out.writeVLong(fileSize);
        }
    }
}
