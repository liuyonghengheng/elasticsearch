package org.elasticsearch.indices.segmentscopy;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CopyFilesInfoRequest extends CopyTransportRequest {

    private ShardId shardId;
    List<String> fileNames;
    List<Long> fileSizes;
    List<String> existingFileNames;
    List<Long> existingFileSizes;
    public CopyFilesInfoRequest(StreamInput in) throws IOException {
        super(in);
        shardId = new ShardId(in);
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

        size = in.readVInt();
        existingFileNames = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            existingFileNames.add(in.readString());
        }

        size = in.readVInt();
        existingFileSizes = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            existingFileSizes.add(in.readVLong());
        }

    }

    CopyFilesInfoRequest(long requestSeqNo, ShardId shardId, List<String> fileNames,
                             List<Long> fileSizes, List<String> existingFileNames, List<Long> existingFileSizes) {
        super(requestSeqNo);
        this.shardId = shardId;
        this.fileNames = fileNames;
        this.fileSizes = fileSizes;
        this.existingFileNames = existingFileNames;
        this.existingFileSizes = existingFileSizes;
    }

    public ShardId shardId() {
        return shardId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);

        out.writeVInt(fileNames.size());
        for (String fileName : fileNames) {
            out.writeString(fileName);
        }

        out.writeVInt(fileSizes.size());
        for (Long fileSize : fileSizes) {
            out.writeVLong(fileSize);
        }

        out.writeVInt(existingFileNames.size());
        for (String existingFileName : existingFileNames) {
            out.writeString(existingFileName);
        }

        out.writeVInt(existingFileSizes.size());
        for (Long existingFileSize : existingFileSizes) {
            out.writeVLong(existingFileSize);
        }
    }
}
