package org.elasticsearch.indices.segmentscopy;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class SegmentsInfoResponse extends TransportResponse {
    final Set<String> fileNames;
    public SegmentsInfoResponse(Set<String> fileNames){
        this.fileNames = fileNames;
    }

    SegmentsInfoResponse(StreamInput in) throws IOException {
        super(in);
        fileNames = new HashSet<>(in.readStringList());
    }


    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(fileNames);
    }
}
