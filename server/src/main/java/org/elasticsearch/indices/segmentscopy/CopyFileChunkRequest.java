package org.elasticsearch.indices.segmentscopy;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

public class CopyFileChunkRequest extends TransportRequest {

    public CopyFileChunkRequest(StreamInput in) throws IOException {

    }
}
