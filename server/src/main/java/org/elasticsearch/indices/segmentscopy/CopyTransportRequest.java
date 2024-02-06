package org.elasticsearch.indices.segmentscopy;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

public abstract class CopyTransportRequest extends TransportRequest {
    private final long requestSeqNo;

    CopyTransportRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(Version.V_7_9_0)) {
            requestSeqNo = in.readLong();
        } else {
            requestSeqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
        }
    }

    CopyTransportRequest(long requestSeqNo) {
        this.requestSeqNo = requestSeqNo;
    }

    public long requestSeqNo() {
        return requestSeqNo;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(org.elasticsearch.Version.V_7_9_0)) {
            out.writeLong(requestSeqNo);
        }
    }
}
