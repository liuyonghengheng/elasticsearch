package org.elasticsearch.indexmanagement.indexstatemanagement;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContentObject;

import java.io.IOException;

public class Response extends AcknowledgedResponse implements ToXContentObject {

    public Response(StreamInput in) throws IOException {
        super(in);
    }

    public Response(boolean acknowledged) {
        super(acknowledged);
    }
}
