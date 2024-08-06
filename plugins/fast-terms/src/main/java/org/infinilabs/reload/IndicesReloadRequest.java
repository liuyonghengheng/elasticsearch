package org.infinilabs.reload;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import java.io.IOException;

public class IndicesReloadRequest
	extends BroadcastRequest<IndicesReloadRequest> {

    public IndicesReloadRequest() {
        super((String[])null);
    }

    public IndicesReloadRequest(String... index) {
        super(index);
    }


    public IndicesReloadRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }
}
