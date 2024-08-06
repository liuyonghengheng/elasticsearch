


package org.elasticsearch.jdbc.protocol.http;

import org.elasticsearch.jdbc.protocol.ConnectionResponse;
import org.elasticsearch.jdbc.protocol.ClusterMetadata;

public class JsonConnectionResponse implements ConnectionResponse {
    private ClusterMetadata clusterMetadata;

    public JsonConnectionResponse(ClusterMetadata clusterMetadata) {
        this.clusterMetadata = clusterMetadata;
    }

    @Override
    public ClusterMetadata getClusterMetadata() {
        return clusterMetadata;
    }
}
