


package org.elasticsearch.jdbc.protocol;

import org.elasticsearch.jdbc.ElasticsearchVersion;

public interface ClusterMetadata {
    String getClusterName();

    String getClusterUUID();

    ElasticsearchVersion getVersion();
}
