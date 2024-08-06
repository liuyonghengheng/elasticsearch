


package org.elasticsearch.jdbc;

public interface ElasticsearchVersion {
    int getMajor();

    int getMinor();

    int getRevision();

    String getFullVersion();
}
