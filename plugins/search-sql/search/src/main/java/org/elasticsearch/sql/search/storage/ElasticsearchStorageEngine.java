

package org.elasticsearch.sql.search.storage;

import static org.elasticsearch.sql.utils.SystemIndexUtils.isSystemIndex;

import lombok.RequiredArgsConstructor;
import org.elasticsearch.sql.common.setting.Settings;
import org.elasticsearch.sql.search.client.ElasticsearchClient;
import org.elasticsearch.sql.search.storage.system.ElasticsearchSystemIndex;
import org.elasticsearch.sql.storage.StorageEngine;
import org.elasticsearch.sql.storage.Table;

/** Elasticsearch storage engine implementation. */
@RequiredArgsConstructor
public class ElasticsearchStorageEngine implements StorageEngine {

  /** Elasticsearch client connection. */
  private final ElasticsearchClient client;

  private final Settings settings;

  @Override
  public Table getTable(String name) {
    if (isSystemIndex(name)) {
      return new ElasticsearchSystemIndex(client, name);
    } else {
      return new ElasticsearchIndex(client, settings, name);
    }
  }
}
