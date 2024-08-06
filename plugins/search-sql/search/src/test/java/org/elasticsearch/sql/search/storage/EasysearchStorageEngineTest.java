


package org.elasticsearch.sql.search.storage;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.elasticsearch.sql.utils.SystemIndexUtils.TABLE_INFO;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.elasticsearch.sql.common.setting.Settings;
import org.elasticsearch.sql.search.client.ElasticsearchClient;
import org.elasticsearch.sql.search.storage.system.ElasticsearchSystemIndex;
import org.elasticsearch.sql.storage.Table;

@ExtendWith(MockitoExtension.class)
class ElasticsearchStorageEngineTest {

  @Mock private ElasticsearchClient client;

  @Mock private Settings settings;

  @Test
  public void getTable() {
    ElasticsearchStorageEngine engine = new ElasticsearchStorageEngine(client, settings);
    Table table = engine.getTable("test");
    assertNotNull(table);
  }

  @Test
  public void getSystemTable() {
    ElasticsearchStorageEngine engine = new ElasticsearchStorageEngine(client, settings);
    Table table = engine.getTable(TABLE_INFO);
    assertNotNull(table);
    assertTrue(table instanceof ElasticsearchSystemIndex);
  }
}
