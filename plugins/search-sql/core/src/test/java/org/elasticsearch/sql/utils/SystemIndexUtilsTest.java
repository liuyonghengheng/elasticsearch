


package org.elasticsearch.sql.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.elasticsearch.sql.utils.SystemIndexUtils.isSystemIndex;
import static org.elasticsearch.sql.utils.SystemIndexUtils.mappingTable;
import static org.elasticsearch.sql.utils.SystemIndexUtils.systemTable;

import org.junit.jupiter.api.Test;

class SystemIndexUtilsTest {

  @Test
  void test_system_index() {
    assertTrue(isSystemIndex("_ODFE_SYS_TABLE_META.ALL"));
    assertFalse(isSystemIndex(".elasticsearch_dashboards"));
  }

  @Test
  void test_compose_mapping_table() {
    assertEquals("_ODFE_SYS_TABLE_MAPPINGS.employee", mappingTable("employee"));
  }

  @Test
  void test_system_info_table() {
    final SystemIndexUtils.SystemTable table = systemTable("_ODFE_SYS_TABLE_META.ALL");

    assertTrue(table.isSystemInfoTable());
    assertFalse(table.isMetaInfoTable());
    assertEquals("ALL", table.getTableName());
  }

  @Test
  void test_mapping_info_table() {
    final SystemIndexUtils.SystemTable table = systemTable("_ODFE_SYS_TABLE_MAPPINGS.employee");

    assertTrue(table.isMetaInfoTable());
    assertFalse(table.isSystemInfoTable());
    assertEquals("employee", table.getTableName());
  }

  @Test
  void throw_exception_for_invalid_index() {
    final IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> systemTable("_ODFE_SYS_TABLE.employee"));
    assertEquals("Invalid system index name: _ODFE_SYS_TABLE.employee", exception.getMessage());
  }
}
