


package org.elasticsearch.sql.search.data.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.elasticsearch.sql.data.type.ExprCoreType.STRING;
import static org.elasticsearch.sql.search.data.type.ElasticsearchDataType.EASYSEARCH_TEXT;
import static org.elasticsearch.sql.search.data.type.ElasticsearchDataType.EASYSEARCH_TEXT_KEYWORD;

import org.junit.jupiter.api.Test;

class ElasticsearchDataTypeTest {
  @Test
  public void testIsCompatible() {
    assertTrue(STRING.isCompatible(EASYSEARCH_TEXT));
    assertFalse(EASYSEARCH_TEXT.isCompatible(STRING));

    assertTrue(STRING.isCompatible(EASYSEARCH_TEXT_KEYWORD));
    assertTrue(EASYSEARCH_TEXT.isCompatible(EASYSEARCH_TEXT_KEYWORD));
  }

  @Test
  public void testTypeName() {
    assertEquals("string", EASYSEARCH_TEXT.typeName());
    assertEquals("string", EASYSEARCH_TEXT_KEYWORD.typeName());
  }

  @Test
  public void legacyTypeName() {
    assertEquals("text", EASYSEARCH_TEXT.legacyTypeName());
    assertEquals("text", EASYSEARCH_TEXT_KEYWORD.legacyTypeName());
  }

  @Test
  public void testShouldCast() {
    assertFalse(EASYSEARCH_TEXT.shouldCast(STRING));
    assertFalse(EASYSEARCH_TEXT_KEYWORD.shouldCast(STRING));
  }
}
