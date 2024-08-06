


package org.elasticsearch.sql.search.planner.logical;

import static org.junit.jupiter.api.Assertions.assertFalse;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

class ElasticsearchLogicalIndexScanTest {

  @Test
  void has_projects() {
    assertFalse(ElasticsearchLogicalIndexScan.builder()
        .projectList(ImmutableSet.of()).build()
        .hasProjects());

    assertFalse(ElasticsearchLogicalIndexScan.builder().build().hasProjects());
  }
}
