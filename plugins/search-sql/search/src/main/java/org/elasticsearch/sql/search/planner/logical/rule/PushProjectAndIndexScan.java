

package org.elasticsearch.sql.search.planner.logical.rule;

import static com.facebook.presto.matching.Pattern.typeOf;
import static org.elasticsearch.sql.search.planner.logical.rule.OptimizationRuleUtils.findReferenceExpressions;
import static org.elasticsearch.sql.planner.optimizer.pattern.Patterns.source;

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import java.util.Set;
import org.elasticsearch.sql.expression.ReferenceExpression;
import org.elasticsearch.sql.search.planner.logical.ElasticsearchLogicalIndexScan;
import org.elasticsearch.sql.planner.logical.LogicalPlan;
import org.elasticsearch.sql.planner.logical.LogicalProject;
import org.elasticsearch.sql.planner.optimizer.Rule;

/**
 * Push Project list into ElasticsearchLogicalIndexScan.
 */
public class PushProjectAndIndexScan implements Rule<LogicalProject> {

  private final Capture<ElasticsearchLogicalIndexScan> indexScanCapture;

  private final Pattern<LogicalProject> pattern;

  private Set<ReferenceExpression> pushDownProjects;

  /**
   * Constructor of MergeProjectAndIndexScan.
   */
  public PushProjectAndIndexScan() {
    this.indexScanCapture = Capture.newCapture();
    this.pattern = typeOf(LogicalProject.class).matching(
        project -> {
          pushDownProjects = findReferenceExpressions(project.getProjectList());
          return !pushDownProjects.isEmpty();
        }).with(source()
        .matching(typeOf(ElasticsearchLogicalIndexScan.class)
            .matching(indexScan -> !indexScan.hasProjects())
            .capturedAs(indexScanCapture)));

  }

  @Override
  public Pattern<LogicalProject> pattern() {
    return pattern;
  }

  @Override
  public LogicalPlan apply(LogicalProject project,
                           Captures captures) {
    ElasticsearchLogicalIndexScan indexScan = captures.get(indexScanCapture);
    indexScan.setProjectList(pushDownProjects);
    return new LogicalProject(indexScan, project.getProjectList(),
        project.getNamedParseExpressions());
  }
}
