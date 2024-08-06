


package org.elasticsearch.sql.sql.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import static org.elasticsearch.sql.ast.dsl.AstDSL.argument;
import static org.elasticsearch.sql.ast.dsl.AstDSL.booleanLiteral;
import static org.elasticsearch.sql.ast.dsl.AstDSL.field;
import static org.elasticsearch.sql.ast.dsl.AstDSL.qualifiedName;
import static org.elasticsearch.sql.ast.tree.Sort.NullOrder.NULL_FIRST;
import static org.elasticsearch.sql.ast.tree.Sort.NullOrder.NULL_LAST;
import static org.elasticsearch.sql.ast.tree.Sort.SortOrder.ASC;
import static org.elasticsearch.sql.ast.tree.Sort.SortOrder.DESC;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.elasticsearch.sql.ast.expression.Argument;
import org.elasticsearch.sql.ast.tree.Sort;
import org.elasticsearch.sql.ast.tree.Sort.SortOption;
import org.elasticsearch.sql.ast.tree.UnresolvedPlan;
import org.elasticsearch.sql.sql.antlr.parser.ElasticsearchSQLParser.OrderByClauseContext;
import org.elasticsearch.sql.sql.parser.context.QuerySpecification;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
class AstSortBuilderTest {

  @Mock
  private QuerySpecification querySpec;

  @Mock
  private OrderByClauseContext orderByClause;

  @Mock
  private UnresolvedPlan child;

  @Test
  void can_build_sort_node() {
    doAnswer(returnsFirstArg()).when(querySpec).replaceIfAliasOrOrdinal(any());
    when(querySpec.getOrderByItems()).thenReturn(ImmutableList.of(qualifiedName("name")));

    ImmutableMap<SortOption, List<Argument>> expects =
        ImmutableMap.<SortOption, List<Argument>>builder()
            .put(new SortOption(null, null),
                ImmutableList.of(argument("asc", booleanLiteral(true))))
            .put(new SortOption(ASC, null),
                ImmutableList.of(argument("asc", booleanLiteral(true))))
            .put(new SortOption(DESC, null),
                ImmutableList.of(argument("asc", booleanLiteral(false))))
            .put(new SortOption(null, NULL_LAST),
                ImmutableList.of(
                    argument("asc", booleanLiteral(true)),
                    argument("nullFirst", booleanLiteral(false))))
            .put(new SortOption(DESC, NULL_FIRST),
                ImmutableList.of(
                    argument("asc", booleanLiteral(false)),
                    argument("nullFirst", booleanLiteral(true))))
            .build();

    expects.forEach((option, expect) -> {
      when(querySpec.getOrderByOptions()).thenReturn(ImmutableList.of(option));

      AstSortBuilder sortBuilder = new AstSortBuilder(querySpec);
      assertEquals(
          new Sort(
              child, // has to mock and attach child otherwise Guava ImmutableList NPE in getChild()
              ImmutableList.of(field("name", expect))),
          sortBuilder.visitOrderByClause(orderByClause).attach(child));
    });
  }

}
