


package org.elasticsearch.sql.search.storage.script.aggregation.dsl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.sql.data.type.ExprCoreType.INTEGER;
import static org.elasticsearch.sql.data.type.ExprCoreType.STRING;
import static org.elasticsearch.sql.expression.DSL.named;
import static org.elasticsearch.sql.expression.DSL.ref;
import static org.elasticsearch.sql.search.data.type.ElasticsearchDataType.EASYSEARCH_TEXT_KEYWORD;

import java.util.Arrays;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.missing.MissingOrder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.sql.expression.DSL;
import org.elasticsearch.sql.expression.NamedExpression;
import org.elasticsearch.sql.expression.ParseExpression;
import org.elasticsearch.sql.search.storage.serialization.ExpressionSerializer;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
class BucketAggregationBuilderTest {

  @Mock
  private ExpressionSerializer serializer;

  private BucketAggregationBuilder aggregationBuilder;

  @BeforeEach
  void set_up() {
    aggregationBuilder = new BucketAggregationBuilder(serializer);
  }

  @Test
  void should_build_bucket_with_field() {
    assertEquals(
        "{\n"
            + "  \"terms\" : {\n"
            + "    \"field\" : \"age\",\n"
            + "    \"missing_bucket\" : true,\n"
            + "    \"missing_order\" : \"first\",\n"
            + "    \"order\" : \"asc\"\n"
            + "  }\n"
            + "}",
        buildQuery(
            Arrays.asList(
                asc(named("age", ref("age", INTEGER))))));
  }

  @Test
  void should_build_bucket_with_keyword_field() {
    assertEquals(
        "{\n"
            + "  \"terms\" : {\n"
            + "    \"field\" : \"name.keyword\",\n"
            + "    \"missing_bucket\" : true,\n"
            + "    \"missing_order\" : \"first\",\n"
            + "    \"order\" : \"asc\"\n"
            + "  }\n"
            + "}",
        buildQuery(
            Arrays.asList(
                asc(named("name", ref("name", EASYSEARCH_TEXT_KEYWORD))))));
  }

  @Test
  void should_build_bucket_with_parse_expression() {
    ParseExpression parseExpression =
        DSL.parsed(ref("name.keyword", STRING), DSL.literal("(?<name>\\w+)"), DSL.literal("name"));
    when(serializer.serialize(parseExpression)).thenReturn("mock-serialize");
    assertEquals(
        "{\n"
            + "  \"terms\" : {\n"
            + "    \"script\" : {\n"
            + "      \"source\" : \"mock-serialize\",\n"
            + "      \"lang\" : \"elasticsearch_query_expression\"\n"
            + "    },\n"
            + "    \"missing_bucket\" : true,\n"
            + "    \"missing_order\" : \"first\",\n"
            + "    \"order\" : \"asc\"\n"
            + "  }\n"
            + "}",
        buildQuery(
            Arrays.asList(
                asc(named("name", parseExpression)))));
  }

  @SneakyThrows
  private String buildQuery(
      List<Triple<NamedExpression, SortOrder, MissingOrder>> groupByExpressions) {
    XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).prettyPrint();
    builder.startObject();
    CompositeValuesSourceBuilder<?> sourceBuilder =
        aggregationBuilder.build(groupByExpressions).get(0);
    sourceBuilder.toXContent(builder, EMPTY_PARAMS);
    builder.endObject();
    return BytesReference.bytes(builder).utf8ToString();
  }

  private Triple<NamedExpression, SortOrder, MissingOrder> asc(NamedExpression expression) {
    return Triple.of(expression, SortOrder.ASC, MissingOrder.FIRST);
  }
}
