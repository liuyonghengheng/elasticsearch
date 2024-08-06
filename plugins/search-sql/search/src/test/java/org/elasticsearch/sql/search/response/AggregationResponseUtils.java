


package org.elasticsearch.sql.search.response;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilter;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.ParsedDateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.ParsedHistogram;
import org.elasticsearch.search.aggregations.bucket.terms.DoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedDoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedLongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ExtendedStatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedAvg;
import org.elasticsearch.search.aggregations.metrics.ParsedExtendedStats;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;
import org.elasticsearch.search.aggregations.metrics.ParsedMin;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.aggregations.metrics.ParsedValueCount;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.ParsedPercentilesBucket;
import org.elasticsearch.search.aggregations.pipeline.PercentilesBucketPipelineAggregationBuilder;

public class AggregationResponseUtils {
  private static final List<NamedXContentRegistry.Entry> entryList =
      new ImmutableMap.Builder<String, ContextParser<Object, ? extends Aggregation>>().put(
          MinAggregationBuilder.NAME, (p, c) -> ParsedMin.fromXContent(p, (String) c))
          .put(MaxAggregationBuilder.NAME, (p, c) -> ParsedMax.fromXContent(p, (String) c))
          .put(SumAggregationBuilder.NAME, (p, c) -> ParsedSum.fromXContent(p, (String) c))
          .put(AvgAggregationBuilder.NAME, (p, c) -> ParsedAvg.fromXContent(p, (String) c))
          .put(ExtendedStatsAggregationBuilder.NAME,
              (p, c) -> ParsedExtendedStats.fromXContent(p, (String) c))
          .put(StringTerms.NAME, (p, c) -> ParsedStringTerms.fromXContent(p, (String) c))
          .put(LongTerms.NAME, (p, c) -> ParsedLongTerms.fromXContent(p, (String) c))
          .put(DoubleTerms.NAME, (p, c) -> ParsedDoubleTerms.fromXContent(p, (String) c))
          .put(ValueCountAggregationBuilder.NAME,
              (p, c) -> ParsedValueCount.fromXContent(p, (String) c))
          .put(PercentilesBucketPipelineAggregationBuilder.NAME,
              (p, c) -> ParsedPercentilesBucket.fromXContent(p, (String) c))
          .put(DateHistogramAggregationBuilder.NAME,
              (p, c) -> ParsedDateHistogram.fromXContent(p, (String) c))
          .put(HistogramAggregationBuilder.NAME,
              (p, c) -> ParsedHistogram.fromXContent(p, (String) c))
          .put(CompositeAggregationBuilder.NAME,
              (p, c) -> ParsedComposite.fromXContent(p, (String) c))
          .put(FilterAggregationBuilder.NAME,
              (p, c) -> ParsedFilter.fromXContent(p, (String) c))
          .build()
          .entrySet()
          .stream()
          .map(entry -> new NamedXContentRegistry.Entry(Aggregation.class,
              new ParseField(entry.getKey()),
              entry.getValue()))
          .collect(Collectors.toList());
  private static final NamedXContentRegistry namedXContentRegistry =
      new NamedXContentRegistry(entryList);
  private static final XContent xContent = XContentFactory.xContent(XContentType.JSON);

  /**
   * Populate {@link Aggregations} from JSON string.
   *
   * @param json json string
   * @return {@link Aggregations}
   */
  public static Aggregations fromJson(String json) {
    try {
      XContentParser contentParser =
          xContent.createParser(namedXContentRegistry, LoggingDeprecationHandler.INSTANCE, json);
      contentParser.nextToken();
      return Aggregations.fromXContent(contentParser);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
