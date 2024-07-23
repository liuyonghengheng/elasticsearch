package org.elasticsearch.knn.query

import org.elasticsearch.common.io.stream.{StreamInput, StreamOutput, Writeable}
import org.elasticsearch.common.lucene.search.function.ScoreFunction
import org.elasticsearch.common.xcontent.{ToXContent, XContentBuilder, XContentParser}
import org.elasticsearch.index.query.QueryShardContext
import org.elasticsearch.index.query.functionscore.{ScoreFunctionBuilder, ScoreFunctionParser}
import org.elasticsearch.knn.ELASTICKNN_NAME
import org.elasticsearch.knn.api.NearestNeighborsQuery

import java.util.Objects


final class KnnScoreFunctionBuilder(val query: NearestNeighborsQuery, val weight: Float)
    extends ScoreFunctionBuilder[KnnScoreFunctionBuilder] {

  setWeight(weight)

  override def doWriteTo(out: StreamOutput): Unit =
    out.writeString(KnnQueryBuilder.encodeB64(query))

  override def getName: String = KnnScoreFunctionBuilder.NAME

  override def doXContent(builder: XContentBuilder, params: ToXContent.Params): Unit = ()

  override def doEquals(other: KnnScoreFunctionBuilder): Boolean = other.query == query && other.weight == weight

  override def doHashCode(): Int = Objects.hash(query, weight.asInstanceOf[java.lang.Float])

  override def doToFunction(context: QueryShardContext): ScoreFunction = {
    ElastiknnQuery(query, context).map(_.toScoreFunction(context.getIndexReader)).get
  }
}

object KnnScoreFunctionBuilder {

  val NAME: String = s"${ELASTICKNN_NAME}_nearest_neighbors"

  object Reader extends Writeable.Reader[KnnScoreFunctionBuilder] {
    override def read(in: StreamInput): KnnScoreFunctionBuilder = {
      val weight = in.readOptionalFloat()
      val s = in.readString()
      val query = KnnQueryBuilder.decodeB64[NearestNeighborsQuery](s)
      new KnnScoreFunctionBuilder(query, weight)
    }
  }

  object Parser extends ScoreFunctionParser[KnnScoreFunctionBuilder] {
    override def fromXContent(parser: XContentParser): KnnScoreFunctionBuilder = {
      val knnqb = KnnQueryBuilder.Parser.fromXContent(parser)
      new KnnScoreFunctionBuilder(knnqb.query, 1f)
    }
  }
}
