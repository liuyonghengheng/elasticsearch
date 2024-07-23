package org.elasticsearch.knn.query

import org.elasticsearch.knn.mapper.VectorMapper
import org.elasticsearch.knn.models.{Cache, SparseIndexedSimilarityFunction}
import org.elasticsearch.knn.models.{ExactSimilarityFunction => ESF}
import org.apache.lucene.index.IndexReader
import org.apache.lucene.search.Query
import org.elasticsearch.common.lucene.search.function.ScoreFunction
import org.elasticsearch.index.mapper.MappedFieldType
import org.elasticsearch.index.query.QueryShardContext
import org.elasticsearch.knn.ElasticknnException.ElasticknnRuntimeException
import org.elasticsearch.knn.api.NearestNeighborsQuery.{CosineLsh, Exact, HammingLsh, JaccardLsh, L2Lsh, PermutationLsh, SparseIndexed}
import org.elasticsearch.knn.api._

import scala.language.implicitConversions
import scala.util._

/**
  * Useful way to represent a query. The name is meh.
  */
trait ElastiknnQuery[V <: Vec] {
  def toLuceneQuery(indexReader: IndexReader): Query
  def toScoreFunction(indexReader: IndexReader): ScoreFunction
}

object ElastiknnQuery {

  private def incompatible(q: NearestNeighborsQuery, m: Mapping): Exception = {
    val msg = s"Query [${ElasticsearchCodec.encode(q).noSpaces}] is not compatible with mapping [${ElasticsearchCodec.encode(m).noSpaces}]"
    new IllegalArgumentException(msg)
  }

  def getMapping(context: QueryShardContext, field: String): Mapping = {
    import VectorMapper._
    val mft: MappedFieldType = context.fieldMapper(field)
    mft match {
      case ft: FieldType => ft.mapping
      case null =>
        throw new ElasticknnRuntimeException(s"Could not find mapped field type for field [${field}]")
      case _ =>
        throw new ElasticknnRuntimeException(
          s"Expected field [${mft.name}] to have type [${denseFloatVector.CONTENT_TYPE}] or [${sparseBoolVector.CONTENT_TYPE}] but had [${mft.typeName}]")
    }
  }

  def apply(query: NearestNeighborsQuery, queryShardContext: QueryShardContext): Try[ElastiknnQuery[_]] =
    apply(query, getMapping(queryShardContext, query.field))

  private implicit def toSuccess[A <: Vec](q: ElastiknnQuery[A]): Try[ElastiknnQuery[A]] = Success(q)

  def apply(query: NearestNeighborsQuery, mapping: Mapping): Try[ElastiknnQuery[_]] =
    (query, mapping) match {

      case (Exact(f, Similarity.Jaccard, v: Vec.SparseBool),
            _: Mapping.SparseBool | _: Mapping.SparseIndexed | _: Mapping.JaccardLsh | _: Mapping.HammingLsh) =>
        new ExactQuery(f, v, ESF.Jaccard)

      case (Exact(f, Similarity.Hamming, v: Vec.SparseBool),
            _: Mapping.SparseBool | _: Mapping.SparseIndexed | _: Mapping.JaccardLsh | _: Mapping.HammingLsh) =>
        new ExactQuery(f, v, ESF.Hamming)

      case (Exact(f, Similarity.L1, v: Vec.DenseFloat),
            _: Mapping.DenseFloat | _: Mapping.CosineLsh | _: Mapping.L2Lsh | _: Mapping.PermutationLsh) =>
        new ExactQuery(f, v, ESF.L1)

      case (Exact(f, Similarity.L2, v: Vec.DenseFloat),
            _: Mapping.DenseFloat | _: Mapping.CosineLsh | _: Mapping.L2Lsh | _: Mapping.PermutationLsh) =>
        new ExactQuery(f, v, ESF.L2)

      case (Exact(f, Similarity.Cosine, v: Vec.DenseFloat),
            _: Mapping.DenseFloat | _: Mapping.CosineLsh | _: Mapping.L2Lsh | _: Mapping.PermutationLsh) =>
        new ExactQuery(f, v, ESF.Cosine)

      case (SparseIndexed(f, Similarity.Jaccard, sbv: Vec.SparseBool), _: Mapping.SparseIndexed) =>
        new SparseIndexedQuery(f, sbv, SparseIndexedSimilarityFunction.Jaccard)

      case (SparseIndexed(f, Similarity.Hamming, sbv: Vec.SparseBool), _: Mapping.SparseIndexed) =>
        new SparseIndexedQuery(f, sbv, SparseIndexedSimilarityFunction.Hamming)

      case (JaccardLsh(f, candidates, v: Vec.SparseBool), m: Mapping.JaccardLsh) =>
        new HashingQuery(f, v, candidates, Cache(m).hash(v.trueIndices, v.totalIndices), ESF.Jaccard)

      case (HammingLsh(f, candidates, v: Vec.SparseBool), m: Mapping.HammingLsh) =>
        new HashingQuery(f, v, candidates, Cache(m).hash(v.trueIndices, v.totalIndices), ESF.Hamming)

      case (CosineLsh(f, candidates, v: Vec.DenseFloat), m: Mapping.CosineLsh) =>
        new HashingQuery(f, v, candidates, Cache(m).hash(v.values), ESF.Cosine)

      case (L2Lsh(f, candidates, probes, v: Vec.DenseFloat), m: Mapping.L2Lsh) =>
        new HashingQuery(f, v, candidates, Cache(m).hash(v.values, probes), ESF.L2)

      case (PermutationLsh(f, Similarity.Cosine, candidates, v: Vec.DenseFloat), m: Mapping.PermutationLsh) =>
        new HashingQuery(f, v, candidates, Cache(m).hash(v.values), ESF.Cosine)

      case (PermutationLsh(f, Similarity.L2, candidates, v: Vec.DenseFloat), m: Mapping.PermutationLsh) =>
        new HashingQuery(f, v, candidates, Cache(m).hash(v.values), ESF.L2)

      case (PermutationLsh(f, Similarity.L1, candidates, v: Vec.DenseFloat), m: Mapping.PermutationLsh) =>
        new HashingQuery(f, v, candidates, Cache(m).hash(v.values), ESF.L1)

      case _ => Failure(incompatible(query, mapping))
    }
}
