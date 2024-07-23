package org.elasticsearch.knn


import java.util
import java.util.{Collections, Optional}
import org.elasticsearch.common.settings.{Setting, Settings}
import org.elasticsearch.index.IndexSettings
import org.elasticsearch.index.engine.{Engine, EngineConfig, EngineFactory, InternalEngine}
import org.elasticsearch.index.mapper.Mapper
import org.elasticsearch.knn.codec.ElasticknnCodecService
import org.elasticsearch.knn.mapper.VectorMapper.{denseFloatVector, sparseBoolVector}
import org.elasticsearch.knn.query.{KnnQueryBuilder, KnnScoreFunctionBuilder}
import org.elasticsearch.plugins.SearchPlugin.{QuerySpec, ScoreFunctionSpec}
import org.elasticsearch.plugins._


class ElasticknnPlugin(settings: Settings) extends Plugin with SearchPlugin with MapperPlugin with EnginePlugin {

  override def getQueries: util.List[SearchPlugin.QuerySpec[_]] = Collections.singletonList(
    new QuerySpec(KnnQueryBuilder.NAME, KnnQueryBuilder.Reader, KnnQueryBuilder.Parser)
  )

  override def getMappers: util.Map[String, Mapper.TypeParser] = {
    new util.HashMap[String, Mapper.TypeParser] {
      put(sparseBoolVector.CONTENT_TYPE, new sparseBoolVector.TypeParser)
      put(denseFloatVector.CONTENT_TYPE, new denseFloatVector.TypeParser)
    }
  }

  override def getSettings: util.List[Setting[_]] = Collections.singletonList(ElasticknnPlugin.Settings.elasticknn)

  override def getScoreFunctions: util.List[SearchPlugin.ScoreFunctionSpec[_]] =
    Collections.singletonList(
      new ScoreFunctionSpec(KnnScoreFunctionBuilder.NAME, KnnScoreFunctionBuilder.Reader, KnnScoreFunctionBuilder.Parser)
    )

  override def getEngineFactory(indexSettings: IndexSettings): Optional[EngineFactory] = if (indexSettings.getValue(ElasticknnPlugin.Settings.elasticknn)) Optional.of {
    new EngineFactory {
      val codecService = new ElasticknnCodecService
      override def newReadWriteEngine(config: EngineConfig) = new InternalEngine(
          new EngineConfig(
            config.getShardId,
            config.getThreadPool,
            config.getIndexSettings,
            config.getWarmer,
            config.getStore,
            config.getMergePolicy,
            config.getAnalyzer,
            config.getSimilarity,
            codecService,
            config.getEventListener,
            config.getQueryCache,
            config.getQueryCachingPolicy,
            config.getTranslogConfig,
            config.getFlushMergesAfter,
            config.getExternalRefreshListener,
            config.getInternalRefreshListener,
            config.getIndexSort,
            config.getCircuitBreakerService,
            config.getGlobalCheckpointSupplier,
            config.retentionLeasesSupplier,
            config.getPrimaryTermSupplier,
            config.getTombstoneDocSupplier
          ))
    }
  } else Optional.empty()
}

object ElasticknnPlugin {

  object Settings {

    // Setting: index.knn
    // Determines whether elasticknn can control the codec used for the index.
    // Highly recommended to set to true. Elastiknn will still work without it, but will be much slower.
    val elasticknn: Setting[java.lang.Boolean] =
      Setting.boolSetting("index.knn", false, Setting.Property.IndexScope)
  }

}
