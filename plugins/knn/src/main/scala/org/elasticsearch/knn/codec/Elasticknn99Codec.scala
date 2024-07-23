package org.elasticsearch.knn.codec

import org.apache.lucene.codecs._
import org.apache.lucene.codecs.lucene99.Lucene99Codec
import org.apache.lucene.codecs.DocValuesFormat

class Elasticknn99Codec extends Codec(ElasticknnCodecService.Elasticknn_99) {
  private val luceneCodec: Codec = new Lucene99Codec()
//  override def docValuesFormat(): DocValuesFormat = new DocValuesFormat()
  override def docValuesFormat(): DocValuesFormat = luceneCodec.docValuesFormat()
  override def postingsFormat(): PostingsFormat = luceneCodec.postingsFormat()
  override def storedFieldsFormat(): StoredFieldsFormat = luceneCodec.storedFieldsFormat()
  override def termVectorsFormat(): TermVectorsFormat = luceneCodec.termVectorsFormat()
  override def fieldInfosFormat(): FieldInfosFormat = luceneCodec.fieldInfosFormat()
  override def segmentInfoFormat(): SegmentInfoFormat = luceneCodec.segmentInfoFormat()
  override def normsFormat(): NormsFormat = luceneCodec.normsFormat()
  override def liveDocsFormat(): LiveDocsFormat = luceneCodec.liveDocsFormat()
  override def compoundFormat(): CompoundFormat = luceneCodec.compoundFormat()
  override def pointsFormat(): PointsFormat = luceneCodec.pointsFormat()
  override def knnVectorsFormat(): KnnVectorsFormat = luceneCodec.knnVectorsFormat()
}
