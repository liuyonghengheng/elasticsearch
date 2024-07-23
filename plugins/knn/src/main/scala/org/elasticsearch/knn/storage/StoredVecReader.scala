package org.elasticsearch.knn.storage


import org.elasticsearch.knn.storage.StoredVec.Decoder
import org.apache.lucene.index.LeafReaderContext
import org.elasticsearch.knn.ElasticknnException.ElasticknnRuntimeException

final class StoredVecReader[S <: StoredVec: Decoder](lrc: LeafReaderContext, field: String) {
  // Important for performance that each of these is instantiated here and not in the apply method.
  private val vecDocVals = lrc.reader.getBinaryDocValues(field)
  private val decoder = implicitly[StoredVec.Decoder[S]]

  def apply(docID: Int): S = {
    val prevDocID = vecDocVals.docID()
    if (prevDocID == docID || vecDocVals.advanceExact(docID)) {
      val bytesRef = vecDocVals.binaryValue()
      decoder(bytesRef.bytes, bytesRef.offset, bytesRef.length)
    } else {
      throw new ElasticknnRuntimeException(
        Seq(
          s"Could not advance binary doc values reader from doc ID [$prevDocID] to doc ID [$docID].",
          s"It is possible that the document [$docID] does not have a vector.",
          s"""Consider trying with an `exists` query: "exists": { "field": "${field}" }"""
        ).mkString(" "))
    }
  }

}
