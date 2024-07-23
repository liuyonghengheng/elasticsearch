package org.elasticsearch.knn.models

import org.elasticsearch.knn.api.{Mapping, Vec}
import org.elasticsearch.knn.storage.StoredVec


trait HashingFunction[M <: Mapping, V <: Vec, S <: StoredVec] extends (V => Array[HashAndFreq]) {
  val mapping: M
}
