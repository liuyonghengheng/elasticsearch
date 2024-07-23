/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.knn.codec

import org.apache.lucene.codecs.Codec
import org.elasticsearch.index.codec.CodecService

class ElasticknnCodecService extends CodecService(null, null) {

  override def codec(name: String): Codec =
    Codec.forName(ElasticknnCodecService.Elasticknn_99)

}

object ElasticknnCodecService {
  val Elasticknn_84 = "Elasticknn84Codec"
  val Elasticknn_86 = "Elasticknn86Codec"
  val Elasticknn_87 = "Elasticknn87Codec"
  val Elasticknn_99 = "Elasticknn99Codec"
}
