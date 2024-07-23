package org.elasticsearch.knn

object ElasticknnException {

  class ElasticknnRuntimeException(msg: String, cause: Throwable = None.orNull) extends RuntimeException(msg, cause)

  class ElasticknnUnsupportedOperationException(msg: String, cause: Throwable = None.orNull)
    extends UnsupportedOperationException(msg, cause)

  class ElasticknnIllegalArgumentException(msg: String, cause: Throwable = None.orNull) extends IllegalArgumentException(msg, cause)

  def vectorDimensions(actual: Int, expected: Int): ElasticknnIllegalArgumentException =
    new ElasticknnIllegalArgumentException(s"Expected dimension $expected but got $actual")
}
