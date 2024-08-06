

package org.elasticsearch.sql.expression.function;

import java.io.Serializable;
import java.util.function.BiFunction;

/**
 * Serializable BiFunction.
 */
public interface SerializableBiFunction<T, U, R> extends BiFunction<T, U, R>, Serializable {
}
