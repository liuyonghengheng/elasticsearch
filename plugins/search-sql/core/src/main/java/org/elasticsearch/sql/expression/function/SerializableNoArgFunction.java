

package org.elasticsearch.sql.expression.function;

import java.io.Serializable;
import java.util.function.Supplier;

/**
 * Serializable no argument function.
 */
public interface SerializableNoArgFunction<T> extends Supplier<T>, Serializable {
}
