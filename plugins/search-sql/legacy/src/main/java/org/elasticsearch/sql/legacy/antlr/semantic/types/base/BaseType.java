


package org.elasticsearch.sql.legacy.antlr.semantic.types.base;

import java.util.List;
import org.elasticsearch.sql.legacy.antlr.semantic.types.Type;

/**
 * Base type interface
 */
public interface BaseType extends Type {

    @Override
    default Type construct(List<Type> others) {
        return this;
    }

    @Override
    default String usage() {
        return getName();
    }
}
