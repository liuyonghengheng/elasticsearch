


package org.elasticsearch.sql.legacy.antlr.semantic.types.operator;

import static org.elasticsearch.sql.legacy.antlr.semantic.types.base.ElasticsearchDataType.TYPE_ERROR;

import java.util.List;
import java.util.Optional;
import org.elasticsearch.sql.legacy.antlr.semantic.types.Type;
import org.elasticsearch.sql.legacy.antlr.semantic.types.base.ElasticsearchIndex;

/**
 * Join operator
 */
public enum JoinOperator implements Type {
    JOIN;

    @Override
    public String getName() {
        return name();
    }

    @Override
    public Type construct(List<Type> others) {
        Optional<Type> isAnyNonIndexType = others.stream().
                                                  filter(type -> !(type instanceof ElasticsearchIndex)).
                                                  findAny();
        if (isAnyNonIndexType.isPresent()) {
            return TYPE_ERROR;
        }
        return others.get(0);
    }

    @Override
    public String usage() {
        return "Please join index with other index or its nested field.";
    }

    @Override
    public String toString() {
        return "Operator [" + getName() + "]";
    }
}
