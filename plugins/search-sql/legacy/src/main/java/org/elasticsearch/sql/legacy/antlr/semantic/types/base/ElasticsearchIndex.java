


package org.elasticsearch.sql.legacy.antlr.semantic.types.base;

import java.util.Objects;
import org.elasticsearch.sql.legacy.antlr.semantic.types.Type;

/**
 * Index type is not Enum because essentially each index is a brand new type.
 */
public class ElasticsearchIndex implements BaseType {

    public enum IndexType {
        INDEX, NESTED_FIELD, INDEX_PATTERN
    }

    private final String indexName;
    private final IndexType indexType;

    public ElasticsearchIndex(String indexName, IndexType indexType) {
        this.indexName = indexName;
        this.indexType = indexType;
    }

    public IndexType type() {
        return indexType;
    }

    @Override
    public String getName() {
        return indexName;
    }

    @Override
    public boolean isCompatible(Type other) {
        return equals(other);
    }

    @Override
    public String usage() {
        return indexType.name();
    }

    @Override
    public String toString() {
        return indexType + " [" + indexName + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ElasticsearchIndex index = (ElasticsearchIndex) o;
        return Objects.equals(indexName, index.indexName)
            && indexType == index.indexType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexName, indexType);
    }
}
