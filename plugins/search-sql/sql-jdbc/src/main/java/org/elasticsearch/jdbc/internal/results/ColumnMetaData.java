


package org.elasticsearch.jdbc.internal.results;

import org.elasticsearch.jdbc.types.ElasticsearchType;
import org.elasticsearch.jdbc.protocol.ColumnDescriptor;

public class ColumnMetaData {
    private String name;
    private String label;
    private String tableSchemaName;
    private int precision = -1;
    private int scale = -1;
    private String tableName;
    private String catalogName;
    private String openSearchTypeName;
    private ElasticsearchType openSearchType;

    public ColumnMetaData(ColumnDescriptor descriptor) {
        this.name = descriptor.getName();

        // if a label isn't specified, the name is the label
        this.label = descriptor.getLabel() == null ? this.name : descriptor.getLabel();

        this.openSearchTypeName = descriptor.getType();
        this.openSearchType = ElasticsearchType.fromTypeName(openSearchTypeName);

        // use canned values until server can return this
        this.precision = this.openSearchType.getPrecision();
        this.scale = 0;

        // JDBC has these, but our protocol does not yet convey these
        this.tableName = "";
        this.catalogName = "";
        this.tableSchemaName = "";
    }

    public String getName() {
        return name;
    }

    public String getLabel() {
        return label;
    }

    public String getTableSchemaName() {
        return tableSchemaName;
    }

    public int getPrecision() {
       return  precision;
    }

    public int getScale() {
        return scale;
    }

    public String getTableName() {
        return tableName;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public ElasticsearchType getElasticsearchType() {
        return openSearchType;
    }

    public String getElasticsearchTypeName() {
        return openSearchTypeName;
    }
}
