


package org.elasticsearch.jdbc.protocol;

public interface ColumnDescriptor {
    /**
     * Column name
     * @return
     */
    String getName();

    /**
     * Label
     */
     String getLabel();

    /**
     * Column data type
     * @return
     */
    String getType();
}
