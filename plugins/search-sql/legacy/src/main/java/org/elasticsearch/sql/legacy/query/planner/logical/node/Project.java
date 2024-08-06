


package org.elasticsearch.sql.legacy.query.planner.logical.node;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.sql.legacy.domain.Field;
import org.elasticsearch.sql.legacy.query.planner.core.PlanNode;
import org.elasticsearch.sql.legacy.query.planner.logical.LogicalOperator;
import org.elasticsearch.sql.legacy.query.planner.physical.PhysicalOperator;
import org.elasticsearch.sql.legacy.query.planner.physical.Row;
import org.elasticsearch.sql.legacy.query.planner.physical.estimation.Cost;

/**
 * Projection expression
 */
public class Project<T> implements LogicalOperator, PhysicalOperator<T> {

    private static final Logger LOG = LogManager.getLogger();

    private final PlanNode next;

    /**
     * All columns being projected in SELECT in each table
     */
    private final Multimap<String, Field> tableAliasColumns;

    /**
     * All columns full name (tableAlias.colName) to alias mapping
     */
    private final Map<String, String> fullNameAlias;


    @SuppressWarnings("unchecked")
    public Project(PlanNode next) {
        this(next, HashMultimap.create());
    }

    @SuppressWarnings("unchecked")
    public Project(PlanNode next, Multimap<String, Field> tableAliasToColumns) {
        this.next = next;
        this.tableAliasColumns = tableAliasToColumns;
        this.fullNameAlias = fullNameAndAlias();
    }

    @Override
    public boolean isNoOp() {
        return tableAliasColumns.isEmpty();
    }

    @Override
    public PlanNode[] children() {
        return new PlanNode[]{next};
    }

    @Override
    public <U> PhysicalOperator[] toPhysical(Map<LogicalOperator, PhysicalOperator<U>> optimalOps) {
        if (!(next instanceof LogicalOperator)) {
            throw new IllegalStateException("Only logical operator can perform this toPhysical() operation");
        }
        return new PhysicalOperator[]{
                new Project<U>(optimalOps.get(next), tableAliasColumns) // Create physical Project instance
        };
    }

    @Override
    public Cost estimate() {
        return new Cost();
    }

    @Override
    public boolean hasNext() {
        return ((PhysicalOperator) next).hasNext();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Row<T> next() {
        Row<T> row = ((PhysicalOperator<T>) this.next).next();

        /*
         * Empty means SELECT * which means retain all fields from both tables
         * Because push down is always applied, only limited support for this.
         */
        if (!fullNameAlias.isEmpty()) {
            row.retain(fullNameAlias);
        }

        LOG.trace("Projected row by fields {}: {}", tableAliasColumns, row);
        return row;
    }

    public void project(String tableAlias, Collection<Field> columns) {
        tableAliasColumns.putAll(tableAlias, columns);
    }

    public void projectAll(String tableAlias) {
        tableAliasColumns.put(tableAlias, new Field("*", ""));
    }

    public void forEach(BiConsumer<String, Collection<Field>> action) {
        tableAliasColumns.asMap().forEach(action);
    }

    public void pushDown(String tableAlias, Project<?> pushedDownProj) {
        Collection<Field> columns = pushedDownProj.tableAliasColumns.get(tableAlias);
        if (columns != null) {
            tableAliasColumns.putAll(tableAlias, columns);
        }
    }

    /**
     * Return mapping from column full name ("e.age") and alias ("a" in "SELECT e.age AS a")
     */
    private Map<String, String> fullNameAndAlias() {
        Map<String, String> fullNamesAlias = new HashMap<>();
        forEach(
                (tableAlias, fields) -> {
                    for (Field field : fields) {
                        fullNamesAlias.put(tableAlias + "." + field.getName(), field.getAlias());
                    }
                }
        );
        return fullNamesAlias;
    }

    @Override
    public String toString() {
        List<String> colStrs = new ArrayList<>();
        for (Map.Entry<String, Field> entry : tableAliasColumns.entries()) {
            colStrs.add(entry.getKey() + "." + entry.getValue().getName());
        }
        return "Project [ columns=[" + String.join(", ", colStrs) + "] ]";
    }

}
