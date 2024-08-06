


package org.elasticsearch.sql.legacy.query.planner.physical.node.project;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.sql.legacy.expression.domain.BindingTuple;
import org.elasticsearch.sql.legacy.query.planner.core.ColumnNode;
import org.elasticsearch.sql.legacy.query.planner.core.PlanNode;
import org.elasticsearch.sql.legacy.query.planner.physical.PhysicalOperator;
import org.elasticsearch.sql.legacy.query.planner.physical.Row;
import org.elasticsearch.sql.legacy.query.planner.physical.estimation.Cost;
import org.elasticsearch.sql.legacy.query.planner.physical.node.scroll.BindingTupleRow;

/**
 * The definition of Project Operator.
 */
@RequiredArgsConstructor
public class PhysicalProject implements PhysicalOperator<BindingTuple> {
    private final PhysicalOperator<BindingTuple> next;
    private final List<ColumnNode> fields;

    @Override
    public Cost estimate() {
        return null;
    }

    @Override
    public PlanNode[] children() {
        return new PlanNode[]{next};
    }

    @Override
    public boolean hasNext() {
        return next.hasNext();
    }

    @Override
    public Row<BindingTuple> next() {
        BindingTuple input = next.next().data();
        BindingTuple.BindingTupleBuilder outputBindingTupleBuilder = BindingTuple.builder();
        fields.forEach(field -> outputBindingTupleBuilder.binding(field.getName(), field.getExpr().valueOf(input)));
        return new BindingTupleRow(outputBindingTupleBuilder.build());
    }
}
