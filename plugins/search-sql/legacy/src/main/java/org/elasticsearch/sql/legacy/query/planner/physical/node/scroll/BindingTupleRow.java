


package org.elasticsearch.sql.legacy.query.planner.physical.node.scroll;

import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.sql.legacy.expression.domain.BindingTuple;
import org.elasticsearch.sql.legacy.query.planner.physical.Row;

@RequiredArgsConstructor
public class BindingTupleRow implements Row<BindingTuple> {
    private final BindingTuple bindingTuple;

    @Override
    public RowKey key(String[] colNames) {
        return null;
    }

    @Override
    public Row<BindingTuple> combine(Row<BindingTuple> otherRow) {
        throw new RuntimeException("unsupported operation");
    }

    @Override
    public void retain(Map<String, String> colNameAlias) {
        // do nothing
    }

    @Override
    public BindingTuple data() {
        return bindingTuple;
    }
}
