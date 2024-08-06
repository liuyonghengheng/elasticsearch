

package org.elasticsearch.sql.search.planner.physical;

import static org.elasticsearch.sql.utils.MLCommonsConstants.SHINGLE_SIZE;
import static org.elasticsearch.sql.utils.MLCommonsConstants.TIME_DECAY;
import static org.elasticsearch.sql.utils.MLCommonsConstants.TIME_FIELD;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.sql.ast.expression.Literal;
import org.elasticsearch.sql.data.model.ExprValue;
import org.elasticsearch.sql.planner.physical.PhysicalPlan;
import org.elasticsearch.sql.planner.physical.PhysicalPlanNodeVisitor;

/**
 * AD Physical operator to call AD interface to get results for
 * algorithm execution.
 */
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class ADOperator extends MLCommonsOperatorActions {

  @Getter
  private final PhysicalPlan input;

  @Getter
  private final Map<String, Literal> arguments;

  @Getter
  private final NodeClient nodeClient;

  @EqualsAndHashCode.Exclude
  private Iterator<ExprValue> iterator;

  //private FunctionName rcfType;

  @Override
  public void open() {
    throw new UnsupportedOperationException("Not support AD");
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitAD(this, context);
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public ExprValue next() {
    return iterator.next();
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return Collections.singletonList(input);
  }

//  protected MLAlgoParams convertArgumentToMLParameter(Map<String, Literal> arguments) {
//    if (arguments.get(TIME_FIELD).getValue() == null) {
//      rcfType = FunctionName.BATCH_RCF;
//      return BatchRCFParams.builder()
//              .shingleSize((Integer) arguments.get(SHINGLE_SIZE).getValue())
//              .build();
//    }
//    rcfType = FunctionName.FIT_RCF;
//    return FitRCFParams.builder()
//            .shingleSize((Integer) arguments.get(SHINGLE_SIZE).getValue())
//            .timeDecay((Double) arguments.get(TIME_DECAY).getValue())
//            .timeField((String) arguments.get(TIME_FIELD).getValue())
//            .dateFormat("yyyy-MM-dd HH:mm:ss")
//            .build();
//  }

}
