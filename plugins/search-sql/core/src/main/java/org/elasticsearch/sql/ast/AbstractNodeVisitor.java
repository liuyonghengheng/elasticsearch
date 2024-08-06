

package org.elasticsearch.sql.ast;

import org.elasticsearch.sql.ast.expression.AggregateFunction;
import org.elasticsearch.sql.ast.expression.Alias;
import org.elasticsearch.sql.ast.expression.AllFields;
import org.elasticsearch.sql.ast.expression.And;
import org.elasticsearch.sql.ast.expression.Argument;
import org.elasticsearch.sql.ast.expression.AttributeList;
import org.elasticsearch.sql.ast.expression.Case;
import org.elasticsearch.sql.ast.expression.Cast;
import org.elasticsearch.sql.ast.expression.Compare;
import org.elasticsearch.sql.ast.expression.EqualTo;
import org.elasticsearch.sql.ast.expression.Field;
import org.elasticsearch.sql.ast.expression.Function;
import org.elasticsearch.sql.ast.expression.In;
import org.elasticsearch.sql.ast.expression.Interval;
import org.elasticsearch.sql.ast.expression.Let;
import org.elasticsearch.sql.ast.expression.Literal;
import org.elasticsearch.sql.ast.expression.Map;
import org.elasticsearch.sql.ast.expression.Not;
import org.elasticsearch.sql.ast.expression.Or;
import org.elasticsearch.sql.ast.expression.QualifiedName;
import org.elasticsearch.sql.ast.expression.Span;
import org.elasticsearch.sql.ast.expression.UnresolvedArgument;
import org.elasticsearch.sql.ast.expression.UnresolvedAttribute;
import org.elasticsearch.sql.ast.expression.When;
import org.elasticsearch.sql.ast.expression.WindowFunction;
import org.elasticsearch.sql.ast.expression.Xor;
import org.elasticsearch.sql.ast.tree.AD;
import org.elasticsearch.sql.ast.tree.Aggregation;
import org.elasticsearch.sql.ast.tree.Dedupe;
import org.elasticsearch.sql.ast.tree.Eval;
import org.elasticsearch.sql.ast.tree.Filter;
import org.elasticsearch.sql.ast.tree.Head;
import org.elasticsearch.sql.ast.tree.Kmeans;
import org.elasticsearch.sql.ast.tree.Limit;
import org.elasticsearch.sql.ast.tree.Parse;
import org.elasticsearch.sql.ast.tree.Project;
import org.elasticsearch.sql.ast.tree.RareTopN;
import org.elasticsearch.sql.ast.tree.Relation;
import org.elasticsearch.sql.ast.tree.RelationSubquery;
import org.elasticsearch.sql.ast.tree.Rename;
import org.elasticsearch.sql.ast.tree.Sort;
import org.elasticsearch.sql.ast.tree.Values;

/**
 * AST nodes visitor Defines the traverse path.
 */
public abstract class AbstractNodeVisitor<T, C> {

  public T visit(Node node, C context) {
    return null;
  }

  /**
   * Visit child node.
   * @param node {@link Node}
   * @param context Context
   * @return Return Type.
   */
  public T visitChildren(Node node, C context) {
    T result = defaultResult();

    for (Node child : node.getChild()) {
      T childResult = child.accept(this, context);
      result = aggregateResult(result, childResult);
    }
    return result;
  }

  private T defaultResult() {
    return null;
  }

  private T aggregateResult(T aggregate, T nextResult) {
    return nextResult;
  }

  public T visitRelation(Relation node, C context) {
    return visitChildren(node, context);
  }

  public T visitRelationSubquery(RelationSubquery node, C context) {
    return visitChildren(node, context);
  }

  public T visitFilter(Filter node, C context) {
    return visitChildren(node, context);
  }

  public T visitProject(Project node, C context) {
    return visitChildren(node, context);
  }

  public T visitAggregation(Aggregation node, C context) {
    return visitChildren(node, context);
  }

  public T visitEqualTo(EqualTo node, C context) {
    return visitChildren(node, context);
  }

  public T visitLiteral(Literal node, C context) {
    return visitChildren(node, context);
  }

  public T visitUnresolvedAttribute(UnresolvedAttribute node, C context) {
    return visitChildren(node, context);
  }

  public T visitAttributeList(AttributeList node, C context) {
    return visitChildren(node, context);
  }

  public T visitMap(Map node, C context) {
    return visitChildren(node, context);
  }

  public T visitNot(Not node, C context) {
    return visitChildren(node, context);
  }

  public T visitOr(Or node, C context) {
    return visitChildren(node, context);
  }

  public T visitAnd(And node, C context) {
    return visitChildren(node, context);
  }

  public T visitXor(Xor node, C context) {
    return visitChildren(node, context);
  }

  public T visitAggregateFunction(AggregateFunction node, C context) {
    return visitChildren(node, context);
  }

  public T visitFunction(Function node, C context) {
    return visitChildren(node, context);
  }

  public T visitWindowFunction(WindowFunction node, C context) {
    return visitChildren(node, context);
  }

  public T visitIn(In node, C context) {
    return visitChildren(node, context);
  }

  public T visitCompare(Compare node, C context) {
    return visitChildren(node, context);
  }

  public T visitArgument(Argument node, C context) {
    return visitChildren(node, context);
  }

  public T visitField(Field node, C context) {
    return visitChildren(node, context);
  }

  public T visitQualifiedName(QualifiedName node, C context) {
    return visitChildren(node, context);
  }

  public T visitRename(Rename node, C context) {
    return visitChildren(node, context);
  }

  public T visitEval(Eval node, C context) {
    return visitChildren(node, context);
  }

  public T visitParse(Parse node, C context) {
    return visitChildren(node, context);
  }

  public T visitLet(Let node, C context) {
    return visitChildren(node, context);
  }

  public T visitSort(Sort node, C context) {
    return visitChildren(node, context);
  }

  public T visitDedupe(Dedupe node, C context) {
    return visitChildren(node, context);
  }

  public T visitHead(Head node, C context) {
    return visitChildren(node, context);
  }

  public T visitRareTopN(RareTopN node, C context) {
    return visitChildren(node, context);
  }

  public T visitValues(Values node, C context) {
    return visitChildren(node, context);
  }

  public T visitAlias(Alias node, C context) {
    return visitChildren(node, context);
  }

  public T visitAllFields(AllFields node, C context) {
    return visitChildren(node, context);
  }

  public T visitInterval(Interval node, C context) {
    return visitChildren(node, context);
  }

  public T visitCase(Case node, C context) {
    return visitChildren(node, context);
  }

  public T visitWhen(When node, C context) {
    return visitChildren(node, context);
  }

  public T visitCast(Cast node, C context) {
    return visitChildren(node, context);
  }

  public T visitUnresolvedArgument(UnresolvedArgument node, C context) {
    return visitChildren(node, context);
  }

  public T visitLimit(Limit node, C context) {
    return visitChildren(node, context);
  }

  public T visitSpan(Span node, C context) {
    return visitChildren(node, context);
  }

  public T visitKmeans(Kmeans node, C context) {
    return visitChildren(node, context);
  }

  public T visitAD(AD node, C context) {
    return visitChildren(node, context);
  }
}
