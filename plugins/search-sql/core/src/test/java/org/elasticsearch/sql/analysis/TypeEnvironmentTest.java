


package org.elasticsearch.sql.analysis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.elasticsearch.sql.data.type.ExprCoreType.INTEGER;
import static org.elasticsearch.sql.data.type.ExprCoreType.STRING;
import static org.elasticsearch.sql.data.type.ExprCoreType.STRUCT;

import org.junit.jupiter.api.Test;
import org.elasticsearch.sql.analysis.symbol.Namespace;
import org.elasticsearch.sql.analysis.symbol.Symbol;
import org.elasticsearch.sql.exception.SemanticCheckException;
import org.elasticsearch.sql.expression.DSL;
import org.elasticsearch.sql.expression.ReferenceExpression;

public class TypeEnvironmentTest {

  /**
   * Use context class for push/pop.
   */
  private AnalysisContext context = new AnalysisContext();

  @Test
  public void defineFieldSymbolInDifferentEnvironmentsShouldBeAbleToResolve() {
    // Root environment
    ReferenceExpression age = DSL.ref("s.age", INTEGER);
    environment().define(age);
    assertEquals(INTEGER, environment().resolve(toSymbol(age)));

    // New environment 1
    context.push();
    ReferenceExpression city = DSL.ref("s.city", STRING);
    environment().define(city);
    assertEquals(INTEGER, environment().resolve(toSymbol(age)));
    assertEquals(STRING, environment().resolve(toSymbol(city)));

    // New environment 2
    context.push();
    ReferenceExpression manager = DSL.ref("s.manager", STRUCT);
    environment().define(manager);
    assertEquals(INTEGER, environment().resolve(toSymbol(age)));
    assertEquals(STRING, environment().resolve(toSymbol(city)));
    assertEquals(STRUCT, environment().resolve(toSymbol(manager)));
  }

  @Test
  public void defineFieldSymbolInDifferentEnvironmentsShouldNotAbleToResolveOncePopped() {
    // Root environment
    ReferenceExpression age = DSL.ref("s.age", INTEGER);
    environment().define(age);

    // New environment
    context.push();
    ReferenceExpression city = DSL.ref("s.city", STRING);
    environment().define(city);
    ReferenceExpression manager = DSL.ref("s.manager", STRUCT);
    environment().define(manager);
    assertEquals(INTEGER, environment().resolve(toSymbol(age)));
    assertEquals(STRING, environment().resolve(toSymbol(city)));
    assertEquals(STRUCT, environment().resolve(toSymbol(manager)));

    context.pop();
    assertEquals(INTEGER, environment().resolve(toSymbol(age)));
    SemanticCheckException exception =
        assertThrows(SemanticCheckException.class, () -> environment().resolve(toSymbol(city)));
    assertEquals("can't resolve Symbol(namespace=FIELD_NAME, name=s.city) in type env",
        exception.getMessage());
    exception = assertThrows(SemanticCheckException.class,
        () -> environment().resolve(toSymbol(manager)));
    assertEquals("can't resolve Symbol(namespace=FIELD_NAME, name=s.manager) in type env",
        exception.getMessage());
  }

  @Test
  public void resolveLiteralInEnvFailed() {
    SemanticCheckException exception = assertThrows(SemanticCheckException.class,
        () -> environment().resolve(new Symbol(Namespace.FIELD_NAME, "1")));
    assertEquals("can't resolve Symbol(namespace=FIELD_NAME, name=1) in type env",
        exception.getMessage());
  }

  private TypeEnvironment environment() {
    return context.peek();
  }

  private Symbol toSymbol(ReferenceExpression ref) {
    return new Symbol(Namespace.FIELD_NAME, ref.getAttr());
  }

}
