


package org.elasticsearch.sql.search.storage.script;

import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.script.FilterScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.sql.expression.DSL;
import org.elasticsearch.sql.expression.Expression;
import org.elasticsearch.sql.search.storage.script.filter.ExpressionFilterScriptFactory;
import org.elasticsearch.sql.search.storage.serialization.ExpressionSerializer;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
class ExpressionScriptEngineTest {

  @Mock
  private ExpressionSerializer serializer;

  private ScriptEngine scriptEngine;

  private final Expression expression = DSL.literal(true);

  @BeforeEach
  void set_up() {
    scriptEngine = new ExpressionScriptEngine(serializer);
  }

  @Test
  void should_return_custom_script_language_name() {
    assertEquals(ExpressionScriptEngine.EXPRESSION_LANG_NAME, scriptEngine.getType());
  }

  @Test
  void can_initialize_filter_script_factory_by_compiled_script() {
    when(serializer.deserialize("test code")).thenReturn(expression);

    assertThat(scriptEngine.getSupportedContexts(),
        contains(FilterScript.CONTEXT, AggregationScript.CONTEXT));

    Object actualFactory = scriptEngine.compile(
        "test", "test code", FilterScript.CONTEXT, emptyMap());
    assertEquals(new ExpressionFilterScriptFactory(expression), actualFactory);
  }

  @Test
  void should_throw_exception_for_unsupported_script_context() {
    ScriptContext<?> unknownCtx = mock(ScriptContext.class);
    assertThrows(IllegalStateException.class, () ->
        scriptEngine.compile("test", "test code", unknownCtx, emptyMap()));
  }

}
