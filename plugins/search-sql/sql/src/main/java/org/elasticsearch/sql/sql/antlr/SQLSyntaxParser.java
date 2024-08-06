


package org.elasticsearch.sql.sql.antlr;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.elasticsearch.sql.common.antlr.CaseInsensitiveCharStream;
import org.elasticsearch.sql.common.antlr.SyntaxAnalysisErrorListener;
import org.elasticsearch.sql.sql.antlr.parser.ElasticsearchSQLLexer;
import org.elasticsearch.sql.sql.antlr.parser.ElasticsearchSQLParser;

/**
 * SQL syntax parser which encapsulates an ANTLR parser.
 */
public class SQLSyntaxParser {

  /**
   * Parse a SQL query by ANTLR parser.
   * @param query   a SQL query
   * @return        parse tree root
   */
  public ParseTree parse(String query) {
    ElasticsearchSQLLexer lexer = new ElasticsearchSQLLexer(new CaseInsensitiveCharStream(query));
    ElasticsearchSQLParser parser = new ElasticsearchSQLParser(new CommonTokenStream(lexer));
    parser.addErrorListener(new SyntaxAnalysisErrorListener());
    return parser.root();
  }

}
