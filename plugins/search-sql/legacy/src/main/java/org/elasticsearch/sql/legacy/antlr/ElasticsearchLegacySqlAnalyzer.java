


package org.elasticsearch.sql.legacy.antlr;

import java.util.Optional;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.sql.legacy.antlr.parser.ElasticsearchLegacySqlLexer;
import org.elasticsearch.sql.legacy.antlr.parser.ElasticsearchLegacySqlParser;
import org.elasticsearch.sql.legacy.antlr.semantic.scope.SemanticContext;
import org.elasticsearch.sql.legacy.antlr.semantic.types.Type;
import org.elasticsearch.sql.legacy.antlr.semantic.visitor.ElasticsearchMappingLoader;
import org.elasticsearch.sql.legacy.antlr.semantic.visitor.SemanticAnalyzer;
import org.elasticsearch.sql.legacy.antlr.semantic.visitor.TypeChecker;
import org.elasticsearch.sql.legacy.antlr.syntax.CaseInsensitiveCharStream;
import org.elasticsearch.sql.legacy.antlr.syntax.SyntaxAnalysisErrorListener;
import org.elasticsearch.sql.legacy.antlr.visitor.AntlrSqlParseTreeVisitor;
import org.elasticsearch.sql.legacy.antlr.visitor.EarlyExitAnalysisException;
import org.elasticsearch.sql.legacy.esdomain.LocalClusterState;

/**
 * Entry point for ANTLR generated parser to perform strict syntax and semantic analysis.
 */
public class ElasticsearchLegacySqlAnalyzer {

    private static final Logger LOG = LogManager.getLogger();

    /** Original sql query */
    private final SqlAnalysisConfig config;

    public ElasticsearchLegacySqlAnalyzer(SqlAnalysisConfig config) {
        this.config = config;
    }

    public Optional<Type> analyze(String sql, LocalClusterState clusterState) {
        // Perform analysis for SELECT only for now because of extra code changes required for SHOW/DESCRIBE.
        if (!isSelectStatement(sql) || !config.isAnalyzerEnabled()) {
            return Optional.empty();
        }

        try {
            return Optional.of(analyzeSemantic(
                    analyzeSyntax(sql),
                    clusterState
            ));
        } catch (EarlyExitAnalysisException e) {
            // Expected if configured so log on debug level to avoid always logging stack trace
            LOG.debug("Analysis exits early and will skip remaining process", e);
            return Optional.empty();
        }
    }

    /**
     * Build lexer and parser to perform syntax analysis only.
     * Runtime exception with clear message is thrown for any verification error.
     *
     * @return      parse tree
     */
    public ParseTree analyzeSyntax(String sql) {
        ElasticsearchLegacySqlParser parser = createParser(createLexer(sql));
        parser.addErrorListener(new SyntaxAnalysisErrorListener());
        return parser.root();
    }

    /**
     * Perform semantic analysis based on syntax analysis output - parse tree.
     *
     * @param tree          parse tree
     * @param clusterState  cluster state required for index mapping query
     */
    public Type analyzeSemantic(ParseTree tree, LocalClusterState clusterState) {
        return tree.accept(new AntlrSqlParseTreeVisitor<>(createAnalyzer(clusterState)));
    }

    /** Factory method for semantic analyzer to help assemble all required components together */
    private SemanticAnalyzer createAnalyzer(LocalClusterState clusterState) {
        SemanticContext context = new SemanticContext();
        ElasticsearchMappingLoader
            mappingLoader = new ElasticsearchMappingLoader(context, clusterState, config.getAnalysisThreshold());
        TypeChecker typeChecker = new TypeChecker(context, config.isFieldSuggestionEnabled());
        return new SemanticAnalyzer(mappingLoader, typeChecker);
    }

    private ElasticsearchLegacySqlParser createParser(Lexer lexer) {
        return new ElasticsearchLegacySqlParser(
                   new CommonTokenStream(lexer));
    }

    private ElasticsearchLegacySqlLexer createLexer(String sql) {
         return new ElasticsearchLegacySqlLexer(
                    new CaseInsensitiveCharStream(sql));
    }

    private boolean isSelectStatement(String sql) {
        sql = sql.replaceAll("\\R", " ").trim();
        int endOfFirstWord = sql.indexOf(' ');
        String firstWord = sql.substring(0, endOfFirstWord > 0 ? endOfFirstWord : sql.length());
        return "SELECT".equalsIgnoreCase(firstWord);
    }

}
