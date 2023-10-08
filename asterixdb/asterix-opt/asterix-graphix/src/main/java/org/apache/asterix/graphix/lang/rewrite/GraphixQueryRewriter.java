/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.graphix.lang.rewrite;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.asterix.common.config.CompilerProperties;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.parser.GraphixParserFactory;
import org.apache.asterix.graphix.lang.rewrite.print.GraphixASTPrintVisitor;
import org.apache.asterix.graphix.lang.rewrite.resolve.ExhaustiveSearchResolver;
import org.apache.asterix.graphix.lang.rewrite.resolve.pattern.SuperPattern;
import org.apache.asterix.graphix.lang.rewrite.resolve.schema.SchemaTable;
import org.apache.asterix.graphix.lang.rewrite.visitor.CorrelatedVertexJoinVisitor;
import org.apache.asterix.graphix.lang.rewrite.visitor.DeclarationNormalizeVisitor;
import org.apache.asterix.graphix.lang.rewrite.visitor.ElementDeclarationVisitor;
import org.apache.asterix.graphix.lang.rewrite.visitor.ExpressionLoweringVisitor;
import org.apache.asterix.graphix.lang.rewrite.visitor.ExpressionScopingVisitor;
import org.apache.asterix.graphix.lang.rewrite.visitor.FillGroupByUnknownsVisitor;
import org.apache.asterix.graphix.lang.rewrite.visitor.FillStartingUnknownsVisitor;
import org.apache.asterix.graphix.lang.rewrite.visitor.FunctionResolutionVisitor;
import org.apache.asterix.graphix.lang.rewrite.visitor.LiveVertexMarkingVisitor;
import org.apache.asterix.graphix.lang.rewrite.visitor.PlaceEvaluationHintsVisitor;
import org.apache.asterix.graphix.lang.rewrite.visitor.PostRewriteCheckVisitor;
import org.apache.asterix.graphix.lang.rewrite.visitor.PreRewriteCheckVisitor;
import org.apache.asterix.graphix.lang.rewrite.visitor.SchemaTableCreationVisitor;
import org.apache.asterix.graphix.lang.statement.GraphElementDeclaration;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.IReturningStatement;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.util.ExpressionUtils;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.SqlppFunctionBodyRewriter;
import org.apache.asterix.lang.sqlpp.rewrites.SqlppQueryRewriter;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SubstituteGroupbyExpressionWithVariableVisitor;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.util.LogRedactionUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Rewriter for Graphix queries, which will lower all graph AST nodes into SQL++ AST nodes. We perform the following:
 * <ol>
 *  <li>Perform an error-checking on the fresh AST (immediately after parsing).</li>
 *  <li>Populate the unknowns in our AST (e.g. vertex / edge variables, projections).</li>
 *  <li>Resolve all of our function calls (Graphix, SQL++, and user-defined).</li>
 *  <li>For all Graphix subqueries whose vertices are correlated, rewrite this correlation to be explicit.</li>
 *  <li>Annotate JOINs with the INDEX-NL hint if specified in the configuration.</li>
 *  <li>Perform resolution of unlabeled vertices / edges, as well as edge directions.</li>
 *  <li>Using the labels of the vertices / edges in our AST, fetch the relevant graph elements from our metadata.</li>
 *  <li>Populate the GROUP-BY unknowns (i.e. GROUP-BY fields) in our AST.</li>
 *  <li>Perform a "dead vertex" identifying pass, to mark which vertices do not need their full vertex body.</li>
 *  <li>Perform a variable-scoping pass to identify illegal variables (either duplicate or out-of-scope).</li>
 *  <li>Perform a lowering pass to transform Graphix AST nodes to SQL++ AST nodes.</li>
 *  <li>Hand our AST downstream and perform all SQL++ rewrites on our newly lowered AST.</li>
 * </ol>
 */
public class GraphixQueryRewriter extends SqlppQueryRewriter {
    private static final Logger LOGGER = LogManager.getLogger();

    private final GraphixParserFactory parserFactory;
    private final SqlppQueryRewriter bodyRewriter;

    public GraphixQueryRewriter(IParserFactory parserFactory) {
        super(parserFactory);
        this.parserFactory = (GraphixParserFactory) parserFactory;
        this.bodyRewriter = getFunctionAndViewBodyRewriter();
    }

    @Override
    public void rewrite(LangRewritingContext langRewritingContext, IReturningStatement topStatement,
            boolean allowNonStoredUDFCalls, boolean inlineUdfsAndViews, Collection<VarIdentifier> externalVars)
            throws CompilationException {
        LOGGER.debug("Starting Graphix AST rewrites.");

        // Perform an initial error-checking pass to validate our user query.
        LOGGER.trace("Performing pre-Graphix-rewrite check (user query validation).");
        GraphixRewritingContext graphixRewritingContext = (GraphixRewritingContext) langRewritingContext;
        topStatement.accept(new PreRewriteCheckVisitor(graphixRewritingContext), null);

        // Perform the Graphix rewrites.
        if (LOGGER.isTraceEnabled()) {
            StringWriter stringWriter = new StringWriter();
            PrintWriter printWriter = new PrintWriter(stringWriter);
            topStatement.accept(new GraphixASTPrintVisitor(printWriter), 0);
            LOGGER.trace("Initial AST: {}\n", LogRedactionUtil.userData(stringWriter.toString()));
        }
        rewriteGraphixASTNodes(graphixRewritingContext, topStatement, allowNonStoredUDFCalls);

        // Sanity check: ensure that no graph AST nodes exist after this point.
        Map<String, Object> queryConfig = graphixRewritingContext.getMetadataProvider().getConfig();
        if (queryConfig.containsKey(CompilerProperties.COMPILER_INTERNAL_SANITYCHECK_KEY)) {
            String configValue = (String) queryConfig.get(CompilerProperties.COMPILER_INTERNAL_SANITYCHECK_KEY);
            if (!configValue.equalsIgnoreCase("false")) {
                LOGGER.trace("Performing post-Graphix-rewrite check (making sure no graph AST nodes exist).");
                topStatement.accept(new PostRewriteCheckVisitor(), null);
            }
        }

        // Perform the remainder of the SQL++ rewrites.
        LOGGER.debug("Ending Graphix AST rewrites. Now starting SQL++ AST rewrites.");
        rewriteSQLPPASTNodes(langRewritingContext, topStatement, allowNonStoredUDFCalls, inlineUdfsAndViews,
                externalVars);

        // Update the variable counter on our context.
        topStatement.setVarCounter(graphixRewritingContext.getVarCounter().get());
        LOGGER.debug("Ending SQL++ AST rewrites.");
    }

    public void loadNormalizedGraphElement(GraphixRewritingContext graphixRewritingContext,
            GraphElementDeclaration graphElementDeclaration) throws CompilationException {
        if (graphElementDeclaration.getNormalizedBody() == null) {
            Dataverse defaultDataverse = graphixRewritingContext.getMetadataProvider().getDefaultDataverse();
            Dataverse targetDataverse;

            // We might need to change our dataverse, if the element definition requires a different one.
            GraphIdentifier graphIdentifier = graphElementDeclaration.getGraphIdentifier();
            DataverseName graphDataverseName = graphIdentifier.getDataverseName();
            if (graphDataverseName == null || graphDataverseName.equals(defaultDataverse.getDataverseName())) {
                targetDataverse = defaultDataverse;

            } else {
                try {
                    targetDataverse = graphixRewritingContext.getMetadataProvider().findDataverse(graphDataverseName);

                } catch (AlgebricksException e) {
                    throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, e,
                            graphElementDeclaration.getSourceLocation(), graphDataverseName);
                }
            }
            graphixRewritingContext.getMetadataProvider().setDefaultDataverse(targetDataverse);

            // Get the body of the rewritten query.
            Expression rawBody = graphElementDeclaration.getRawBody();
            try {
                SourceLocation sourceLocation = graphElementDeclaration.getSourceLocation();
                Query wrappedQuery = ExpressionUtils.createWrappedQuery(rawBody, sourceLocation);
                bodyRewriter.rewrite(graphixRewritingContext, wrappedQuery, false, false, List.of());
                graphElementDeclaration.setNormalizedBody(wrappedQuery.getBody());

            } catch (CompilationException e) {
                // TODO (GLENN): How should we handle unmanaged graph bodies?
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, rawBody.getSourceLocation(),
                        "Bad definition for a graph element: " + e.getMessage());

            } finally {
                // Switch back to the working dataverse.
                graphixRewritingContext.getMetadataProvider().setDefaultDataverse(defaultDataverse);
            }
        }
    }

    public void rewriteGraphixASTNodes(GraphixRewritingContext graphixRewritingContext,
            IReturningStatement topStatement, boolean allowNonStoredUDFCalls) throws CompilationException {
        // Generate names for unnamed graph elements, projections in our SELECT CLAUSE.
        LOGGER.trace("Populating unknowns (both graph and non-graph) in our AST.");
        rewriteExpr(topStatement, new FillStartingUnknownsVisitor(graphixRewritingContext));

        // Resolve all of our (Graphix, SQL++, and user-defined) function calls.
        LOGGER.trace("Resolving Graphix, SQL++, and user-defined function calls.");
        rewriteExpr(topStatement, new FunctionResolutionVisitor(graphixRewritingContext, allowNonStoredUDFCalls));

        // Rewrite implicit correlated vertex JOINs as explicit JOINs.
        LOGGER.trace("Rewriting correlated implicit vertex JOINs into explicit JOINs.");
        rewriteExpr(topStatement, new CorrelatedVertexJoinVisitor(graphixRewritingContext));

        // If our user has given any global join annotations, apply them here.
        LOGGER.trace("Applying global INDEX-NL JOIN hints (if specified in the config).");
        rewriteExpr(topStatement, new PlaceEvaluationHintsVisitor(graphixRewritingContext));

        // Resolve our vertex labels, edge labels, and edge directions, and paths.
        LOGGER.trace("Performing label and direction resolution.");
        SchemaTableCreationVisitor creationVisitor = new SchemaTableCreationVisitor(graphixRewritingContext);
        topStatement.accept(creationVisitor, null);
        for (Pair<GraphIdentifier, SuperPattern> mapEntry : creationVisitor) {
            SchemaTable schemaTable = creationVisitor.getSchemaTableMap().get(mapEntry.getFirst());
            new ExhaustiveSearchResolver(schemaTable, graphixRewritingContext).resolve(mapEntry.getSecond());
        }

        // Fetch all relevant graph element declarations, using the element labels.
        LOGGER.trace("Fetching relevant edge and vertex bodies from our graph schema.");
        rewriteExpr(topStatement, new ElementDeclarationVisitor(graphixRewritingContext, parserFactory));
        rewriteExpr(topStatement, new DeclarationNormalizeVisitor(graphixRewritingContext, this));

        // Populate the unknowns for all GROUP-BYs (i.e. GROUP-BY fields).
        LOGGER.trace("Populating unknown GROUP-BY fields in our AST.");
        rewriteExpr(topStatement, new FillGroupByUnknownsVisitor());
        rewriteExpr(topStatement, new SubstituteGroupbyExpressionWithVariableVisitor(graphixRewritingContext));

        // Determine which vertices and path do not require their body.
        LOGGER.trace("Identifying dead vertices and paths (those that don't require their body) in our AST.");
        rewriteExpr(topStatement, new LiveVertexMarkingVisitor(graphixRewritingContext));

        // Verify that variables are properly within scope (this must be done after the GROUP-BY unknown handler).
        LOGGER.trace("Verifying that variables are unique and are properly scoped.");
        rewriteExpr(topStatement, new ExpressionScopingVisitor(graphixRewritingContext));

        // Transform all graph AST nodes (i.e. perform the representation lowering).
        LOGGER.trace("Lowering the Graphix AST-specific nodes representation to a SQL++ representation.");
        rewriteExpr(topStatement, new ExpressionLoweringVisitor(graphixRewritingContext));
    }

    /**
     * Rewrite a SQLPP AST. We do not perform the following:
     * <ul>
     *  <li>Function call resolution (this is handled in {@link FunctionResolutionVisitor}).</li>
     *  <li>Column name generation (this is handled in {@link FillStartingUnknownsVisitor}).</li>
     *  <li>SQL-compat rewrites (not supported).</li>
     * </ul>
     */
    public void rewriteSQLPPASTNodes(LangRewritingContext langRewritingContext, IReturningStatement topStatement,
            boolean allowNonStoredUDFCalls, boolean inlineUdfsAndViews, Collection<VarIdentifier> externalVars)
            throws CompilationException {
        super.setup(langRewritingContext, topStatement, externalVars, allowNonStoredUDFCalls, inlineUdfsAndViews);
        super.rewriteGroupBys();
        super.rewriteSetOperations();
        super.inlineColumnAlias();
        super.rewriteWindowExpressions();
        super.rewriteGroupingSets();
        super.variableCheckAndRewrite();
        super.extractAggregatesFromCaseExpressions();
        super.rewriteGroupByAggregationSugar();
        super.rewriteWindowAggregationSugar();
        super.rewriteOperatorExpression();
        super.rewriteCaseExpressions();
        super.rewriteListInputFunctions();
        super.rewriteRightJoins();
        super.loadAndInlineUdfsAndViews();
        super.rewriteSpecialFunctionNames();
        super.inlineWithExpressions();
    }

    @Override
    protected SqlppFunctionBodyRewriter getFunctionAndViewBodyRewriter() {
        return new SqlppFunctionBodyRewriter(parserFactory) {
            @Override
            public void rewrite(LangRewritingContext langRewritingContext, IReturningStatement topStatement,
                    boolean allowNonStoredUDFCalls, boolean inlineUdfsAndViews, Collection<VarIdentifier> externalVars)
                    throws CompilationException {
                // Perform an initial error-checking pass to validate our body.
                GraphixRewritingContext graphixRewritingContext = (GraphixRewritingContext) langRewritingContext;
                topStatement.accept(new PreRewriteCheckVisitor(graphixRewritingContext), null);

                // Perform the Graphix rewrites.
                rewriteGraphixASTNodes(graphixRewritingContext, topStatement, allowNonStoredUDFCalls);

                // Sanity check: ensure that no graph AST nodes exist after this point.
                Map<String, Object> queryConfig = graphixRewritingContext.getMetadataProvider().getConfig();
                if (queryConfig.containsKey(CompilerProperties.COMPILER_INTERNAL_SANITYCHECK_KEY)) {
                    String configValue = (String) queryConfig.get(CompilerProperties.COMPILER_INTERNAL_SANITYCHECK_KEY);
                    if (!configValue.equalsIgnoreCase("false")) {
                        topStatement.accept(new PostRewriteCheckVisitor(), null);
                    }
                }

                // Perform the remainder of the SQL++ (body specific) rewrites.
                super.setup(langRewritingContext, topStatement, externalVars, allowNonStoredUDFCalls,
                        inlineUdfsAndViews);
                super.substituteGroupbyKeyExpression();
                super.rewriteGroupBys();
                super.rewriteSetOperations();
                super.inlineColumnAlias();
                super.rewriteWindowExpressions();
                super.rewriteGroupingSets();
                super.variableCheckAndRewrite();
                super.extractAggregatesFromCaseExpressions();
                super.rewriteGroupByAggregationSugar();
                super.rewriteWindowAggregationSugar();
                super.rewriteOperatorExpression();
                super.rewriteCaseExpressions();
                super.rewriteListInputFunctions();
                super.rewriteRightJoins();

                // Update the variable counter in our context.
                topStatement.setVarCounter(langRewritingContext.getVarCounter().get());
            }
        };
    }

    private <R, T> void rewriteExpr(IReturningStatement returningStatement, ILangVisitor<R, T> visitor)
            throws CompilationException {
        String beforePlanAsString = null;
        if (LOGGER.isTraceEnabled()) {
            StringWriter stringWriter = new StringWriter();
            PrintWriter printWriter = new PrintWriter(stringWriter);
            returningStatement.accept(new GraphixASTPrintVisitor(printWriter), 0);
            beforePlanAsString = LogRedactionUtil.userData(stringWriter.toString());
        }
        returningStatement.accept(visitor, null);
        if (LOGGER.isTraceEnabled()) {
            StringWriter stringWriter = new StringWriter();
            PrintWriter printWriter = new PrintWriter(stringWriter);
            returningStatement.accept(new GraphixASTPrintVisitor(printWriter), 0);
            String afterPlanAsString = LogRedactionUtil.userData(stringWriter.toString());
            if (Objects.equals(beforePlanAsString, afterPlanAsString)) {
                LOGGER.trace("AST unchanged.");

            } else {
                LOGGER.trace("AST before rewrite: {}\n", beforePlanAsString);
                LOGGER.trace("AST after rewrite: {}\n", afterPlanAsString);
            }
        }
    }
}
