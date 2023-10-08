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
package org.apache.asterix.graphix.lang.rewrite.visitor;

import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.clause.BranchingClause;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.FromGraphTerm;
import org.apache.asterix.graphix.lang.clause.LetGraphClause;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.clause.SequenceClause;
import org.apache.asterix.graphix.lang.expression.mapping.GraphConstructor;
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.ElementDirection;
import org.apache.asterix.graphix.lang.expression.pattern.IPatternConstruct;
import org.apache.asterix.graphix.lang.expression.pattern.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.QueryPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.optype.MatchType;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.lower.action.algorithm.EdgeAlgorithmAction;
import org.apache.asterix.graphix.lang.rewrite.lower.action.algorithm.PathAlgorithmAction;
import org.apache.asterix.graphix.lang.rewrite.lower.action.algorithm.VertexAlgorithmAction;
import org.apache.asterix.graphix.lang.rewrite.lower.action.base.IEnvironmentAction;
import org.apache.asterix.graphix.lang.rewrite.lower.action.finalize.FinalizeLeftMatchAction;
import org.apache.asterix.graphix.lang.rewrite.lower.action.semantics.MatchSemanticsAction;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.ClauseSequence;
import org.apache.asterix.graphix.lang.statement.GraphElementDeclaration;
import org.apache.asterix.graphix.lang.visitor.base.AbstractGraphixQueryVisitor;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Rewrite a graph AST to utilize non-graph AST nodes (i.e. replace {@link FromGraphTerm}). For each
 * {@link MatchClause}, our goal is to construct a series of {@link JoinClause} | {@link LetClause} |
 * {@link WhereClause} that correspond to the connections in the graph pattern.
 * <p>
 * For each {@link VertexPatternExpr}, we consider the following:
 * <ul>
 *   <li>Is the vertex already introduced? If so, then perform no action.</li>
 *   <li>Can the vertex be "folded-into" an attached edge (i.e. can we reuse an existing edge to refer to our vertex)?
 *   If so, then introduce aliases (i.e. {@link LetClause}) into the working {@link SelectBlock}.</li>
 *   <li>Is the vertex body "inline-able" (i.e. can we merge the AST subgraph associated with vertex body with the
 *   working {@link SelectBlock})? If so, then introduce the a) the vertex dataset, and b) the appropriate
 *   {@link JoinClause} | {@link LetClause} | {@link WhereClause} to realize the vertex body. Otherwise, nest the
 *   entire vertex body in a {@link SelectExpression} and JOIN the vertex through its keys.</li>
 *   <li>Does the vertex have an associated parent vertex (i.e. is the vertex correlated with some outer vertex)?
 *   If so, then introduce a JOIN to connect the parent vertex and the working vertex.</li>
 * </ul>
 * For each {@link EdgePatternExpr}, we consider the following:
 * <ul>
 *   <li>Is the edge body "fold-able" (i.e. can we reuse an inlined vertex to refer to the edge)? If so, then
 *   a) introduce aliases (i.e. {@link LetClause}) into the working {@link SelectBlock} and b) introduce a JOIN to
 *   connect the attached vertices. Otherwise, consider the choice below. </li>
 *   <li>Is the edge body "inline-able"? If so, then a) introduce the edge dataset + the appropriate {@link JoinClause}
 *  | {@link LetClause} | {@link WhereClause} to realize the edge body, and b) introduce a JOIN to connect the
 *  attached vertices. Otherwise, nest the entire edge body in a {@link SelectExpression} and JOIN the
 *  attached vertices.</li>
 *  <li>Is the edge undirected? If so, then a) lower instances of our edge directed from
 *  {@link ElementDirection#LEFT_TO_RIGHT} and our edge directed from {@link ElementDirection#RIGHT_TO_LEFT} into their
 *  own {@link SelectBlock}s, b) wrap both instances in a {@link BranchingClause}, and c) introduce the
 *  {@link BranchingClause} into our main {@link SelectBlock}.</li>
 * </ul>
 * For each {@link PathPatternExpr}, we consider the following:
 * <ul>
 *   <li>Is the right vertex introduced? If so, we will (physically) navigate from right to left. Otherwise, we will
 *   (physically) navigate from the left to right. TODO (GLENN): We should add this decision to the cost model.</li>
 * </ul>
 *
 * @see DeclarationAnalysisVisitor
 */
public class ExpressionLoweringVisitor extends AbstractGraphixQueryVisitor {
    private static final Logger LOGGER = LogManager.getLogger();

    private final DeclarationAnalysisVisitor declarationAnalysisVisitor;
    private final GraphixRewritingContext graphixRewritingContext;
    private final Deque<LoweringEnvironment> environmentStack;
    private final IEnvironmentAction.Callback loweringCallback = new IEnvironmentAction.Callback() {
        @Override
        public <E extends AbstractExpression, T extends ILangExpression> void invoke(E expr, T arg)
                throws CompilationException {
            expr.accept(ExpressionLoweringVisitor.this, arg);
        }
    };

    public ExpressionLoweringVisitor(GraphixRewritingContext graphixRewritingContext) {
        this.declarationAnalysisVisitor = new DeclarationAnalysisVisitor();
        this.graphixRewritingContext = graphixRewritingContext;
        this.environmentStack = new ArrayDeque<>();
    }

    @Override
    public Expression visit(SelectExpression se, ILangExpression arg) throws CompilationException {
        super.visit(se, arg);

        // We want to prune any LET-GRAPH-CLAUSEs we have here.
        se.getLetList().removeIf(c -> c instanceof LetGraphClause);
        return se;
    }

    @Override
    public Expression visit(GraphConstructor gc, ILangExpression arg) throws CompilationException {
        return null;
    }

    @Override
    public Expression visit(SelectBlock sb, ILangExpression arg) throws CompilationException {
        // Visit our SELECT-BLOCK.
        if (sb.hasFromClause() && sb.getFromClause() instanceof FromGraphClause) {
            int beforeStackSize = environmentStack.size();
            super.visit(sb, arg);
            int afterStackSize = environmentStack.size();
            for (int i = 0; i < afterStackSize - beforeStackSize; i++) {
                environmentStack.removeLast();
            }

        } else {
            super.visit(sb, arg);
        }

        // We want to prune any LET-GRAPH-CLAUSEs we have here.
        sb.getLetWhereList().removeIf(c -> c instanceof LetGraphClause);
        sb.getLetHavingListAfterGroupby().removeIf(c -> c instanceof LetGraphClause);
        return null;
    }

    @Override
    public Expression visit(FromGraphTerm fgt, ILangExpression arg) throws CompilationException {
        environmentStack.addLast(new LoweringEnvironment(graphixRewritingContext));

        // Perform an analysis pass over each element body. We need to determine what we can and can't inline.
        fgt.accept(new AbstractGraphixQueryVisitor() {
            @Override
            public Expression visit(PathPatternExpr ppe, ILangExpression arg) throws CompilationException {
                for (EdgePatternExpr decomposition : ppe.getDecompositions()) {
                    decomposition.getLeftVertex().accept(this, arg);
                    decomposition.getRightVertex().accept(this, arg);
                    decomposition.accept(this, arg);
                }
                return super.visit(ppe, arg);
            }

            @Override
            public Expression visit(EdgePatternExpr epe, ILangExpression arg) throws CompilationException {
                for (GraphElementDeclaration declaration : epe.getDeclarationSet()) {
                    declarationAnalysisVisitor.visit(declaration, null);
                }
                return super.visit(epe, arg);
            }

            @Override
            public Expression visit(VertexPatternExpr vpe, ILangExpression arg) throws CompilationException {
                for (GraphElementDeclaration declaration : vpe.getDeclarationSet()) {
                    declarationAnalysisVisitor.visit(declaration, null);
                }
                return super.visit(vpe, arg);
            }
        }, null);

        // Visit all of our MATCH clauses.
        super.visit(fgt, arg);

        // Introduce our MATCH-CLAUSE semantics.
        LoweringEnvironment environment = environmentStack.getLast();
        environment.acceptAction(new MatchSemanticsAction(fgt));

        // Move our main clause-sequence to the containing SELECT-BLOCK.
        ClauseSequence clauseSequence = environment.popClauseSequence();
        fgt.setLowerClause(new SequenceClause(clauseSequence));

        // Finally, if we have any correlate clauses, add them to our tail FROM-TERM.
        if (!fgt.getCorrelateClauses().isEmpty()) {
            fgt.getCorrelateClauses().forEach(clauseSequence::addUserDefinedCorrelateClause);
        }
        return null;
    }

    @Override
    public Expression visit(MatchClause mc, ILangExpression arg) throws CompilationException {
        LoweringEnvironment environment = environmentStack.getLast();
        if (mc.getMatchType() == MatchType.LEFTOUTER) {
            environment.pushClauseSequence();
        }

        // Our query-patterns are iterated over, individually.
        for (QueryPatternExpr queryPatternExpr : mc) {
            // First, iterate through all of our edges and paths.
            for (IPatternConstruct pattern : queryPatternExpr) {
                switch (pattern.getPatternType()) {
                    case EDGE_PATTERN:
                    case PATH_PATTERN:
                        pattern.accept(this, arg);
                }
            }

            // Next, iterate through all of our vertices.
            for (IPatternConstruct pattern : queryPatternExpr) {
                if (pattern.getPatternType() == IPatternConstruct.PatternType.VERTEX_PATTERN) {
                    pattern.accept(this, null);
                }
            }
        }

        if (mc.getMatchType() == MatchType.LEFTOUTER) {
            ClauseSequence leftOuterClauseSequence = environment.popClauseSequence();
            if (leftOuterClauseSequence.getNonRepresentativeClauses().isEmpty()) {
                // This is an extraneous LEFT-MATCH. Do not modify anything.
                if (LOGGER.isDebugEnabled()) {
                    String line = String.format("line %s", mc.getSourceLocation().getLine());
                    String column = String.format("column %s", mc.getSourceLocation().getColumn());
                    String location = String.format("(%s, %s)", line, column);
                    LOGGER.debug("LEFT MATCH clause at {} is extraneous. This is a NO-OP.", location);
                }

            } else {
                environment.acceptAction(new FinalizeLeftMatchAction(leftOuterClauseSequence, mc));
            }
        }
        return null;
    }

    @Override
    public Expression visit(VertexPatternExpr vpe, ILangExpression arg) throws CompilationException {
        environmentStack.getLast().acceptAction(new VertexAlgorithmAction(vpe, arg, loweringCallback));
        return vpe;
    }

    @Override
    public Expression visit(EdgePatternExpr epe, ILangExpression arg) throws CompilationException {
        environmentStack.getLast().acceptAction(new EdgeAlgorithmAction(epe, loweringCallback));
        return epe;
    }

    @Override
    public Expression visit(PathPatternExpr ppe, ILangExpression arg) throws CompilationException {
        environmentStack.getLast().acceptAction(new PathAlgorithmAction(ppe, loweringCallback));
        return ppe;
    }
}
