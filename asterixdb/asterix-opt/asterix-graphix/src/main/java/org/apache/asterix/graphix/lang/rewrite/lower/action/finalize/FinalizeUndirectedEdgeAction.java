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
package org.apache.asterix.graphix.lang.rewrite.lower.action.finalize;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.graphix.lang.clause.BranchingClause;
import org.apache.asterix.graphix.lang.clause.FromGraphTerm;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.clause.SequenceClause;
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.ElementDirection;
import org.apache.asterix.graphix.lang.expression.pattern.IPatternConstruct.PatternType;
import org.apache.asterix.graphix.lang.expression.pattern.QueryPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.optype.MatchType;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.lower.action.correlate.CorrelationGeneralAction;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.AliasLookupTable;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.ClauseSequence;
import org.apache.asterix.graphix.lang.rewrite.util.LowerRewritingUtil;
import org.apache.asterix.graphix.lang.rewrite.visitor.common.GraphixDeepCopyVisitor;
import org.apache.asterix.graphix.lang.visitor.base.AbstractGraphixQueryVisitor;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.IntegerLiteral;
import org.apache.asterix.lang.common.literal.TrueLiteral;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.UnnestClause;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.optype.JoinType;
import org.apache.asterix.lang.sqlpp.optype.UnnestType;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class FinalizeUndirectedEdgeAction extends AbstractFinalizeAction {
    private final ClauseSequence leftToRightSequence;
    private final ClauseSequence rightToLeftSequence;
    private final SequenceClause leftToRightClause;
    private final SequenceClause rightToLeftClause;
    private final BranchingClause branchingClause;
    private final EdgePatternExpr edgePatternExpr;
    private VariableExpr directionVariable;

    // We will build up the following on apply.
    private final Map<VariableExpr, Pair<List<VariableExpr>, PatternType>> sourceEdgeMapping;
    private final Map<VariableExpr, List<VariableExpr>> projectionMapping;

    public FinalizeUndirectedEdgeAction(EdgePatternExpr edgePatternExpr, ClauseSequence leftToRightSequence,
            ClauseSequence rightToLeftSequence) {
        super(edgePatternExpr.getSourceLocation());
        this.leftToRightSequence = leftToRightSequence;
        this.rightToLeftSequence = rightToLeftSequence;
        this.edgePatternExpr = edgePatternExpr;
        this.sourceEdgeMapping = new HashMap<>();
        this.projectionMapping = new HashMap<>();

        // We build our extension clause here.
        this.leftToRightClause = new SequenceClause(leftToRightSequence);
        this.rightToLeftClause = new SequenceClause(rightToLeftSequence);
        this.branchingClause = new BranchingClause(List.of(leftToRightClause, rightToLeftClause));
        this.branchingClause.setSourceLocation(edgePatternExpr.getSourceLocation());
    }

    @Override
    protected FromGraphTerm getFromGraphTerm() {
        VertexPatternExpr leftVertex = edgePatternExpr.getLeftVertex();
        VertexPatternExpr rightVertex = edgePatternExpr.getRightVertex();
        QueryPatternExpr queryPatternExpr = new QueryPatternExpr(List.of(leftVertex, edgePatternExpr, rightVertex));
        MatchClause matchClause = new MatchClause(List.of(queryPatternExpr), MatchType.LEADING);
        return new FromGraphTerm(branchingClause, List.of(matchClause));
    }

    @Override
    protected List<Projection> buildProjectionList(Consumer<VariableExpr> substitutionAdder) {
        List<Projection> projectionList = sourceEdgeMapping.keySet().stream().map(v -> {
            substitutionAdder.accept(v);
            VarIdentifier varIdentifier = SqlppVariableUtil.toUserDefinedVariableName(v.getVar());
            return new Projection(Projection.Kind.NAMED_EXPR, v, varIdentifier.getValue());
        }).collect(Collectors.toCollection(ArrayList::new));

        // Also add our direction to our projection list.
        String directionName = directionVariable.getVar().getValue();
        substitutionAdder.accept(directionVariable);
        projectionList.add(new Projection(Projection.Kind.NAMED_EXPR, directionVariable, directionName));
        return projectionList;
    }

    @Override
    protected void mergeExternalSequences(SelectExpression selectExpression, LoweringEnvironment environment)
            throws CompilationException {
        GraphixRewritingContext graphixRewritingContext = environment.getGraphixRewritingContext();
        GraphixDeepCopyVisitor deepCopyVisitor = new GraphixDeepCopyVisitor();

        // The resulting SELECT-EXPRESSION is connected back with a JOIN-CLAUSE or LET-CLAUSE -> UNNEST-CLAUSE.
        JoinTermVisitor l2RJoinTermVisitor = new JoinTermVisitor();
        JoinTermVisitor r2LJoinTermVisitor = new JoinTermVisitor();
        List<Expression> l2RJoinTerms = l2RJoinTermVisitor.buildJoinTerms(leftToRightClause.getVisitorExtension());
        List<Expression> r2LJoinTerms = r2LJoinTermVisitor.buildJoinTerms(rightToLeftClause.getVisitorExtension());
        l2RJoinTermVisitor.getUsedWhereClauses().forEach(leftToRightSequence::removeClause);
        r2LJoinTermVisitor.getUsedWhereClauses().forEach(rightToLeftSequence::removeClause);
        VariableExpr nestingVariableCopy = new VariableExpr(nestingVariable.getVar());
        environment.acceptTransformer(clauseSequence -> {
            // If we have not introduced any JOIN-CLAUSEs yet, then inline our right expression...
            Predicate<AbstractClause> isJoinClause = c -> c.getClauseType() == Clause.ClauseType.JOIN_CLAUSE;
            if (clauseSequence.getNonRepresentativeClauses().stream().noneMatch(isJoinClause)) {
                JoinClause joinClause = new JoinClause(JoinType.INNER, selectExpression, nestingVariableCopy, null,
                        new LiteralExpr(TrueLiteral.INSTANCE), null);
                clauseSequence.addNonRepresentativeClause(joinClause);
                return;
            }

            // ...otherwise, we want to expose our "left" scope to our right expression.
            VariableExpr variableExprForUnnest1 = new VariableExpr(graphixRewritingContext.newVariable());
            VariableExpr variableExprForUnnest2 = new VariableExpr(variableExprForUnnest1.getVar());
            clauseSequence.addNonRepresentativeClause(new LetClause(variableExprForUnnest1, selectExpression));
            clauseSequence.addNonRepresentativeClause(
                    new UnnestClause(UnnestType.INNER, variableExprForUnnest2, nestingVariableCopy, null, null));
        });

        // We need to add a directional component to each branch.
        VariableExpr l2RDirectionVariable = new VariableExpr(graphixRewritingContext.newVariable());
        VariableExpr r2LDirectionVariable = new VariableExpr(graphixRewritingContext.newVariable());
        LiteralExpr l2RDirectionValue = new LiteralExpr(new IntegerLiteral(ElementDirection.LEFT_TO_RIGHT.ordinal()));
        LiteralExpr r2LDirectionValue = new LiteralExpr(new IntegerLiteral(ElementDirection.RIGHT_TO_LEFT.ordinal()));
        leftToRightSequence.addNonRepresentativeClause(new LetClause(l2RDirectionVariable, l2RDirectionValue));
        rightToLeftSequence.addNonRepresentativeClause(new LetClause(r2LDirectionVariable, r2LDirectionValue));
        projectionMapping.put(directionVariable, List.of(l2RDirectionVariable, r2LDirectionVariable));
        projectionMapping.forEach(branchingClause::putProjection);

        // If we have a cross-product, there is no need to further consider our conditions...
        if (l2RJoinTerms.isEmpty() && r2LJoinTerms.isEmpty()) {
            return;
        }

        // Currently, our conditions refer to variable copies. Ensure these are appropriately qualified.
        AbstractGraphixQueryVisitor remapSourceVariables = new AbstractGraphixQueryVisitor() {
            @Override
            public Expression visit(VariableExpr v, ILangExpression arg) throws CompilationException {
                VariableExpr sourceVar = graphixRewritingContext.getGraphixVariableSource(v);
                if (sourceVar != null && sourceEdgeMapping.containsKey(sourceVar)) {
                    VarIdentifier varIdentifier = SqlppVariableUtil.toUserDefinedVariableName(sourceVar.getVar());
                    VariableExpr nestingVarCopy = new VariableExpr(nestingVariable.getVar());
                    return new FieldAccessor(nestingVarCopy, varIdentifier);
                }
                return super.visit(v, arg);
            }
        };

        // Locate the common term across both L2R and R2L conditions.
        List<Triple<Expression, Expression, Expression>> commonTerms =
                LowerRewritingUtil.extractCommonJoinTerms(l2RJoinTerms, r2LJoinTerms);
        for (Triple<Expression, Expression, Expression> commonTermTriple : commonTerms) {
            Expression l2RTerm = (Expression) commonTermTriple.second.accept(deepCopyVisitor, null);
            Expression r2LTerm = (Expression) commonTermTriple.third.accept(deepCopyVisitor, null);

            // Create a JOIN key from the non-common term (using a SWITCH-CASE).
            VariableExpr joinKeyVarExpr1 = new VariableExpr(graphixRewritingContext.newVariable());
            VariableExpr joinKeyVarExpr2 = new VariableExpr(joinKeyVarExpr1.getVar());
            FunctionIdentifier switchFunctionId = BuiltinFunctions.SWITCH_CASE;
            List<Expression> switchCallArgs = new ArrayList<>();
            switchCallArgs.add((Expression) remapCloneVisitor.substitute(directionVariable));
            switchCallArgs.add(new LiteralExpr(new IntegerLiteral(ElementDirection.LEFT_TO_RIGHT.ordinal())));
            switchCallArgs.add(l2RTerm.accept(remapSourceVariables, null));
            switchCallArgs.add(new LiteralExpr(new IntegerLiteral(ElementDirection.RIGHT_TO_LEFT.ordinal())));
            switchCallArgs.add(r2LTerm.accept(remapSourceVariables, null));
            CallExpr switchCallExpr = new CallExpr(new FunctionSignature(switchFunctionId), switchCallArgs);
            LetClause joinKeyBinding = new LetClause(joinKeyVarExpr1, switchCallExpr);
            environment.acceptTransformer(c -> c.addNonRepresentativeClause(joinKeyBinding));

            // Perform our JOIN.
            Expression commonJoinTerm = (Expression) commonTermTriple.first.accept(deepCopyVisitor, null);
            List<Expression> leftList = List.of(joinKeyVarExpr2);
            List<Expression> rightList = List.of(commonJoinTerm);
            environment.acceptAction(new CorrelationGeneralAction(leftList, rightList));
        }
    }

    @Override
    protected void introduceBindings(LoweringEnvironment environment) throws CompilationException {
        GraphixRewritingContext graphixRewritingContext = environment.getGraphixRewritingContext();
        AliasLookupTable aliasLookupTable = environment.getAliasLookupTable();

        // Again, we only care about our representative variables.
        for (Map.Entry<VariableExpr, Pair<List<VariableExpr>, PatternType>> entry : sourceEdgeMapping.entrySet()) {
            VariableExpr originalVariable = entry.getKey();
            FieldAccessor qualifiedVariable1 = (FieldAccessor) remapCloneVisitor.substitute(originalVariable);
            FieldAccessor qualifiedVariable2 = (FieldAccessor) remapCloneVisitor.substitute(originalVariable);
            FieldAccessor qualifiedVariable3 = (FieldAccessor) remapCloneVisitor.substitute(originalVariable);
            environment.acceptTransformer(clauseSequence -> {
                VariableExpr iterationAlias = graphixRewritingContext.getGraphixVariableCopy(originalVariable);
                VariableExpr joinAlias = graphixRewritingContext.getGraphixVariableCopy(originalVariable);
                LetClause iterationBinding = new LetClause(iterationAlias, qualifiedVariable1);
                LetClause joinBinding = new LetClause(joinAlias, qualifiedVariable2);
                aliasLookupTable.addJoinAlias(originalVariable, joinAlias);
                aliasLookupTable.addIterationAlias(originalVariable, iterationAlias);

                // Add our aliases and representative bindings.
                clauseSequence.addNonRepresentativeClause(iterationBinding);
                clauseSequence.addNonRepresentativeClause(joinBinding);
                switch (entry.getValue().getSecond()) {
                    case VERTEX_PATTERN:
                        clauseSequence.setVertexBinding(entry.getKey(), qualifiedVariable3);
                        break;
                    case EDGE_PATTERN:
                        clauseSequence.addEdgeBinding(entry.getKey(), qualifiedVariable3);
                        break;
                }
            });
        }
    }

    @Override
    public void apply(LoweringEnvironment loweringEnvironment) throws CompilationException {
        GraphixRewritingContext graphixRewritingContext = loweringEnvironment.getGraphixRewritingContext();

        // Build an association with our L2R and R2L representative variables.
        for (LetClause vertexBinding : leftToRightSequence.getRepresentativeVertexBindings()) {
            VariableExpr sourceVariable = graphixRewritingContext.getGraphixVariableSource(vertexBinding.getVarExpr());
            List<VariableExpr> outputList = new ArrayList<>(List.of(vertexBinding.getVarExpr()));
            sourceEdgeMapping.put(sourceVariable, new Pair<>(outputList, PatternType.VERTEX_PATTERN));
        }
        for (LetClause edgeBinding : leftToRightSequence.getRepresentativeEdgeBindings()) {
            VariableExpr sourceVariable = graphixRewritingContext.getGraphixVariableSource(edgeBinding.getVarExpr());
            List<VariableExpr> outputList = new ArrayList<>(List.of(edgeBinding.getVarExpr()));
            sourceEdgeMapping.put(sourceVariable, new Pair<>(outputList, PatternType.EDGE_PATTERN));
        }
        for (LetClause vertexBinding : rightToLeftSequence.getRepresentativeVertexBindings()) {
            VariableExpr sourceVariable = graphixRewritingContext.getGraphixVariableSource(vertexBinding.getVarExpr());
            sourceEdgeMapping.get(sourceVariable).first.add(vertexBinding.getVarExpr());
        }
        for (LetClause edgeBinding : rightToLeftSequence.getRepresentativeEdgeBindings()) {
            VariableExpr sourceVariable = graphixRewritingContext.getGraphixVariableSource(edgeBinding.getVarExpr());
            sourceEdgeMapping.get(sourceVariable).first.add(edgeBinding.getVarExpr());
        }

        // Update the projection mapping of our branching clause.
        sourceEdgeMapping.forEach((key, value) -> projectionMapping.put(key, value.getFirst()));
        directionVariable = new VariableExpr(graphixRewritingContext.newVariable());

        // Continue to the rest of the finalize action.
        super.apply(loweringEnvironment);
    }
}
