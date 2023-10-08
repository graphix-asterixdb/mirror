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
import java.util.List;
import java.util.function.Consumer;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.FromGraphTerm;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.clause.SequenceClause;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.ClauseSequence;
import org.apache.asterix.graphix.lang.rewrite.util.LowerRewritingUtil;
import org.apache.asterix.graphix.lang.rewrite.visitor.common.GraphixDeepCopyVisitor;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IVisitorExtension;
import org.apache.asterix.lang.common.base.Literal;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.TrueLiteral;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.optype.JoinType;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;

public class FinalizeLeftMatchAction extends AbstractFinalizeAction {
    private final SequenceClause leftSequenceClause;
    private final MatchClause matchClause;

    public FinalizeLeftMatchAction(ClauseSequence leftClauseSequence, MatchClause matchClause) {
        super(matchClause.getSourceLocation());
        this.leftSequenceClause = new SequenceClause(leftClauseSequence);
        this.matchClause = matchClause;
    }

    @Override
    protected FromGraphTerm getFromGraphTerm() {
        return new FromGraphTerm(leftSequenceClause, List.of(matchClause));
    }

    @Override
    protected List<Projection> buildProjectionList(Consumer<VariableExpr> substitutionAdder) {
        List<Projection> projectionList = new ArrayList<>();
        ClauseSequence leftClauseSequence = leftSequenceClause.getClauseSequence();
        for (AbstractClause workingClause : leftClauseSequence.getNonRepresentativeClauses()) {
            if (workingClause.getClauseType() == Clause.ClauseType.WHERE_CLAUSE) {
                continue;
            }

            // Identify our right variable.
            VariableExpr rightVariable;
            if (workingClause.getClauseType() == Clause.ClauseType.LET_CLAUSE) {
                LetClause letClause = (LetClause) workingClause;
                if (isLoopingClause(letClause)) {
                    continue;
                }
                rightVariable = letClause.getVarExpr();

            } else if (workingClause instanceof AbstractBinaryCorrelateClause) {
                rightVariable = ((AbstractBinaryCorrelateClause) workingClause).getRightVariable();

            } else {
                throw new IllegalStateException("Illegal clause found! Expected LET, WHERE, or JOIN.");
            }
            VarIdentifier rightIdentifier = SqlppVariableUtil.toUserDefinedVariableName(rightVariable.getVar());
            projectionList.add(new Projection(Projection.Kind.NAMED_EXPR, rightVariable, rightIdentifier.getValue()));
            substitutionAdder.accept(rightVariable);
        }
        return projectionList;
    }

    @Override
    protected void mergeExternalSequences(SelectExpression selectExpression, LoweringEnvironment environment)
            throws CompilationException {
        // LEFT-JOIN our main-sequence with our left-match-sequence.
        GraphixDeepCopyVisitor deepCopyVisitor = new GraphixDeepCopyVisitor();
        IVisitorExtension visitorExtension = leftSequenceClause.getVisitorExtension();
        ClauseSequence leftMatchSequence = leftSequenceClause.getClauseSequence();
        JoinTermVisitor joinTermVisitor = new JoinTermVisitor();
        List<Expression> joinTerms = joinTermVisitor.buildJoinTerms(visitorExtension);
        Expression conditionExpression = joinTerms.isEmpty() ? new LiteralExpr(TrueLiteral.INSTANCE)
                : LowerRewritingUtil.buildConnectedClauses(joinTerms, OperatorType.AND);
        joinTermVisitor.getUsedWhereClauses().forEach(leftMatchSequence::removeClause);
        VariableExpr nestingVariableCopy = deepCopyVisitor.visit(nestingVariable, null);
        JoinClause leftJoinClause = new JoinClause(JoinType.LEFTOUTER, selectExpression, nestingVariableCopy, null,
                (Expression) remapCloneVisitor.substitute(conditionExpression), Literal.Type.MISSING);
        environment.acceptTransformer(clauseSequence -> clauseSequence.addNonRepresentativeClause(leftJoinClause));
    }

    @Override
    protected void introduceBindings(LoweringEnvironment environment) throws CompilationException {
        ClauseSequence leftMatchSequence = leftSequenceClause.getClauseSequence();
        GraphixDeepCopyVisitor deepCopyVisitor = new GraphixDeepCopyVisitor();
        environment.acceptTransformer(clauseSequence -> {
            // Introduce our non-representative bindings back into our main sequence to handle more than one LEFT-MATCH.
            for (AbstractClause nonRepresentativeClause : leftMatchSequence.getNonRepresentativeClauses()) {
                if (nonRepresentativeClause.getClauseType() != Clause.ClauseType.LET_CLAUSE) {
                    continue;
                }

                // We do not want to introduce the variable binding our LOOPING-CLAUSE.
                LetClause oldLetClause = (LetClause) nonRepresentativeClause;
                if (isLoopingClause(oldLetClause)) {
                    continue;
                }

                // Add our LEFT-MATCH bindings back in.
                VariableExpr newVariableExpr = deepCopyVisitor.visit(oldLetClause.getVarExpr(), null);
                Expression newBindingExpression = (Expression) remapCloneVisitor.substitute(oldLetClause.getVarExpr());
                clauseSequence.addLeftMatchBinding(newVariableExpr, newBindingExpression);
            }

            // Introduce the remainder of our bindings (these need to be deep copies).
            for (LetClause representativeVertexBinding : leftMatchSequence.getRepresentativeVertexBindings()) {
                VariableExpr representativeVariable = representativeVertexBinding.getVarExpr();
                VariableExpr representativeVariableCopy = deepCopyVisitor.visit(representativeVariable, null);
                Expression rightExpression = representativeVertexBinding.getBindingExpr();
                Expression reboundExpression = (Expression) remapCloneVisitor.substitute(rightExpression);
                clauseSequence.setVertexBinding(representativeVariableCopy, reboundExpression);
            }
            for (LetClause representativeEdgeBinding : leftMatchSequence.getRepresentativeEdgeBindings()) {
                VariableExpr representativeVariable = representativeEdgeBinding.getVarExpr();
                VariableExpr representativeVariableCopy = deepCopyVisitor.visit(representativeVariable, null);
                Expression rightExpression = representativeEdgeBinding.getBindingExpr();
                Expression reboundExpression = (Expression) remapCloneVisitor.substitute(rightExpression);
                clauseSequence.addEdgeBinding(representativeVariableCopy, reboundExpression);
            }
            for (LetClause representativePathBinding : leftMatchSequence.getRepresentativePathBindings()) {
                VariableExpr representativeVariable = representativePathBinding.getVarExpr();
                VariableExpr representativeVariableCopy = deepCopyVisitor.visit(representativeVariable, null);
                Expression rightExpression = representativePathBinding.getBindingExpr();
                Expression reboundExpression = (Expression) remapCloneVisitor.substitute(rightExpression);
                clauseSequence.addPathBinding(representativeVariableCopy, reboundExpression);
            }
        });
        leftMatchSequence.getRepresentativeEdgeBindings().forEach(leftMatchSequence::removeClause);
        leftMatchSequence.getRepresentativeVertexBindings().forEach(leftMatchSequence::removeClause);
        leftMatchSequence.getRepresentativePathBindings().forEach(leftMatchSequence::removeClause);
    }

    private boolean isLoopingClause(LetClause letClause) {
        Expression letClauseBinding = letClause.getBindingExpr();
        if (letClauseBinding.getKind() == Expression.Kind.SELECT_EXPRESSION) {
            SelectExpression letClauseSelectExpr = (SelectExpression) letClauseBinding;
            SelectSetOperation letSelectSetOperation = letClauseSelectExpr.getSelectSetOperation();
            SetOperationInput letSelectLeftInput = letSelectSetOperation.getLeftInput();
            if (letSelectLeftInput.selectBlock() && !letSelectSetOperation.hasRightInputs()) {
                SelectBlock letClauseSelectBlock = letSelectLeftInput.getSelectBlock();
                if (letClauseSelectBlock.hasFromClause()) {
                    FromClause letFromClause = letClauseSelectBlock.getFromClause();
                    return letFromClause instanceof FromGraphClause;
                }
            }
        }
        return false;
    }
}
