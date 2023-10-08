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
package org.apache.asterix.graphix.lang.rewrite.lower.action.semantics;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.FromGraphTerm;
import org.apache.asterix.graphix.lang.clause.SequenceClause;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.lower.action.base.IEnvironmentAction;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.ClauseSequence;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.MorphismConstraint;
import org.apache.asterix.graphix.lang.rewrite.util.LowerRewritingUtil;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.clause.UnnestClause;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.optype.JoinType;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;

public class LeftMatchSemanticsAction implements IEnvironmentAction {
    private final Map<VariableExpr, Expression> unknownScopeMap;
    private final List<MorphismConstraint> morphismConstraints;
    private final Set<VariableExpr> visitedVariables;

    public LeftMatchSemanticsAction(List<MorphismConstraint> morphismConstraints) {
        this.morphismConstraints = Objects.requireNonNull(morphismConstraints);
        this.unknownScopeMap = new LinkedHashMap<>();
        this.visitedVariables = new LinkedHashSet<>();
    }

    @Override
    public void apply(LoweringEnvironment loweringEnvironment) throws CompilationException {
        loweringEnvironment.acceptTransformer(clauseSequence -> {
            List<WhereClause> whereClauseList = new ArrayList<>();
            for (AbstractClause nonRepresentativeClause : clauseSequence.getNonRepresentativeClauses()) {
                switch (nonRepresentativeClause.getClauseType()) {
                    case LET_CLAUSE:
                        LetClause letClause = (LetClause) nonRepresentativeClause;
                        visitedVariables.add(letClause.getVarExpr());
                        break;

                    case UNNEST_CLAUSE:
                        UnnestClause unnestClause = (UnnestClause) nonRepresentativeClause;
                        visitedVariables.add(unnestClause.getRightVariable());
                        break;

                    case JOIN_CLAUSE:
                        JoinClause joinClause = (JoinClause) nonRepresentativeClause;
                        visitedVariables.add(joinClause.getRightVariable());

                        // If we encounter a LEFT-JOIN, then we have created a LEFT-MATCH branch.
                        if (joinClause.getJoinType() == JoinType.LEFTOUTER) {
                            WhereClause whereClause = acceptLeftMatchJoin(joinClause);
                            if (whereClause != null) {
                                whereClauseList.add(whereClause);
                            }
                        }
                        break;
                }
            }
            whereClauseList.forEach(clauseSequence::addNonRepresentativeClause);
        });
    }

    private WhereClause acceptLeftMatchJoin(JoinClause joinClause) throws CompilationException {
        // We can make the following assumptions about our JOIN here (i.e. the casts here are valid).
        Expression rightExpression = joinClause.getRightExpression();
        SelectExpression selectExpression = (SelectExpression) rightExpression;
        SelectSetOperation selectSetOperation = selectExpression.getSelectSetOperation();
        SelectBlock selectBlock = selectSetOperation.getLeftInput().getSelectBlock();
        FromGraphClause fromGraphClause = (FromGraphClause) selectBlock.getFromClause();
        FromGraphTerm fromGraphTerm = (FromGraphTerm) fromGraphClause.getTerms().get(0);
        SequenceClause leftLowerClause = (SequenceClause) fromGraphTerm.getLowerClause();
        ClauseSequence leftClauseSequence = leftLowerClause.getClauseSequence();

        // Walk through our inner clause sequence, determine our live variables here.
        Set<VariableExpr> localVariables = new LinkedHashSet<>();
        Expression forUnknownExpr = null;
        for (AbstractClause clause : leftClauseSequence.getNonRepresentativeClauses()) {
            switch (clause.getClauseType()) {
                case WHERE_CLAUSE:
                    break;

                case LET_CLAUSE:
                    LetClause letClause = (LetClause) clause;
                    localVariables.add(letClause.getVarExpr());
                    break;

                case UNNEST_CLAUSE:
                case JOIN_CLAUSE:
                    AbstractBinaryCorrelateClause correlateClause = (AbstractBinaryCorrelateClause) clause;
                    localVariables.add(correlateClause.getRightVariable());

                    // We will use the last JOIN clause here to condition for our unknown.
                    VarIdentifier correlateVarId = correlateClause.getRightVariable().getVar();
                    forUnknownExpr = new FieldAccessor(new VariableExpr(joinClause.getRightVariable().getVar()),
                            SqlppVariableUtil.toUserDefinedVariableName(correlateVarId));
                    break;
            }
        }
        for (VariableExpr localVariable : localVariables) {
            if (unknownScopeMap.containsKey(localVariable)) {
                throw new CompilationException(ErrorCode.ILLEGAL_STATE, "Local variable already exists?");
            }
            unknownScopeMap.put(localVariable, forUnknownExpr);
        }

        // Determine which conjuncts should be applied here.
        Set<MorphismConstraint> appliedConjuncts = new LinkedHashSet<>();
        for (MorphismConstraint morphismConstraint : morphismConstraints) {
            MorphismConstraint.IsomorphismOperand leftOperand = morphismConstraint.getLeftOperand();
            MorphismConstraint.IsomorphismOperand rightOperand = morphismConstraint.getRightOperand();

            // Determine which operand is local (i.e., comes from this LEFT-MATCH).
            boolean isLeftOpLocal = localVariables.contains(leftOperand.getVariableExpr());
            boolean isRightOpLocal = localVariables.contains(rightOperand.getVariableExpr());
            boolean isLeftOpGlobal = visitedVariables.contains(leftOperand.getVariableExpr());
            boolean isRightOpGlobal = visitedVariables.contains(rightOperand.getVariableExpr());
            if ((isLeftOpLocal && isRightOpGlobal) || (isLeftOpGlobal && isRightOpLocal)) {
                Stream.of(leftOperand.getVariableExpr(), rightOperand.getVariableExpr()).map(unknownScopeMap::get)
                        .filter(Objects::nonNull).map(e -> {
                            FunctionSignature functionSignature = new FunctionSignature(BuiltinFunctions.IS_UNKNOWN);
                            return new CallExpr(functionSignature, List.of(e));
                        }).forEach(morphismConstraint::addDisjunct);
                appliedConjuncts.add(morphismConstraint);
            }
        }
        morphismConstraints.removeAll(appliedConjuncts);
        if (appliedConjuncts.isEmpty()) {
            return null;
        }

        // Apply our conjuncts **outside** the JOIN condition expression (to enable HHJ LOJ plans).
        List<Expression> assembledConjuncts = new ArrayList<>();
        for (MorphismConstraint appliedConjunct : appliedConjuncts) {
            assembledConjuncts.add(appliedConjunct.build());
        }
        return new WhereClause(LowerRewritingUtil.buildConnectedClauses(assembledConjuncts, OperatorType.AND));
    }
}
