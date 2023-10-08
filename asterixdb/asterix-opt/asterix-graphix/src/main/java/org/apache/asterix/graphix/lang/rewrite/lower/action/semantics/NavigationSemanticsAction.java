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
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.graphix.algebra.compiler.option.SemanticsNavigationOption;
import org.apache.asterix.graphix.function.GraphixFunctionIdentifiers;
import org.apache.asterix.graphix.lang.clause.BranchingClause;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.FromGraphTerm;
import org.apache.asterix.graphix.lang.clause.LoopingClause;
import org.apache.asterix.graphix.lang.clause.SequenceClause;
import org.apache.asterix.graphix.lang.expression.pattern.IPatternConstruct;
import org.apache.asterix.graphix.lang.expression.pattern.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.QueryPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.lower.action.base.IEnvironmentAction;
import org.apache.asterix.graphix.lang.rewrite.lower.action.base.ISequenceTransformer;
import org.apache.asterix.graphix.lang.rewrite.lower.action.finalize.FinalizePathPatternAction;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.AliasLookupTable;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.ClauseSequence;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.MorphismConstraint;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.AbstractExtensionClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.IntegerLiteral;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class NavigationSemanticsAction implements IEnvironmentAction {
    private final List<MorphismConstraint> morphismConstraints;
    private final SemanticsNavigationOption semanticsNavigationOption;

    public NavigationSemanticsAction(List<MorphismConstraint> morphismConstraints,
            SemanticsNavigationOption semanticsNavigationOption) {
        this.morphismConstraints = Objects.requireNonNull(morphismConstraints);
        this.semanticsNavigationOption = Objects.requireNonNull(semanticsNavigationOption);
    }

    @Override
    public void apply(LoweringEnvironment loweringEnvironment) throws CompilationException {
        AliasLookupTable aliasLookupTable = loweringEnvironment.getAliasLookupTable();
        loweringEnvironment.acceptTransformer(new ISequenceTransformer() {
            private void gatherLoopingClauseFromGraphTerms(Expression inputExpr, List<FromGraphTerm> fromGraphTerms)
                    throws CompilationException {
                if (inputExpr.getKind() != Expression.Kind.SELECT_EXPRESSION) {
                    return;
                }
                SelectExpression selectExpression = (SelectExpression) inputExpr;
                if (selectExpression.getSelectSetOperation().hasRightInputs()) {
                    return;
                }
                SetOperationInput leftInput = selectExpression.getSelectSetOperation().getLeftInput();
                if (!leftInput.selectBlock()) {
                    return;
                }
                SelectBlock selectBlock = leftInput.getSelectBlock();
                if (!(selectBlock.getFromClause() instanceof FromGraphClause)) {
                    return;
                }
                FromGraphClause fromGraphClause = (FromGraphClause) selectBlock.getFromClause();
                FromGraphTerm fromGraphTerm = (FromGraphTerm) fromGraphClause.getTerms().get(0);
                AbstractExtensionClause lowerClause = fromGraphTerm.getLowerClause();
                if (lowerClause instanceof LoopingClause) {
                    fromGraphTerms.add(fromGraphTerm);

                } else if (lowerClause instanceof BranchingClause) {
                    BranchingClause branchingClause = (BranchingClause) lowerClause;
                    for (SequenceClause clauseBranch : branchingClause.getClauseBranches()) {
                        accept(clauseBranch.getClauseSequence());
                    }

                } else if (lowerClause instanceof SequenceClause) {
                    SequenceClause sequenceClause = (SequenceClause) lowerClause;
                    accept(sequenceClause.getClauseSequence());
                }
            }

            @Override
            public void accept(ClauseSequence clauseSequence) throws CompilationException {
                for (AbstractClause nonRepresentativeClause : clauseSequence.getNonRepresentativeClauses()) {
                    List<FromGraphTerm> fromGraphTerms = new ArrayList<>();
                    switch (nonRepresentativeClause.getClauseType()) {
                        case LET_CLAUSE:
                            LetClause letClause = (LetClause) nonRepresentativeClause;
                            gatherLoopingClauseFromGraphTerms(letClause.getBindingExpr(), fromGraphTerms);
                            for (FromGraphTerm fromGraphTerm : fromGraphTerms) {
                                acceptLoopingClause(fromGraphTerm, aliasLookupTable);
                            }
                            fromGraphTerms.clear();
                            break;

                        case JOIN_CLAUSE:
                            JoinClause joinClause = (JoinClause) nonRepresentativeClause;
                            gatherLoopingClauseFromGraphTerms(joinClause.getRightExpression(), fromGraphTerms);
                            for (FromGraphTerm fromGraphTerm : fromGraphTerms) {
                                acceptLoopingClause(fromGraphTerm, aliasLookupTable);
                            }
                            fromGraphTerms.clear();
                            break;
                    }
                }
            }
        });
    }

    private void acceptLoopingClause(FromGraphTerm fromGraphTerm, AliasLookupTable aliasLookupTable)
            throws CompilationException {
        // Determine our cycle prevention function.
        FunctionIdentifier cyclePreventionIdentifier = null;
        switch (semanticsNavigationOption) {
            case NO_REPEAT_ANYTHING:
                cyclePreventionIdentifier = GraphixFunctionIdentifiers.IS_DISTINCT_EVERYTHING;
                break;
            case NO_REPEAT_EDGES:
                cyclePreventionIdentifier = GraphixFunctionIdentifiers.IS_DISTINCT_EDGE;
                break;
            case NO_REPEAT_VERTICES:
                cyclePreventionIdentifier = GraphixFunctionIdentifiers.IS_DISTINCT_VERTEX;
                break;
        }

        // Locate the current vertex, current edge, and previous path in our sequence.
        LoopingClause loopingClause = (LoopingClause) fromGraphTerm.getLowerClause();
        for (SequenceClause sequenceClause : loopingClause.getSequenceClauses()) {
            Mutable<ClauseSequence> recursiveClauseSequence = new MutableObject<>();
            List<Expression> appendToPathArgs =
                    findFirstCallInSequence(GraphixFunctionIdentifiers.APPEND_TO_EXISTING_PATH,
                            sequenceClause.getClauseSequence(), recursiveClauseSequence::setValue);
            if (appendToPathArgs == null) {
                throw new CompilationException(ErrorCode.ILLEGAL_STATE,
                        "Path is not being built! Could not find APPEND_TO_PATH!");
            }
            VariableExpr vertexVar = (VariableExpr) appendToPathArgs.get(0);
            VariableExpr edgeVar = (VariableExpr) appendToPathArgs.get(1);
            VariableExpr pathVar = (VariableExpr) appendToPathArgs.get(2);

            // Add our navigation constraints.
            List<Expression> callExprArgs = new ArrayList<>();
            if (cyclePreventionIdentifier == GraphixFunctionIdentifiers.IS_DISTINCT_VERTEX) {
                callExprArgs.add(new VariableExpr(vertexVar.getVar()));
                callExprArgs.add(new VariableExpr(pathVar.getVar()));

            } else if (cyclePreventionIdentifier == GraphixFunctionIdentifiers.IS_DISTINCT_EDGE) {
                callExprArgs.add(new VariableExpr(edgeVar.getVar()));
                callExprArgs.add(new VariableExpr(pathVar.getVar()));

            } else { // cyclePreventionIdentifier == GraphixFunctionIdentifiers.IS_DISTINCT_EVERYTHING
                callExprArgs.add(new VariableExpr(vertexVar.getVar()));
                callExprArgs.add(new VariableExpr(edgeVar.getVar()));
                callExprArgs.add(new VariableExpr(pathVar.getVar()));
            }
            FunctionSignature functionSignature = new FunctionSignature(cyclePreventionIdentifier);
            CallExpr callExpr = new CallExpr(functionSignature, callExprArgs);
            callExpr.setSourceLocation(loopingClause.getSourceLocation());
            WhereClause whereClause = new WhereClause(callExpr);
            whereClause.setSourceLocation(loopingClause.getSourceLocation());

            // Note: we'll rely on select pushdown to move this below our APPEND-TO-PATH.
            recursiveClauseSequence.getValue().addNonRepresentativeClause(whereClause);
        }

        // If we allow zero-hop edges, then we'll need to add a separate case for our isomorphism here.
        QueryPatternExpr queryPatternExpr = fromGraphTerm.getMatchClauses().get(0).iterator().next();
        PathPatternExpr pathPatternExpr = null;
        for (IPatternConstruct patternConstruct : queryPatternExpr) {
            if (patternConstruct.getPatternType() == IPatternConstruct.PatternType.PATH_PATTERN) {
                pathPatternExpr = (PathPatternExpr) patternConstruct;
                break;
            }
        }
        if (pathPatternExpr == null) {
            throw new CompilationException(ErrorCode.ILLEGAL_STATE, "PATH_PATTERN not found!");

        } else if (pathPatternExpr.getPathDescriptor().getMinimumHops() > 0) {
            return;
        }
        VariableExpr leftVariable = pathPatternExpr.getLeftVertex().getVertexDescriptor().getVariableExpr();
        VariableExpr rightVariable = pathPatternExpr.getRightVertex().getVertexDescriptor().getVariableExpr();
        VariableExpr leftJoinAlias = aliasLookupTable.getJoinAlias(leftVariable);
        VariableExpr rightJoinAlias = aliasLookupTable.getJoinAlias(rightVariable);

        // Iterate through our current conjuncts.
        for (MorphismConstraint morphismConstraint : morphismConstraints) {
            MorphismConstraint.IsomorphismOperand leftOperand = morphismConstraint.getLeftOperand();
            MorphismConstraint.IsomorphismOperand rightOperand = morphismConstraint.getRightOperand();
            if (!Objects.equals(leftOperand.getVariableExpr(), leftJoinAlias)
                    || !Objects.equals(rightOperand.getVariableExpr(), rightJoinAlias)) {
                continue;
            }

            // We will condition on our path hop count being zero...
            VariableExpr pathVariableExpr = pathPatternExpr.getPathDescriptor().getVariableExpr();
            VariableExpr pathJoinAlias = aliasLookupTable.getJoinAlias(pathVariableExpr);
            Expression hopCountExpr = FinalizePathPatternAction.generatePathHopCountExpression(pathJoinAlias);
            LiteralExpr zeroLiteralExpr = new LiteralExpr(new IntegerLiteral(0));
            List<Expression> opExprArgs = List.of(hopCountExpr, zeroLiteralExpr);
            morphismConstraint.addDisjunct(new OperatorExpr(opExprArgs, List.of(OperatorType.EQ), false));
        }
    }

    private List<Expression> findFirstCallInSequence(FunctionIdentifier functionId, ClauseSequence clauseSequence,
            Consumer<ClauseSequence> clauseSequenceCallback) {
        for (AbstractClause workingClause : clauseSequence.getNonRepresentativeClauses()) {
            switch (workingClause.getClauseType()) {
                case LET_CLAUSE:
                    LetClause letClause = (LetClause) workingClause;
                    if (letClause.getBindingExpr().getKind() != Expression.Kind.CALL_EXPRESSION) {
                        break;
                    }
                    CallExpr callExpr = (CallExpr) letClause.getBindingExpr();
                    if (Objects.equals(callExpr.getFunctionSignature().getName(), functionId.getName())) {
                        clauseSequenceCallback.accept(clauseSequence);
                        return callExpr.getExprList();
                    }
                    break;

                case JOIN_CLAUSE:
                    // This is recursive. Walk down any inner sequences we find.
                    JoinClause joinClause = (JoinClause) workingClause;
                    Expression rightExpression = joinClause.getRightExpression();
                    if (rightExpression.getKind() != Expression.Kind.SELECT_EXPRESSION) {
                        break;
                    }
                    SelectExpression selectExpression = (SelectExpression) rightExpression;
                    if (selectExpression.getSelectSetOperation().hasRightInputs()) {
                        break;
                    }
                    SetOperationInput leftInput = selectExpression.getSelectSetOperation().getLeftInput();
                    if (!leftInput.selectBlock()) {
                        break;
                    }
                    SelectBlock selectBlock = leftInput.getSelectBlock();
                    if (!(selectBlock.getFromClause() instanceof FromGraphClause)) {
                        break;
                    }
                    FromGraphClause fromGraphClause = (FromGraphClause) selectBlock.getFromClause();
                    FromGraphTerm innerFromGraphTerm = (FromGraphTerm) fromGraphClause.getTerms().get(0);
                    AbstractExtensionClause lowerClause = innerFromGraphTerm.getLowerClause();
                    if (lowerClause instanceof SequenceClause) {
                        SequenceClause sequenceClause = (SequenceClause) lowerClause;
                        List<Expression> result = findFirstCallInSequence(functionId,
                                sequenceClause.getClauseSequence(), clauseSequenceCallback);
                        if (result != null) {
                            return result;
                        }

                    } else if (lowerClause instanceof BranchingClause) {
                        BranchingClause branchingClause = (BranchingClause) lowerClause;
                        for (SequenceClause clauseBranch : branchingClause.getClauseBranches()) {
                            List<Expression> result = findFirstCallInSequence(functionId,
                                    clauseBranch.getClauseSequence(), clauseSequenceCallback);
                            if (result != null) {
                                return result;
                            }
                        }

                    } else if (lowerClause instanceof LoopingClause) {
                        LoopingClause loopingClause = (LoopingClause) lowerClause;
                        for (SequenceClause sequenceClause : loopingClause.getSequenceClauses()) {
                            List<Expression> result = findFirstCallInSequence(functionId,
                                    sequenceClause.getClauseSequence(), clauseSequenceCallback);
                            if (result != null) {
                                return result;
                            }
                        }
                    }
                    break;
            }
        }
        return null;
    }
}
