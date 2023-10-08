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
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Consumer;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.FromGraphTerm;
import org.apache.asterix.graphix.lang.clause.SequenceClause;
import org.apache.asterix.graphix.lang.clause.extension.SequenceClauseExtension;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.lower.action.base.IEnvironmentAction;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.ClauseSequence;
import org.apache.asterix.graphix.lang.rewrite.visitor.common.VariableRemapCloneVisitor;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IVisitorExtension;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.TrueLiteral;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateWithConditionClause;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectRegular;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.FreeVariableVisitor;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Abstract action for handling separate {@link ClauseSequence}(s), with the end goal of merging the given sequences
 * back into the main / working {@link ClauseSequence}. There are four major steps to each implementation:
 * <ol>
 *   <li>Define a list of {@link Projection}s, which will expose variables from some
 *   {@link org.apache.asterix.lang.sqlpp.clause.FromClause} or {@link FromGraphTerm} to be used downstream.</li>
 *   <li>Nestle our {@link ClauseSequence}(s) in a {@link SelectExpression}.</li>
 *   <li>Merge (i.e. JOIN) the {@link SelectExpression} from the previous step with the main / working
 *   {@link ClauseSequence}.</li>
 *   <li>Expose any bindings from the separate {@link ClauseSequence}(s), qualified with the nesting variable of the
 *   previous {@link SelectExpression}.</li>
 * </ol>
 */
public abstract class AbstractFinalizeAction implements IEnvironmentAction {
    protected final SourceLocation sourceLocation;

    // We will build the following on apply.
    protected VariableRemapCloneVisitor remapCloneVisitor;
    protected VariableExpr nestingVariable;

    protected AbstractFinalizeAction(SourceLocation sourceLocation) {
        this.sourceLocation = sourceLocation;
    }

    protected abstract FromGraphTerm getFromGraphTerm();

    protected abstract List<Projection> buildProjectionList(Consumer<VariableExpr> substitutionAdder);

    protected abstract void mergeExternalSequences(SelectExpression selectExpression, LoweringEnvironment environment)
            throws CompilationException;

    protected abstract void introduceBindings(LoweringEnvironment environment) throws CompilationException;

    @Override
    public void apply(LoweringEnvironment loweringEnvironment) throws CompilationException {
        GraphixRewritingContext graphixRewritingContext = loweringEnvironment.getGraphixRewritingContext();

        // Build our substitution visitor and environment.
        remapCloneVisitor = new VariableRemapCloneVisitor(graphixRewritingContext);
        nestingVariable = new VariableExpr(graphixRewritingContext.newVariable());
        final Consumer<VariableExpr> substitutionAdder = v -> {
            VarIdentifier varIdentifier = SqlppVariableUtil.toUserDefinedVariableName(v.getVar());
            VariableExpr nestingVariableCopy = new VariableExpr(nestingVariable.getVar());
            FieldAccessor fieldAccessor = new FieldAccessor(nestingVariableCopy, varIdentifier);
            remapCloneVisitor.addSubstitution(v, fieldAccessor);
        };

        // Build up our projection list.
        List<Projection> projectionList = buildProjectionList(substitutionAdder);

        // Nestle our clauses in a SELECT-EXPRESSION.
        SelectClause selectClause = new SelectClause(null, new SelectRegular(projectionList), false);
        selectClause.setSourceLocation(sourceLocation);
        SelectBlock selectBlock = new SelectBlock(selectClause, null, null, null, null);
        selectBlock.setSourceLocation(sourceLocation);
        FromGraphClause fromGraphClause = new FromGraphClause(List.of(getFromGraphTerm()));
        fromGraphClause.setSourceLocation(sourceLocation);
        selectBlock.setFromClause(fromGraphClause);
        SetOperationInput setOperationInput = new SetOperationInput(selectBlock, null);
        SelectSetOperation selectSetOperation = new SelectSetOperation(setOperationInput, null);
        selectSetOperation.setSourceLocation(sourceLocation);
        SelectExpression selectExpression = new SelectExpression(null, selectSetOperation, null, null, true);
        selectExpression.setSourceLocation(sourceLocation);

        // Perform our merge action, then introduce our representative variables back into the main sequence.
        mergeExternalSequences(selectExpression, loweringEnvironment);
        introduceBindings(loweringEnvironment);
    }

    protected static class JoinTermVisitor extends FreeVariableVisitor {
        private final List<Expression> joinConditionExpressions = new ArrayList<>();
        private final List<WhereClause> prunedWhereClauses = new ArrayList<>();

        protected void visitImmediateBinaryCorrelateClause(AbstractBinaryCorrelateClause correlateClause,
                Collection<VariableExpr> freeVars, Collection<VariableExpr> bindingVariables)
                throws CompilationException {
            Collection<VariableExpr> clauseFreeVars = new HashSet<>();
            Collection<VariableExpr> conditionFreeVars = new HashSet<>();

            correlateClause.getRightExpression().accept(this, clauseFreeVars);
            if (correlateClause.getClauseType() == Clause.ClauseType.UNNEST_CLAUSE) {
                clauseFreeVars.removeAll(bindingVariables);
                if (!clauseFreeVars.isEmpty()) {
                    throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                            "Encountered UNNEST-CLAUSE with free variables.");
                }

            } else {
                AbstractBinaryCorrelateWithConditionClause clauseWithCondition =
                        (AbstractBinaryCorrelateWithConditionClause) correlateClause;
                clauseWithCondition.getConditionExpression().accept(this, conditionFreeVars);
                conditionFreeVars.removeAll(bindingVariables);
                conditionFreeVars.remove(correlateClause.getRightVariable());
                if (!conditionFreeVars.isEmpty()) {
                    // We have found a JOIN with a free variable.
                    joinConditionExpressions.add(clauseWithCondition.getConditionExpression());
                    clauseWithCondition.setConditionExpression(new LiteralExpr(TrueLiteral.INSTANCE));
                }
                clauseFreeVars.addAll(conditionFreeVars);
            }

            // Adds binding variables.
            bindingVariables.add(correlateClause.getRightVariable());
            freeVars.addAll(clauseFreeVars);
        }

        protected void visitImmediateWhereClause(WhereClause whereClause, Collection<VariableExpr> freeVars,
                Collection<VariableExpr> bindingVariables) throws CompilationException {
            Collection<VariableExpr> clauseFreeVars = new HashSet<>();
            whereClause.getWhereExpr().accept(this, clauseFreeVars);
            clauseFreeVars.removeAll(bindingVariables);
            if (!clauseFreeVars.isEmpty()) {
                joinConditionExpressions.add(whereClause.getWhereExpr());
                prunedWhereClauses.add(whereClause);
            }
            freeVars.addAll(clauseFreeVars);
        }

        @SuppressWarnings("unused")
        protected void visitImmediateLetClause(LetClause letClause, Collection<VariableExpr> freeVars,
                Collection<VariableExpr> bindingVariables) throws CompilationException {
            Collection<VariableExpr> clauseFreeVars = new HashSet<>();
            letClause.getBindingExpr().accept(this, clauseFreeVars);
            clauseFreeVars.removeAll(bindingVariables);
            bindingVariables.add(letClause.getVarExpr());
        }

        @Override
        public Void visit(IVisitorExtension visitorExtension, Collection<VariableExpr> freeVars)
                throws CompilationException {
            Collection<VariableExpr> bindingVariables = new LinkedHashSet<>();
            if (visitorExtension instanceof SequenceClauseExtension) {
                SequenceClause sequenceClause = ((SequenceClauseExtension) visitorExtension).getSequenceClause();
                for (AbstractClause lowerClause : sequenceClause.getClauseSequence()) {
                    if (lowerClause instanceof AbstractBinaryCorrelateClause) {
                        AbstractBinaryCorrelateClause correlateClause = (AbstractBinaryCorrelateClause) lowerClause;
                        visitImmediateBinaryCorrelateClause(correlateClause, freeVars, bindingVariables);

                    } else if (lowerClause.getClauseType() == Clause.ClauseType.WHERE_CLAUSE) {
                        visitImmediateWhereClause((WhereClause) lowerClause, freeVars, bindingVariables);

                    } else if (lowerClause.getClauseType() == Clause.ClauseType.LET_CLAUSE) {
                        visitImmediateLetClause((LetClause) lowerClause, freeVars, bindingVariables);
                    }
                }

            } else {
                super.visit(visitorExtension, freeVars);
            }
            return null;
        }

        public List<Expression> buildJoinTerms(IVisitorExtension visitorExtension) throws CompilationException {
            this.visit(visitorExtension, new HashSet<>());
            return joinConditionExpressions;
        }

        public List<WhereClause> getUsedWhereClauses() {
            return prunedWhereClauses;
        }
    }
}
