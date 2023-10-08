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
package org.apache.asterix.graphix.lang.clause.extension;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.clause.BranchingClause;
import org.apache.asterix.graphix.lang.clause.FromGraphTerm;
import org.apache.asterix.graphix.lang.clause.SequenceClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.context.Scope;
import org.apache.asterix.lang.common.expression.AbstractCallExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.parser.ScopeChecker;
import org.apache.asterix.lang.common.rewrites.VariableSubstitutionEnvironment;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.util.VariableCloneAndSubstitutionUtil;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class BranchingClauseExtension implements IGraphixVisitorExtension {
    private final BranchingClause branchingClause;

    public BranchingClauseExtension(BranchingClause branchingClause) {
        this.branchingClause = branchingClause;
    }

    public BranchingClause getBranchingClause() {
        return branchingClause;
    }

    @Override
    public Expression simpleExpressionDispatch(ILangVisitor<Expression, ILangExpression> simpleExpressionVisitor,
            ILangExpression argument) throws CompilationException {
        for (SequenceClause sequenceClause : branchingClause.getClauseBranches()) {
            sequenceClause.getVisitorExtension().simpleExpressionDispatch(simpleExpressionVisitor, argument);
        }
        for (Map.Entry<VariableExpr, List<VariableExpr>> entry : branchingClause.getProjectionMapping().entrySet()) {
            entry.getKey().accept(simpleExpressionVisitor, argument);
            for (VariableExpr variableExpr : entry.getValue()) {
                variableExpr.accept(simpleExpressionVisitor, argument);
            }
        }
        return null;
    }

    @Override
    public Void freeVariableDispatch(ILangVisitor<Void, Collection<VariableExpr>> freeVariableVisitor,
            Collection<VariableExpr> freeVariables) throws CompilationException {
        for (SequenceClause sequenceClause : branchingClause.getClauseBranches()) {
            sequenceClause.getVisitorExtension().freeVariableDispatch(freeVariableVisitor, freeVariables);
        }
        return null;
    }

    @Override
    public Void bindingVariableDispatch(ILangVisitor<Void, Collection<VariableExpr>> bindingVariableVisitor,
            Collection<VariableExpr> bindingVariables) throws CompilationException {
        for (SequenceClause sequenceClause : branchingClause.getClauseBranches()) {
            sequenceClause.getVisitorExtension().bindingVariableDispatch(bindingVariableVisitor, bindingVariables);
        }
        for (Map.Entry<VariableExpr, List<VariableExpr>> entry : branchingClause.getProjectionMapping().entrySet()) {
            bindingVariableVisitor.visit(entry.getKey(), bindingVariables);
        }
        return null;
    }

    @Override
    public Expression variableScopeDispatch(ILangVisitor<Expression, ILangExpression> scopingVisitor,
            ILangExpression arg, ScopeChecker scopeChecker) throws CompilationException {
        for (SequenceClause sequenceClause : branchingClause.getClauseBranches()) {
            // Note: two branches cannot be correlated (similar to JOINs), we defer this check to Algebricks.
            scopeChecker.createNewScope();
            sequenceClause.getVisitorExtension().variableScopeDispatch(scopingVisitor, arg, scopeChecker);
        }
        for (Map.Entry<VariableExpr, List<VariableExpr>> entry : branchingClause.getProjectionMapping().entrySet()) {
            Scope currentScope = scopeChecker.getCurrentScope();
            currentScope.addNewVarSymbolToScope(entry.getKey().getVar());
            for (VariableExpr inputVariableExpr : entry.getValue()) {
                VarIdentifier workingId = inputVariableExpr.getVar();
                VarIdentifier existingId = (VarIdentifier) currentScope.findSymbol(workingId.getValue()).getFirst();
                workingId.setId(existingId.getId());
            }
        }
        return null;
    }

    @Override
    public ILangExpression deepCopyDispatch(ILangVisitor<ILangExpression, Void> deepCopyVisitor)
            throws CompilationException {
        List<SequenceClause> copyBranches = new ArrayList<>();
        for (SequenceClause sequenceClause : branchingClause.getClauseBranches()) {
            ILangExpression deepCopy = sequenceClause.getVisitorExtension().deepCopyDispatch(deepCopyVisitor);
            copyBranches.add((SequenceClause) ((FromGraphTerm) deepCopy).getLowerClause());
        }
        BranchingClause copyBranchingClause = new BranchingClause(copyBranches);
        copyBranchingClause.setSourceLocation(branchingClause.getSourceLocation());
        for (Map.Entry<VariableExpr, List<VariableExpr>> entry : branchingClause.getProjectionMapping().entrySet()) {
            VariableExpr copyKey = (VariableExpr) deepCopyVisitor.visit(entry.getKey(), null);
            List<VariableExpr> copyVariableExprs = new ArrayList<>();
            for (VariableExpr variableExpr : entry.getValue()) {
                VariableExpr copyVariableExpr = new VariableExpr(variableExpr.getVar());
                copyVariableExpr.setSourceLocation(variableExpr.getSourceLocation());
                copyVariableExprs.add(copyVariableExpr);
            }
            copyBranchingClause.putProjection(copyKey, copyVariableExprs);
        }
        FromGraphTerm copyFromGraphTerm = new FromGraphTerm(copyBranchingClause);
        copyFromGraphTerm.setSourceLocation(branchingClause.getSourceLocation());
        return copyFromGraphTerm;
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> remapCloneDispatch(
            ILangVisitor<Pair<ILangExpression, VariableSubstitutionEnvironment>, VariableSubstitutionEnvironment> remapCloneVisitor,
            VariableSubstitutionEnvironment substitutionEnv) throws CompilationException {
        List<SequenceClause> copyBranches = new ArrayList<>();
        for (SequenceClause sequenceClause : branchingClause.getClauseBranches()) {
            Pair<ILangExpression, VariableSubstitutionEnvironment> resultPair =
                    sequenceClause.getVisitorExtension().remapCloneDispatch(remapCloneVisitor, substitutionEnv);
            copyBranches.add((SequenceClause) ((FromGraphTerm) resultPair.getFirst()).getLowerClause());
        }
        BranchingClause copyBranchingClause = new BranchingClause(copyBranches);
        copyBranchingClause.setSourceLocation(branchingClause.getSourceLocation());
        VariableSubstitutionEnvironment workingEnv = substitutionEnv;
        for (Map.Entry<VariableExpr, List<VariableExpr>> entry : branchingClause.getProjectionMapping().entrySet()) {
            Pair<ILangExpression, VariableSubstitutionEnvironment> resultKeyPair =
                    entry.getKey().accept(remapCloneVisitor, substitutionEnv);
            VariableExpr copyKey = (VariableExpr) resultKeyPair.getFirst();
            List<VariableExpr> copyVariableExprs = new ArrayList<>();
            for (VariableExpr variableExpr : entry.getValue()) {
                Pair<ILangExpression, VariableSubstitutionEnvironment> resultValuePair =
                        variableExpr.accept(remapCloneVisitor, substitutionEnv);
                copyVariableExprs.add((VariableExpr) resultValuePair.getFirst());
            }
            copyBranchingClause.putProjection(copyKey, copyVariableExprs);
            workingEnv = VariableCloneAndSubstitutionUtil.eliminateSubstFromList(entry.getKey(), workingEnv);
        }
        FromGraphTerm copyFromGraphTerm = new FromGraphTerm(copyBranchingClause);
        copyFromGraphTerm.setSourceLocation(branchingClause.getSourceLocation());
        return new Pair<>(copyFromGraphTerm, workingEnv);
    }

    @Override
    public Boolean inlineUDFsDispatch(ILangVisitor<Boolean, Void> inlineUDFsVisitor) throws CompilationException {
        boolean changed = false;
        for (SequenceClause sequenceClause : branchingClause.getClauseBranches()) {
            changed |= sequenceClause.getVisitorExtension().inlineUDFsDispatch(inlineUDFsVisitor);
        }
        return changed;
    }

    @Override
    public Void gatherFunctionsDispatch(ILangVisitor<Void, Void> gatherFunctionsVisitor,
            Collection<? super AbstractCallExpression> functionCalls) throws CompilationException {
        for (SequenceClause sequenceClause : branchingClause.getClauseBranches()) {
            sequenceClause.getVisitorExtension().gatherFunctionsDispatch(gatherFunctionsVisitor, functionCalls);
        }
        return null;
    }

    @Override
    public Boolean checkSubqueryDispatch(ILangVisitor<Boolean, ILangExpression> checkSubqueryVisitor,
            ILangExpression arg) throws CompilationException {
        for (SequenceClause sequenceClause : branchingClause.getClauseBranches()) {
            if (sequenceClause.getVisitorExtension().checkSubqueryDispatch(checkSubqueryVisitor, null)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Boolean check92AggregateDispatch(ILangVisitor<Boolean, ILangExpression> check92AggregateVisitor,
            ILangExpression arg) {
        return false;
    }

    @Override
    public Boolean checkNonFunctionalDispatch(ILangVisitor<Boolean, Void> checkNonFunctionalVisitor)
            throws CompilationException {
        for (SequenceClause sequenceClause : branchingClause.getClauseBranches()) {
            if (sequenceClause.getVisitorExtension().checkNonFunctionalDispatch(checkNonFunctionalVisitor)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Boolean checkDatasetOnlyDispatch(ILangVisitor<Boolean, VariableExpr> checkDatasetOnlyVisitor,
            VariableExpr datasetCandidate) {
        return false;
    }

    @Override
    public Kind getKind() {
        return Kind.BRANCHING_CLAUSE;
    }
}
