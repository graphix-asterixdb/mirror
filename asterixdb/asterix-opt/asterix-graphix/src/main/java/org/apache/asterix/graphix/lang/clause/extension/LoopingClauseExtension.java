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
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.clause.FromGraphTerm;
import org.apache.asterix.graphix.lang.clause.LoopingClause;
import org.apache.asterix.graphix.lang.clause.SequenceClause;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.base.IVisitorExtension;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.context.Scope;
import org.apache.asterix.lang.common.expression.AbstractCallExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.parser.ScopeChecker;
import org.apache.asterix.lang.common.rewrites.VariableSubstitutionEnvironment;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.util.VariableCloneAndSubstitutionUtil;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class LoopingClauseExtension implements IGraphixVisitorExtension {
    private final LoopingClause loopingClause;

    public LoopingClauseExtension(LoopingClause loopingClause) {
        this.loopingClause = loopingClause;
    }

    public LoopingClause getLoopingClause() {
        return loopingClause;
    }

    @Override
    public Expression simpleExpressionDispatch(ILangVisitor<Expression, ILangExpression> simpleExpressionVisitor,
            ILangExpression argument) throws CompilationException {
        for (SequenceClause sequenceClause : loopingClause.getSequenceClauses()) {
            sequenceClause.getVisitorExtension().simpleExpressionDispatch(simpleExpressionVisitor, argument);
        }
        return null;
    }

    @Override
    public Void freeVariableDispatch(ILangVisitor<Void, Collection<VariableExpr>> freeVariableVisitor,
            Collection<VariableExpr> freeVariables) throws CompilationException {
        // Our previous variables are NOT exposed to the SQL++ rewrites.
        for (int i = 0; i < loopingClause.getSequenceClauses().size(); i++) {
            SequenceClause sequenceClause = loopingClause.getSequenceClauses().get(i);
            List<VariableExpr> previousVariables = loopingClause.getPreviousVariables().get(i);
            sequenceClause.getVisitorExtension().freeVariableDispatch(freeVariableVisitor, freeVariables);
            freeVariables.removeIf(previousVariables::contains);
        }
        return null;
    }

    @Override
    public Void bindingVariableDispatch(ILangVisitor<Void, Collection<VariableExpr>> bindingVariableVisitor,
            Collection<VariableExpr> bindingVariables) throws CompilationException {
        for (SequenceClause sequenceClause : loopingClause.getSequenceClauses()) {
            sequenceClause.getVisitorExtension().bindingVariableDispatch(bindingVariableVisitor, bindingVariables);
        }
        bindingVariables.addAll(loopingClause.getOutputVariables());
        return null;
    }

    @Override
    public Expression variableScopeDispatch(ILangVisitor<Expression, ILangExpression> scopingVisitor,
            ILangExpression argument, ScopeChecker scopeChecker) throws CompilationException {
        // Our previous variables are in scope (for the purpose of our SQL++ rewrites).
        Scope currentScope = scopeChecker.getCurrentScope();
        for (int i = 0; i < loopingClause.getSequenceClauses().size(); i++) {
            SequenceClause sequenceClause = loopingClause.getSequenceClauses().get(i);
            for (VariableExpr previousVariable : loopingClause.getPreviousVariables().get(i)) {
                currentScope.addNewVarSymbolToScope(previousVariable.getVar());
            }
            sequenceClause.getVisitorExtension().variableScopeDispatch(scopingVisitor, argument, scopeChecker);
        }

        // Update our variable IDs.
        for (List<VariableExpr> previousVariableList : loopingClause.getPreviousVariables()) {
            for (VariableExpr previousVariable : previousVariableList) {
                VarIdentifier workingId = previousVariable.getVar();
                VarIdentifier existingId = (VarIdentifier) currentScope.findSymbol(workingId.getValue()).getFirst();
                workingId.setId(existingId.getId());
            }
        }
        for (List<VariableExpr> nextVariableList : loopingClause.getNextVariables()) {
            for (VariableExpr nextVariable : nextVariableList) {
                VarIdentifier workingId = nextVariable.getVar();
                VarIdentifier existingId = (VarIdentifier) currentScope.findSymbol(workingId.getValue()).getFirst();
                workingId.setId(existingId.getId());
            }
        }
        for (VariableExpr auxiliaryVariable : loopingClause.getAuxiliaryVariables()) {
            VarIdentifier workingId = auxiliaryVariable.getVar();
            VarIdentifier existingId = (VarIdentifier) currentScope.findSymbol(workingId.getValue()).getFirst();
            workingId.setId(existingId.getId());
        }

        // Our output variables should now be in scope.
        for (VariableExpr outputVariable : loopingClause.getOutputVariables()) {
            currentScope.addNewVarSymbolToScope(outputVariable.getVar());
        }
        return null;
    }

    @Override
    public ILangExpression deepCopyDispatch(ILangVisitor<ILangExpression, Void> deepCopyVisitor)
            throws CompilationException {
        BiFunction<VariableExpr, SequenceClause, VariableExpr> variableRemapper = (v, s) -> {
            for (AbstractClause abstractClause : s.getClauseSequence()) {
                if (abstractClause.getClauseType() == Clause.ClauseType.LET_CLAUSE) {
                    LetClause letClause = (LetClause) abstractClause;
                    if (v.equals(letClause.getVarExpr())) {
                        return letClause.getVarExpr();
                    }
                }
            }
            return null;
        };

        // Our previous variables must refer to the same variable used in each sequence clause.
        List<SequenceClause> copySequenceClauses = new ArrayList<>();
        List<List<VariableExpr>> copyPreviousVariables = new ArrayList<>();
        List<List<VariableExpr>> copyNextVariables = new ArrayList<>();
        for (int i = 0; i < loopingClause.getSequenceClauses().size(); i++) {
            SequenceClause sequenceClause = loopingClause.getSequenceClauses().get(i);
            IVisitorExtension visitorExtension = sequenceClause.getVisitorExtension();
            FromGraphTerm fromGraphTerm = (FromGraphTerm) visitorExtension.deepCopyDispatch(deepCopyVisitor);
            SequenceClause copySequenceClause = (SequenceClause) fromGraphTerm.getLowerClause();
            copySequenceClauses.add(copySequenceClause);

            // Our previous and next variables must be in the same order when we copy them...
            List<VariableExpr> copyPreviousVariableList = new ArrayList<>();
            for (VariableExpr previousVariable : loopingClause.getPreviousVariables().get(i)) {
                copyPreviousVariableList.add((VariableExpr) deepCopyVisitor.visit(previousVariable, null));
            }
            List<VariableExpr> copyNextVariableList = loopingClause.getNextVariables().get(i).stream()
                    .map(v -> variableRemapper.apply(v, copySequenceClause)).map(Objects::requireNonNull)
                    .collect(Collectors.toList());
            copyPreviousVariables.add(copyPreviousVariableList);
            copyNextVariables.add(copyNextVariableList);
        }

        // Next, our input variables...
        List<VariableExpr> copyInputVariables = new ArrayList<>();
        for (VariableExpr inputVariable : loopingClause.getInputVariables()) {
            VariableExpr copyInputVariable = new VariableExpr(inputVariable.getVar());
            copyInputVariable.setSourceLocation(inputVariable.getSourceLocation());
            copyInputVariables.add(copyInputVariable);
        }

        // ...our auxiliary variables...
        List<VariableExpr> copyAuxiliaryVariables = new ArrayList<>();
        for (VariableExpr auxiliaryVariable : loopingClause.getAuxiliaryVariables()) {
            VariableExpr copyAuxiliaryVariable = new VariableExpr(auxiliaryVariable.getVar());
            copyAuxiliaryVariable.setSourceLocation(auxiliaryVariable.getSourceLocation());
            copyAuxiliaryVariables.add(copyAuxiliaryVariable);
        }

        // ...our down variables...
        List<VariableExpr> copyDownVariables = new ArrayList<>();
        for (VariableExpr downVariable : loopingClause.getDownVariables()) {
            VariableExpr copyDownVariable = new VariableExpr(downVariable.getVar());
            copyDownVariable.setSourceLocation(downVariable.getSourceLocation());
            copyDownVariables.add(copyDownVariable);
        }

        // ...and now our output variables.
        List<VariableExpr> copyOutputVariables = new ArrayList<>();
        for (VariableExpr outputVariable : loopingClause.getOutputVariables()) {
            VariableExpr copyOutputVariable = new VariableExpr(outputVariable.getVar());
            copyOutputVariable.setSourceLocation(outputVariable.getSourceLocation());
            copyOutputVariables.add(copyOutputVariable);
        }

        // Finally, wrap our LOOPING-CLAUSE in a FROM-GRAPH-CLAUSE.
        LoopingClause copyLoopingClause = new LoopingClause(copyInputVariables, copyOutputVariables,
                copyAuxiliaryVariables, copyPreviousVariables, copyNextVariables, copySequenceClauses);
        copyDownVariables.forEach(copyLoopingClause::addDownVariable);
        copyLoopingClause.setSourceLocation(loopingClause.getSourceLocation());
        FromGraphTerm copyFromGraphTerm = new FromGraphTerm(copyLoopingClause);
        copyFromGraphTerm.setSourceLocation(loopingClause.getSourceLocation());
        return copyFromGraphTerm;
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> remapCloneDispatch(
            ILangVisitor<Pair<ILangExpression, VariableSubstitutionEnvironment>, VariableSubstitutionEnvironment> remapCloneVisitor,
            VariableSubstitutionEnvironment substitutionEnv) throws CompilationException {
        BiFunction<VariableExpr, SequenceClause, VariableExpr> variableRemapper = (v, s) -> {
            for (AbstractClause abstractClause : s.getClauseSequence()) {
                if (abstractClause.getClauseType() == Clause.ClauseType.LET_CLAUSE) {
                    LetClause letClause = (LetClause) abstractClause;
                    if (v.equals(letClause.getVarExpr())) {
                        return letClause.getVarExpr();
                    }
                }
            }
            return null;
        };

        // Our previous variables must refer to the same variable used in each sequence clause.
        List<SequenceClause> copySequenceClauses = new ArrayList<>();
        List<List<VariableExpr>> copyPreviousVariables = new ArrayList<>();
        List<List<VariableExpr>> copyNextVariables = new ArrayList<>();
        VariableSubstitutionEnvironment workingEnv = substitutionEnv;
        for (int i = 0; i < loopingClause.getSequenceClauses().size(); i++) {
            SequenceClause sequenceClause = loopingClause.getSequenceClauses().get(i);
            IVisitorExtension visitorExtension = sequenceClause.getVisitorExtension();
            Pair<ILangExpression, VariableSubstitutionEnvironment> resultPair =
                    visitorExtension.remapCloneDispatch(remapCloneVisitor, workingEnv);
            FromGraphTerm fromGraphTerm = (FromGraphTerm) resultPair.getFirst();
            SequenceClause copySequenceClause = (SequenceClause) fromGraphTerm.getLowerClause();
            copySequenceClauses.add(copySequenceClause);

            // Our previous and next variables must be in the same order when we copy them...
            VariableSubstitutionEnvironment finalWorkingEnv = workingEnv;
            List<VariableExpr> copyPreviousVariableList = new ArrayList<>();
            for (VariableExpr previousVariable : loopingClause.getPreviousVariables().get(i)) {
                Pair<ILangExpression, VariableSubstitutionEnvironment> previousPair =
                        remapCloneVisitor.visit(previousVariable, workingEnv);
                copyPreviousVariableList.add((VariableExpr) previousPair.getFirst());
                workingEnv = VariableCloneAndSubstitutionUtil.eliminateSubstFromList(previousVariable, workingEnv);
            }
            List<VariableExpr> copyNextVariableList = loopingClause.getNextVariables().get(i).stream()
                    .map(v -> Objects.requireNonNullElse((VariableExpr) finalWorkingEnv.findSubstitution(v), v))
                    .map(v -> variableRemapper.apply(v, copySequenceClause)).map(Objects::requireNonNull)
                    .collect(Collectors.toList());
            copyPreviousVariables.add(copyPreviousVariableList);
            copyNextVariables.add(copyNextVariableList);
        }

        // Next, our input variables...
        List<VariableExpr> copyInputVariables = new ArrayList<>();
        for (VariableExpr inputVariable : loopingClause.getInputVariables()) {
            Pair<ILangExpression, VariableSubstitutionEnvironment> resultPair =
                    inputVariable.accept(remapCloneVisitor, workingEnv);
            copyInputVariables.add((VariableExpr) resultPair.getFirst());
        }

        // ...our auxiliary variables...
        List<VariableExpr> copyAuxiliaryVariables = new ArrayList<>();
        for (VariableExpr auxiliaryVariable : loopingClause.getAuxiliaryVariables()) {
            Pair<ILangExpression, VariableSubstitutionEnvironment> resultPair =
                    auxiliaryVariable.accept(remapCloneVisitor, workingEnv);
            copyAuxiliaryVariables.add((VariableExpr) resultPair.getFirst());
        }

        // ...our down variables...
        List<VariableExpr> copyDownVariables = new ArrayList<>();
        for (VariableExpr downVariable : loopingClause.getDownVariables()) {
            Pair<ILangExpression, VariableSubstitutionEnvironment> resultPair =
                    downVariable.accept(remapCloneVisitor, workingEnv);
            copyDownVariables.add((VariableExpr) resultPair.getFirst());
        }

        // ...and now our output variables.
        List<VariableExpr> copyOutputVariables = new ArrayList<>();
        for (VariableExpr outputVariable : loopingClause.getOutputVariables()) {
            Pair<ILangExpression, VariableSubstitutionEnvironment> resultPair =
                    outputVariable.accept(remapCloneVisitor, workingEnv);
            copyOutputVariables.add((VariableExpr) resultPair.getFirst());
            workingEnv = VariableCloneAndSubstitutionUtil.eliminateSubstFromList(outputVariable, workingEnv);
        }

        // Finally, wrap our LOOPING-CLAUSE in a FROM-GRAPH-CLAUSE.
        LoopingClause copyLoopingClause = new LoopingClause(copyInputVariables, copyOutputVariables,
                copyAuxiliaryVariables, copyPreviousVariables, copyNextVariables, copySequenceClauses);
        copyDownVariables.forEach(copyLoopingClause::addDownVariable);
        copyLoopingClause.setSourceLocation(loopingClause.getSourceLocation());
        FromGraphTerm copyFromGraphTerm = new FromGraphTerm(copyLoopingClause);
        copyFromGraphTerm.setSourceLocation(loopingClause.getSourceLocation());
        return new Pair<>(copyFromGraphTerm, workingEnv);
    }

    @Override
    public Boolean inlineUDFsDispatch(ILangVisitor<Boolean, Void> inlineUDFsVisitor) throws CompilationException {
        boolean isInlineOccurred = false;
        for (SequenceClause sequenceClause : loopingClause.getSequenceClauses()) {
            isInlineOccurred |= sequenceClause.getVisitorExtension().inlineUDFsDispatch(inlineUDFsVisitor);
        }
        return isInlineOccurred;
    }

    @Override
    public Void gatherFunctionsDispatch(ILangVisitor<Void, Void> gatherFunctionsVisitor,
            Collection<? super AbstractCallExpression> functionCalls) throws CompilationException {
        for (SequenceClause sequenceClause : loopingClause.getSequenceClauses()) {
            sequenceClause.getVisitorExtension().gatherFunctionsDispatch(gatherFunctionsVisitor, functionCalls);
        }
        return null;
    }

    @Override
    public Boolean checkSubqueryDispatch(ILangVisitor<Boolean, ILangExpression> checkSubqueryVisitor,
            ILangExpression argument) throws CompilationException {
        boolean isFound = false;
        for (SequenceClause sequenceClause : loopingClause.getSequenceClauses()) {
            isFound |= sequenceClause.getVisitorExtension().checkSubqueryDispatch(checkSubqueryVisitor, argument);
        }
        return isFound;
    }

    @Override
    public Boolean check92AggregateDispatch(ILangVisitor<Boolean, ILangExpression> check92AggregateVisitor,
            ILangExpression argument) {
        return false;
    }

    @Override
    public Boolean checkNonFunctionalDispatch(ILangVisitor<Boolean, Void> checkNonFunctionalVisitor)
            throws CompilationException {
        boolean isFound = false;
        for (SequenceClause sequenceClause : loopingClause.getSequenceClauses()) {
            isFound |= sequenceClause.getVisitorExtension().checkNonFunctionalDispatch(checkNonFunctionalVisitor);
        }
        return isFound;
    }

    @Override
    public Boolean checkDatasetOnlyDispatch(ILangVisitor<Boolean, VariableExpr> checkDatasetOnlyVisitor,
            VariableExpr datasetCandidate) {
        return false;
    }

    @Override
    public Kind getKind() {
        return Kind.LOOPING_CLAUSE;
    }
}
