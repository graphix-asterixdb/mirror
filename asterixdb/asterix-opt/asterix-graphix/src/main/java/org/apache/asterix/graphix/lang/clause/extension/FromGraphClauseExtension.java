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

import static org.apache.asterix.graphix.lang.clause.extension.IGraphixVisitorExtension.Kind.FROM_GRAPH_CLAUSE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.FromGraphTerm;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.base.IVisitorExtension;
import org.apache.asterix.lang.common.context.Scope;
import org.apache.asterix.lang.common.expression.AbstractCallExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.parser.ScopeChecker;
import org.apache.asterix.lang.common.rewrites.VariableSubstitutionEnvironment;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.BindingVariableVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class FromGraphClauseExtension implements IGraphixVisitorExtension {
    private final FromGraphClause fromGraphClause;

    public FromGraphClauseExtension(FromGraphClause fromGraphClause) {
        this.fromGraphClause = Objects.requireNonNull(fromGraphClause);
    }

    public FromGraphClause getFromGraphClause() {
        return fromGraphClause;
    }

    @Override
    public Expression simpleExpressionDispatch(ILangVisitor<Expression, ILangExpression> simpleExpressionVisitor,
            ILangExpression argument) throws CompilationException {
        for (AbstractClause fromTerm : fromGraphClause.getTerms()) {
            if (fromTerm instanceof FromGraphTerm) {
                FromGraphTerm fromGraphTerm = (FromGraphTerm) fromTerm;
                IVisitorExtension visitorExtension = fromGraphTerm.getLowerClause().getVisitorExtension();
                visitorExtension.simpleExpressionDispatch(simpleExpressionVisitor, argument);

            } else {
                fromTerm.accept(simpleExpressionVisitor, argument);
            }
        }
        return null;
    }

    @Override
    public Void freeVariableDispatch(ILangVisitor<Void, Collection<VariableExpr>> freeVariableVisitor,
            Collection<VariableExpr> freeVariables) throws CompilationException {
        Collection<VariableExpr> bindingVars = new HashSet<>();
        for (AbstractClause fromTerm : fromGraphClause.getTerms()) {
            Collection<VariableExpr> fromTermFreeVars = new HashSet<>();
            if (fromTerm instanceof FromGraphTerm) {
                FromGraphTerm fromGraphTerm = (FromGraphTerm) fromTerm;
                IVisitorExtension visitorExtension = fromGraphTerm.getLowerClause().getVisitorExtension();
                visitorExtension.freeVariableDispatch(freeVariableVisitor, fromTermFreeVars);
                List<VariableExpr> bindingVarsFromTerm = new ArrayList<>();
                visitorExtension.bindingVariableDispatch(new BindingVariableVisitor(), bindingVarsFromTerm);
                fromTermFreeVars.removeAll(bindingVars);
                bindingVars.addAll(bindingVarsFromTerm);

            } else {
                fromTerm.accept(freeVariableVisitor, fromTermFreeVars);
                fromTermFreeVars.removeAll(bindingVars);
                bindingVars.addAll(SqlppVariableUtil.getBindingVariables(fromTerm));
            }
            freeVariables.addAll(fromTermFreeVars);
        }
        return null;
    }

    @Override
    public Void bindingVariableDispatch(ILangVisitor<Void, Collection<VariableExpr>> bindingVariableVisitor,
            Collection<VariableExpr> bindingVariables) throws CompilationException {
        for (AbstractClause fromTerm : fromGraphClause.getTerms()) {
            if (fromTerm instanceof FromGraphTerm) {
                FromGraphTerm fromGraphTerm = (FromGraphTerm) fromTerm;
                IVisitorExtension visitorExtension = fromGraphTerm.getLowerClause().getVisitorExtension();
                visitorExtension.bindingVariableDispatch(bindingVariableVisitor, bindingVariables);

            } else {
                fromTerm.accept(bindingVariableVisitor, bindingVariables);
            }
        }
        return null;
    }

    @Override
    public Expression variableScopeDispatch(ILangVisitor<Expression, ILangExpression> scopingVisitor,
            ILangExpression argument, ScopeChecker scopeChecker) throws CompilationException {
        Scope scopeForFromClause = scopeChecker.extendCurrentScope();
        for (AbstractClause fromTerm : fromGraphClause.getTerms()) {
            if (fromTerm instanceof FromGraphTerm) {
                FromGraphTerm fromGraphTerm = (FromGraphTerm) fromTerm;

                // Note: Our extensions do not extend their scope, so we create a new scope here.
                scopeChecker.createNewScope();
                IVisitorExtension visitorExtension = fromGraphTerm.getLowerClause().getVisitorExtension();
                visitorExtension.variableScopeDispatch(scopingVisitor, argument, scopeChecker);

            } else {
                fromTerm.accept(scopingVisitor, fromGraphClause);
            }

            // Merge the variables defined in the current FROM-TERM into our parent scope.
            Scope scopeForFromTerm = scopeChecker.removeCurrentScope();
            for (String fromTermSymbol : scopeForFromTerm.getLocalSymbols()) {
                if (scopeForFromClause.findLocalSymbol(fromTermSymbol) != null) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, fromTerm.getSourceLocation(),
                            "Duplicate alias definitions: " + SqlppVariableUtil.toUserDefinedName(fromTermSymbol));
                }
            }
            scopeForFromClause.merge(scopeForFromTerm);
        }
        return null;
    }

    @Override
    public ILangExpression deepCopyDispatch(ILangVisitor<ILangExpression, Void> deepCopyVisitor)
            throws CompilationException {
        List<AbstractClause> copyFromTerms = new ArrayList<>();
        for (AbstractClause fromTerm : fromGraphClause.getTerms()) {
            if (fromTerm instanceof FromGraphTerm) {
                FromGraphTerm fromGraphTerm = (FromGraphTerm) fromTerm;
                IVisitorExtension visitorExtension = fromGraphTerm.getLowerClause().getVisitorExtension();
                copyFromTerms.add((AbstractClause) visitorExtension.deepCopyDispatch(deepCopyVisitor));

            } else {
                copyFromTerms.add((AbstractClause) fromTerm.accept(deepCopyVisitor, null));
            }
        }
        FromGraphClause copyFromGraphClause = new FromGraphClause(copyFromTerms);
        copyFromGraphClause.setSourceLocation(fromGraphClause.getSourceLocation());
        return copyFromGraphClause;
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> remapCloneDispatch(
            ILangVisitor<Pair<ILangExpression, VariableSubstitutionEnvironment>, VariableSubstitutionEnvironment> remapCloneVisitor,
            VariableSubstitutionEnvironment substitutionEnv) throws CompilationException {
        VariableSubstitutionEnvironment currentEnv = new VariableSubstitutionEnvironment(substitutionEnv);
        List<AbstractClause> copyFromTerms = new ArrayList<>();
        for (AbstractClause fromTerm : fromGraphClause.getTerms()) {
            if (fromTerm instanceof FromGraphTerm) {
                FromGraphTerm fromGraphTerm = (FromGraphTerm) fromTerm;
                IVisitorExtension visitorExtension = fromGraphTerm.getLowerClause().getVisitorExtension();
                Pair<ILangExpression, VariableSubstitutionEnvironment> resultPair =
                        visitorExtension.remapCloneDispatch(remapCloneVisitor, currentEnv);
                copyFromTerms.add((FromGraphTerm) resultPair.getFirst());
                currentEnv = resultPair.getSecond();

            } else {
                Pair<ILangExpression, VariableSubstitutionEnvironment> resultPair =
                        fromTerm.accept(remapCloneVisitor, currentEnv);
                copyFromTerms.add((AbstractClause) resultPair.getFirst());
                currentEnv = resultPair.getSecond();
            }
        }
        FromGraphClause copyFromGraphClause = new FromGraphClause(copyFromTerms);
        copyFromGraphClause.setSourceLocation(fromGraphClause.getSourceLocation());
        return new Pair<>(copyFromGraphClause, currentEnv);
    }

    @Override
    public Boolean inlineUDFsDispatch(ILangVisitor<Boolean, Void> inlineUDFsVisitor) throws CompilationException {
        boolean isInlineOccurred = false;
        for (AbstractClause fromTerm : fromGraphClause.getTerms()) {
            if (fromTerm instanceof FromGraphTerm) {
                FromGraphTerm fromGraphTerm = (FromGraphTerm) fromTerm;
                IVisitorExtension visitorExtension = fromGraphTerm.getLowerClause().getVisitorExtension();
                isInlineOccurred |= visitorExtension.inlineUDFsDispatch(inlineUDFsVisitor);

            } else {
                isInlineOccurred = fromTerm.accept(inlineUDFsVisitor, null);
            }
        }
        return isInlineOccurred;
    }

    @Override
    public Void gatherFunctionsDispatch(ILangVisitor<Void, Void> gatherFunctionsVisitor,
            Collection<? super AbstractCallExpression> functionCalls) throws CompilationException {
        for (AbstractClause fromTerm : fromGraphClause.getTerms()) {
            if (fromTerm instanceof FromGraphTerm) {
                FromGraphTerm fromGraphTerm = (FromGraphTerm) fromTerm;
                IVisitorExtension visitorExtension = fromGraphTerm.getLowerClause().getVisitorExtension();
                visitorExtension.gatherFunctionsDispatch(gatherFunctionsVisitor, functionCalls);

            } else {
                fromTerm.accept(gatherFunctionsVisitor, null);
            }
        }
        return null;
    }

    @Override
    public Boolean checkSubqueryDispatch(ILangVisitor<Boolean, ILangExpression> checkSubqueryVisitor,
            ILangExpression argument) throws CompilationException {
        for (AbstractClause fromTerm : fromGraphClause.getTerms()) {
            if (fromTerm instanceof FromGraphTerm) {
                FromGraphTerm fromGraphTerm = (FromGraphTerm) fromTerm;
                IVisitorExtension visitorExtension = fromGraphTerm.getLowerClause().getVisitorExtension();
                if (visitorExtension.checkSubqueryDispatch(checkSubqueryVisitor, argument)) {
                    return false;
                }

            } else if (fromTerm.accept(checkSubqueryVisitor, argument)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Boolean check92AggregateDispatch(ILangVisitor<Boolean, ILangExpression> check92AggregateVisitor,
            ILangExpression argument) {
        return false;
    }

    @Override
    public Boolean checkNonFunctionalDispatch(ILangVisitor<Boolean, Void> checkNonFunctionalVisitor)
            throws CompilationException {
        for (AbstractClause fromTerm : fromGraphClause.getTerms()) {
            if (fromTerm instanceof FromGraphTerm) {
                FromGraphTerm fromGraphTerm = (FromGraphTerm) fromTerm;
                IVisitorExtension visitorExtension = fromGraphTerm.getLowerClause().getVisitorExtension();
                if (visitorExtension.checkNonFunctionalDispatch(checkNonFunctionalVisitor)) {
                    return true;
                }

            } else if (fromTerm.accept(checkNonFunctionalVisitor, null)) {
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
        return FROM_GRAPH_CLAUSE;
    }
}
