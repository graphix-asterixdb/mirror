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
package org.apache.asterix.graphix.lang.rewrite.lower.action;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.lower.action.base.IEnvironmentAction;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.DeclarationAnalysis;
import org.apache.asterix.graphix.lang.rewrite.visitor.common.VariableRemapCloneVisitor;
import org.apache.asterix.graphix.lang.statement.GraphElementDeclaration;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.UnnestClause;

/**
 * Inline an element body into a {@link LoweringEnvironment}. This includes:
 * <ol>
 *  <li>Copying {@link UnnestClause}, {@link LetClause}, and {@link WhereClause} AST nodes from our body analysis
 *  (i.e. {@link GraphElementDeclaration}).</li>
 *  <li>Creating {@link RecordConstructor} AST nodes to inline
 *  {@link org.apache.asterix.lang.sqlpp.clause.SelectRegular} nodes.</li>
 * </ol>
 */
public abstract class AbstractPatternInlineAction implements IEnvironmentAction {
    protected VariableRemapCloneVisitor variableRemapCloneVisitor;
    protected GraphixRewritingContext graphixRewritingContext;
    protected VariableExpr elementVariable;

    protected final DeclarationAnalysis declarationAnalysis;

    protected AbstractPatternInlineAction(GraphElementDeclaration graphElementDeclaration) {
        this.declarationAnalysis = graphElementDeclaration.getAnalysis();
    }

    protected abstract void preInline(LoweringEnvironment loweringEnvironment) throws CompilationException;

    protected abstract void postInline(LoweringEnvironment loweringEnvironment) throws CompilationException;

    @Override
    public final void apply(LoweringEnvironment loweringEnvironment) throws CompilationException {
        graphixRewritingContext = loweringEnvironment.getGraphixRewritingContext();
        variableRemapCloneVisitor = new VariableRemapCloneVisitor(graphixRewritingContext);

        preInline(loweringEnvironment);
        if (!declarationAnalysis.isExpressionInlineable()) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                    "Action applied illegally! Declaration must be inlineable!");
        }

        // To inline, we need to ensure that we substitute variables accordingly.
        final Function<VariableExpr, VariableExpr> substitutionAdder = v -> {
            VariableExpr reboundVariableExpr = graphixRewritingContext.getGraphixVariableCopy(v);
            variableRemapCloneVisitor.addSubstitution(v, reboundVariableExpr);
            return reboundVariableExpr;
        };
        variableRemapCloneVisitor.resetSubstitutions();
        if (declarationAnalysis.getFromTermVariable() != null) {
            VariableExpr fromTermVariableExpr = declarationAnalysis.getFromTermVariable();
            VariableExpr elementVariableExpr = new VariableExpr(elementVariable.getVar());
            variableRemapCloneVisitor.addSubstitution(fromTermVariableExpr, elementVariableExpr);
        }

        // If we have any UNNEST clauses, we need to add these.
        for (AbstractBinaryCorrelateClause unnestClause : declarationAnalysis.getUnnestClauses()) {
            loweringEnvironment.acceptTransformer(clauseSequence -> {
                UnnestClause copiedClause = (UnnestClause) variableRemapCloneVisitor.substitute(unnestClause);
                if (copiedClause.hasPositionalVariable()) {
                    substitutionAdder.apply(copiedClause.getPositionalVariable());
                }
                UnnestClause newUnnestClause = new UnnestClause(copiedClause.getUnnestType(),
                        copiedClause.getRightExpression(), substitutionAdder.apply(copiedClause.getRightVariable()),
                        null, copiedClause.getOuterUnnestMissingValueType());
                newUnnestClause.setSourceLocation(unnestClause.getSourceLocation());
                clauseSequence.addNonRepresentativeClause(newUnnestClause);
            });
        }

        // If we have any LET clauses, we need to substitute them in our WHERE and SELECT clauses.
        for (LetClause letClause : declarationAnalysis.getLetClauses()) {
            LetClause copiedClause = (LetClause) variableRemapCloneVisitor.substitute(letClause);
            VariableExpr reboundLetVariable = substitutionAdder.apply(copiedClause.getVarExpr());
            VariableExpr reboundLetVariableCopy = new VariableExpr(reboundLetVariable.getVar());
            Expression copiedBindingExpr = copiedClause.getBindingExpr();
            loweringEnvironment.acceptTransformer(clauseSequence -> {
                LetClause reboundLetClause = new LetClause(reboundLetVariableCopy, copiedBindingExpr);
                reboundLetClause.setSourceLocation(letClause.getSourceLocation());
                clauseSequence.addNonRepresentativeClause(reboundLetClause);
            });
        }

        // If we have any WHERE clauses, we need to add these.
        for (WhereClause whereClause : declarationAnalysis.getWhereClauses()) {
            WhereClause copiedClause = (WhereClause) variableRemapCloneVisitor.substitute(whereClause);
            loweringEnvironment.acceptTransformer(lowerList -> {
                WhereClause newWhereClause = new WhereClause(copiedClause.getWhereExpr());
                newWhereClause.setSourceLocation(whereClause.getSourceLocation());
                lowerList.addNonRepresentativeClause(newWhereClause);
            });
        }
        postInline(loweringEnvironment);
    }

    protected RecordConstructor buildRecordConstructor() throws CompilationException {
        List<Projection> selectClauseProjections = declarationAnalysis.getProjections();
        if (!selectClauseProjections.isEmpty()) {
            // Map our original variable to our element variable.
            List<FieldBinding> fieldBindings = new ArrayList<>();
            Set<String> boundNames = new HashSet<>();
            for (Projection projection : selectClauseProjections) {
                String projectionName = projection.getName();
                if (boundNames.contains(projectionName)) {
                    continue;
                }
                boundNames.add(projectionName);
                LiteralExpr fieldNameExpr = new LiteralExpr(new StringLiteral(projectionName));
                ILangExpression fieldValueExpr = variableRemapCloneVisitor.substitute(projection.getExpression());
                fieldBindings.add(new FieldBinding(fieldNameExpr, (Expression) fieldValueExpr));
            }
            return new RecordConstructor(fieldBindings);

        } else {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                    "Non-inlineable SELECT clause encountered, but was body was marked as inline!");
        }
    }
}
