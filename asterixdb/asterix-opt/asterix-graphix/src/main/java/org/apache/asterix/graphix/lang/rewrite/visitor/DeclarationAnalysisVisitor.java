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

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.DeclarationAnalysis;
import org.apache.asterix.graphix.lang.statement.GraphElementDeclaration;
import org.apache.asterix.graphix.lang.visitor.base.AbstractGraphixQueryVisitor;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectRegular;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.clause.UnnestClause;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.util.FunctionMapUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Perform an analysis of a graph element body to determine if we can inline this graph element body with the greater
 * {@link SelectBlock} node during lowering.
 * <ol>
 *  <li>Is this a dataset {@link CallExpr}? If so, we can inline this directly.</li>
 *  <li>Is this expression a {@link SelectExpression} containing a single {@link FromTerm} w/ possibly only
 *  {@link UnnestClause} clauses? If so, we can inline this expression.</li>
 *  <li>Are there are {@link LetClause} / {@link WhereClause} expressions? We can inline these if the two previous
 *  questions are true.</li>
 *  <li>Are there any aggregate functions (or any aggregation)? If so, we cannot inline this expression.</li>
 *  <li>Are there any {@link SelectSetOperation}s? If so, we cannot inline this expression.</li>
 *  <li>Are there any {@link org.apache.asterix.lang.common.clause.OrderbyClause} or
 *  {@link org.apache.asterix.lang.common.clause.LimitClause} clauses? If so, we cannot inline this expression.</li>
 * </ol>
 *
 * @see DeclarationAnalysis
 */
public class DeclarationAnalysisVisitor extends AbstractGraphixQueryVisitor {
    private DeclarationAnalysis workingDeclarationAnalysis;

    public Expression visit(GraphElementDeclaration ged, ILangExpression arg) throws CompilationException {
        workingDeclarationAnalysis = ged.getAnalysis();
        if (ged.getNormalizedBody() == null) {
            // If we could not normalize our body, we cannot inline our declaration.
            workingDeclarationAnalysis.markAsNonInlineable();
            return ged.getRawBody();
        }
        return ged.getNormalizedBody().accept(this, arg);
    }

    @Override
    public Expression visit(CallExpr ce, ILangExpression arg) throws CompilationException {
        FunctionSignature functionSignature = ce.getFunctionSignature();
        FunctionIdentifier functionIdentifier = functionSignature.createFunctionIdentifier();
        if (functionIdentifier.equals(BuiltinFunctions.DATASET)) {
            LiteralExpr dataverseNameExpr = (LiteralExpr) ce.getExprList().get(0);
            LiteralExpr datasetNameExpr = (LiteralExpr) ce.getExprList().get(1);
            String dataverseName = ((StringLiteral) dataverseNameExpr.getValue()).getValue();
            String datasetName = ((StringLiteral) datasetNameExpr.getValue()).getValue();
            workingDeclarationAnalysis.setDatasetCall(ce, datasetName);
            try {
                workingDeclarationAnalysis.setDataverseName(DataverseName.createFromCanonicalForm(dataverseName));
                return ce;
            } catch (AsterixException e) {
                SourceLocation sourceLoc = ce.getSourceLocation();
                throw new CompilationException(ErrorCode.INVALID_DATABASE_OBJECT_NAME, sourceLoc, dataverseName);
            }

        } else if (FunctionMapUtil.isSql92AggregateFunction(functionSignature)
                || FunctionMapUtil.isCoreAggregateFunction(functionSignature)
                || BuiltinFunctions.getWindowFunction(functionIdentifier) != null) {
            workingDeclarationAnalysis.markAsNonInlineable();
            return ce;
        }
        return ce;
    }

    @Override
    public Expression visit(SelectExpression se, ILangExpression arg) throws CompilationException {
        if (arg != null) {
            // We are not in a top-level SELECT-EXPR. Do not proceed.
            return se;
        }
        if (se.hasOrderby() || se.hasLimit() || se.hasLetClauses()) {
            workingDeclarationAnalysis.markAsNonInlineable();
            return se;
        }
        se.getSelectSetOperation().accept(this, se);
        return se;
    }

    @Override
    public Expression visit(SelectSetOperation sso, ILangExpression arg) throws CompilationException {
        if (sso.hasRightInputs()) {
            workingDeclarationAnalysis.markAsNonInlineable();
            return null;
        }
        SetOperationInput leftInput = sso.getLeftInput();
        if (leftInput.subquery()) {
            workingDeclarationAnalysis.markAsNonInlineable();
            return null;
        }
        leftInput.getSelectBlock().accept(this, arg);
        return null;
    }

    @Override
    public Expression visit(SelectBlock sb, ILangExpression arg) throws CompilationException {
        if (sb.hasGroupbyClause() || sb.hasLetHavingClausesAfterGroupby()) {
            workingDeclarationAnalysis.markAsNonInlineable();
            return null;
        }
        if (sb.hasFromClause()) {
            sb.getFromClause().accept(this, arg);
        }
        if (sb.hasLetWhereClauses()) {
            sb.getLetWhereList().forEach(c -> {
                if (c.getClauseType() == Clause.ClauseType.LET_CLAUSE) {
                    workingDeclarationAnalysis.addLetClause((LetClause) c);

                } else { // c.getClauseType() == Clause.ClauseType.WHERE_CLAUSE
                    workingDeclarationAnalysis.addWhereClause((WhereClause) c);
                }
            });
        }
        sb.getSelectClause().accept(this, arg);
        return null;
    }

    @Override
    public Expression visit(FromGraphClause fgc, ILangExpression arg) throws CompilationException {
        workingDeclarationAnalysis.markAsNonInlineable();
        return null;
    }

    @Override
    public Expression visit(FromClause fc, ILangExpression arg) throws CompilationException {
        if (fc.getFromTerms().size() > 1) {
            workingDeclarationAnalysis.markAsNonInlineable();
            return null;
        }
        fc.getFromTerms().get(0).accept(this, arg);
        return null;
    }

    @Override
    public Expression visit(FromTerm ft, ILangExpression arg) throws CompilationException {
        List<AbstractBinaryCorrelateClause> correlateClauses = ft.getCorrelateClauses();
        List<AbstractBinaryCorrelateClause> unnestClauses = new ArrayList<>();
        for (AbstractBinaryCorrelateClause c : correlateClauses) {
            if (c.getClauseType().equals(Clause.ClauseType.UNNEST_CLAUSE)) {
                UnnestClause unnestClause = (UnnestClause) c;
                unnestClauses.add(unnestClause);
            }
        }
        if (correlateClauses.size() != unnestClauses.size() || ft.hasPositionalVariable()) {
            workingDeclarationAnalysis.markAsNonInlineable();
            return null;
        }
        if (!unnestClauses.isEmpty()) {
            unnestClauses.forEach(u -> workingDeclarationAnalysis.addUnnestClause((UnnestClause) u));
        }
        ft.getLeftExpression().accept(this, arg);
        workingDeclarationAnalysis.setFromTermVariable(ft.getLeftVariable());
        return null;
    }

    @Override
    public Expression visit(SelectClause sc, ILangExpression arg) throws CompilationException {
        if (sc.selectElement()) {
            workingDeclarationAnalysis.setSelectElement(sc.getSelectElement().getExpression());

        } else if (sc.selectRegular()) {
            SelectRegular selectRegular = sc.getSelectRegular();
            List<Projection> projectionList = selectRegular.getProjections();
            if (projectionList.stream().anyMatch(p -> p.getKind() != Projection.Kind.NAMED_EXPR)) {
                workingDeclarationAnalysis.markAsNonInlineable();

            } else {
                for (Projection projection : projectionList) {
                    workingDeclarationAnalysis.addProjection(projection);
                    projection.getExpression().accept(this, arg);
                }
            }
        }
        return null;
    }
}
