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
package org.apache.asterix.graphix.lang.rewrite.lower.struct;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.lang.rewrite.visitor.DeclarationAnalysisVisitor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.UnnestClause;

/**
 * A data-class used for analyzing the body of a {@link org.apache.asterix.graphix.lang.statement.GraphElementDeclaration}.
 *
 * @see DeclarationAnalysisVisitor
 */
public class DeclarationAnalysis {
    private final List<UnnestClause> unnestClauses = new ArrayList<>();
    private final List<LetClause> letClauses = new ArrayList<>();
    private final List<WhereClause> whereClauses = new ArrayList<>();
    private final List<Projection> projections = new ArrayList<>();

    private VariableExpr fromTermVariable = null;
    private Expression selectElement = null;

    // At a minimum, these fields must be defined.
    private CallExpr datasetCallExpression = null;
    private DataverseName dataverseName = null;
    private String datasetName = null;

    // We take an optimistic approach, and look for cases where we _cannot_ inline our body.
    private boolean isExpressionInlineable = true;

    public void markAsNonInlineable() {
        this.isExpressionInlineable = false;
    }

    public boolean isExpressionInlineable() {
        return isExpressionInlineable;
    }

    public VariableExpr getFromTermVariable() {
        return fromTermVariable;
    }

    public List<UnnestClause> getUnnestClauses() {
        return Collections.unmodifiableList(unnestClauses);
    }

    public List<LetClause> getLetClauses() {
        return Collections.unmodifiableList(letClauses);
    }

    public List<WhereClause> getWhereClauses() {
        return Collections.unmodifiableList(whereClauses);
    }

    public List<Projection> getProjections() {
        return Collections.unmodifiableList(projections);
    }

    public CallExpr getDatasetCall() {
        return datasetCallExpression;
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public boolean isSelectClauseInlineable() {
        if (selectElement == null && projections.isEmpty()) {
            return true;

        } else if (selectElement != null && selectElement.getKind() == Expression.Kind.VARIABLE_EXPRESSION) {
            VariableExpr selectElementVariableExpr = (VariableExpr) selectElement;
            return selectElementVariableExpr.getVar() == fromTermVariable.getVar();
        }
        return false;
    }

    public void setDatasetCall(CallExpr datasetCallExpression, String datasetName) {
        this.datasetCallExpression = datasetCallExpression;
        this.datasetName = datasetName;
    }

    public void setDataverseName(DataverseName dataverseName) {
        this.dataverseName = dataverseName;
    }

    public void setFromTermVariable(VariableExpr fromTermVariable) {
        this.fromTermVariable = fromTermVariable;
    }

    public void setSelectElement(Expression selectElement) {
        this.selectElement = selectElement;
    }

    public void addProjection(Projection projection) {
        projections.add(projection);
    }

    public void addLetClause(LetClause letClause) {
        letClauses.add(letClause);
    }

    public void addWhereClause(WhereClause whereClause) {
        whereClauses.add(whereClause);
    }

    public void addUnnestClause(UnnestClause unnestClause) {
        unnestClauses.add(unnestClause);
    }
}
