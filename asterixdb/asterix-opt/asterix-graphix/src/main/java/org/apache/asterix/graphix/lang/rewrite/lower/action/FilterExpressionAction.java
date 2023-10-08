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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.lower.action.base.IEnvironmentAction;
import org.apache.asterix.graphix.lang.rewrite.visitor.common.VariableRemapCloneVisitor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.VariableExpr;

/**
 * Introduce a generic {@link WhereClause} into a {@link LoweringEnvironment}.
 */
public class FilterExpressionAction implements IEnvironmentAction {
    private final VariableExpr elementVariable;
    private final VariableExpr iterationVariable;
    private final Expression filterExpr;

    public FilterExpressionAction(Expression filterExpr, VariableExpr elementVariable, VariableExpr iterationVariable) {
        this.filterExpr = filterExpr;
        this.elementVariable = elementVariable;
        this.iterationVariable = iterationVariable;
    }

    @Override
    public void apply(LoweringEnvironment loweringEnvironment) throws CompilationException {
        GraphixRewritingContext graphixRewritingContext = loweringEnvironment.getGraphixRewritingContext();
        VariableRemapCloneVisitor remapCloneVisitor = new VariableRemapCloneVisitor(graphixRewritingContext);

        // Introduce a new WHERE-CLAUSE.
        VariableExpr iterationVarCopy = new VariableExpr(iterationVariable.getVar());
        VariableExpr elementVarCopy = new VariableExpr(elementVariable.getVar());
        iterationVarCopy.setSourceLocation(filterExpr.getSourceLocation());
        remapCloneVisitor.addSubstitution(elementVarCopy, iterationVarCopy);
        loweringEnvironment.acceptTransformer(clauseSequence -> {
            ILangExpression remapCopyFilterExpr = remapCloneVisitor.substitute(filterExpr);
            WhereClause filterWhereClause = new WhereClause((Expression) remapCopyFilterExpr);
            filterWhereClause.setSourceLocation(filterExpr.getSourceLocation());
            clauseSequence.addNonRepresentativeClause(filterWhereClause);
            remapCloneVisitor.resetSubstitutions();
        });
    }
}
