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

import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.lower.action.base.IEnvironmentAction;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.AliasLookupTable;
import org.apache.asterix.graphix.lang.statement.GraphElementDeclaration;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.TrueLiteral;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.optype.JoinType;
import org.apache.asterix.lang.sqlpp.util.SqlppRewriteUtil;

public class EdgeSingleNestedAction implements IEnvironmentAction {
    private final EdgePatternExpr edgePatternExpr;

    public EdgeSingleNestedAction(EdgePatternExpr edgePatternExpr) {
        this.edgePatternExpr = Objects.requireNonNull(edgePatternExpr);
    }

    @Override
    public void apply(LoweringEnvironment loweringEnvironment) throws CompilationException {
        GraphixRewritingContext graphixRewritingContext = loweringEnvironment.getGraphixRewritingContext();
        AliasLookupTable aliasLookupTable = loweringEnvironment.getAliasLookupTable();
        EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();

        // Gather our edge variables.
        VariableExpr edgeVar = edgeDescriptor.getVariableExpr();
        VariableExpr iterationVar = graphixRewritingContext.getGraphixVariableCopy(edgeVar);
        VariableExpr intermediateVar = graphixRewritingContext.getGraphixVariableCopy(edgeVar);

        // Introduce our iteration expression.
        GraphElementDeclaration declaration = edgePatternExpr.getDeclarationSet().iterator().next();
        loweringEnvironment.acceptTransformer(clauseSequence -> {
            Expression declBody = Objects.requireNonNullElse(declaration.getNormalizedBody(), declaration.getRawBody());
            AbstractExpression declBodyCopy = (AbstractExpression) SqlppRewriteUtil.deepCopy(declBody);
            VariableExpr iterationVarCopy = new VariableExpr(iterationVar.getVar());
            JoinClause joinClause = new JoinClause(JoinType.INNER, declBodyCopy, iterationVarCopy, null,
                    new LiteralExpr(TrueLiteral.INSTANCE), null);
            joinClause.setSourceLocation(edgePatternExpr.getSourceLocation());
            clauseSequence.addNonRepresentativeClause(joinClause);
        });

        // If we have a filter expression, add it as a WHERE clause here.
        final Expression filterExpr = edgeDescriptor.getFilterExpr();
        if (filterExpr != null) {
            loweringEnvironment.acceptAction(new FilterExpressionAction(filterExpr, edgeVar, iterationVar));
        }

        // Bind our intermediate (join) variable and edge variable.
        loweringEnvironment.acceptTransformer(clauseSequence -> {
            VariableExpr iterationVarCopy1 = new VariableExpr(iterationVar.getVar());
            VariableExpr iterationVarCopy2 = new VariableExpr(iterationVar.getVar());
            VariableExpr intermediateVarCopy = new VariableExpr(intermediateVar.getVar());
            LetClause nonRepresentativeBinding = new LetClause(intermediateVarCopy, iterationVarCopy1);
            clauseSequence.addNonRepresentativeClause(nonRepresentativeBinding);
            clauseSequence.addEdgeBinding(edgeVar, iterationVarCopy2);
        });
        aliasLookupTable.addIterationAlias(edgeVar, iterationVar);
        aliasLookupTable.addJoinAlias(edgeVar, intermediateVar);
    }
}
