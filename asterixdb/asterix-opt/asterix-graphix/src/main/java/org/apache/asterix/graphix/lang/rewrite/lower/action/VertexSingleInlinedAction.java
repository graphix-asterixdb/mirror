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
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.AliasLookupTable;
import org.apache.asterix.graphix.lang.struct.VertexDescriptor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.TrueLiteral;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.optype.JoinType;

public class VertexSingleInlinedAction extends AbstractPatternInlineAction {
    private final VertexPatternExpr vertexPatternExpr;

    public VertexSingleInlinedAction(VertexPatternExpr vertexPatternExpr) {
        super(vertexPatternExpr.getDeclarationSet().iterator().next());
        this.vertexPatternExpr = vertexPatternExpr;
    }

    @Override
    protected void preInline(LoweringEnvironment loweringEnvironment) throws CompilationException {
        VertexDescriptor vertexDescriptor = vertexPatternExpr.getVertexDescriptor();
        elementVariable = graphixRewritingContext.getGraphixVariableCopy(vertexDescriptor.getVariableExpr());

        // Introduce our iteration expression.
        loweringEnvironment.acceptTransformer(clauseSequence -> {
            CallExpr datasetCallExpression = declarationAnalysis.getDatasetCall();
            VariableExpr elementVarCopy = new VariableExpr(elementVariable.getVar());
            JoinClause joinClause = new JoinClause(JoinType.INNER, datasetCallExpression, elementVarCopy, null,
                    new LiteralExpr(TrueLiteral.INSTANCE), null);
            joinClause.setSourceLocation(vertexPatternExpr.getSourceLocation());
            clauseSequence.addNonRepresentativeClause(joinClause);
        });
    }

    @Override
    protected void postInline(LoweringEnvironment loweringEnvironment) throws CompilationException {
        VertexDescriptor vertexDescriptor = vertexPatternExpr.getVertexDescriptor();
        AliasLookupTable aliasLookupTable = loweringEnvironment.getAliasLookupTable();

        // Gather our vertex-variables.
        VariableExpr vertexVar = vertexDescriptor.getVariableExpr();
        VariableExpr intermediateVar = graphixRewritingContext.getGraphixVariableCopy(vertexVar);

        // If we have a filter expression, add it as a WHERE clause here.
        final Expression filterExpr = vertexDescriptor.getFilterExpr();
        if (filterExpr != null) {
            loweringEnvironment.acceptAction(new FilterExpressionAction(filterExpr, vertexVar, elementVariable));
        }

        // Bind our intermediate (join) variable and vertex variable.
        if (declarationAnalysis.isSelectClauseInlineable()) {
            loweringEnvironment.acceptTransformer(clauseSequence -> {
                VariableExpr elementVarCopy1 = new VariableExpr(elementVariable.getVar());
                VariableExpr elementVarCopy2 = new VariableExpr(elementVariable.getVar());
                VariableExpr intermediateVarCopy = new VariableExpr(intermediateVar.getVar());
                LetClause nonRepresentativeBinding = new LetClause(intermediateVarCopy, elementVarCopy1);
                clauseSequence.addNonRepresentativeClause(nonRepresentativeBinding);
                clauseSequence.setVertexBinding(vertexVar, elementVarCopy2);
            });

        } else {
            // Build a record constructor from our context to bind to our vertex variable.
            loweringEnvironment.acceptTransformer(clauseSequence -> {
                VariableExpr intermediateVarCopy = new VariableExpr(intermediateVar.getVar());
                RecordConstructor recordConstructor1 = buildRecordConstructor();
                RecordConstructor recordConstructor2 = buildRecordConstructor();
                LetClause nonRepresentativeBinding = new LetClause(intermediateVarCopy, recordConstructor1);
                clauseSequence.addNonRepresentativeClause(nonRepresentativeBinding);
                clauseSequence.setVertexBinding(vertexVar, recordConstructor2);
            });
        }
        aliasLookupTable.addIterationAlias(vertexVar, elementVariable);
        aliasLookupTable.addJoinAlias(vertexVar, intermediateVar);
    }
}
