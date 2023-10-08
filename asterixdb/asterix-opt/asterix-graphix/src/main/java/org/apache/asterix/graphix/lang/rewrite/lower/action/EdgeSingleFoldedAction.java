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
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.AliasLookupTable;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.VertexDescriptor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.VariableExpr;

public class EdgeSingleFoldedAction extends AbstractPatternInlineAction {
    private final VertexPatternExpr vertexPatternExpr;
    private final EdgePatternExpr edgePatternExpr;

    public EdgeSingleFoldedAction(VertexPatternExpr vertexPatternExpr, EdgePatternExpr edgePatternExpr) {
        super(edgePatternExpr.getDeclarationSet().iterator().next());
        this.vertexPatternExpr = vertexPatternExpr;
        this.edgePatternExpr = edgePatternExpr;
    }

    @Override
    protected void preInline(LoweringEnvironment loweringEnvironment) {
        // We want to bind directly to the iteration variable of our vertex, not the join variable.
        VertexDescriptor vertexDescriptor = vertexPatternExpr.getVertexDescriptor();
        AliasLookupTable aliasLookupTable = loweringEnvironment.getAliasLookupTable();
        elementVariable = aliasLookupTable.getIterationAlias(vertexDescriptor.getVariableExpr());
    }

    @Override
    protected void postInline(LoweringEnvironment loweringEnvironment) throws CompilationException {
        EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
        AliasLookupTable aliasLookupTable = loweringEnvironment.getAliasLookupTable();

        // Gather our edge variables.
        VariableExpr edgeVar = edgeDescriptor.getVariableExpr();
        VariableExpr intermediateVar = graphixRewritingContext.getGraphixVariableCopy(edgeVar);

        // If we have a filter expression, add it as a WHERE clause here.
        final Expression filterExpr = edgeDescriptor.getFilterExpr();
        if (filterExpr != null) {
            loweringEnvironment.acceptAction(new FilterExpressionAction(filterExpr, edgeVar, elementVariable));
        }

        if (declarationAnalysis.isSelectClauseInlineable()) {
            // Build a binding for our edge variable.
            loweringEnvironment.acceptTransformer(clauseSequence -> {
                VariableExpr elementVarCopy1 = new VariableExpr(elementVariable.getVar());
                VariableExpr elementVarCopy2 = new VariableExpr(elementVariable.getVar());
                VariableExpr intermediateVarCopy = new VariableExpr(intermediateVar.getVar());
                LetClause nonRepresentativeBinding = new LetClause(intermediateVarCopy, elementVarCopy1);
                clauseSequence.addNonRepresentativeClause(nonRepresentativeBinding);
                clauseSequence.addEdgeBinding(edgeVar, elementVarCopy2);
            });

        } else {
            // Build a record constructor from our context to bind to our edge and intermediate (join) var.
            loweringEnvironment.acceptTransformer(clauseSequence -> {
                VariableExpr intermediateVarCopy = new VariableExpr(intermediateVar.getVar());
                RecordConstructor recordConstructor1 = buildRecordConstructor();
                RecordConstructor recordConstructor2 = buildRecordConstructor();
                LetClause nonRepresentativeBinding = new LetClause(intermediateVarCopy, recordConstructor1);
                clauseSequence.addNonRepresentativeClause(nonRepresentativeBinding);
                clauseSequence.addEdgeBinding(edgeVar, recordConstructor2);
            });
        }
        aliasLookupTable.addIterationAlias(edgeVar, elementVariable);
        aliasLookupTable.addJoinAlias(edgeVar, intermediateVar);
    }
}
