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

import static org.apache.asterix.graphix.lang.rewrite.util.LowerRewritingUtil.buildRecordConstructor;

import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.lower.action.base.IEnvironmentAction;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.AliasLookupTable;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.VariableExpr;

public class VertexSingleFoldedAction implements IEnvironmentAction {
    private final VertexPatternExpr vertexPatternExpr;
    private final EdgePatternExpr edgePatternExpr;

    public VertexSingleFoldedAction(VertexPatternExpr vertexPatternExpr, EdgePatternExpr edgePatternExpr) {
        this.edgePatternExpr = Objects.requireNonNull(edgePatternExpr);
        this.vertexPatternExpr = Objects.requireNonNull(vertexPatternExpr);
    }

    @Override
    public void apply(LoweringEnvironment loweringEnvironment) throws CompilationException {
        GraphixRewritingContext rewritingContext = loweringEnvironment.getGraphixRewritingContext();
        AliasLookupTable aliasLookupTable = loweringEnvironment.getAliasLookupTable();

        // Gather our vertex variables.
        VariableExpr vertexVar = vertexPatternExpr.getVertexDescriptor().getVariableExpr();
        VariableExpr intermediateVar = rewritingContext.getGraphixVariableCopy(vertexVar);

        // Build our record constructor.
        RecordConstructor recordConstructor;
        EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
        VariableExpr edgeJoinVar = aliasLookupTable.getJoinAlias(edgeDescriptor.getVariableExpr());
        ElementLabel vertexLabel = vertexPatternExpr.getVertexDescriptor().getLabels().iterator().next();
        if (edgePatternExpr.getLeftVertex().equals(vertexPatternExpr)) {
            recordConstructor = buildRecordConstructor(vertexPatternExpr.getKeyFieldNames(vertexLabel),
                    edgePatternExpr.getLeftKeyFieldNames(vertexLabel), edgeJoinVar);
        } else {
            recordConstructor = buildRecordConstructor(vertexPatternExpr.getKeyFieldNames(vertexLabel),
                    edgePatternExpr.getRightKeyFieldNames(vertexLabel), edgeJoinVar);
        }

        // Bind our intermediate (join) variable and vertex variable.
        loweringEnvironment.acceptTransformer(clauseSequence -> {
            VariableExpr intermediateVarCopy1 = new VariableExpr(intermediateVar.getVar());
            VariableExpr intermediateVarCopy2 = new VariableExpr(intermediateVar.getVar());
            LetClause nonRepresentativeBinding = new LetClause(intermediateVarCopy1, recordConstructor);
            clauseSequence.addNonRepresentativeClause(nonRepresentativeBinding);
            clauseSequence.setVertexBinding(vertexVar, intermediateVarCopy2);
        });
        aliasLookupTable.addIterationAlias(vertexVar, intermediateVar);
        aliasLookupTable.addJoinAlias(vertexVar, intermediateVar);

        // If we have a filter expression, add it as a WHERE clause here. This should only include the primary key.
        final Expression filterExpr = vertexPatternExpr.getVertexDescriptor().getFilterExpr();
        if (filterExpr != null) {
            loweringEnvironment.acceptAction(new FilterExpressionAction(filterExpr, vertexVar, intermediateVar));
        }
    }
}
