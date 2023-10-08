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
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.lower.action.base.IEnvironmentAction;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.AliasLookupTable;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.VariableExpr;

public class VertexSingleRebindAction implements IEnvironmentAction {
    private final VertexPatternExpr vertexPatternExpr;
    private final VariableExpr rebindingVariable;

    public VertexSingleRebindAction(VertexPatternExpr vertexPatternExpr, VariableExpr rebindingVariable) {
        this.vertexPatternExpr = Objects.requireNonNull(vertexPatternExpr);
        this.rebindingVariable = Objects.requireNonNull(rebindingVariable);
    }

    @Override
    public void apply(LoweringEnvironment loweringEnvironment) throws CompilationException {
        VariableExpr vertexVariable = vertexPatternExpr.getVertexDescriptor().getVariableExpr();

        // Rebind our vertex variable here.
        AliasLookupTable aliasLookupTable = loweringEnvironment.getAliasLookupTable();
        loweringEnvironment.acceptTransformer(clauseSequence -> {
            aliasLookupTable.addIterationAlias(vertexVariable, rebindingVariable);
            aliasLookupTable.addJoinAlias(vertexVariable, rebindingVariable);
            clauseSequence.setVertexBinding(vertexVariable, rebindingVariable);
        });

        // If we have a filter expression, add it as a WHERE clause here. This should only include the primary key.
        final Expression filterExpr = vertexPatternExpr.getVertexDescriptor().getFilterExpr();
        if (filterExpr != null) {
            loweringEnvironment.acceptAction(new FilterExpressionAction(filterExpr, vertexVariable, rebindingVariable));
        }
    }
}
