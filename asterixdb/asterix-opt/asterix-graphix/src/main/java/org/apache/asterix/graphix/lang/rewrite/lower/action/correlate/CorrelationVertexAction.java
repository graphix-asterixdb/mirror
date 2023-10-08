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
package org.apache.asterix.graphix.lang.rewrite.lower.action.correlate;

import static org.apache.asterix.graphix.lang.rewrite.util.LowerRewritingUtil.buildSingleAccessorList;

import java.util.List;
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.hint.CorrelationJoinAnnotation;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.lower.action.base.IEnvironmentAction;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.AliasLookupTable;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.graphix.lang.struct.VertexDescriptor;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;

public class CorrelationVertexAction implements IEnvironmentAction {
    private final VertexPatternExpr vertexPatternExpr;
    private final VertexDescriptor vertexDescriptor;

    public CorrelationVertexAction(VertexPatternExpr vertexPatternExpr) {
        this.vertexPatternExpr = Objects.requireNonNull(vertexPatternExpr);
        this.vertexDescriptor = vertexPatternExpr.getVertexDescriptor();
    }

    @Override
    public void apply(LoweringEnvironment environment) throws CompilationException {
        AliasLookupTable aliasLookupTable = environment.getAliasLookupTable();
        VariableExpr parentExpr = vertexPatternExpr.getParentVertexExpr().getVertexDescriptor().getVariableExpr();
        VariableExpr currentExpr = aliasLookupTable.getJoinAlias(vertexDescriptor.getVariableExpr());
        ElementLabel vertexLabel = vertexPatternExpr.getVertexDescriptor().getLabels().iterator().next();
        List<List<String>> vertexKeyFields = vertexPatternExpr.getKeyFieldNames(vertexLabel);

        // We will join using our primary keys and the join alias of our parent.
        List<FieldAccessor> parentAccessors = buildSingleAccessorList(parentExpr, vertexKeyFields);
        List<FieldAccessor> vertexAccessors = buildSingleAccessorList(currentExpr, vertexKeyFields);
        CorrelationJoinAnnotation correlationHint = vertexPatternExpr.findHint(CorrelationJoinAnnotation.class);
        if (correlationHint != null) {
            List<IExpressionAnnotation> joinHints = List.of(correlationHint.getJoinAnnotation());
            environment.acceptAction(new CorrelationGeneralAction(parentAccessors, vertexAccessors, joinHints));
            return;
        }
        environment.acceptAction(new CorrelationGeneralAction(parentAccessors, vertexAccessors));
    }
}
