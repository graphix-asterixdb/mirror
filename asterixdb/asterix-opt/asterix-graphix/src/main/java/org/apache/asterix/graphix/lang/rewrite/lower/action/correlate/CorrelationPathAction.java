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
import org.apache.asterix.graphix.lang.expression.pattern.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.hint.LeftLinkJoinAnnotation;
import org.apache.asterix.graphix.lang.hint.RightLinkJoinAnnotation;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.lower.action.base.IEnvironmentAction;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.AliasLookupTable;
import org.apache.asterix.graphix.lang.struct.VertexDescriptor;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;

public class CorrelationPathAction implements IEnvironmentAction {
    private final VariableExpr vertexVariable;
    private final PathPatternExpr pathPatternExpr;
    private final boolean isEvaluatingLeftToRight;

    // The following must be passed in after instantiation.
    private VariableExpr pathVariable;
    private List<List<String>> keyFields;

    public CorrelationPathAction(VertexPatternExpr vertexPatternExpr, PathPatternExpr pathPatternExpr,
            AliasLookupTable aliasLookupTable) throws CompilationException {
        VertexDescriptor vertexDescriptor = vertexPatternExpr.getVertexDescriptor();
        this.vertexVariable = aliasLookupTable.getJoinAlias(vertexDescriptor.getVariableExpr());
        this.pathPatternExpr = Objects.requireNonNull(pathPatternExpr);
        if (pathPatternExpr.getRightVertex().equals(vertexPatternExpr)) {
            isEvaluatingLeftToRight = true;

        } else if (pathPatternExpr.getLeftVertex().equals(vertexPatternExpr)) {
            isEvaluatingLeftToRight = false;

        } else {
            throw new IllegalArgumentException("Given vertex should be an endpoint of the given path!");
        }
    }

    public void setFieldInformation(List<List<String>> keyFields, VariableExpr pathVariable) {
        this.keyFields = keyFields;
        this.pathVariable = pathVariable;
    }

    @Override
    public void apply(LoweringEnvironment environment) throws CompilationException {
        List<FieldAccessor> vertexAccess = buildSingleAccessorList(vertexVariable, keyFields);
        List<FieldAccessor> fromPathAccess = buildSingleAccessorList(pathVariable, keyFields);
        if (pathPatternExpr.hasHints()) {
            LeftLinkJoinAnnotation leftHint = pathPatternExpr.findHint(LeftLinkJoinAnnotation.class);
            RightLinkJoinAnnotation rightHint = pathPatternExpr.findHint(RightLinkJoinAnnotation.class);
            if (rightHint != null && isEvaluatingLeftToRight) {
                List<IExpressionAnnotation> rightJoinHints = List.of(rightHint.getJoinAnnotation());
                environment.acceptAction(new CorrelationGeneralAction(vertexAccess, fromPathAccess, rightJoinHints));
                return;

            } else if (leftHint != null && !isEvaluatingLeftToRight) {
                List<IExpressionAnnotation> leftJoinHints = List.of(leftHint.getJoinAnnotation());
                environment.acceptAction(new CorrelationGeneralAction(vertexAccess, fromPathAccess, leftJoinHints));
                return;
            }
        }
        environment.acceptAction(new CorrelationGeneralAction(vertexAccess, fromPathAccess));
    }
}
