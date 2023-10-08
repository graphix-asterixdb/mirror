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
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.hint.LeftLinkJoinAnnotation;
import org.apache.asterix.graphix.lang.hint.RightLinkJoinAnnotation;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.lower.action.base.IEnvironmentAction;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.AliasLookupTable;
import org.apache.asterix.graphix.lang.struct.VertexDescriptor;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;

public class CorrelationEdgeAction implements IEnvironmentAction {
    private final VariableExpr leftVertexVariable;
    private final VariableExpr rightVertexVariable;
    private final VariableExpr leftEdgeVariable;
    private final VariableExpr rightEdgeVariable;
    private final EdgePatternExpr edgePatternExpr;

    // The following must be passed in after instantiation.
    private List<List<String>> leftVertexFields;
    private List<List<String>> rightVertexFields;
    private List<List<String>> leftEdgeKeyFields;
    private List<List<String>> rightEdgeKeyFields;

    public CorrelationEdgeAction(EdgePatternExpr edgePatternExpr, AliasLookupTable aliasLookupTable)
            throws CompilationException {
        this.edgePatternExpr = Objects.requireNonNull(edgePatternExpr);
        VertexDescriptor leftDescriptor = edgePatternExpr.getLeftVertex().getVertexDescriptor();
        VertexDescriptor rightDescriptor = edgePatternExpr.getRightVertex().getVertexDescriptor();
        this.leftVertexVariable = aliasLookupTable.getJoinAlias(leftDescriptor.getVariableExpr());
        this.rightVertexVariable = aliasLookupTable.getJoinAlias(rightDescriptor.getVariableExpr());
        this.leftEdgeVariable = aliasLookupTable.getJoinAlias(edgePatternExpr.getEdgeDescriptor().getVariableExpr());
        this.rightEdgeVariable = aliasLookupTable.getJoinAlias(edgePatternExpr.getEdgeDescriptor().getVariableExpr());
    }

    public void setLeftFieldInformation(List<List<String>> leftVertexFields, List<List<String>> leftEdgeKeyFields) {
        this.leftVertexFields = leftVertexFields;
        this.leftEdgeKeyFields = leftEdgeKeyFields;
    }

    public void setRightFieldInformation(List<List<String>> rightVertexFields, List<List<String>> rightEdgeKeyFields) {
        this.rightVertexFields = rightVertexFields;
        this.rightEdgeKeyFields = rightEdgeKeyFields;
    }

    @Override
    public void apply(LoweringEnvironment environment) throws CompilationException {
        List<FieldAccessor> leftVertex = buildSingleAccessorList(leftVertexVariable, leftVertexFields);
        List<FieldAccessor> leftLink = buildSingleAccessorList(leftEdgeVariable, leftEdgeKeyFields);
        List<FieldAccessor> rightVertex = buildSingleAccessorList(rightVertexVariable, rightVertexFields);
        List<FieldAccessor> rightLink = buildSingleAccessorList(rightEdgeVariable, rightEdgeKeyFields);
        if (edgePatternExpr.hasHints()) {
            LeftLinkJoinAnnotation leftHint = edgePatternExpr.findHint(LeftLinkJoinAnnotation.class);
            RightLinkJoinAnnotation rightHint = edgePatternExpr.findHint(RightLinkJoinAnnotation.class);
            if (leftHint != null && rightHint != null) {
                List<IExpressionAnnotation> leftJoinHints = List.of(leftHint.getJoinAnnotation());
                List<IExpressionAnnotation> rightJoinHints = List.of(rightHint.getJoinAnnotation());
                environment.acceptAction(new CorrelationGeneralAction(leftVertex, leftLink, leftJoinHints));
                environment.acceptAction(new CorrelationGeneralAction(rightLink, rightVertex, rightJoinHints));
                return;

            } else if (leftHint != null) {
                List<IExpressionAnnotation> leftJoinHints = List.of(leftHint.getJoinAnnotation());
                environment.acceptAction(new CorrelationGeneralAction(leftVertex, leftLink, leftJoinHints));
                environment.acceptAction(new CorrelationGeneralAction(rightLink, rightVertex));
                return;

            } else if (rightHint != null) {
                List<IExpressionAnnotation> rightJoinHints = List.of(rightHint.getJoinAnnotation());
                environment.acceptAction(new CorrelationGeneralAction(leftVertex, leftLink));
                environment.acceptAction(new CorrelationGeneralAction(rightLink, rightVertex, rightJoinHints));
                return;
            }
        }
        environment.acceptAction(new CorrelationGeneralAction(leftVertex, leftLink));
        environment.acceptAction(new CorrelationGeneralAction(rightLink, rightVertex));
    }
}
