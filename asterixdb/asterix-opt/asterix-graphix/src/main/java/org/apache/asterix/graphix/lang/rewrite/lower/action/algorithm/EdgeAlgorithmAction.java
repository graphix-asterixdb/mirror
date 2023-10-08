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
package org.apache.asterix.graphix.lang.rewrite.lower.action.algorithm;

import static org.apache.asterix.graphix.lang.rewrite.util.LowerRewritingUtil.buildSingleAccessorList;

import java.util.List;
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.algebra.compiler.option.EvaluationMinimizeJoinsOption;
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.ElementDirection;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.hint.LeftLinkJoinAnnotation;
import org.apache.asterix.graphix.lang.hint.RightLinkJoinAnnotation;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.lower.action.EdgeSingleFoldedAction;
import org.apache.asterix.graphix.lang.rewrite.lower.action.EdgeSingleInlinedAction;
import org.apache.asterix.graphix.lang.rewrite.lower.action.EdgeSingleNestedAction;
import org.apache.asterix.graphix.lang.rewrite.lower.action.EdgeUndirectedAction;
import org.apache.asterix.graphix.lang.rewrite.lower.action.correlate.CorrelationEdgeAction;
import org.apache.asterix.graphix.lang.rewrite.lower.action.correlate.CorrelationGeneralAction;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.DeclarationAnalysis;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.graphix.lang.struct.VertexDescriptor;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;

public class EdgeAlgorithmAction extends AbstractAlgorithmAction {
    private final EdgePatternExpr edgePatternExpr;
    private final VertexPatternExpr leftVertexExpr;
    private final VertexPatternExpr rightVertexExpr;
    private final EdgeDescriptor edgeDescriptor;
    private final VertexDescriptor leftDescriptor;
    private final VertexDescriptor rightDescriptor;
    private List<List<String>> leftEdgeKeyFields;
    private List<List<String>> rightEdgeKeyFields;
    private List<List<String>> leftVertexFields;
    private List<List<String>> rightVertexFields;

    // We compute the following to determine our lowering strategy.
    protected boolean isEdgeUndirected;
    protected boolean isEdgeInlineable;
    protected boolean areLeftPropertiesUsed;
    protected boolean areRightPropertiesUsed;
    protected boolean isLeftPartiallyLowered;
    protected boolean isRightPartiallyLowered;
    protected boolean isEdgeLeftFoldable;
    protected boolean isEdgeRightFoldable;
    protected boolean isLeftVertexCaptured;
    protected boolean isRightVertexCaptured;
    protected boolean isRequired;

    public EdgeAlgorithmAction(EdgePatternExpr epe, Callback loweringCallback) {
        super(loweringCallback);
        this.edgePatternExpr = Objects.requireNonNull(epe);
        this.leftVertexExpr = epe.getLeftVertex();
        this.rightVertexExpr = epe.getRightVertex();
        this.edgeDescriptor = epe.getEdgeDescriptor();
        this.leftDescriptor = leftVertexExpr.getVertexDescriptor();
        this.rightDescriptor = rightVertexExpr.getVertexDescriptor();
    }

    @Override
    protected void throwIfNotSupported() throws CompilationException {
        if (leftVertexExpr.getDeclarationSet().size() > 1 || rightVertexExpr.getDeclarationSet().size() > 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, edgePatternExpr.getSourceLocation(),
                    "Multi-label vertex expressions are currently not supported!");
        }
        if (edgePatternExpr.getDeclarationSet().size() > 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, edgePatternExpr.getSourceLocation(),
                    "Multi-label edge expressions are currently not supported!");
        }
    }

    @Override
    protected void gatherPreliminaries(LoweringEnvironment environment) throws CompilationException {
        isEdgeUndirected = edgeDescriptor.getElementDirection() == ElementDirection.UNDIRECTED;
        if (isEdgeUndirected) {
            // We can short-circuit here if we have an undirected edge...
            return;
        }

        // ...otherwise, we will continue. Gather our analysis...
        DeclarationAnalysis leftAnalysis = leftVertexExpr.getDeclarationSet().iterator().next().getAnalysis();
        DeclarationAnalysis rightAnalysis = rightVertexExpr.getDeclarationSet().iterator().next().getAnalysis();
        DeclarationAnalysis edgeAnalysis = edgePatternExpr.getDeclarationSet().iterator().next().getAnalysis();

        // ...and our key fields.
        ElementLabel leftVertexLabel = leftDescriptor.getLabels().iterator().next();
        ElementLabel rightVertexLabel = rightDescriptor.getLabels().iterator().next();
        leftEdgeKeyFields = edgePatternExpr.getLeftKeyFieldNames(leftVertexLabel);
        rightEdgeKeyFields = edgePatternExpr.getRightKeyFieldNames(rightVertexLabel);
        leftVertexFields = leftVertexExpr.getKeyFieldNames(leftVertexLabel);
        rightVertexFields = rightVertexExpr.getKeyFieldNames(rightVertexLabel);

        // We compute the following to determine our lowering strategy.
        boolean doesLeftDVMatch = Objects.equals(leftAnalysis.getDataverseName(), edgeAnalysis.getDataverseName());
        boolean doesRightDVMatch = Objects.equals(rightAnalysis.getDataverseName(), edgeAnalysis.getDataverseName());
        boolean doesLeftNameMatch = Objects.equals(leftAnalysis.getDatasetName(), edgeAnalysis.getDatasetName());
        boolean doesRightNameMatch = Objects.equals(rightAnalysis.getDatasetName(), edgeAnalysis.getDatasetName());
        boolean areLeftFieldsEqual = Objects.equals(leftVertexFields, leftEdgeKeyFields);
        boolean areRightFieldsEqual = Objects.equals(rightVertexFields, rightEdgeKeyFields);
        boolean isCapturedByPath = edgePatternExpr.getParentPathExpr() != null;
        isEdgeInlineable = edgeAnalysis.isExpressionInlineable();
        areLeftPropertiesUsed = edgePatternExpr.getLeftVertex().arePropertiesUsed();
        areRightPropertiesUsed = edgePatternExpr.getRightVertex().arePropertiesUsed();
        isLeftPartiallyLowered = environment.isElementPartiallyLowered(leftDescriptor.getVariableExpr());
        isRightPartiallyLowered = environment.isElementPartiallyLowered(rightDescriptor.getVariableExpr());
        isEdgeLeftFoldable = isEdgeInlineable && doesLeftDVMatch && doesLeftNameMatch && areLeftFieldsEqual;
        isEdgeRightFoldable = isEdgeInlineable && doesRightDVMatch && doesRightNameMatch && areRightFieldsEqual;
        isLeftVertexCaptured = !areLeftPropertiesUsed && !isCapturedByPath;
        isRightVertexCaptured = !areRightPropertiesUsed && !isCapturedByPath;
        isRequired = graphixRewritingContext.getSetting(EvaluationMinimizeJoinsOption.OPTION_KEY_NAME)
                .equals(EvaluationMinimizeJoinsOption.FALSE);
    }

    @Override
    protected void decideAndApply(LoweringEnvironment environment) throws CompilationException {
        if (isEdgeUndirected) {
            // Case #0: We have an undirected edge. We need to handle this separately.
            environment.acceptAction(new EdgeUndirectedAction(loweringCallback, edgePatternExpr));
            return;

        } else if (isEdgeLeftFoldable && !isRequired) {
            // Case #1: We can fold our edge into our left vertex.
            VariableExpr previousJoinAlias = null;
            if (isLeftPartiallyLowered) {
                previousJoinAlias = aliasLookupTable.removeJoinAlias(leftDescriptor.getVariableExpr());
                aliasLookupTable.removeIterationAlias(leftDescriptor.getVariableExpr());
            }
            loweringCallback.invoke(leftVertexExpr, null);
            if (isLeftPartiallyLowered) {
                VariableExpr currentJoinAlias = aliasLookupTable.getJoinAlias(leftDescriptor.getVariableExpr());
                List<FieldAccessor> previousAccessors = buildSingleAccessorList(previousJoinAlias, leftVertexFields);
                List<FieldAccessor> currentAccessors = buildSingleAccessorList(currentJoinAlias, leftVertexFields);
                LeftLinkJoinAnnotation leftAnnotation = edgePatternExpr.findHint(LeftLinkJoinAnnotation.class);
                List<IExpressionAnnotation> joinHints =
                        leftAnnotation != null ? List.of(leftAnnotation.getJoinAnnotation()) : List.of();
                environment.acceptAction(new CorrelationGeneralAction(previousAccessors, currentAccessors, joinHints));
            }
            environment.acceptAction(new EdgeSingleFoldedAction(leftVertexExpr, edgePatternExpr));
            environment.setElementPartiallyLowered(edgeDescriptor.getVariableExpr(), true);
            loweringCallback.invoke(rightVertexExpr, isRightVertexCaptured ? edgePatternExpr : null);

        } else if (isEdgeRightFoldable && !isRequired) {
            // Case #2: We can fold our edge into our right vertex.
            VariableExpr previousJoinAlias = null;
            if (isRightPartiallyLowered) {
                previousJoinAlias = aliasLookupTable.removeJoinAlias(rightDescriptor.getVariableExpr());
                aliasLookupTable.removeIterationAlias(rightDescriptor.getVariableExpr());
            }
            loweringCallback.invoke(rightVertexExpr, null);
            if (isRightPartiallyLowered) {
                VariableExpr currentJoinAlias = aliasLookupTable.getJoinAlias(rightDescriptor.getVariableExpr());
                List<FieldAccessor> previousAccessors = buildSingleAccessorList(previousJoinAlias, rightVertexFields);
                List<FieldAccessor> currentAccessors = buildSingleAccessorList(currentJoinAlias, rightVertexFields);
                RightLinkJoinAnnotation rightAnnotation = edgePatternExpr.findHint(RightLinkJoinAnnotation.class);
                List<IExpressionAnnotation> joinHints =
                        rightAnnotation != null ? List.of(rightAnnotation.getJoinAnnotation()) : List.of();
                environment.acceptAction(new CorrelationGeneralAction(previousAccessors, currentAccessors, joinHints));
            }
            environment.acceptAction(new EdgeSingleFoldedAction(rightVertexExpr, edgePatternExpr));
            environment.setElementPartiallyLowered(edgeDescriptor.getVariableExpr(), true);
            loweringCallback.invoke(leftVertexExpr, isLeftVertexCaptured ? edgePatternExpr : null);

        } else {
            // Case #3: We might be able to fold our vert(ices) into our edge.
            if (isEdgeInlineable) {
                environment.acceptAction(new EdgeSingleInlinedAction(edgePatternExpr));
            } else {
                environment.acceptAction(new EdgeSingleNestedAction(edgePatternExpr));
            }
            loweringCallback.invoke(leftVertexExpr, !areLeftPropertiesUsed ? edgePatternExpr : null);
            loweringCallback.invoke(rightVertexExpr, !areRightPropertiesUsed ? edgePatternExpr : null);
        }

        // After our lowering, correlate all of our elements.
        CorrelationEdgeAction correlationAction = new CorrelationEdgeAction(edgePatternExpr, aliasLookupTable);
        correlationAction.setLeftFieldInformation(leftVertexFields, leftEdgeKeyFields);
        correlationAction.setRightFieldInformation(rightVertexFields, rightEdgeKeyFields);
        environment.acceptAction(correlationAction);
    }
}
