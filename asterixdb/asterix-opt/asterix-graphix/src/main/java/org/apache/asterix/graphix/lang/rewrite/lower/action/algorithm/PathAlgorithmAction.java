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

import java.util.List;
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.lang.expression.pattern.ElementDirection;
import org.apache.asterix.graphix.lang.expression.pattern.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.lower.action.VertexSingleFoldedAction;
import org.apache.asterix.graphix.lang.rewrite.lower.action.correlate.CorrelationPathAction;
import org.apache.asterix.graphix.lang.rewrite.lower.action.finalize.FinalizePathPatternAction;
import org.apache.asterix.graphix.lang.rewrite.lower.action.recursive.AnchorCorrectionAction;
import org.apache.asterix.graphix.lang.rewrite.lower.action.recursive.AnchorSequenceAction;
import org.apache.asterix.graphix.lang.rewrite.lower.action.recursive.RecursiveSequenceAction;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.graphix.lang.struct.PathDescriptor;
import org.apache.asterix.graphix.lang.struct.VertexDescriptor;
import org.apache.asterix.lang.common.expression.VariableExpr;

public class PathAlgorithmAction extends AbstractAlgorithmAction {
    private final PathPatternExpr pathPatternExpr;
    private final VertexPatternExpr leftVertexExpr;
    private final VertexPatternExpr rightVertexExpr;
    private final PathDescriptor pathDescriptor;
    private final VertexDescriptor leftDescriptor;
    private final VertexDescriptor rightDescriptor;
    private List<List<String>> leftVertexFields;
    private List<List<String>> rightVertexFields;

    // We compute the following to determine our lowering strategy.
    private boolean isEvaluatingLeftToRight;
    private boolean areLeftPropertiesUsed;
    private boolean areRightPropertiesUsed;

    public PathAlgorithmAction(PathPatternExpr ppe, Callback loweringCallback) {
        super(loweringCallback);
        this.pathPatternExpr = Objects.requireNonNull(ppe);
        this.leftVertexExpr = ppe.getLeftVertex();
        this.rightVertexExpr = ppe.getRightVertex();
        this.pathDescriptor = pathPatternExpr.getPathDescriptor();
        this.leftDescriptor = leftVertexExpr.getVertexDescriptor();
        this.rightDescriptor = rightVertexExpr.getVertexDescriptor();
    }

    @Override
    protected void throwIfNotSupported() throws CompilationException {
        if (pathDescriptor.getElementDirection() == ElementDirection.UNDIRECTED) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, pathPatternExpr.getSourceLocation(),
                    "Undirected paths are currently not supported!");
        }
        if (leftVertexExpr.getDeclarationSet().size() > 1 || rightVertexExpr.getDeclarationSet().size() > 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, pathPatternExpr.getSourceLocation(),
                    "Multi-label vertex expressions are currently not supported!");
        }
        if (pathPatternExpr.getDecompositions().size() > 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, pathPatternExpr.getSourceLocation(),
                    "Complex RPQ (|N| > 1, |E| > 1) expressions are currently not supported!");
        }
    }

    @Override
    protected void gatherPreliminaries(LoweringEnvironment environment) {
        ElementLabel leftVertexLabel = leftDescriptor.getLabels().iterator().next();
        ElementLabel rightVertexLabel = rightDescriptor.getLabels().iterator().next();
        leftVertexFields = leftVertexExpr.getKeyFieldNames(leftVertexLabel);
        rightVertexFields = rightVertexExpr.getKeyFieldNames(rightVertexLabel);

        // We compute the following to determine our lowering strategy.
        isEvaluatingLeftToRight = !environment.isElementIntroduced(rightDescriptor.getVariableExpr());
        areLeftPropertiesUsed = leftVertexExpr.arePropertiesUsed();
        areRightPropertiesUsed = rightVertexExpr.arePropertiesUsed();
    }

    @Override
    protected void decideAndApply(LoweringEnvironment environment) throws CompilationException {
        // Introduce our source vertex, then apply our ANCHOR-action.
        loweringCallback.invoke((isEvaluatingLeftToRight ? leftVertexExpr : rightVertexExpr), null);
        AnchorSequenceAction anchorAction = new AnchorSequenceAction(pathPatternExpr);
        environment.acceptAction(anchorAction);

        // Apply our RECURSIVE-action. We need to keep track of how many VertexFoldedActions are applied.
        RecursiveSequenceAction recursiveAction = new RecursiveSequenceAction(loweringCallback, pathPatternExpr);
        long numberOfFoldedVertexActionsBefore = environment.getAppliedActions().stream().map(Class::getSimpleName)
                .filter(c -> c.equals(VertexSingleFoldedAction.class.getSimpleName())).count();
        environment.acceptAction(recursiveAction);
        long numberOfFoldedVertexActionsAfter = environment.getAppliedActions().stream().map(Class::getSimpleName)
                .filter(c -> c.equals(VertexSingleFoldedAction.class.getSimpleName())).count();
        if (numberOfFoldedVertexActionsBefore < numberOfFoldedVertexActionsAfter) {
            // We need to correct the destination vertex binding here.
            environment.acceptAction(new AnchorCorrectionAction(anchorAction, pathPatternExpr));
        }

        // Wrap our recursive decompositions in a LOOPING-CLAUSE and merge this back into our main sequence.
        VariableExpr destinationVariable = graphixRewritingContext.getGraphixVariableCopy(
                isEvaluatingLeftToRight ? rightDescriptor.getVariableExpr() : leftDescriptor.getVariableExpr());
        VariableExpr pathVariable = graphixRewritingContext.getGraphixVariableCopy(pathDescriptor.getVariableExpr());
        VariableExpr schemaVariable = new VariableExpr(graphixRewritingContext.newVariable());
        FinalizePathPatternAction finalizePathAction = new FinalizePathPatternAction(pathPatternExpr, anchorAction,
                recursiveAction, destinationVariable, pathVariable, schemaVariable, isEvaluatingLeftToRight);
        environment.acceptAction(finalizePathAction);

        // Introduce our destination vertex (if not already introduced) and correlate our vertex + path.
        CorrelationPathAction correlationAction;
        if (isEvaluatingLeftToRight) {
            VariableExpr rightPathVariable = aliasLookupTable.getJoinAlias(destinationVariable);
            loweringCallback.invoke(rightVertexExpr, !areRightPropertiesUsed ? rightPathVariable : null);
            correlationAction = new CorrelationPathAction(rightVertexExpr, pathPatternExpr, aliasLookupTable);
            correlationAction.setFieldInformation(rightVertexFields, rightPathVariable);
            finalizePathAction.getLoopingClause()
                    .addDownVariable(aliasLookupTable.getIterationAlias(rightDescriptor.getVariableExpr()));
        } else {
            VariableExpr leftPathVariable = aliasLookupTable.getJoinAlias(destinationVariable);
            loweringCallback.invoke(leftVertexExpr, !areLeftPropertiesUsed ? leftPathVariable : null);
            correlationAction = new CorrelationPathAction(leftVertexExpr, pathPatternExpr, aliasLookupTable);
            correlationAction.setFieldInformation(leftVertexFields, leftPathVariable);
            finalizePathAction.getLoopingClause()
                    .addDownVariable(aliasLookupTable.getIterationAlias(leftDescriptor.getVariableExpr()));
        }
        environment.acceptAction(correlationAction);
    }
}
