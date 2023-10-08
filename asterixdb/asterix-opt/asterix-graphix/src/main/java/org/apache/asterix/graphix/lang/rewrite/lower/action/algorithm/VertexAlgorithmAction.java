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

import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.algebra.compiler.option.EvaluationMinimizeJoinsOption;
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.lower.action.VertexSingleFoldedAction;
import org.apache.asterix.graphix.lang.rewrite.lower.action.VertexSingleInlinedAction;
import org.apache.asterix.graphix.lang.rewrite.lower.action.VertexSingleNestedAction;
import org.apache.asterix.graphix.lang.rewrite.lower.action.VertexSingleRebindAction;
import org.apache.asterix.graphix.lang.rewrite.lower.action.correlate.CorrelationVertexAction;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.DeclarationAnalysis;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;

public class VertexAlgorithmAction extends AbstractAlgorithmAction {
    private final VertexPatternExpr vertexPatternExpr;
    private final VariableExpr vertexVariable;
    private final ILangExpression argExpr;

    // We compute the following to determine our lowering strategy.
    private boolean isCapturedByEdge;
    private boolean isCapturedByPath;
    private boolean isInlineable;
    private boolean isIntroduced;
    private boolean isRequired;
    private boolean hasParentExpr;

    public VertexAlgorithmAction(VertexPatternExpr vpe, ILangExpression argExpr, Callback loweringCallback) {
        super(loweringCallback);
        this.vertexPatternExpr = Objects.requireNonNull(vpe);
        this.vertexVariable = vertexPatternExpr.getVertexDescriptor().getVariableExpr();
        this.argExpr = argExpr;
    }

    @Override
    protected void throwIfNotSupported() throws CompilationException {
        if (vertexPatternExpr.getDeclarationSet().size() > 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, vertexPatternExpr.getSourceLocation(),
                    "Multi-label vertex expressions are currently not supported!");
        }
    }

    @Override
    protected void gatherPreliminaries(LoweringEnvironment environment) throws CompilationException {
        DeclarationAnalysis analysis = vertexPatternExpr.getDeclarationSet().iterator().next().getAnalysis();
        isCapturedByEdge = argExpr instanceof EdgePatternExpr;
        isCapturedByPath = argExpr instanceof VariableExpr;
        isInlineable = analysis.isExpressionInlineable();
        isIntroduced = environment.isElementIntroduced(vertexVariable);
        hasParentExpr = vertexPatternExpr.getParentVertexExpr() != null;
        isRequired = graphixRewritingContext.getSetting(EvaluationMinimizeJoinsOption.OPTION_KEY_NAME)
                .equals(EvaluationMinimizeJoinsOption.FALSE);
    }

    @Override
    protected void decideAndApply(LoweringEnvironment environment) throws CompilationException {
        if (isIntroduced) {
            // Case #1: We do not need to lower our vertex.
            return;

        } else if (isCapturedByEdge && !isRequired) {
            // Case #2: We can fold our vertex into our edge.
            EdgePatternExpr edgePatternExpr = (EdgePatternExpr) argExpr;
            environment.acceptAction(new VertexSingleFoldedAction(vertexPatternExpr, edgePatternExpr));
            environment.setElementPartiallyLowered(vertexVariable, true);

        } else if (isCapturedByPath && !isRequired) {
            // Case #3: We rebind an existing variable to our vertex.
            VariableExpr pathVertexVariableExpr = (VariableExpr) argExpr;
            environment.acceptAction(new VertexSingleRebindAction(vertexPatternExpr, pathVertexVariableExpr));
            environment.setElementPartiallyLowered(vertexVariable, true);

        } else {
            // Case #4: Introduce our vertex.
            if (isInlineable) {
                environment.acceptAction(new VertexSingleInlinedAction(vertexPatternExpr));
            } else {
                environment.acceptAction(new VertexSingleNestedAction(vertexPatternExpr));
            }
        }

        // If this vertex has a parent, explicitly JOIN them here.
        if (hasParentExpr) {
            environment.acceptAction(new CorrelationVertexAction(vertexPatternExpr));
        }
    }
}
