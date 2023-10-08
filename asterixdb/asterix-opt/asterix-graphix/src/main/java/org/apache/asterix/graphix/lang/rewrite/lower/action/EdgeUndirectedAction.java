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

import static org.apache.asterix.graphix.lang.rewrite.util.LowerRewritingUtil.remapVariablesInPattern;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.ElementDirection;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.lower.action.base.IEnvironmentAction;
import org.apache.asterix.graphix.lang.rewrite.lower.action.finalize.FinalizeUndirectedEdgeAction;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.ClauseSequence;
import org.apache.asterix.graphix.lang.rewrite.visitor.common.GraphixDeepCopyVisitor;
import org.apache.asterix.graphix.lang.rewrite.visitor.common.VariableRemapCloneVisitor;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.VertexDescriptor;
import org.apache.asterix.lang.common.expression.VariableExpr;

public class EdgeUndirectedAction implements IEnvironmentAction {
    private final IEnvironmentAction.Callback loweringCallback;
    private final EdgePatternExpr edgePatternExpr;

    public EdgeUndirectedAction(Callback loweringCallback, EdgePatternExpr edgePatternExpr) {
        this.loweringCallback = loweringCallback;
        this.edgePatternExpr = edgePatternExpr;
    }

    @Override
    public void apply(LoweringEnvironment environment) throws CompilationException {
        GraphixRewritingContext graphixRewritingContext = environment.getGraphixRewritingContext();
        VariableRemapCloneVisitor remapCloneVisitor = new VariableRemapCloneVisitor(graphixRewritingContext);
        EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
        VertexDescriptor leftDescriptor = edgePatternExpr.getLeftVertex().getVertexDescriptor();
        VertexDescriptor rightDescriptor = edgePatternExpr.getRightVertex().getVertexDescriptor();

        // Note: to avoid the same vertex copies being used between edges, we need two unique deep-copy-visitors.
        GraphixDeepCopyVisitor deepCopyVisitor1 = new GraphixDeepCopyVisitor();
        GraphixDeepCopyVisitor deepCopyVisitor2 = new GraphixDeepCopyVisitor();
        deepCopyVisitor1.visit(edgePatternExpr.getLeftVertex(), null);
        deepCopyVisitor1.visit(edgePatternExpr.getRightVertex(), null);
        deepCopyVisitor2.visit(edgePatternExpr.getLeftVertex(), null);
        deepCopyVisitor2.visit(edgePatternExpr.getRightVertex(), null);
        EdgePatternExpr edgeCopy1 = deepCopyVisitor1.visit(edgePatternExpr, null);
        EdgePatternExpr edgeCopy2 = deepCopyVisitor2.visit(edgePatternExpr, null);
        EdgeDescriptor copyDescriptor1 = edgeCopy1.getEdgeDescriptor();
        EdgeDescriptor copyDescriptor2 = edgeCopy2.getEdgeDescriptor();
        VariableExpr leftVar = leftDescriptor.getVariableExpr();
        VariableExpr rightVar = rightDescriptor.getVariableExpr();
        VariableExpr edgeVar = edgeDescriptor.getVariableExpr();

        // Remap our first edge, and use new variables.
        if (!environment.isElementIntroduced(leftVar)) {
            remapCloneVisitor.addSubstitution(leftVar, graphixRewritingContext.getGraphixVariableCopy(leftVar));
        }
        remapCloneVisitor.addSubstitution(edgeVar, graphixRewritingContext.getGraphixVariableCopy(edgeVar));
        if (!environment.isElementIntroduced(rightVar)) {
            remapCloneVisitor.addSubstitution(rightVar, graphixRewritingContext.getGraphixVariableCopy(rightVar));
        }
        copyDescriptor1.setElementDirection(ElementDirection.LEFT_TO_RIGHT);
        remapVariablesInPattern(edgeCopy1, remapCloneVisitor);
        environment.pushClauseSequence();
        loweringCallback.invoke(edgeCopy1, null);
        ClauseSequence l2RSequence = environment.popClauseSequence();

        // Remap our second edge, and use new variables.
        if (!environment.isElementIntroduced(leftVar)) {
            remapCloneVisitor.addSubstitution(leftVar, graphixRewritingContext.getGraphixVariableCopy(leftVar));
        }
        remapCloneVisitor.addSubstitution(edgeVar, graphixRewritingContext.getGraphixVariableCopy(edgeVar));
        if (!environment.isElementIntroduced(rightVar)) {
            remapCloneVisitor.addSubstitution(rightVar, graphixRewritingContext.getGraphixVariableCopy(rightVar));
        }
        copyDescriptor2.setElementDirection(ElementDirection.RIGHT_TO_LEFT);
        remapVariablesInPattern(edgeCopy2, remapCloneVisitor);
        environment.pushClauseSequence();
        loweringCallback.invoke(edgeCopy2, null);
        ClauseSequence r2LSequence = environment.popClauseSequence();

        // Merge both sequences back into our main sequence.
        environment.acceptAction(new FinalizeUndirectedEdgeAction(edgePatternExpr, l2RSequence, r2LSequence));

    }
}
