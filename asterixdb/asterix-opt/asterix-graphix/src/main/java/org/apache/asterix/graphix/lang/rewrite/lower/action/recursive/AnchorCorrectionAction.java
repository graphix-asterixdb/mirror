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
package org.apache.asterix.graphix.lang.rewrite.lower.action.recursive;

import static org.apache.asterix.graphix.lang.rewrite.util.LowerRewritingUtil.buildRecordConstructor;

import java.util.List;
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.expression.pattern.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.lower.action.base.IEnvironmentAction;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.AliasLookupTable;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.graphix.lang.struct.VertexDescriptor;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.VariableExpr;

public class AnchorCorrectionAction implements IEnvironmentAction {
    private final AnchorSequenceAction anchorSequenceAction;
    private final PathPatternExpr pathPatternExpr;

    public AnchorCorrectionAction(AnchorSequenceAction anchorSequenceAction, PathPatternExpr pathPatternExpr) {
        this.anchorSequenceAction = Objects.requireNonNull(anchorSequenceAction);
        this.pathPatternExpr = Objects.requireNonNull(pathPatternExpr);
    }

    @Override
    public void apply(LoweringEnvironment environment) throws CompilationException {
        AliasLookupTable aliasLookupTable = environment.getAliasLookupTable();

        // Determine our source and destination vertices.
        VertexPatternExpr leftVertex = pathPatternExpr.getLeftVertex();
        VertexPatternExpr rightVertex = pathPatternExpr.getRightVertex();
        VertexDescriptor rightDescriptor = rightVertex.getVertexDescriptor();
        VertexDescriptor leftDescriptor = leftVertex.getVertexDescriptor();

        // Determine if we are evaluating from left to right again.
        boolean isEvaluatingLeftToRight = !environment.isElementIntroduced(rightDescriptor.getVariableExpr());
        VertexDescriptor sourceDescriptor = isEvaluatingLeftToRight ? leftDescriptor : rightDescriptor;
        VertexDescriptor destDescriptor = isEvaluatingLeftToRight ? rightDescriptor : leftDescriptor;
        VariableExpr sourceVariable = sourceDescriptor.getVariableExpr();
        VariableExpr sourceJoinAlias = aliasLookupTable.getJoinAlias(sourceVariable);

        // Correct our destination vertex binding.
        VertexPatternExpr destVertexExpr = isEvaluatingLeftToRight ? rightVertex : leftVertex;
        ElementLabel destVertexLabel = destDescriptor.getLabels().iterator().next();
        List<List<String>> destVertexFields = destVertexExpr.getKeyFieldNames(destVertexLabel);
        RecordConstructor recordConstructor =
                buildRecordConstructor(destVertexFields, destVertexFields, sourceJoinAlias);
        anchorSequenceAction.getNextVertexBindings().get(0).setBindingExpr(recordConstructor);
    }
}
