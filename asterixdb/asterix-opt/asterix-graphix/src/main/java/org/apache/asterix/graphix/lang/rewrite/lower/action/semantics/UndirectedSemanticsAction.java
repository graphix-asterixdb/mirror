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
package org.apache.asterix.graphix.lang.rewrite.lower.action.semantics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.clause.FromGraphTerm;
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.ElementDirection;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.lower.action.base.IEnvironmentAction;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.AliasLookupTable;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.MorphismConstraint;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.visitor.base.AbstractGraphixQueryVisitor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.VarIdentifier;

public class UndirectedSemanticsAction implements IEnvironmentAction {
    private final List<MorphismConstraint> morphismConstraints;
    private final FromGraphTerm fromGraphTerm;

    public UndirectedSemanticsAction(List<MorphismConstraint> morphismConstraints, FromGraphTerm fromGraphTerm) {
        this.morphismConstraints = Collections.unmodifiableList(Objects.requireNonNull(morphismConstraints));
        this.fromGraphTerm = Objects.requireNonNull(fromGraphTerm);
    }

    @Override
    public void apply(LoweringEnvironment loweringEnvironment) throws CompilationException {
        AliasLookupTable aliasLookupTable = loweringEnvironment.getAliasLookupTable();
        List<VarIdentifier> undirectedEdgeVariables = new ArrayList<>();
        fromGraphTerm.accept(new AbstractGraphixQueryVisitor() {
            @Override
            public Expression visit(EdgePatternExpr epe, ILangExpression arg) {
                EdgeDescriptor edgeDescriptor = epe.getEdgeDescriptor();
                if (edgeDescriptor.getElementDirection() == ElementDirection.UNDIRECTED) {
                    VariableExpr joinAlias = aliasLookupTable.getJoinAlias(edgeDescriptor.getVariableExpr());
                    undirectedEdgeVariables.add(joinAlias.getVar());
                }
                return null;
            }
        }, null);

        // Walk through our conjuncts and mark the edges involved.
        for (MorphismConstraint morphismConstraint : morphismConstraints) {
            MorphismConstraint.IsomorphismOperand leftOperand = morphismConstraint.getLeftOperand();
            MorphismConstraint.IsomorphismOperand rightOperand = morphismConstraint.getRightOperand();
            if (undirectedEdgeVariables.contains(leftOperand.getVariableExpr().getVar())) {
                leftOperand.setRequiresUnknownCheck(true);
            }
            if (undirectedEdgeVariables.contains(rightOperand.getVariableExpr().getVar())) {
                rightOperand.setRequiresUnknownCheck(true);
            }
        }
    }
}
