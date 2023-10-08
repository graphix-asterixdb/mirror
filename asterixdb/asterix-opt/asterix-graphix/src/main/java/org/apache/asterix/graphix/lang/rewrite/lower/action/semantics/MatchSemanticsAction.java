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

import static org.apache.asterix.graphix.lang.rewrite.lower.struct.MorphismConstraint.generate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.algebra.compiler.option.SemanticsNavigationOption;
import org.apache.asterix.graphix.algebra.compiler.option.SemanticsPatternOption;
import org.apache.asterix.graphix.lang.clause.FromGraphTerm;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.lang.rewrite.lower.action.base.IEnvironmentAction;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.AliasLookupTable;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.MorphismConstraint;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.MorphismConstraint.IsomorphismOperand;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.graphix.lang.struct.VertexDescriptor;
import org.apache.asterix.graphix.lang.visitor.base.AbstractGraphixQueryVisitor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.VariableExpr;

/**
 * Define the semantics of evaluating a basic graph pattern query (i.e. how much isomorphism do we enforce), and b)
 * b) the semantics of navigating between vertices (i.e. what type of uniqueness in the path should be enforced). We
 * assume that all elements are named at this point.
 * <p>
 * We enforce the following basic graph pattern query semantics (by default, we enforce total isomorphism):
 * <ul>
 *  <li>For total isomorphism, no vertex and no edge can appear more than once across all {@link MatchClause}
 *  nodes.</li>
 *  <li>For vertex-isomorphism, we enforce that no vertex can appear more than once across all {@link MatchClause}
 *  nodes.</li>
 *  <li>For edge-isomorphism, we enforce that no edge can appear more than once across all {@link MatchClause}
 *  nodes.</li>
 *  <li>For homomorphism, we enforce nothing. Edge adjacency is already implicitly preserved.</li>
 * </ul>
 * <p>
 * We enforce the following navigation query semantics (by default, we enforce no-repeat-anything):
 * <ul>
 *  <li>For no-repeat-vertices, no vertex instance can appear more than once in a path instance.</li>
 *  <li>For no-repeat-edges, no edge instance can appear more than once in a path instance.</li>
 *  <li>For no-repeat-anything, no vertex or edge instance can appear more than once in a path instance.</li>
 * </ul>
 * <p>
 *
 * @see UndirectedSemanticsAction
 * @see LeftMatchSemanticsAction
 * @see NavigationSemanticsAction
 */
public class MatchSemanticsAction implements IEnvironmentAction {
    private final FromGraphTerm fromGraphTerm;

    public MatchSemanticsAction(FromGraphTerm fromGraphTerm) {
        this.fromGraphTerm = fromGraphTerm;
    }

    @Override
    public void apply(LoweringEnvironment loweringEnvironment) throws CompilationException {
        GraphixRewritingContext graphixRewritingContext = loweringEnvironment.getGraphixRewritingContext();
        AliasLookupTable aliasLookupTable = loweringEnvironment.getAliasLookupTable();

        // Determine our BGP query semantics.
        String patternConfigKeyName = SemanticsPatternOption.OPTION_KEY_NAME;
        SemanticsPatternOption semanticsPatternOption =
                (SemanticsPatternOption) graphixRewritingContext.getSetting(patternConfigKeyName);

        // Determine our navigation query semantics.
        String navConfigKeyName = SemanticsNavigationOption.OPTION_KEY_NAME;
        SemanticsNavigationOption semanticsNavigationOption =
                (SemanticsNavigationOption) graphixRewritingContext.getSetting(navConfigKeyName);

        // Populate our operands maps.
        Map<ElementLabel, List<IsomorphismOperand>> vertexOperandMap = new HashMap<>();
        Map<ElementLabel, List<IsomorphismOperand>> edgeOperandMap = new HashMap<>();
        fromGraphTerm.accept(new AbstractGraphixQueryVisitor() {
            @Override
            public Expression visit(FromGraphTerm fgt, ILangExpression arg) throws CompilationException {
                // We only want to explore the top level of our FROM-GRAPH-TERMs.
                for (MatchClause matchClause : fgt.getMatchClauses()) {
                    matchClause.accept(this, arg);
                }
                return null;
            }

            @Override
            public Expression visit(VertexPatternExpr vpe, ILangExpression arg) {
                VertexDescriptor vertexDescriptor = vpe.getVertexDescriptor();
                VariableExpr vertexVariable = vertexDescriptor.getVariableExpr();
                VariableExpr joinAlias = aliasLookupTable.getJoinAlias(vertexVariable);
                ElementLabel elementLabel = vertexDescriptor.getLabels().iterator().next();
                List<List<String>> keyFieldNames = vpe.getKeyFieldNames(elementLabel);
                joinAlias.setSourceLocation(vertexVariable.getSourceLocation());
                if (vertexOperandMap.containsKey(elementLabel)) {
                    boolean isNoMatchFound =
                            vertexOperandMap.get(elementLabel).stream().map(IsomorphismOperand::getVariableExpr)
                                    .noneMatch(v -> v.getVar().getValue().equals(joinAlias.getVar().getValue()));
                    if (isNoMatchFound) {
                        vertexOperandMap.get(elementLabel).add(new IsomorphismOperand(keyFieldNames, joinAlias));
                    }

                } else {
                    List<IsomorphismOperand> operandList = new ArrayList<>();
                    operandList.add(new IsomorphismOperand(keyFieldNames, joinAlias));
                    vertexOperandMap.put(elementLabel, operandList);
                }
                return null;
            }

            @Override
            public Expression visit(EdgePatternExpr epe, ILangExpression arg) {
                EdgeDescriptor edgeDescriptor = epe.getEdgeDescriptor();
                VariableExpr edgeVariable = edgeDescriptor.getVariableExpr();
                VariableExpr joinAlias = aliasLookupTable.getJoinAlias(edgeVariable);
                ElementLabel elementLabel = edgeDescriptor.getLabels().iterator().next();
                ElementLabel leftLabel = epe.getLeftVertex().getVertexDescriptor().getLabels().iterator().next();
                ElementLabel rightLabel = epe.getRightVertex().getVertexDescriptor().getLabels().iterator().next();
                joinAlias.setSourceLocation(edgeVariable.getSourceLocation());
                if (edgeOperandMap.containsKey(elementLabel)) {
                    boolean isNoMatchFound =
                            edgeOperandMap.get(elementLabel).stream().map(IsomorphismOperand::getVariableExpr)
                                    .noneMatch(v -> v.getVar().getValue().equals(joinAlias.getVar().getValue()));
                    if (isNoMatchFound) {
                        List<List<String>> compositeNames = new ArrayList<>();
                        compositeNames.addAll(epe.getLeftKeyFieldNames(leftLabel));
                        compositeNames.addAll(epe.getRightKeyFieldNames(rightLabel));
                        edgeOperandMap.get(elementLabel).add(new IsomorphismOperand(compositeNames, joinAlias));
                    }

                } else {
                    List<IsomorphismOperand> operandList = new ArrayList<>();
                    List<List<String>> compositeNames = new ArrayList<>();
                    compositeNames.addAll(epe.getLeftKeyFieldNames(leftLabel));
                    compositeNames.addAll(epe.getRightKeyFieldNames(rightLabel));
                    operandList.add(new IsomorphismOperand(compositeNames, joinAlias));
                    edgeOperandMap.put(elementLabel, operandList);
                }
                return null;
            }
        }, null);

        // Construct our isomorphism conjuncts.
        List<MorphismConstraint> workingConjuncts = new ArrayList<>();
        if (semanticsPatternOption == SemanticsPatternOption.ISOMORPHISM
                || semanticsPatternOption == SemanticsPatternOption.VERTEX_ISOMORPHISM) {
            for (List<IsomorphismOperand> operandList : vertexOperandMap.values()) {
                workingConjuncts.addAll(generate(operandList));
            }
        }
        if (semanticsPatternOption == SemanticsPatternOption.ISOMORPHISM
                || semanticsPatternOption == SemanticsPatternOption.EDGE_ISOMORPHISM) {
            for (List<IsomorphismOperand> operandList : edgeOperandMap.values()) {
                workingConjuncts.addAll(generate(operandList));
            }
        }

        // Handle our LOOPING-CLAUSEs, LEFT-MATCH, and the rest of our conjuncts.
        loweringEnvironment.acceptAction(new NavigationSemanticsAction(workingConjuncts, semanticsNavigationOption));
        loweringEnvironment.acceptAction(new UndirectedSemanticsAction(workingConjuncts, fromGraphTerm));
        loweringEnvironment.acceptAction(new LeftMatchSemanticsAction(workingConjuncts));
        loweringEnvironment.acceptTransformer(clauseSequence -> {
            for (MorphismConstraint morphismConstraint : workingConjuncts) {
                clauseSequence.addNonRepresentativeClause(new WhereClause(morphismConstraint.build()));
            }
        });
    }
}
