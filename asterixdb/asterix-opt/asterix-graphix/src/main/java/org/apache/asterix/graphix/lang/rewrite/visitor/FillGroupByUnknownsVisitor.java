/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.graphix.lang.rewrite.visitor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.FromGraphTerm;
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.PathDescriptor;
import org.apache.asterix.graphix.lang.struct.VertexDescriptor;
import org.apache.asterix.graphix.lang.visitor.base.AbstractGraphixQueryVisitor;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.hyracks.algebricks.common.utils.Pair;

/**
 * A Graphix transformation pass to populate the GROUP-BY fields in our Graphix AST.
 * <p>
 * We assume that the following rewrites have already been invoked:
 * <ol>
 *   <li>{@link PreRewriteCheckVisitor}</li>
 *   <li>{@link FillStartingUnknownsVisitor}</li>
 *   <li>{@link ElementDeclarationVisitor}</li>
 * </ol>
 */
public class FillGroupByUnknownsVisitor extends AbstractGraphixQueryVisitor {
    @Override
    public Expression visit(SelectBlock sb, ILangExpression arg) throws CompilationException {
        super.visit(sb, arg);
        if (sb.hasGroupbyClause()) {
            // Collect all variables that should belong in the GROUP-BY field list.
            Set<VariableExpr> userLiveVariables = new HashSet<>();
            if (sb.hasFromClause() && sb.getFromClause() instanceof FromGraphClause) {
                FromGraphClause fromGraphClause = (FromGraphClause) sb.getFromClause();
                for (AbstractClause fromTerm : fromGraphClause.getTerms()) {
                    if (fromTerm instanceof FromGraphTerm) {
                        FromGraphTerm fromGraphTerm = (FromGraphTerm) fromTerm;
                        fromGraphTerm.accept(new AbstractGraphixQueryVisitor() {
                            @Override
                            public Expression visit(PathPatternExpr ppe, ILangExpression arg) {
                                PathDescriptor pathDescriptor = ppe.getPathDescriptor();
                                if (!pathDescriptor.isVariableGenerated()) {
                                    userLiveVariables.add(pathDescriptor.getVariableExpr());
                                }
                                return ppe;
                            }

                            @Override
                            public Expression visit(EdgePatternExpr epe, ILangExpression arg) {
                                EdgeDescriptor edgeDescriptor = epe.getEdgeDescriptor();
                                if (!edgeDescriptor.isVariableGenerated()) {
                                    userLiveVariables.add(edgeDescriptor.getVariableExpr());
                                }
                                return epe;
                            }

                            @Override
                            public Expression visit(VertexPatternExpr vpe, ILangExpression arg) {
                                VertexDescriptor vertexDescriptor = vpe.getVertexDescriptor();
                                if (!vertexDescriptor.isVariableGenerated()) {
                                    userLiveVariables.add(vertexDescriptor.getVariableExpr());
                                }
                                return vpe;
                            }
                        }, null);
                        List<AbstractBinaryCorrelateClause> correlateClauses = fromGraphTerm.getCorrelateClauses();
                        for (AbstractBinaryCorrelateClause correlateClause : correlateClauses) {
                            userLiveVariables.add(correlateClause.getRightVariable());
                        }

                    } else {
                        FromTerm castedFromTerm = (FromTerm) fromTerm;
                        userLiveVariables.add(castedFromTerm.getLeftVariable());
                        for (AbstractBinaryCorrelateClause correlateClause : castedFromTerm.getCorrelateClauses()) {
                            userLiveVariables.add(correlateClause.getRightVariable());
                        }
                    }
                }

            } else if (sb.hasFromClause()) {
                FromClause fromClause = sb.getFromClause();
                for (FromTerm fromTerm : fromClause.getFromTerms()) {
                    userLiveVariables.add(fromTerm.getLeftVariable());
                    for (AbstractBinaryCorrelateClause correlateClause : fromTerm.getCorrelateClauses()) {
                        userLiveVariables.add(correlateClause.getRightVariable());
                    }
                }
            }
            if (sb.hasLetWhereClauses()) {
                for (AbstractClause abstractClause : sb.getLetWhereList()) {
                    if (abstractClause.getClauseType() == Clause.ClauseType.LET_CLAUSE) {
                        LetClause letClause = (LetClause) abstractClause;
                        userLiveVariables.add(letClause.getVarExpr());
                    }
                }
            }

            // Add the live variables to our GROUP-BY field list.
            List<Pair<Expression, Identifier>> newGroupFieldList = new ArrayList<>();
            for (VariableExpr userLiveVariable : userLiveVariables) {
                String variableName = SqlppVariableUtil.toUserDefinedName(userLiveVariable.getVar().getValue());
                newGroupFieldList.add(new Pair<>(userLiveVariable, new Identifier(variableName)));
            }
            sb.getGroupbyClause().setGroupFieldList(newGroupFieldList);
        }
        return null;
    }
}
