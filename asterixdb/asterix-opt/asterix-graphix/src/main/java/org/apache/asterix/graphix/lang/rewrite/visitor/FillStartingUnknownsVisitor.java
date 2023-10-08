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

import java.util.function.Supplier;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.PathDescriptor;
import org.apache.asterix.graphix.lang.struct.VertexDescriptor;
import org.apache.asterix.graphix.lang.visitor.base.AbstractGraphixQueryVisitor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.sqlpp.annotation.ExcludeFromSelectStarAnnotation;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.GenerateColumnNameVisitor;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A pre-Graphix transformation pass to populate a number of unknowns in our Graphix AST.
 * <ol>
 *  <li>Populate all unknown graph elements (vertices, edges, and paths).</li>
 *  <li>Populate all unknown column names in SELECT-CLAUSEs.</li>
 * </ol>
 * Note that we <b>do not</b> handle {@link org.apache.asterix.lang.common.clause.GroupbyClause} unknowns here. See
 * {@link FillGroupByUnknownsVisitor} for this portion. We assume that the following rewrites have already been invoked:
 * <ol>
 *   <li>{@link PreRewriteCheckVisitor}</li>
 * </ol>
 */
public class FillStartingUnknownsVisitor extends AbstractGraphixQueryVisitor {
    private static final Logger LOGGER = LogManager.getLogger();

    private final GenerateColumnNameVisitor generateColumnNameVisitor;
    private final Supplier<VariableExpr> newVariableSupplier;

    public FillStartingUnknownsVisitor(GraphixRewritingContext graphixRewritingContext) {
        generateColumnNameVisitor = new GenerateColumnNameVisitor(graphixRewritingContext);
        newVariableSupplier = () -> {
            VariableExpr variableExpr = new VariableExpr(graphixRewritingContext.newVariable());
            variableExpr.addHint(ExcludeFromSelectStarAnnotation.INSTANCE);
            return variableExpr;
        };
    }

    @Override
    public Expression visit(SelectExpression se, ILangExpression arg) throws CompilationException {
        se.accept(generateColumnNameVisitor, arg);
        return super.visit(se, arg);
    }

    @Override
    public Expression visit(VertexPatternExpr vpe, ILangExpression arg) throws CompilationException {
        VertexDescriptor vertexDescriptor = vpe.getVertexDescriptor();
        if (vertexDescriptor.getVariableExpr() == null) {
            vertexDescriptor.setVariableExpr(newVariableSupplier.get());
            if (LOGGER.isDebugEnabled()) {
                SourceLocation sourceLocation = vpe.getSourceLocation();
                String vertexLine = String.format("line %s", sourceLocation.getLine());
                String vertexColumn = String.format("column %s", sourceLocation.getColumn());
                String vertexLocation = String.format("(%s, %s)", vertexLine, vertexColumn);
                String vertexName = vertexDescriptor.getVariableExpr().toString();
                LOGGER.debug("Name {} generated for vertex {}.", vertexName, vertexLocation);
            }
        }
        return super.visit(vpe, arg);
    }

    @Override
    public Expression visit(EdgePatternExpr epe, ILangExpression arg) throws CompilationException {
        EdgeDescriptor edgeDescriptor = epe.getEdgeDescriptor();
        if (edgeDescriptor.getVariableExpr() == null) {
            edgeDescriptor.setVariableExpr(newVariableSupplier.get());
            if (LOGGER.isDebugEnabled()) {
                SourceLocation sourceLocation = epe.getSourceLocation();
                String edgeLine = String.format("line %s", sourceLocation.getLine());
                String edgeColumn = String.format("column %s", sourceLocation.getColumn());
                String edgeLocation = String.format("(%s, %s)", edgeLine, edgeColumn);
                String edgeName = edgeDescriptor.getVariableExpr().toString();
                LOGGER.debug("Name {} generated for edge {}.", edgeName, edgeLocation);
            }
        }
        return super.visit(epe, arg);
    }

    @Override
    public Expression visit(PathPatternExpr ppe, ILangExpression arg) throws CompilationException {
        PathDescriptor pathDescriptor = ppe.getPathDescriptor();
        if (pathDescriptor.getVariableExpr() == null) {
            pathDescriptor.setVariableExpr(newVariableSupplier.get());
            if (LOGGER.isDebugEnabled()) {
                SourceLocation sourceLocation = ppe.getSourceLocation();
                String pathLine = String.format("line %s", sourceLocation.getLine());
                String pathColumn = String.format("column %s", sourceLocation.getColumn());
                String pathLocation = String.format("(%s, %s)", pathLine, pathColumn);
                String pathName = pathDescriptor.getVariableExpr().toString();
                LOGGER.debug("Name {} generated for path {}.", pathName, pathLocation);
            }
        }
        return super.visit(ppe, arg);
    }
}
