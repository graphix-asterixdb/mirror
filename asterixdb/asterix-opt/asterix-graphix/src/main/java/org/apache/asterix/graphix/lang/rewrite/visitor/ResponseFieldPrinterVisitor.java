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
package org.apache.asterix.graphix.lang.rewrite.visitor;

import java.io.PrintWriter;

import org.apache.asterix.common.api.IResponseFieldPrinter;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.algebra.compiler.option.CompilationAddContextOption;
import org.apache.asterix.graphix.algebra.compiler.option.IGraphixCompilerOption;
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.IPatternConstruct;
import org.apache.asterix.graphix.lang.expression.pattern.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.struct.AbstractDescriptor;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.PathDescriptor;
import org.apache.asterix.graphix.lang.struct.VertexDescriptor;
import org.apache.asterix.graphix.lang.visitor.base.AbstractGraphixQueryVisitor;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ResponseFieldPrinterVisitor extends AbstractGraphixQueryVisitor implements IResponseFieldPrinter {
    private final ObjectNode topLevelNode;
    private final ArrayNode verticesArray;
    private final ArrayNode edgesArray;
    private final ArrayNode pathsArray;

    public ResponseFieldPrinterVisitor(GraphixRewritingContext graphixRewritingContext) throws CompilationException {
        String optionKeyName = CompilationAddContextOption.OPTION_KEY_NAME;
        IGraphixCompilerOption compilerOption = graphixRewritingContext.getSetting(optionKeyName);
        if (compilerOption == CompilationAddContextOption.TRUE) {
            graphixRewritingContext.getResponsePrinter().addFooterPrinter(this);
        }

        // Our top-level node will start with "graphix".
        topLevelNode = new ObjectMapper().createObjectNode();
        ObjectNode patternsNode = topLevelNode.putObject("patterns");
        verticesArray = patternsNode.putArray("vertices");
        edgesArray = patternsNode.putArray("edges");
        pathsArray = patternsNode.putArray("paths");
    }

    /**
     * We build the structure below:
     * <pre>
     * {
     *   "graphElement": {
     *     "variable": $$$,
     *     "labels": [ $$$, $$$, ... ]
     *   },
     *   "location": {
     *     "line": $$$,
     *     "column": $$$
     *   },
     *   "hints": [ $$$, $$$, ]
     * }
     * </pre>
     */
    @Override
    public Expression visit(VertexPatternExpr vpe, ILangExpression arg) throws CompilationException {
        ObjectNode outputObject = verticesArray.addObject();
        addBaseGraphElementInformation(outputObject, vpe);
        return super.visit(vpe, arg);
    }

    /**
     * We build the structure below:
     * <pre>
     * {
     *   "graphElement": {
     *     "variable": $$$,
     *     "labels": [ $$$, $$$, ... ]
     *   },
     *   "location": {
     *     "line": $$$,
     *     "column": $$$
     *   },
     *   "hints": [ $$$, $$$, ],
     *   "edgeElement": {
     *     "direction": $$$,
     *     "leftVertex": {
     *       "variable": $$$,
     *       "location": {
     *         "line": $$$,
     *         "column": $$$
     *       }
     *     },
     *     "rightVertex": {
     *       "variable": $$$,
     *       "location": {
     *         "line": $$$,
     *         "column": $$$
     *       }
     *     }
     *   }
     * }
     * </pre>
     */
    @Override
    public Expression visit(EdgePatternExpr epe, ILangExpression arg) throws CompilationException {
        ObjectNode outputObject = edgesArray.addObject();
        addBaseGraphElementInformation(outputObject, epe);

        // Add connection-related-info here.
        EdgeDescriptor edgeDescriptor = epe.getEdgeDescriptor();
        ObjectNode edgeElementNode = outputObject.putObject("edgeElement");
        ObjectNode leftVertexNode = edgeElementNode.putObject("leftVertex");
        ObjectNode rightVertexNode = edgeElementNode.putObject("rightVertex");
        edgeElementNode.put("direction", edgeDescriptor.getElementDirection().name().toLowerCase());
        addVariableInformation(leftVertexNode, epe.getLeftVertex());
        addVariableInformation(rightVertexNode, epe.getRightVertex());
        addSourceLocationInformation(leftVertexNode, epe.getLeftVertex());
        addSourceLocationInformation(rightVertexNode, epe.getRightVertex());
        return super.visit(epe, arg);
    }

    /**
     * We build the structure below:
     * <pre>
     * {
     *   "graphElement": {
     *     "variable": $$$,
     *     "labels": [ $$$, $$$, ... ]
     *   },
     *   "location": {
     *     "line": $$$,
     *     "column": $$$
     *   },
     *   "hints": [ $$$, $$$, ],
     *   "pathElement": {
     *     "direction": $$$,
     *     "minimumHops": $$$,
     *     "maximumHops": $$$,
     *     "leftVertex": {
     *       "variable": $$$,
     *       "location": {
     *         "line": $$$,
     *         "column": $$$
     *       }
     *     },
     *     "rightVertex": {
     *       "variable": $$$,
     *       "location": {
     *         "line": $$$,
     *         "column": $$$
     *       }
     *     }
     *   }
     * }
     * </pre>
     */
    @Override
    public Expression visit(PathPatternExpr ppe, ILangExpression arg) throws CompilationException {
        ObjectNode outputObject = pathsArray.addObject();
        addBaseGraphElementInformation(outputObject, ppe);

        // Add connection-related-info here.
        PathDescriptor pathDescriptor = ppe.getPathDescriptor();
        ObjectNode edgeElementNode = outputObject.putObject("edgeElement");
        ObjectNode leftVertexNode = edgeElementNode.putObject("leftVertex");
        ObjectNode rightVertexNode = edgeElementNode.putObject("rightVertex");
        edgeElementNode.put("direction", pathDescriptor.getElementDirection().name().toLowerCase());
        edgeElementNode.put("minimumHops", pathDescriptor.getMinimumHops());
        edgeElementNode.put("maximumHops", pathDescriptor.getMaximumHops());
        addVariableInformation(leftVertexNode, ppe.getLeftVertex());
        addVariableInformation(rightVertexNode, ppe.getRightVertex());
        addSourceLocationInformation(leftVertexNode, ppe.getLeftVertex());
        addSourceLocationInformation(rightVertexNode, ppe.getRightVertex());
        return super.visit(ppe, arg);
    }

    /**
     * We build the structure below:
     * <pre>
     * {
     *   "graphElement": {
     *     "variable": $$$,
     *     "labels": [ $$$, $$$, ... ]
     *   },
     *   "location": {
     *     "line": $$$,
     *     "column": $$$
     *   },
     *   "hints": [ $$$, $$$, ]
     * }
     * </pre>
     */
    private void addBaseGraphElementInformation(ObjectNode parentObjectNode, IPatternConstruct patternConstruct) {
        AbstractDescriptor abstractDescriptor;
        AbstractExpression abstractExpression;
        switch (patternConstruct.getPatternType()) {
            case VERTEX_PATTERN:
                VertexPatternExpr vertexPatternExpr = (VertexPatternExpr) patternConstruct;
                abstractDescriptor = vertexPatternExpr.getVertexDescriptor();
                abstractExpression = vertexPatternExpr;
                break;

            case EDGE_PATTERN:
                EdgePatternExpr edgePatternExpr = (EdgePatternExpr) patternConstruct;
                abstractDescriptor = edgePatternExpr.getEdgeDescriptor();
                abstractExpression = edgePatternExpr;
                break;

            case PATH_PATTERN:
                PathPatternExpr pathPatternExpr = (PathPatternExpr) patternConstruct;
                abstractDescriptor = pathPatternExpr.getPathDescriptor();
                abstractExpression = pathPatternExpr;
                break;

            default:
                throw new IllegalStateException("Illegal pattern given?");
        }

        // We always expect the fields below to appear.
        ObjectNode graphElementNode = parentObjectNode.putObject("graphElement");
        addVariableInformation(graphElementNode, patternConstruct);
        ArrayNode labelsArrayNode = graphElementNode.putArray("labels");
        abstractDescriptor.getLabels().forEach(l -> labelsArrayNode.add(l.getLabelName()));
        addSourceLocationInformation(parentObjectNode, abstractExpression);

        // If we have hints, add them here.
        if (abstractExpression.hasHints() && !abstractExpression.getHints().isEmpty()) {
            ArrayNode hintsArrayNode = parentObjectNode.putArray("hints");
            for (IExpressionAnnotation hint : abstractExpression.getHints()) {
                hintsArrayNode.add(hint.toString());
            }
        }
    }

    /**
     * We build the structure below:
     * <pre>
     * {
     *   "location": {
     *     "line": $$$,
     *     "column": $$$
     *   }
     * }
     * </pre>
     */
    private void addSourceLocationInformation(ObjectNode parentObjectNode, AbstractExpression abstractExpression) {
        ObjectNode locationNode = parentObjectNode.putObject("location");
        locationNode.put("line", abstractExpression.getSourceLocation().getLine());
        locationNode.put("column", abstractExpression.getSourceLocation().getColumn());
    }

    /**
     * We build the structure below:
     * <pre>
     * {
     *   "variable": $$$
     * }
     * </pre>
     */
    private void addVariableInformation(ObjectNode parentObjectNode, IPatternConstruct patternConstruct) {
        VariableExpr variableExpr;
        switch (patternConstruct.getPatternType()) {
            case VERTEX_PATTERN:
                VertexPatternExpr vertexPatternExpr = (VertexPatternExpr) patternConstruct;
                VertexDescriptor vertexDescriptor = vertexPatternExpr.getVertexDescriptor();
                variableExpr = vertexDescriptor.getVariableExpr();
                break;

            case EDGE_PATTERN:
                EdgePatternExpr edgePatternExpr = (EdgePatternExpr) patternConstruct;
                EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
                variableExpr = edgeDescriptor.getVariableExpr();
                break;

            case PATH_PATTERN:
                PathPatternExpr pathPatternExpr = (PathPatternExpr) patternConstruct;
                PathDescriptor pathDescriptor = pathPatternExpr.getPathDescriptor();
                variableExpr = pathDescriptor.getVariableExpr();
                break;

            default:
                throw new IllegalStateException("Illegal pattern given?");
        }
        parentObjectNode.put("variable", SqlppVariableUtil.toUserDefinedName(variableExpr.getVar().getValue()));
    }

    @Override
    public void print(PrintWriter pw) throws HyracksDataException {
        pw.print("\t\"graphix\":");
        pw.print(this);
    }

    @Override
    public String getName() {
        return "graphix";
    }

    @Override
    public String toString() {
        return topLevelNode.toPrettyString();
    }
}
