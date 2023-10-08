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
package org.apache.asterix.graphix.algebra.optimizer.function;

import static org.apache.asterix.graphix.function.GraphixFunctionIdentifiers.EDGE_COUNT_FROM_HEADER;
import static org.apache.asterix.om.functions.BuiltinFunctions.FIELD_ACCESS_BY_INDEX;
import static org.apache.asterix.om.functions.BuiltinFunctions.FIELD_ACCESS_BY_NAME;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.apache.asterix.graphix.algebra.finder.DownstreamFunctionFinder;
import org.apache.asterix.graphix.algebra.finder.InExpressionFunctionFinder;
import org.apache.asterix.graphix.function.GraphixFunctionIdentifiers;
import org.apache.asterix.graphix.function.GraphixFunctionInfoCollection;
import org.apache.asterix.graphix.type.TranslatePathTypeComputer;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.ErrorCode;

public class RewriteGraphixFunctionsRule implements IAlgebraicRewriteRule {
    private final RewriteGraphixFunctionsTransform functionsTransform = new RewriteGraphixFunctionsTransform();

    // We pass the following onto our transform.
    private AbstractLogicalOperator workingOp;

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        workingOp = (AbstractLogicalOperator) opRef.getValue();

        // Compute our primary keys (our paths might be nested in some field-accesses).
        if (workingOp.acceptExpressionTransform(functionsTransform)) {
            OperatorManipulationUtil.computeTypeEnvironmentBottomUp(workingOp, context);
            return true;
        }
        return false;
    }

    private final class RewriteGraphixFunctionsTransform implements ILogicalExpressionReferenceTransform {
        private final FunctionIdentifier[] TRANSLATE_PATH_IDENTIFIERS = new FunctionIdentifier[] {
                GraphixFunctionIdentifiers.TRANSLATE_FORWARD_PATH, GraphixFunctionIdentifiers.TRANSLATE_REVERSE_PATH };

        @Override
        public boolean transform(Mutable<ILogicalExpression> exprRef) throws AlgebricksException {
            // Perform our transformation (if applicable).
            boolean isLenEdgesTransformed = transformLenEdges(exprRef);
            boolean isPathEdgesTransformed = transformPathEdges(exprRef);
            boolean isPathVerticesTransformed = transformPathVertices(exprRef);
            return isLenEdgesTransformed || isPathEdgesTransformed || isPathVerticesTransformed;
        }

        private boolean transformLenEdges(Mutable<ILogicalExpression> exprRef) throws AlgebricksException {
            InExpressionFunctionFinder callFinder = new InExpressionFunctionFinder(BuiltinFunctions.LEN);
            if (callFinder.search(exprRef)) {
                Consumer<AbstractFunctionCallExpression> functionTransformer = f -> {
                    VariableReferenceExpression translatePathArgExpr =
                            (VariableReferenceExpression) f.getArguments().get(0).getValue();
                    LogicalVariable pathVariable = translatePathArgExpr.getVariableReference();
                    AbstractFunctionCallExpression countPathHopsExpr = new ScalarFunctionCallExpression(
                            GraphixFunctionInfoCollection.getFunctionInfo(EDGE_COUNT_FROM_HEADER),
                            List.of(new MutableObject<>(new VariableReferenceExpression(pathVariable))));
                    callFinder.get().setValue(countPathHopsExpr);
                };

                // We expect our argument of LEN to be another function call (EDGES or FIELD-ACCESSOR).
                AbstractFunctionCallExpression lenCallExpr =
                        (AbstractFunctionCallExpression) callFinder.get().getValue();
                ILogicalExpression lenCallArgExpr = lenCallExpr.getArguments().get(0).getValue();
                if (lenCallArgExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                    return false;
                }
                ILogicalExpression yieldPathOrVarExpr;
                AbstractFunctionCallExpression lenCallFuncArgExpr = (AbstractFunctionCallExpression) lenCallArgExpr;
                if (lenCallFuncArgExpr.getFunctionIdentifier().equals(FIELD_ACCESS_BY_NAME)) {
                    // Case #1: LEN(FIELD_ACCESS($v, "Edges"))
                    ILogicalExpression nameArgExpr = lenCallFuncArgExpr.getArguments().get(1).getValue();
                    if (nameArgExpr.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                        return false;
                    }
                    ConstantExpression nameArgConstExpr = (ConstantExpression) nameArgExpr;
                    AString nameString = (AString) ((AsterixConstantValue) nameArgConstExpr.getValue()).getObject();
                    if (!nameString.getStringValue().equals(TranslatePathTypeComputer.EDGES_FIELD_NAME)) {
                        return false;
                    }
                    yieldPathOrVarExpr = lenCallFuncArgExpr.getArguments().get(0).getValue();

                } else if (lenCallFuncArgExpr.getFunctionIdentifier().equals(FIELD_ACCESS_BY_INDEX)) {
                    // Case #2: LEN(FIELD_ACCESS($v, 1))
                    ILogicalExpression indexArgExpr = lenCallFuncArgExpr.getArguments().get(1).getValue();
                    if (indexArgExpr.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                        return false;
                    }
                    ConstantExpression indexArgConstExpr = (ConstantExpression) indexArgExpr;
                    AInt32 indexInt = (AInt32) ((AsterixConstantValue) indexArgConstExpr.getValue()).getObject();
                    if (indexInt.getIntegerValue() != TranslatePathTypeComputer.DEFAULT_RECORD_TYPE
                            .getFieldIndex(TranslatePathTypeComputer.EDGES_FIELD_NAME)) {
                        return false;
                    }
                    yieldPathOrVarExpr = lenCallFuncArgExpr.getArguments().get(0).getValue();

                } else if (lenCallFuncArgExpr.getFunctionIdentifier().equals(GraphixFunctionIdentifiers.PATH_EDGES)) {
                    // Case #3: LEN(EDGES($v))
                    yieldPathOrVarExpr = lenCallFuncArgExpr.getArguments().get(0).getValue();

                } else {
                    return false;
                }

                // Is our TRANSLATE_PATH call inlined? If so, we can grab the path variable from here.
                if (yieldPathOrVarExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    functionTransformer.accept((AbstractFunctionCallExpression) yieldPathOrVarExpr);
                    return true;
                }

                // Our TRANSLATE_PATH call is not inlined. Grab the variable used by PATH_HOP_COUNT.
                VariableReferenceExpression fromCallVarExpr = (VariableReferenceExpression) yieldPathOrVarExpr;
                LogicalVariable fromCallUsedVariable = fromCallVarExpr.getVariableReference();

                // Find the matching TRANSLATE_PATH function call.
                ILogicalOperator opToSearchUnder = workingOp;
                while (opToSearchUnder.hasInputs()) {
                    DownstreamFunctionFinder translateCallFinder =
                            new DownstreamFunctionFinder(TRANSLATE_PATH_IDENTIFIERS);
                    if (translateCallFinder.search(opToSearchUnder)) {
                        ILogicalOperator containingOp = translateCallFinder.getContainingOp();
                        AssignOperator containingAssign = (AssignOperator) containingOp;

                        if (containingAssign.getVariables().stream().anyMatch(fromCallUsedVariable::equals)) {
                            // We have found the matching TRANSLATE_PATH function call. Use the argument variable here.
                            ILogicalExpression translatePathExpr = translateCallFinder.get().getValue();
                            functionTransformer.accept((AbstractFunctionCallExpression) translatePathExpr);
                            return true;

                        } else {
                            // Our path variable is beneath our working loop.
                            opToSearchUnder = containingOp.getInputs().get(0).getValue();
                        }
                    }
                }
                throw new AlgebricksException(ErrorCode.ILLEGAL_STATE, "No TRANSLATE_PATH calls found!");
            }
            return false;
        }

        private boolean transformPathVertices(Mutable<ILogicalExpression> exprRef) {
            InExpressionFunctionFinder callFinder =
                    new InExpressionFunctionFinder(GraphixFunctionIdentifiers.PATH_VERTICES);
            if (callFinder.search(exprRef)) {
                Mutable<ILogicalExpression> pathVerticesExprRef = callFinder.get();
                ILogicalExpression pathVerticesExpr = pathVerticesExprRef.getValue();

                // Build our argument list.
                List<Mutable<ILogicalExpression>> fieldAccessArgList = new ArrayList<>();
                fieldAccessArgList.add(((AbstractFunctionCallExpression) pathVerticesExpr).getArguments().get(0));
                fieldAccessArgList.add(new MutableObject<>(new ConstantExpression(
                        new AsterixConstantValue(new AString(TranslatePathTypeComputer.VERTICES_FIELD_NAME)))));

                // Replace our function call.
                AbstractFunctionCallExpression fieldAccessExpr = new ScalarFunctionCallExpression(
                        BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.FIELD_ACCESS_BY_NAME),
                        fieldAccessArgList);
                pathVerticesExprRef.setValue(fieldAccessExpr);
                return true;
            }
            return false;
        }

        private boolean transformPathEdges(Mutable<ILogicalExpression> exprRef) {
            InExpressionFunctionFinder callFinder =
                    new InExpressionFunctionFinder(GraphixFunctionIdentifiers.PATH_EDGES);
            if (callFinder.search(exprRef)) {
                Mutable<ILogicalExpression> pathEdgesExprRef = callFinder.get();
                ILogicalExpression pathEdgesExpr = pathEdgesExprRef.getValue();

                // Build our argument list.
                List<Mutable<ILogicalExpression>> fieldAccessArgList = new ArrayList<>();
                fieldAccessArgList.add(((AbstractFunctionCallExpression) pathEdgesExpr).getArguments().get(0));
                fieldAccessArgList.add(new MutableObject<>(new ConstantExpression(
                        new AsterixConstantValue(new AString(TranslatePathTypeComputer.EDGES_FIELD_NAME)))));

                // Replace our function call.
                AbstractFunctionCallExpression fieldAccessExpr = new ScalarFunctionCallExpression(
                        BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.FIELD_ACCESS_BY_NAME),
                        fieldAccessArgList);
                pathEdgesExprRef.setValue(fieldAccessExpr);
                return true;
            }
            return false;
        }
    }
}
