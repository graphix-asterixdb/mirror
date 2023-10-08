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
package org.apache.asterix.graphix.algebra.optimizer.typing;

import static org.apache.asterix.graphix.function.GraphixFunctionIdentifiers.APPEND_TO_EXISTING_PATH;
import static org.apache.asterix.graphix.function.GraphixFunctionIdentifiers.TRANSLATE_FORWARD_PATH;
import static org.apache.asterix.graphix.function.GraphixFunctionIdentifiers.TRANSLATE_REVERSE_PATH;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import org.apache.asterix.graphix.algebra.finder.DownstreamFunctionFinder;
import org.apache.asterix.graphix.algebra.finder.InExpressionFunctionFinder;
import org.apache.asterix.graphix.function.GraphixFunctionIdentifiers;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.FixedPointOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.ErrorCode;

/**
 * For all {@link org.apache.asterix.graphix.function.GraphixFunctionIdentifiers#TRANSLATE_FORWARD_PATH} and
 * {@link GraphixFunctionIdentifiers#TRANSLATE_REVERSE_PATH} function calls, set their vertex and edge
 * {@link org.apache.asterix.om.types.IAType}s.
 */
public class SetTranslatePathTypeInformationRule implements IAlgebraicRewriteRule {
    private final FunctionIdentifier[] TRANSLATE_PATH_IDENTIFIERS =
            new FunctionIdentifier[] { TRANSLATE_FORWARD_PATH, TRANSLATE_REVERSE_PATH };

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }
        context.addToDontApplySet(this, op);

        // Does there exist a call to TRANSLATE_PATH?
        InExpressionFunctionFinder translatePathFinder = new InExpressionFunctionFinder(TRANSLATE_PATH_IDENTIFIERS);
        if (!op.acceptExpressionTransform(translatePathFinder::search)) {
            return false;
        }
        Mutable<ILogicalExpression> yieldPathRef = translatePathFinder.get();

        // We have found a matching function call. Now find the closest APPEND_TO_PATH call.
        Mutable<ILogicalExpression> appendToPathRef;
        ILogicalOperator containingAppendToPathOp;
        DownstreamFunctionFinder appendToPathClosestFinder = new DownstreamFunctionFinder(APPEND_TO_EXISTING_PATH);
        if (appendToPathClosestFinder.search(op)) {
            appendToPathRef = appendToPathClosestFinder.get();
            containingAppendToPathOp = appendToPathClosestFinder.getContainingOp();

        } else {
            // If we could not find such a call, then walk down to the closest FIXED-POINT operator.
            FixedPointOperator fpOp = null;
            Deque<ILogicalOperator> operatorStack = new ArrayDeque<>(List.of(op));
            while (!operatorStack.isEmpty()) {
                ILogicalOperator workingOp = operatorStack.removeLast();
                switch (workingOp.getOperatorTag()) {
                    case FIXED_POINT:
                        fpOp = (FixedPointOperator) workingOp;
                        break;

                    case NESTEDTUPLESOURCE:
                        NestedTupleSourceOperator ntsOp = (NestedTupleSourceOperator) workingOp;
                        operatorStack.addFirst(ntsOp.getDataSourceReference().getValue());
                        break;
                }
                for (Mutable<ILogicalOperator> inputRef : workingOp.getInputs()) {
                    operatorStack.addFirst(inputRef.getValue());
                }
            }
            if (fpOp == null) {
                throw new AlgebricksException(ErrorCode.ILLEGAL_STATE, "FIXED-POINT operator not found!");
            }

            // From the nested-plan of our FIXED-POINT, find the closest APPEND_TO_PATH call.
            ILogicalOperator rootOp = fpOp.getNestedPlans().get(0).getRoots().get(0).getValue();
            DownstreamFunctionFinder underFpAppendFinder = new DownstreamFunctionFinder(APPEND_TO_EXISTING_PATH);
            if (!underFpAppendFinder.search(rootOp)) {
                throw new AlgebricksException(ErrorCode.ILLEGAL_STATE, "APPEND-TO-PATH call not found!");
            }
            appendToPathRef = underFpAppendFinder.get();
            containingAppendToPathOp = underFpAppendFinder.getContainingOp();
        }
        AbstractFunctionCallExpression appendToPath = (AbstractFunctionCallExpression) appendToPathRef.getValue();
        ILogicalExpression vertexExpr = appendToPath.getArguments().get(0).getValue();
        ILogicalExpression edgeExpr = appendToPath.getArguments().get(1).getValue();

        // Attach the types of our vertices and edges to our TRANSLATE_PATH call.
        IVariableTypeEnvironment typeEnvironment = containingAppendToPathOp.computeInputTypeEnvironment(context);
        IAType vertexType = (IAType) typeEnvironment.getType(vertexExpr);
        IAType edgeType = (IAType) typeEnvironment.getType(edgeExpr);
        Object[] opaqueParameters = new Object[] { vertexType, edgeType };
        ((AbstractFunctionCallExpression) yieldPathRef.getValue()).setOpaqueParameters(opaqueParameters);
        return true;
    }
}
