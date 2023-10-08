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

import static org.apache.asterix.graphix.function.GraphixFunctionIdentifiers.APPEND_TO_EXISTING_PATH;
import static org.apache.asterix.graphix.function.GraphixFunctionIdentifiers.CREATE_NEW_ZERO_HOP_PATH;
import static org.apache.asterix.graphix.function.GraphixFunctionIdentifiers.IS_DISTINCT_;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.asterix.graphix.algebra.finder.DownstreamFunctionFinder;
import org.apache.asterix.graphix.algebra.finder.PrimaryKeyVariableFinder;
import org.apache.asterix.graphix.function.GraphixFunctionIdentifiers;
import org.apache.asterix.graphix.lang.hint.PropertiesRequiredAnnotation;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.FixedPointOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.ErrorCode;

public class ReplacePathFunctionArgumentsRule implements IAlgebraicRewriteRule {
    private final PathBuilderTransform pathBuilderTransform = new PathBuilderTransform();

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (opRef.getValue().getOperatorTag() != LogicalOperatorTag.FIXED_POINT) {
            return false;
        }
        FixedPointOperator fpOp = (FixedPointOperator) opRef.getValue();

        // We have found a FIXED-POINT. Walk down to the nearest CREATE_PATH call.
        ILogicalOperator anchorOp = fpOp.getInputs().get(0).getValue();
        DownstreamFunctionFinder createPathFinder = new DownstreamFunctionFinder(CREATE_NEW_ZERO_HOP_PATH);
        if (!createPathFinder.search(anchorOp)) {
            throw new AlgebricksException(ErrorCode.ILLEGAL_STATE, "Could not find CREATE_PATH!");
        }
        AssignOperator createPathAssignOp = (AssignOperator) createPathFinder.getContainingOp();

        // Now walk down to the nearest APPEND_PATH call...
        ILogicalOperator rootOp = fpOp.getNestedPlans().get(0).getRoots().get(0).getValue();
        DownstreamFunctionFinder appendCallFinder = new DownstreamFunctionFinder(APPEND_TO_EXISTING_PATH);
        if (!appendCallFinder.search(rootOp)) {
            throw new AlgebricksException(ErrorCode.ILLEGAL_STATE, "Could not find APPEND_TO_PATH!");
        }
        AssignOperator appendPathAssignOp = (AssignOperator) appendCallFinder.getContainingOp();

        // ...and our cycle constraint call.
        DownstreamFunctionFinder cycleConstraintFinder = new DownstreamFunctionFinder(IS_DISTINCT_);
        if (!cycleConstraintFinder.search(rootOp)) {
            throw new AlgebricksException(ErrorCode.ILLEGAL_STATE, "Could not find CYCLE_CONSTRAINT!");
        }

        // Remember these calls.
        pathBuilderTransform.gatherPathFunctionCalls(createPathAssignOp, appendPathAssignOp,
                (ScalarFunctionCallExpression) createPathFinder.get().getValue(),
                (ScalarFunctionCallExpression) appendCallFinder.get().getValue(),
                (ScalarFunctionCallExpression) cycleConstraintFinder.get().getValue());
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (context.checkIfInDontApplySet(this, opRef.getValue())) {
            return false;
        }

        // Pass our expression transform.
        if (opRef.getValue().getOperatorTag() != LogicalOperatorTag.ASSIGN
                || !pathBuilderTransform.reset(context, (AssignOperator) opRef.getValue())
                || !opRef.getValue().acceptExpressionTransform(pathBuilderTransform)) {
            return false;
        }
        context.addToDontApplySet(this, opRef.getValue());
        OperatorManipulationUtil.computeTypeEnvironmentBottomUp(opRef.getValue(), context);
        return true;
    }

    private final static class PathBuilderTransform implements ILogicalExpressionReferenceTransform {
        private final Map<AssignOperator, BuilderContext> builderContextMap = new LinkedHashMap<>();

        private final static class BuilderContext {
            private List<LogicalVariable> createPathVertexPrimaryKeys;
            private List<LogicalVariable> appendPathVertexPrimaryKeys;
            private ScalarFunctionCallExpression createPathExpr;
            private ScalarFunctionCallExpression appendPathExpr;
            private ScalarFunctionCallExpression cycleConstraintPathExpr;
        }

        // The following are set on post-visit.
        private IOptimizationContext workingContext;
        private AssignOperator assignOp;

        public void gatherPathFunctionCalls(AssignOperator createPathAssignOp, AssignOperator appendPathAssignOp,
                ScalarFunctionCallExpression createPathExprCall, ScalarFunctionCallExpression appendPathExprCall,
                ScalarFunctionCallExpression cycleConstraintExprCall) {
            if (createPathExprCall.hasAnnotation(PropertiesRequiredAnnotation.class)
                    || appendPathExprCall.hasAnnotation(PropertiesRequiredAnnotation.class)) {
                // If our path is being used outside of checking for cycles, then we cannot replace our arguments.
                return;
            }

            BuilderContext builderContext = new BuilderContext();
            builderContext.createPathExpr = createPathExprCall;
            builderContext.appendPathExpr = appendPathExprCall;
            builderContext.cycleConstraintPathExpr = cycleConstraintExprCall;
            builderContext.createPathVertexPrimaryKeys = new ArrayList<>();
            builderContext.appendPathVertexPrimaryKeys = new ArrayList<>();
            builderContextMap.put(createPathAssignOp, builderContext);
            builderContextMap.put(appendPathAssignOp, builderContext);
        }

        public boolean reset(IOptimizationContext context, AssignOperator assignOp) {
            if (!builderContextMap.containsKey(assignOp)) {
                return false;
            }
            this.workingContext = context;
            this.assignOp = assignOp;
            return true;
        }

        @Override
        public boolean transform(Mutable<ILogicalExpression> exprRef) throws AlgebricksException {
            if (exprRef.getValue().getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                // We should not reach here (but this is more like a sanity check).
                return false;
            }
            BuilderContext builderContext = builderContextMap.get(assignOp);
            AtomicBoolean hasChanged = new AtomicBoolean(false);

            // Grab our primary keys.
            AbstractFunctionCallExpression funcCallExpr = (AbstractFunctionCallExpression) exprRef.getValue();
            FunctionIdentifier functionIdentifier = funcCallExpr.getFunctionIdentifier();
            if (Objects.equals(CREATE_NEW_ZERO_HOP_PATH, functionIdentifier)) {
                // We have a sole argument. See if we can grab the primary keys here.
                ILogicalExpression destArgVertexExpr = funcCallExpr.getArguments().get(0).getValue();
                if (destArgVertexExpr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                    return false;
                }
                VariableReferenceExpression destArgVertexVarExpr = (VariableReferenceExpression) destArgVertexExpr;
                LogicalVariable destArgVertexVariable = destArgVertexVarExpr.getVariableReference();
                PrimaryKeyVariableFinder primaryKeyVariableFinder =
                        new PrimaryKeyVariableFinder(workingContext, assignOp);
                if (!primaryKeyVariableFinder.search(destArgVertexVariable)) {
                    // If we cannot find the primary key here, exit here.
                    return false;
                }
                builderContext.createPathVertexPrimaryKeys.addAll(primaryKeyVariableFinder.get());

            } else if (Objects.equals(APPEND_TO_EXISTING_PATH, functionIdentifier)) {
                // We have two arguments to consider. See if we can grab the primary keys here (start with our dest).
                ILogicalExpression destVertexArgExpr = funcCallExpr.getArguments().get(0).getValue();
                if (destVertexArgExpr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                    VariableReferenceExpression destVertexArgVarExpr = (VariableReferenceExpression) destVertexArgExpr;
                    LogicalVariable destVertexArgVariable = destVertexArgVarExpr.getVariableReference();
                    new PrimaryKeyVariableFinder(workingContext, assignOp).searchAndUse(destVertexArgVariable,
                            builderContext.appendPathVertexPrimaryKeys::addAll);
                }

                // Now try to grab the primary keys of our edge.
                ILogicalExpression edgeArgExpr = funcCallExpr.getArguments().get(1).getValue();
                if (edgeArgExpr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                    VariableReferenceExpression edgeArgVarExpr = (VariableReferenceExpression) edgeArgExpr;
                    LogicalVariable edgeArgVariable = edgeArgVarExpr.getVariableReference();
                    PrimaryKeyVariableFinder primaryKeyVariableFinder =
                            new PrimaryKeyVariableFinder(workingContext, assignOp);
                    primaryKeyVariableFinder.searchAndUse(edgeArgVariable, pkList -> {
                        if (pkList.size() == 1) {
                            // We will eagerly apply our transform here, iff we have a single primary key.
                            ILogicalExpression varRefExpr = new VariableReferenceExpression(pkList.get(0));
                            funcCallExpr.getArguments().get(1).setValue(varRefExpr);
                            hasChanged.set(true);
                        }
                    });
                }

            } else {
                return false;
            }

            // Try to change our vertex arguments. We will only do this if we find primary keys for both calls.
            hasChanged.set(hasChanged.get() | transformVerticesInCall(builderContext));
            return hasChanged.get();
        }

        private boolean transformVerticesInCall(BuilderContext builderContext) {
            // For now, we'll only perform this transform if we have a sole primary key for both calls.
            if (builderContext.createPathVertexPrimaryKeys.size() != 1
                    || builderContext.appendPathVertexPrimaryKeys.size() != 1) {
                return false;
            }

            // Apply our transformation for our CREATE_PATH and APPEND_TO_PATH calls.
            LogicalVariable createPKVar = builderContext.createPathVertexPrimaryKeys.get(0);
            LogicalVariable appendPKVar = builderContext.appendPathVertexPrimaryKeys.get(0);
            Mutable<ILogicalExpression> createPathVertexExprRef = builderContext.createPathExpr.getArguments().get(0);
            Mutable<ILogicalExpression> appendPathVertexExprRef = builderContext.appendPathExpr.getArguments().get(0);
            VariableReferenceExpression createPKVarRefExpr = new VariableReferenceExpression(createPKVar);
            VariableReferenceExpression appendPKVarRefExpr = new VariableReferenceExpression(appendPKVar);
            createPathVertexExprRef.setValue(createPKVarRefExpr);
            appendPathVertexExprRef.setValue(appendPKVarRefExpr);

            // Apply our transform to our cycle constraint call (if applicable).
            ScalarFunctionCallExpression cycleConstraintPathExpr = builderContext.cycleConstraintPathExpr;
            FunctionIdentifier cycleConstraintId = cycleConstraintPathExpr.getFunctionIdentifier();
            if (cycleConstraintId.equals(GraphixFunctionIdentifiers.IS_DISTINCT_VERTEX)
                    || cycleConstraintId.equals(GraphixFunctionIdentifiers.IS_DISTINCT_EVERYTHING)) {
                Mutable<ILogicalExpression> vertexExprRef = cycleConstraintPathExpr.getArguments().get(0);
                VariableReferenceExpression newVertexVarRefExpr = new VariableReferenceExpression(appendPKVar);
                vertexExprRef.setValue(newVertexVarRefExpr);
            }
            return true;
        }
    }
}
