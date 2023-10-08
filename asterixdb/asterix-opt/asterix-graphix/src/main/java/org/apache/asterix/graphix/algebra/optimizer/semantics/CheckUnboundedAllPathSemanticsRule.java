/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.graphix.algebra.optimizer.semantics;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.algebra.finder.InExpressionFunctionFinder;
import org.apache.asterix.graphix.function.GraphixFunctionIdentifiers;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.RecursiveHeadOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.StateReleasePOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;

public class CheckUnboundedAllPathSemanticsRule implements IAlgebraicRewriteRule {
    // The following is set in the configuration file or with a SET statement.
    public static final String UAP_OPTION_KEY_NAME = "graphix.compiler.permit.unbounded-all-paths";

    // By default, we will disable unbounded ALL-PATHs that haven't been reduced to CHEAPEST/ANY PATHs.
    private static final String UAP_OPTION_DEFAULT_VALUE = Boolean.FALSE.toString();

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (opRef.getValue().getOperatorTag() != LogicalOperatorTag.RECURSIVE_HEAD) {
            return false;
        }
        RecursiveHeadOperator headOp = (RecursiveHeadOperator) opRef.getValue();

        // Before exploring our plan in-the-loop, check our option...
        PhysicalOptimizationConfig physicalOptimizationConfig = context.getPhysicalOptimizationConfig();
        Object propertyValue = physicalOptimizationConfig.getExtensionProperty(UAP_OPTION_KEY_NAME);
        boolean areUnboundedAllPathsPermitted =
                Boolean.parseBoolean((String) Objects.requireNonNullElse(propertyValue, UAP_OPTION_DEFAULT_VALUE));
        if (areUnboundedAllPathsPermitted) {
            return false;
        }

        // ...otherwise, we need to perform our check.
        List<ILogicalOperator> operatorsInLoop = new ArrayList<>();
        OperatorPropertiesUtil.getOperatorsInLoop(headOp, operatorsInLoop, true);
        for (ILogicalOperator workingOp : operatorsInLoop) {
            switch (workingOp.getOperatorTag()) {
                case DISTINCT:
                    DistinctOperator distinctOp = (DistinctOperator) workingOp;
                    IPhysicalOperator distinctPOp = distinctOp.getPhysicalOperator();
                    if (distinctPOp.getOperatorTag() != PhysicalOperatorTag.STATE_RELEASE) {
                        break;
                    }
                    StateReleasePOperator stateReleasePOp = (StateReleasePOperator) distinctPOp;
                    if (stateReleasePOp.getPhysicalDelegateOp().getOperatorTag() == PhysicalOperatorTag.LOCAL_TOP_K) {
                        return false;
                    }
                    break;

                case SELECT:
                    SelectOperator selectOp = (SelectOperator) workingOp;
                    MutableBoolean isEdgeCountFilterFound = new MutableBoolean(false);
                    new InExpressionFunctionFinder(BuiltinFunctions.LE).searchAndUse(selectOp.getCondition(), e -> {
                        FunctionIdentifier id = GraphixFunctionIdentifiers.EDGE_COUNT_FROM_HEADER;
                        isEdgeCountFilterFound.setValue(new InExpressionFunctionFinder(id).search(e));
                    });
                    if (isEdgeCountFilterFound.isTrue()) {
                        return false;
                    }
                    break;
            }
        }
        throw new CompilationException(ErrorCode.COMPILATION_ERROR, headOp.getSourceLocation(),
                "Encountered an unbounded path with ALL-PATH semantics!");
    }
}
