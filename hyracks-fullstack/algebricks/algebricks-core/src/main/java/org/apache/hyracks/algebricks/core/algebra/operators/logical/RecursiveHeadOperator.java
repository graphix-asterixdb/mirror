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
package org.apache.hyracks.algebricks.core.algebra.operators.logical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;
import org.apache.hyracks.api.exceptions.ErrorCode;

public class RecursiveHeadOperator extends AbstractLogicalOperator {
    private final Map<LogicalVariable, LogicalVariable> recursiveOutputMap;
    private Map<LogicalVariable, LogicalVariable> invertedOutputMap;

    public RecursiveHeadOperator(Map<LogicalVariable, LogicalVariable> recursiveOutputMap) {
        this.recursiveOutputMap = Objects.requireNonNull(recursiveOutputMap);
        this.invertedOutputMap = recursiveOutputMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    }

    public Map<LogicalVariable, LogicalVariable> getRecursiveOutputMap() {
        return Collections.unmodifiableMap(recursiveOutputMap);
    }

    public Map<LogicalVariable, LogicalVariable> getInvertedOutputMap() {
        return Collections.unmodifiableMap(invertedOutputMap);
    }

    public void replaceOutputMap(Map<LogicalVariable, LogicalVariable> recursiveOutputMap) {
        this.recursiveOutputMap.clear();
        this.recursiveOutputMap.putAll(Objects.requireNonNull(recursiveOutputMap));
        this.invertedOutputMap = recursiveOutputMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    }

    @Override
    public void recomputeSchema() throws AlgebricksException {
        // First, compute our schema from our direct input.
        schema = new ArrayList<>(inputs.get(0).getValue().getSchema());

        // Next, gather all operators in the loop.
        List<ILogicalOperator> operatorsInLoop = new ArrayList<>();
        OperatorPropertiesUtil.getOperatorsInLoop(this, operatorsInLoop, false);

        // Finally, iterate through each operator to recompute its schema.
        for (int i = operatorsInLoop.size() - 1; i >= 0; i--) {
            ILogicalOperator workingOp = operatorsInLoop.get(i);
            if (workingOp.getOperatorTag() == LogicalOperatorTag.RECURSIVE_HEAD) {
                schema = new ArrayList<>(inputs.get(0).getValue().getSchema());
            } else {
                workingOp.recomputeSchema();
            }
        }
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform transform)
            throws AlgebricksException {
        return false;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitRecursiveHeadOperator(this, arg);
    }

    @Override
    public boolean isMap() {
        return false;
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return VariablePropagationPolicy.ALL;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        // Gather all operators in the loop, and locate our recursive tail + fixed point.
        List<ILogicalOperator> operatorsInLoop = new ArrayList<>();
        OperatorPropertiesUtil.getOperatorsInLoop(this, operatorsInLoop, false);
        RecursiveTailOperator tailOp = (RecursiveTailOperator) operatorsInLoop.stream()
                .filter(op -> op.getOperatorTag() == LogicalOperatorTag.RECURSIVE_TAIL).findFirst()
                .orElseThrow(() -> new AlgebricksException(ErrorCode.ILLEGAL_STATE, "RECURSIVE_TAIL not found!"));

        // Next, we can make our rounds. For our first pass, we'll compute our recursive tail using our anchor input.
        for (int i = operatorsInLoop.size() - 1; i >= 0; i--) {
            ILogicalOperator workingOp = operatorsInLoop.get(i);
            IVariableTypeEnvironment generatedEnv;
            switch (workingOp.getOperatorTag()) {
                case RECURSIVE_TAIL:
                    generatedEnv = tailOp.computeFirstPassTypeEnvironment(ctx);
                    break;
                case RECURSIVE_HEAD:
                    generatedEnv = createPropagatingAllInputsTypeEnvironment(ctx);
                    break;
                default:
                    generatedEnv = workingOp.computeOutputTypeEnvironment(ctx);
                    break;
            }
            ctx.setOutputTypeEnvironment(workingOp, generatedEnv);
        }

        // Repeat this process again using the head environment we just created (this is our second pass).
        for (int i = operatorsInLoop.size() - 1; i >= 0; i--) {
            ILogicalOperator workingOp = operatorsInLoop.get(i);
            IVariableTypeEnvironment generatedEnv;
            switch (workingOp.getOperatorTag()) {
                case RECURSIVE_TAIL:
                    generatedEnv = tailOp.computeSecondPassTypeEnvironment(ctx);
                    break;
                case RECURSIVE_HEAD:
                    generatedEnv = createPropagatingAllInputsTypeEnvironment(ctx);
                    break;
                default:
                    generatedEnv = workingOp.computeOutputTypeEnvironment(ctx);
                    break;
            }
            ctx.setOutputTypeEnvironment(workingOp, generatedEnv);
        }
        return ctx.getOutputTypeEnvironment(this);
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.RECURSIVE_HEAD;
    }
}
