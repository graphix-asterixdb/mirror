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
package org.apache.hyracks.algebricks.core.algebra.operators.physical;

import static org.apache.hyracks.dataflow.std.iteration.FixedPointBaseOperatorDescriptor.RECURSIVE_INPUT_INDEX;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.FixedPointOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.algebricks.core.jobgen.impl.OperatorSchemaImpl;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.misc.ReplicateOperatorDescriptor;

public class RecursiveHeadPOperator extends AbstractPhysicalOperator {
    private final Map<Mutable<ILogicalOperator>, List<Mutable<ILogicalOperator>>> parentVisitMap = new HashMap<>();

    private FixedPointOperator fpOp;

    public void setFixedPointOp(FixedPointOperator fpOp) {
        this.fpOp = fpOp;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.RECURSIVE_HEAD;
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) throws AlgebricksException {
        return emptyUnaryRequirements();
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator inputOp = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        deliveredProperties = inputOp.getDeliveredPhysicalProperties().clone();
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return false;
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        JobSpecification spec = builder.getJobSpec();
        IVariableTypeEnvironment typeEnv = context.getTypeEnvironment(op);
        RecordDescriptor recDesc = JobGenHelper.mkRecordDescriptor(typeEnv, propagatedSchema, context);

        // Build our replicate operator.
        ReplicateOperatorDescriptor replicateOp = new ReplicateOperatorDescriptor(spec, recDesc, 2);
        replicateOp.setSourceLocation(op.getSourceLocation());
        contributeOpDesc(builder, (AbstractLogicalOperator) op, replicateOp);
        ILogicalOperator inputOp = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(inputOp, 0, op, 0);

        // Build a new 1:1 connector here.
        ExchangeOperator exchangeOp = new ExchangeOperator();
        exchangeOp.getInputs().add(new MutableObject<>(op));
        IConnectorDescriptor oneToOneConn = new OneToOneConnectorDescriptor(spec);
        IHyracksJobBuilder.TargetConstraint targetConstraint = IHyracksJobBuilder.TargetConstraint.SAME_COUNT;
        builder.contributeConnectorWithTargetConstraint(exchangeOp, oneToOneConn, targetConstraint);
        parentVisitMap.clear();

        // Walk down and assemble our plan in-the-loop.
        Mutable<ILogicalOperator> rootOpRef = fpOp.getNestedPlans().get(0).getRoots().get(0);
        compileOpRefInPlanLoop(rootOpRef, context, builder, propagatedSchema, outerPlanSchema);

        // Find the operator directly above our tail op (as our tail op has no runtime).
        ILogicalOperator aboveTailOp = null;
        Deque<ILogicalOperator> operatorStack = new ArrayDeque<>();
        operatorStack.add(rootOpRef.getValue());
        while (!operatorStack.isEmpty()) {
            ILogicalOperator workingOpInLoop = operatorStack.removeLast();
            if (workingOpInLoop.getInputs().stream().map(i -> i.getValue().getOperatorTag())
                    .anyMatch(LogicalOperatorTag.RECURSIVE_TAIL::equals)) {
                aboveTailOp = workingOpInLoop;
                break;
            }
            for (Mutable<ILogicalOperator> inputRef : workingOpInLoop.getInputs()) {
                operatorStack.addLast(inputRef.getValue());
            }
        }
        if (aboveTailOp == null) {
            throw new AlgebricksException(ErrorCode.ILLEGAL_STATE, "Could not find recursive tail operator!");
        }

        // Connect our replicate to our connector, then to our (above) tail op, then our fixed-point op.
        ILogicalOperator rootOp = rootOpRef.getValue();
        builder.contributeGraphEdge(op, 1, exchangeOp, 0);
        builder.contributeGraphEdge(exchangeOp, 0, aboveTailOp, 0);
        builder.contributeGraphEdge(rootOp, 0, fpOp, RECURSIVE_INPUT_INDEX);
    }

    private void compileOpRefInPlanLoop(Mutable<ILogicalOperator> opRef, JobGenContext context,
            IHyracksJobBuilder builder, IOperatorSchema headOpSchema, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        // We compile our plan on bottom-up (similar to PlanCompiler).
        ILogicalOperator op = opRef.getValue();
        IOperatorSchema[] schemas = new IOperatorSchema[op.getInputs().size()];
        for (int i = 0; i < op.getInputs().size(); i++) {
            Mutable<ILogicalOperator> inputOpRef = op.getInputs().get(i);
            List<Mutable<ILogicalOperator>> parentList = parentVisitMap.get(inputOpRef);
            if (parentList == null) {
                parentVisitMap.put(inputOpRef, new ArrayList<>());
                parentVisitMap.get(inputOpRef).add(opRef);
                compileOpRefInPlanLoop(inputOpRef, context, builder, headOpSchema, outerPlanSchema);

            } else if (!parentList.contains(opRef)) {
                // We should only compile operators once.
                parentList.add(opRef);
            }
            schemas[i] = context.getSchema(inputOpRef.getValue());
        }

        // We handle our recursive tail separately.
        IOperatorSchema opSchema = new OperatorSchemaImpl();
        context.putSchema(op, opSchema);
        if (op.getOperatorTag() == LogicalOperatorTag.RECURSIVE_TAIL) {
            op.getVariablePropagationPolicy().propagateVariables(opSchema, headOpSchema);
            op.contributeRuntimeOperator(builder, context, opSchema, new IOperatorSchema[] { headOpSchema },
                    outerPlanSchema);

        } else {
            op.getVariablePropagationPolicy().propagateVariables(opSchema, schemas);
            op.contributeRuntimeOperator(builder, context, opSchema, schemas, outerPlanSchema);
        }
    }
}
