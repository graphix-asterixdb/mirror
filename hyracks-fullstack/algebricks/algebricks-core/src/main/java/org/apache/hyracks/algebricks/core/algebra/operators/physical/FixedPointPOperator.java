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

import java.util.ArrayList;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.FixedPointOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalMemoryRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.RandomPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.iteration.FixedPointOperatorDescriptor;

public class FixedPointPOperator extends AbstractPhysicalOperator {
    private final byte markerDesignation;

    public FixedPointPOperator(byte markerDesignation) {
        this.markerDesignation = markerDesignation;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.FIXED_POINT;
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
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) throws AlgebricksException {
        // We do not require any properties from our children.
        return emptyUnaryRequirements();
    }

    @Override
    public void createLocalMemoryRequirements(ILogicalOperator op) {
        // We need at least **one** frame to buffer our input, and one output frame.
        localMemoryRequirements = LocalMemoryRequirements.variableMemoryBudget(2);
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        // TODO (GLENN): We might be able to do better here... our anchor and recursive deliver different properties.
        RandomPartitioningProperty prop = new RandomPartitioningProperty(context.getComputationNodeDomain());
        deliveredProperties = new StructuralPropertiesVector(prop, new ArrayList<>());
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        // We will not compile our nested plans here... we will defer this compilation to our recursive head.
        FixedPointOperator fpOp = (FixedPointOperator) op;
        if (fpOp.getNestedPlans().size() != 1 || fpOp.getNestedPlans().get(0).getRoots().size() != 1) {
            throw new AlgebricksException(ErrorCode.ILLEGAL_STATE, "Cannot compile nested plan with multiple roots!");
        }

        // Contribute our FP operator.
        IVariableTypeEnvironment fpTypeEnv = context.getTypeEnvironment(fpOp);
        JobSpecification spec = builder.getJobSpec();
        RecordDescriptor recDesc = JobGenHelper.mkRecordDescriptor(fpTypeEnv, opSchema, context);
        FixedPointOperatorDescriptor fpOpDesc = new FixedPointOperatorDescriptor(spec, recDesc,
                getLocalMemoryRequirements().getMemoryBudgetInFrames(), markerDesignation);
        contributeOpDesc(builder, fpOp, fpOpDesc);

        // Connect our **anchor** input operator to our FP operator.
        ILogicalOperator anchorOp = fpOp.getInputs().get(0).getValue();
        builder.contributeGraphEdge(anchorOp, 0, fpOp, FixedPointOperatorDescriptor.ANCHOR_INPUT_INDEX);
    }
}
