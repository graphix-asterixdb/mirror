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

import java.util.List;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.aggreg.SimpleAlgebricksAccumulatingAggregatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.group.MicroPreClusteredGroupRuntimeFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.dataflow.std.group.AbstractAggregatorDescriptorFactory;

public class MicroPreSortedDistinctByPOperator extends AbstractPreSortedDistinctByPOperator {

    public MicroPreSortedDistinctByPOperator(List<LogicalVariable> columnList) {
        super(columnList);
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.MICRO_PRE_SORTED_DISTINCT_BY;
    }

    @Override
    public boolean isMicroOperator() {
        return true;
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {

        int[] keysAndDecs = getKeysAndDecs(inputSchemas[0]);

        IBinaryComparatorFactory[] comparatorFactories = JobGenHelper
                .variablesToAscBinaryComparatorFactories(columnList, context.getTypeEnvironment(op), context);
        IAggregateEvaluatorFactory[] aggFactories = new IAggregateEvaluatorFactory[] {};
        AbstractAggregatorDescriptorFactory aggregatorFactory =
                new SimpleAlgebricksAccumulatingAggregatorFactory(aggFactories, keysAndDecs);
        aggregatorFactory.setSourceLocation(op.getSourceLocation());

        RecordDescriptor recordDescriptor =
                JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), opSchema, context);
        RecordDescriptor inputRecordDesc = JobGenHelper.mkRecordDescriptor(
                context.getTypeEnvironment(op.getInputs().get(0).getValue()), inputSchemas[0], context);

        /* make fd columns part of the key but the comparator only compares the distinct key columns */
        MicroPreClusteredGroupRuntimeFactory runtime = new MicroPreClusteredGroupRuntimeFactory(keysAndDecs,
                comparatorFactories, aggregatorFactory, inputRecordDesc, recordDescriptor, null, -1);
        runtime.setSourceLocation(op.getSourceLocation());
        builder.contributeMicroOperator(op, runtime, recordDescriptor);
        ILogicalOperator src = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, op, 0);
    }
}
