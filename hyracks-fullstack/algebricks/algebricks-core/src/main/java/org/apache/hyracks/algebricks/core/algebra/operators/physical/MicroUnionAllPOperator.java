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

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.algebricks.runtime.operators.union.MicroUnionAllRuntimeFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;

public class MicroUnionAllPOperator extends AbstractUnionAllPOperator {

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.MICRO_UNION_ALL;
    }

    @Override
    public boolean isMicroOperator() {
        return true;
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        RecordDescriptor recordDescriptor =
                JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), opSchema, context);

        MicroUnionAllRuntimeFactory runtime = new MicroUnionAllRuntimeFactory(op.getInputs().size());
        runtime.setSourceLocation(op.getSourceLocation());
        builder.contributeMicroOperator(op, runtime, recordDescriptor);

        super.contributeRuntimeOperator(builder, context, op, opSchema, inputSchemas, outerPlanSchema);
    }
}
