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
package org.apache.asterix.graphix.algebra.finder;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;

public class ProducerOperatorFinder implements IAlgebraicEntityFinder<LogicalVariable, ILogicalOperator> {
    private final ILogicalOperator parentOp;
    private ILogicalOperator producerOp;

    public ProducerOperatorFinder(ILogicalOperator parentOp) {
        this.parentOp = Objects.requireNonNull(parentOp);
    }

    @Override
    public boolean search(LogicalVariable searchingVariable) throws AlgebricksException {
        producerOp = findProducerOfVariable(parentOp, searchingVariable);
        return producerOp != null;
    }

    @Override
    public ILogicalOperator get() throws AlgebricksException {
        return producerOp;
    }

    private ILogicalOperator findProducerOfVariable(ILogicalOperator workingOp, LogicalVariable v)
            throws AlgebricksException {
        List<LogicalVariable> producedVariables = new ArrayList<>();
        VariableUtilities.getProducedVariables(workingOp, producedVariables);
        if (producedVariables.contains(v)) {
            return workingOp;
        }
        for (Mutable<ILogicalOperator> inputRef : workingOp.getInputs()) {
            ILogicalOperator producerOp = findProducerOfVariable(inputRef.getValue(), v);
            if (producerOp != null) {
                return producerOp;
            }
        }
        if (workingOp.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
            NestedTupleSourceOperator ntsOp = (NestedTupleSourceOperator) workingOp;
            ILogicalOperator ntsParentOp = ntsOp.getDataSourceReference().getValue();
            ILogicalOperator ntsInputOp = ntsParentOp.getInputs().get(0).getValue();
            return findProducerOfVariable(ntsInputOp, v);

        } else {
            return null;
        }
    }
}
