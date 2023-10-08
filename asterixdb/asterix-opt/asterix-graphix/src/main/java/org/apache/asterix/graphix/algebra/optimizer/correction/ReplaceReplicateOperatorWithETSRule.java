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
package org.apache.asterix.graphix.algebra.optimizer.correction;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Initial translation-cleanup rule to a) avoid replicating from ETS, and b) set the REPLICATE outputs.
 */
public class ReplaceReplicateOperatorWithETSRule implements IAlgebraicRewriteRule {
    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        List<Mutable<ILogicalOperator>> opInputs = opRef.getValue().getInputs();
        List<Mutable<ILogicalOperator>> matchingInputs = IntStream.range(0, opInputs.size())
                .filter(i -> opInputs.get(i).getValue().getOperatorTag() == LogicalOperatorTag.REPLICATE).boxed()
                .map(opInputs::get).collect(Collectors.toList());
        if (matchingInputs.isEmpty()) {
            return false;
        }

        // We have found a REPLICATE input operator.
        boolean isChanged = false;
        for (Mutable<ILogicalOperator> replicateOpRef : matchingInputs) {
            ReplicateOperator replicateOp = (ReplicateOperator) replicateOpRef.getValue();
            ILogicalOperator replicateInputOp = replicateOp.getInputs().get(0).getValue();
            if (replicateInputOp.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE) {
                // We have found a useless REPLICATE. Prune this.
                replicateOpRef.setValue(new EmptyTupleSourceOperator());
                isChanged = true;

            } else {
                // We need this REPLICATE. Ensure that our outputs are appropriately mapped.
                if (replicateOp.getOutputs().stream().noneMatch(r -> r.getValue().equals(opRef.getValue()))) {
                    replicateOp.getOutputs().add(new MutableObject<>(opRef.getValue()));
                }
            }
        }

        // (we won't update our types, as this is meant to be an initial-translation-fix).
        return isChanged;
    }
}
