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
package org.apache.asterix.graphix.algebra.optimizer.cleanup;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.asterix.graphix.algebra.annotations.FixedPointOperatorAnnotations;
import org.apache.asterix.graphix.algebra.variable.LoopVariableContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.FixedPointOperator;
import org.apache.hyracks.algebricks.rewriter.rules.RemoveRedundantVariablesRule;

public class RemoveRedundantGraphixVariablesRule extends RemoveRedundantVariablesRule {
    @Override
    protected boolean replaceFixedPointVars(FixedPointOperator fpOp) {
        if (super.replaceFixedPointVars(fpOp)) {
            // We also need to update our loop variable context. We'll start with our path variable...
            LoopVariableContext loopVariableContext = FixedPointOperatorAnnotations.getLoopVariableContext(fpOp);
            LogicalVariable oldPathVariable = loopVariableContext.getPathVariable();
            LogicalVariable newPathVariable =
                    Objects.requireNonNullElse(findEquivalentRepresentativeVar(oldPathVariable), oldPathVariable);

            // ...and now our schema variable...
            LogicalVariable oldSchemaVariable = loopVariableContext.getSchemaVariable();
            LogicalVariable newSchemaVariable =
                    Objects.requireNonNullElse(findEquivalentRepresentativeVar(oldSchemaVariable), oldSchemaVariable);

            // ...and now our destination variables...
            List<LogicalVariable> newDestinationVariables = loopVariableContext.getEndingVariables().stream().map(v -> {
                LogicalVariable representativeVar = findEquivalentRepresentativeVar(v);
                return Objects.requireNonNullElse(representativeVar, v);
            }).collect(Collectors.toList());

            // ...and our downstream variables...
            List<LogicalVariable> newDownVariables = loopVariableContext.getDownVariables().stream().map(v -> {
                LogicalVariable representativeVar = findEquivalentRepresentativeVar(v);
                return Objects.requireNonNullElse(representativeVar, v);
            }).collect(Collectors.toList());

            // ...and finally our source variables.
            List<LogicalVariable> newSourceVariables = loopVariableContext.getStartingVariables().stream().map(v -> {
                LogicalVariable representativeVar = findEquivalentRepresentativeVar(v);
                return Objects.requireNonNullElse(representativeVar, v);
            }).collect(Collectors.toList());
            LoopVariableContext newLoopVariableContext = new LoopVariableContext(newPathVariable, newSchemaVariable,
                    newDestinationVariables, newSourceVariables, newDownVariables);
            FixedPointOperatorAnnotations.setLoopVariableContext(fpOp, newLoopVariableContext);
            return true;
        }
        return false;
    }

}
