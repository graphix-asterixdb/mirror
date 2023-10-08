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
package org.apache.asterix.graphix.algebra.variable;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.algebra.annotations.FixedPointOperatorAnnotations;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.FixedPointOperator;

public class LoopVariableCopyCallback implements FixedPointOperator.IIsomorphismCopyCallback {
    private Map<LogicalVariable, LogicalVariable> inputToOutputMap;
    private FixedPointOperator inputFpOp;
    private FixedPointOperator outputFpOp;
    private boolean isPropagated = false;

    @Override
    public void accept(Map<LogicalVariable, LogicalVariable> inputToOutputMap, FixedPointOperator inputFpOp,
            FixedPointOperator outputFpOp) throws AlgebricksException {
        this.inputToOutputMap = Objects.requireNonNull(inputToOutputMap);
        this.inputFpOp = Objects.requireNonNull(inputFpOp);
        this.outputFpOp = Objects.requireNonNull(outputFpOp);

        // If our output is copied again, then we'll need to propagate these variables as well.
        this.outputFpOp.setOnIsomorphicCopyCallback(new LoopVariableCopyCallback());
    }

    public boolean propagate() throws AlgebricksException {
        if (isPropagated || inputToOutputMap == null || inputFpOp == null || outputFpOp == null) {
            return false;
        }

        // Fetch our old context instance.
        LoopVariableContext oldLoopVariableContext = FixedPointOperatorAnnotations.getLoopVariableContext(inputFpOp);
        List<LogicalVariable> oldStartingVariables = oldLoopVariableContext.getStartingVariables();
        List<LogicalVariable> oldEndingVariables = oldLoopVariableContext.getEndingVariables();
        List<LogicalVariable> oldDownVariables = oldLoopVariableContext.getDownVariables();
        LogicalVariable oldSchemaVariable = oldLoopVariableContext.getSchemaVariable();
        LogicalVariable oldPathVariable = oldLoopVariableContext.getPathVariable();

        // All of our old variables should have an entry in our replacement map.
        Stream<LogicalVariable> oldVariableStream = oldStartingVariables.stream();
        oldVariableStream = Stream.concat(oldVariableStream, oldEndingVariables.stream());
        oldVariableStream = Stream.concat(oldVariableStream, oldDownVariables.stream());
        oldVariableStream = Stream.concat(oldVariableStream, Stream.of(oldSchemaVariable));
        oldVariableStream = Stream.concat(oldVariableStream, Stream.of(oldPathVariable));
        for (Iterator<LogicalVariable> it = oldVariableStream.iterator(); it.hasNext();) {
            LogicalVariable workingOldVariable = it.next();
            if (!inputToOutputMap.containsKey(workingOldVariable)) {
                throw new CompilationException(ErrorCode.ILLEGAL_STATE,
                        String.format("Variable %s not found?", workingOldVariable.toString()));
            }
        }

        // Apply our map to generate a new context instance.
        LoopVariableContext newLoopVariableContext =
                new LoopVariableContext(inputToOutputMap.get(oldPathVariable), inputToOutputMap.get(oldSchemaVariable),
                        oldEndingVariables.stream().map(inputToOutputMap::get).collect(Collectors.toList()),
                        oldStartingVariables.stream().map(inputToOutputMap::get).collect(Collectors.toList()),
                        oldDownVariables.stream().map(inputToOutputMap::get).collect(Collectors.toList()));
        FixedPointOperatorAnnotations.setLoopVariableContext(outputFpOp, newLoopVariableContext);
        isPropagated = true;
        return true;
    }
}
