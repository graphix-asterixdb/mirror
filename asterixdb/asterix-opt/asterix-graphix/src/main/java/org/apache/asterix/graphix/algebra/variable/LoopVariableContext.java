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

import static org.apache.asterix.graphix.algebra.variable.LoopVariableConstants.DEST_VARIABLES_INDEX;
import static org.apache.asterix.graphix.algebra.variable.LoopVariableConstants.PATH_VARIABLE_INDEX;
import static org.apache.asterix.graphix.algebra.variable.LoopVariableConstants.SCHEMA_VARIABLE_INDEX;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.FixedPointOperator;

public class LoopVariableContext {
    private final List<LogicalVariable> startingVariables = new ArrayList<>();
    private final List<LogicalVariable> endingVariables = new ArrayList<>();
    private final List<LogicalVariable> downVariables = new ArrayList<>();
    private final LogicalVariable pathVariable;
    private final LogicalVariable schemaVariable;

    public LoopVariableContext(FixedPointOperator fpOp, LogicalVariable sourceVariable) {
        Iterator<LogicalVariable> outputIterator = fpOp.getOutputVariableList().iterator();
        LogicalVariable pathVariable = null;
        LogicalVariable schemaVariable = null;
        for (int i = 0; outputIterator.hasNext(); i++) {
            LogicalVariable outputVariable = outputIterator.next();
            switch (i) {
                case PATH_VARIABLE_INDEX:
                    pathVariable = outputVariable;
                    break;
                case DEST_VARIABLES_INDEX:
                    this.endingVariables.add(outputVariable);
                    break;
                case SCHEMA_VARIABLE_INDEX:
                    schemaVariable = outputVariable;
                    break;
            }
        }
        this.pathVariable = pathVariable;
        this.schemaVariable = schemaVariable;
        this.startingVariables.add(sourceVariable);
    }

    public LoopVariableContext(LogicalVariable pathVariable, LogicalVariable schemaVariable,
            List<LogicalVariable> endingVariables, List<LogicalVariable> startingVariables,
            List<LogicalVariable> downVariables) {
        this.pathVariable = pathVariable;
        this.schemaVariable = schemaVariable;
        if (endingVariables != null) {
            this.endingVariables.addAll(endingVariables);
        }
        if (startingVariables != null) {
            this.startingVariables.addAll(startingVariables);
        }
        if (downVariables != null) {
            this.downVariables.addAll(downVariables);
        }
    }

    public LogicalVariable getPathVariable() {
        return pathVariable;
    }

    public LogicalVariable getSchemaVariable() {
        return schemaVariable;
    }

    public List<LogicalVariable> getEndingVariables() {
        return Collections.unmodifiableList(endingVariables);
    }

    public List<LogicalVariable> getStartingVariables() {
        return Collections.unmodifiableList(startingVariables);
    }

    public List<LogicalVariable> getDownVariables() {
        return Collections.unmodifiableList(downVariables);
    }

    public void addDownVariable(LogicalVariable downVariable) {
        this.downVariables.add(downVariable);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LoopVariableContext that = (LoopVariableContext) o;
        return Objects.equals(startingVariables, that.startingVariables)
                && Objects.equals(endingVariables, that.endingVariables)
                && Objects.equals(downVariables, that.downVariables) && Objects.equals(pathVariable, that.pathVariable)
                && Objects.equals(schemaVariable, that.schemaVariable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startingVariables, endingVariables, downVariables, pathVariable, schemaVariable);
    }
}
