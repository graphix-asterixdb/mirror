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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.properties.FunctionalDependency;

/**
 * An {@link IAlgebraicEntityFinder} to ask whether some "queryVariable" exists in the attribute <i>reduction</i> of the
 * "leftVariable" passed on instantiation. Basically, an {@link IAlgebraicEntityFinder} to find if some variable
 * determines another.
 */
public class VariableInReductionFinder implements IAlgebraicEntityFinder<LogicalVariable, Void> {
    private final Set<LogicalVariable> reductionVariableSet = new LinkedHashSet<>();

    public VariableInReductionFinder(List<FunctionalDependency> inputDependencies, LogicalVariable leftVariable) {
        Deque<LogicalVariable> sourceQueue = new ArrayDeque<>(List.of(leftVariable));
        while (!sourceQueue.isEmpty()) {
            LogicalVariable workingVariable = sourceQueue.removeFirst();
            reductionVariableSet.add(workingVariable);
            for (FunctionalDependency inputDependency : Objects.requireNonNull(inputDependencies)) {
                if (inputDependency.getHead().contains(workingVariable)) {
                    for (LogicalVariable determinantVariable : inputDependency.getTail()) {
                        if (!reductionVariableSet.contains(determinantVariable)) {
                            sourceQueue.add(determinantVariable);
                        }
                    }
                }
            }
        }
    }

    @Override
    public boolean search(LogicalVariable queryVariable) {
        return reductionVariableSet.contains(queryVariable);
    }

    @Override
    public Void get() throws AlgebricksException {
        throw new NotImplementedException();
    }
}
