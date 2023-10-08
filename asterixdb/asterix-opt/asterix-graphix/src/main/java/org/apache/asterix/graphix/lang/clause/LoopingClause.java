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
package org.apache.asterix.graphix.lang.clause;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.graphix.lang.clause.extension.LoopingClauseExtension;
import org.apache.asterix.graphix.lang.expression.pattern.PathPatternExpr;
import org.apache.asterix.lang.common.base.AbstractExtensionClause;
import org.apache.asterix.lang.common.base.IVisitorExtension;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;

/**
 * A functional equivalent to the {@link FromTerm}, used as a container for lowering {@link PathPatternExpr} AST nodes.
 * A {@link LoopingClause} contains the following:
 * <ol>
 *   <li>A set of input {@link VariableExpr} expressions that are used for iterations 1+.</li>
 *   <li>A set of {@link SequenceClause} expressions that represent the computation done within the loop. We require
 *   that these {@link SequenceClause} expressions eventually terminate.</li>
 *   <li>A set of previous variables, which the aforementioned {@link SequenceClause} expressions will reference to
 *   refer to work done in a previous iteration.</li>
 *   <li>A set of output {@link VariableExpr} expressions that are used downstream.</li>
 *   <li>A set of auxiliary {@link VariableExpr} expressions that are carried across each loop.</li>
 *   <li>A set of {@link VariableExpr} expressions that represent the destination vertices. </li>
 * </ol>
 */
public class LoopingClause extends AbstractExtensionClause {
    private final LoopingClauseExtension loopingClauseExtension;

    // We will always have one list of inputs and one list of outputs.
    private final List<VariableExpr> inputVariables;
    private final List<VariableExpr> outputVariables;
    private final List<VariableExpr> auxiliaryVariables;

    // The following should be added when the destination vertex is lowered.
    private final List<VariableExpr> downVariables = new ArrayList<>();

    // The following are dependent on how many sequence clauses we have.
    private final List<List<VariableExpr>> previousVariables;
    private final List<List<VariableExpr>> nextVariables;
    private final List<SequenceClause> sequenceClauses;

    public LoopingClause(List<VariableExpr> inputVariables, List<VariableExpr> outputVariables,
            List<VariableExpr> auxiliaryVariables, List<List<VariableExpr>> previousVariables,
            List<List<VariableExpr>> nextVariables, List<SequenceClause> sequenceClauses) {
        this.loopingClauseExtension = new LoopingClauseExtension(this);
        this.inputVariables = Objects.requireNonNull(inputVariables);
        this.outputVariables = Objects.requireNonNull(outputVariables);
        this.auxiliaryVariables = Objects.requireNonNull(auxiliaryVariables);
        this.previousVariables = Objects.requireNonNull(previousVariables);
        this.nextVariables = Objects.requireNonNull(nextVariables);
        this.sequenceClauses = Objects.requireNonNull(sequenceClauses);

        // We expect our input to adhere to the following:
        if (sequenceClauses.isEmpty()) {
            throw new IllegalArgumentException("No sequence clauses added?");
        }
        if (outputVariables.size() != inputVariables.size()) {
            throw new IllegalArgumentException("All given variable lists must be of the same size!");
        }
        if (previousVariables.size() != sequenceClauses.size() || nextVariables.size() != sequenceClauses.size()) {
            throw new IllegalArgumentException("Clauses and previous/next variable lists must be of the same size!");
        }
        for (int i = 0; i < previousVariables.size(); i++) {
            List<VariableExpr> previousVariableList = previousVariables.get(i);
            List<VariableExpr> nextVariableList = nextVariables.get(i);
            if (previousVariableList.size() != inputVariables.size()
                    || nextVariableList.size() != inputVariables.size()) {
                throw new IllegalArgumentException("All given variable lists must be of the same size!");
            }
        }
    }

    @Override
    public IVisitorExtension getVisitorExtension() {
        return loopingClauseExtension;
    }

    public List<VariableExpr> getInputVariables() {
        return Collections.unmodifiableList(inputVariables);
    }

    public List<List<VariableExpr>> getPreviousVariables() {
        return Collections.unmodifiableList(previousVariables);
    }

    public List<List<VariableExpr>> getNextVariables() {
        return Collections.unmodifiableList(nextVariables);
    }

    public List<VariableExpr> getOutputVariables() {
        return Collections.unmodifiableList(outputVariables);
    }

    public List<VariableExpr> getAuxiliaryVariables() {
        return Collections.unmodifiableList(auxiliaryVariables);
    }

    public List<VariableExpr> getDownVariables() {
        return Collections.unmodifiableList(downVariables);
    }

    public List<SequenceClause> getSequenceClauses() {
        return Collections.unmodifiableList(sequenceClauses);
    }

    public void addDownVariable(VariableExpr downVariable) {
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
        LoopingClause that = (LoopingClause) o;
        return Objects.equals(inputVariables, that.inputVariables)
                && Objects.equals(outputVariables, that.outputVariables)
                && Objects.equals(auxiliaryVariables, that.auxiliaryVariables)
                && Objects.equals(downVariables, that.downVariables)
                && Objects.equals(previousVariables, that.previousVariables)
                && Objects.equals(nextVariables, that.nextVariables)
                && Objects.equals(sequenceClauses, that.sequenceClauses);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inputVariables, outputVariables, auxiliaryVariables, downVariables, previousVariables,
                nextVariables, sequenceClauses);
    }
}
