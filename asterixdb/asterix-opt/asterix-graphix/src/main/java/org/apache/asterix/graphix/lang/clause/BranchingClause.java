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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.asterix.graphix.lang.clause.extension.BranchingClauseExtension;
import org.apache.asterix.graphix.lang.expression.pattern.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.pattern.ElementDirection;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.ClauseSequence;
import org.apache.asterix.lang.common.base.AbstractExtensionClause;
import org.apache.asterix.lang.common.base.IVisitorExtension;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;

/**
 * A functional equivalent to the {@link FromTerm}, used (for now) as a container for lowering both directions of an
 * {@link ElementDirection#UNDIRECTED} {@link EdgePatternExpr} AST node. To merge the two resulting
 * {@link ClauseSequence}s, we maintain a map of output variables (of this {@link BranchingClause}) to variables
 * produced in each branch, as well as any variables that should survive before branching.
 */
public class BranchingClause extends AbstractExtensionClause {
    private final BranchingClauseExtension branchingClauseExtension;
    private final List<SequenceClause> clauseBranches;

    private final Map<VariableExpr, List<VariableExpr>> projectionMapping;

    public BranchingClause(List<SequenceClause> clauseBranches) {
        this.branchingClauseExtension = new BranchingClauseExtension(this);
        this.clauseBranches = Objects.requireNonNull(clauseBranches);
        this.projectionMapping = new LinkedHashMap<>();
    }

    public List<SequenceClause> getClauseBranches() {
        return Collections.unmodifiableList(clauseBranches);
    }

    public Map<VariableExpr, List<VariableExpr>> getProjectionMapping() {
        return Collections.unmodifiableMap(projectionMapping);
    }

    public void putProjection(VariableExpr representativeVariable, List<VariableExpr> outputList) {
        if (outputList.size() != clauseBranches.size()) {
            throw new IllegalArgumentException("Projection list size does not match clause branch list size!");
        }
        this.projectionMapping.put(representativeVariable, outputList);
    }

    @Override
    public IVisitorExtension getVisitorExtension() {
        return branchingClauseExtension;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BranchingClause)) {
            return false;
        }
        BranchingClause that = (BranchingClause) o;
        return Objects.equals(clauseBranches, that.clauseBranches)
                && Objects.equals(projectionMapping, that.projectionMapping);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clauseBranches, projectionMapping);
    }
}
