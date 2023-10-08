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
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.optimizer.rules.util.EquivalenceClassUtils;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.PrimaryKeyVariablesVisitor;
import org.apache.hyracks.api.exceptions.ErrorCode;

/**
 * Given an operator <i>subgraph</i>, determine the primary keys of a given variable.
 * <p>
 * The purpose of this finder is to get around the limitations of {@link PrimaryKeyVariablesVisitor} being data-model
 * agnostic and also strictly for the entire query plan (at the root).
 */
public class PrimaryKeyVariableFinder implements IAlgebraicEntityFinder<LogicalVariable, List<LogicalVariable>> {
    private final List<LogicalVariable> primaryKeyVars;
    private final IOptimizationContext workingContext;
    private final ILogicalOperator rootOp;

    public PrimaryKeyVariableFinder(IOptimizationContext workingContext, ILogicalOperator rootOp) {
        this.workingContext = Objects.requireNonNull(workingContext);
        this.rootOp = Objects.requireNonNull(rootOp);
        this.primaryKeyVars = new ArrayList<>();
    }

    @Override
    public boolean search(LogicalVariable searchingVariable) throws AlgebricksException {
        EquivalenceClassUtils.computePrimaryKeys(rootOp, workingContext);
        ProducerOperatorFinder producerOperatorFinder = new ProducerOperatorFinder(rootOp);
        if (!producerOperatorFinder.search(searchingVariable)) {
            throw new AlgebricksException(ErrorCode.ILLEGAL_STATE, "Producer op not found?");
        }
        if (producerOperatorFinder.get().getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
            DataSourceScanOperator dataSourceOp = (DataSourceScanOperator) producerOperatorFinder.get();
            List<LogicalVariable> dataSourceVariables = dataSourceOp.getVariables();
            if (dataSourceVariables.get(dataSourceVariables.size() - 1).equals(searchingVariable)) {
                // We can replace this variable with this SCAN's leading fields.
                primaryKeyVars.addAll(dataSourceVariables.subList(0, dataSourceVariables.size() - 1));

            } else if (dataSourceVariables.contains(searchingVariable)) {
                // We already have the primary key.
                primaryKeyVars.add(searchingVariable);
            }
        } else if (workingContext.findPrimaryKey(searchingVariable) != null) {
            primaryKeyVars.addAll(workingContext.findPrimaryKey(searchingVariable));
        }
        return !primaryKeyVars.isEmpty();
    }

    @Override
    public List<LogicalVariable> get() throws AlgebricksException {
        return Collections.unmodifiableList(primaryKeyVars);
    }
}
