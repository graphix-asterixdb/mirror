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
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class InExpressionFunctionFinder
        implements IAlgebraicEntityFinder<Mutable<ILogicalExpression>, Mutable<ILogicalExpression>> {
    private final Set<FunctionIdentifier> functionIdentifiers = new LinkedHashSet<>();
    private Mutable<ILogicalExpression> matchingFunctionCallRef;

    public InExpressionFunctionFinder(FunctionIdentifier functionIdentifier) {
        this.functionIdentifiers.add(functionIdentifier);
    }

    public InExpressionFunctionFinder(FunctionIdentifier... functionIdentifiers) {
        this.functionIdentifiers.addAll(Arrays.asList(functionIdentifiers));
    }

    public InExpressionFunctionFinder(Collection<FunctionIdentifier> functionIdentifiers) {
        this.functionIdentifiers.addAll(functionIdentifiers);
    }

    @Override
    public boolean search(Mutable<ILogicalExpression> searchingExprRef) {
        List<Mutable<ILogicalExpression>> conjunctRefs = new ArrayList<>();
        if (searchingExprRef.getValue().splitIntoConjuncts(conjunctRefs)) {
            for (Mutable<ILogicalExpression> conjunctRef : conjunctRefs) {
                Mutable<ILogicalExpression> resultExprRef = searchForFunctionCallExpr(conjunctRef);
                if (resultExprRef != null) {
                    matchingFunctionCallRef = resultExprRef;
                    break;
                }
            }
        } else {
            Mutable<ILogicalExpression> resultExprRef = searchForFunctionCallExpr(searchingExprRef);
            if (resultExprRef != null) {
                matchingFunctionCallRef = resultExprRef;
            }
        }
        return matchingFunctionCallRef != null;
    }

    @Override
    public Mutable<ILogicalExpression> get() {
        return matchingFunctionCallRef;
    }

    private Mutable<ILogicalExpression> searchForFunctionCallExpr(Mutable<ILogicalExpression> exprRef) {
        if (exprRef.getValue().getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return null;
        }
        AbstractFunctionCallExpression funcCallExpr = (AbstractFunctionCallExpression) exprRef.getValue();
        if (functionIdentifiers.contains(funcCallExpr.getFunctionIdentifier())) {
            return exprRef;
        }
        for (Mutable<ILogicalExpression> argExprRef : funcCallExpr.getArguments()) {
            Mutable<ILogicalExpression> argCallExpr = searchForFunctionCallExpr(argExprRef);
            if (argCallExpr != null) {
                return argCallExpr;
            }
        }
        return null;
    }
}
