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
package org.apache.asterix.graphix.lang.rewrite.lower;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.common.metadata.IElementIdentifier;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.lower.action.base.IEnvironmentAction;
import org.apache.asterix.graphix.lang.rewrite.lower.action.base.ISequenceTransformer;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.AliasLookupTable;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.ClauseSequence;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LoweringEnvironment {
    private static final Logger LOGGER = LogManager.getLogger();

    private final List<Class<? extends IEnvironmentAction>> appliedActions;
    private final GraphixRewritingContext graphixRewritingContext;
    private final Map<IElementIdentifier, Integer> identifierMap;
    private final Set<VariableExpr> partialElementSet;
    private final AliasLookupTable aliasLookupTable;

    // We will always be working with a clause-sequence.
    private final Deque<ClauseSequence> sequenceStack;

    public LoweringEnvironment(GraphixRewritingContext graphixRewritingContext) {
        this.graphixRewritingContext = graphixRewritingContext;
        this.aliasLookupTable = new AliasLookupTable();
        this.partialElementSet = new LinkedHashSet<>();
        this.identifierMap = new LinkedHashMap<>();
        this.appliedActions = new ArrayList<>();
        this.sequenceStack = new ArrayDeque<>();
        this.sequenceStack.add(new ClauseSequence());
    }

    public AliasLookupTable getAliasLookupTable() {
        return aliasLookupTable;
    }

    public GraphixRewritingContext getGraphixRewritingContext() {
        return graphixRewritingContext;
    }

    public void setElementPartiallyLowered(VariableExpr variableExpr, boolean isElementPartiallyLowered) {
        if (isElementPartiallyLowered) {
            partialElementSet.add(variableExpr);
        } else {
            partialElementSet.remove(variableExpr);
        }
    }

    public boolean isElementPartiallyLowered(VariableExpr variableExpr) {
        return partialElementSet.contains(variableExpr);
    }

    public boolean isElementIntroduced(VariableExpr variableExpr) {
        return aliasLookupTable.getIterationAlias(variableExpr) != null;
    }

    public int getIntegerIdentifierFor(IElementIdentifier elementIdentifier) {
        if (!identifierMap.containsKey(elementIdentifier)) {
            int maximumInteger = identifierMap.values().stream().max(Comparator.naturalOrder()).orElse(-1);
            identifierMap.put(elementIdentifier, maximumInteger + 1);
        }
        return identifierMap.get(elementIdentifier);
    }

    public List<Class<? extends IEnvironmentAction>> getAppliedActions() {
        return Collections.unmodifiableList(appliedActions);
    }

    public void pushClauseSequence() {
        sequenceStack.addFirst(new ClauseSequence());
    }

    public ClauseSequence popClauseSequence() {
        return sequenceStack.removeFirst();
    }

    public void acceptAction(IEnvironmentAction environmentAction) throws CompilationException {
        Class<? extends IEnvironmentAction> actionClass = environmentAction.getClass();
        LOGGER.debug("Applying action to environment: {}", actionClass.getSimpleName());
        LOGGER.trace("Before application: " + this);
        appliedActions.add(actionClass);
        environmentAction.apply(this);
        LOGGER.trace("After application: " + this);
    }

    public void acceptTransformer(ISequenceTransformer sequenceTransformer) throws CompilationException {
        sequenceTransformer.accept(sequenceStack.getFirst());
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        int clauseSequenceIndex = 0;
        for (ClauseSequence clauseSequence : sequenceStack) {
            if (clauseSequenceIndex > 0) {
                stringBuilder.append("\n");
            }
            stringBuilder.append("CLAUSE-SEQUENCE (").append(clauseSequenceIndex++).append(" ):");
            stringBuilder.append(clauseSequence);
        }
        stringBuilder.append("\nPARTIALLY-LOWERED: ");
        for (VariableExpr variableExpr : partialElementSet) {
            stringBuilder.append(variableExpr.getVar().toString());
            stringBuilder.append(" ");
        }
        stringBuilder.append("\nAPPLIED-ACTIONS: ");
        for (Class<? extends IEnvironmentAction> appliedAction : appliedActions) {
            stringBuilder.append("\n").append(appliedAction.getSimpleName());
        }
        return stringBuilder.toString();
    }
}
