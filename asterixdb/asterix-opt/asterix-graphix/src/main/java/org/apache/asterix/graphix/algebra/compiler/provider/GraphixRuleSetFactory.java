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
package org.apache.asterix.graphix.algebra.compiler.provider;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.compiler.provider.DefaultRuleSetFactory;
import org.apache.asterix.compiler.provider.IRuleSetFactory;
import org.apache.asterix.graphix.algebra.optimizer.physical.SetGraphixMemoryRequirementsRule;
import org.apache.asterix.graphix.algebra.optimizer.physical.SetGraphixPhysicalOperatorsRule;
import org.apache.asterix.optimizer.base.RuleCollections;
import org.apache.asterix.optimizer.rules.SetAsterixMemoryRequirementsRule;
import org.apache.asterix.optimizer.rules.SetAsterixPhysicalOperatorsRule;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.compiler.rewriter.rulecontrollers.SequentialFixpointRuleController;
import org.apache.hyracks.algebricks.compiler.rewriter.rulecontrollers.SequentialOnceRuleController;
import org.apache.hyracks.algebricks.core.rewriter.base.AbstractRuleController;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.core.rewriter.base.IRuleSetKind;

public class GraphixRuleSetFactory implements IRuleSetFactory {
    @Override
    public List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> getLogicalRewrites(
            ICcApplicationContext appCtx) {
        List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> rewrites = new ArrayList<>();
        SequentialFixpointRuleController seqCtrlNoDfs = new SequentialFixpointRuleController(false);
        SequentialFixpointRuleController seqCtrlFullDfs = new SequentialFixpointRuleController(true);
        SequentialOnceRuleController seqOnceCtrl = new SequentialOnceRuleController(true);
        rewrites.add(new Pair<>(seqOnceCtrl, GraphixRuleCollections.buildTranslationCleanupRuleCollection()));
        rewrites.add(new Pair<>(seqOnceCtrl, RuleCollections.buildTypeInferenceRuleCollection()));
        rewrites.add(new Pair<>(seqOnceCtrl, RuleCollections.buildAutogenerateIDRuleCollection()));
        rewrites.add(new Pair<>(seqCtrlFullDfs, RuleCollections.buildNormalizationRuleCollection(appCtx)));
        rewrites.add(new Pair<>(seqCtrlNoDfs, GraphixRuleCollections.buildJoinInferenceRuleCollection()));
        rewrites.add(new Pair<>(seqCtrlFullDfs, GraphixRuleCollections.buildLoadFieldsRuleCollection(appCtx)));
        rewrites.add(new Pair<>(seqCtrlFullDfs, RuleCollections.buildNormalizationRuleCollection(appCtx)));
        rewrites.add(new Pair<>(seqCtrlNoDfs, GraphixRuleCollections.buildJoinInferenceRuleCollection()));
        rewrites.add(new Pair<>(seqCtrlFullDfs, GraphixRuleCollections.buildLoadFieldsRuleCollection(appCtx)));
        rewrites.add(new Pair<>(seqOnceCtrl, RuleCollections.buildFulltextContainsRuleCollection()));
        rewrites.add(new Pair<>(seqOnceCtrl, RuleCollections.buildDataExchangeRuleCollection()));
        rewrites.add(new Pair<>(seqCtrlFullDfs, GraphixRuleCollections.buildInitialGraphixRuleCollection()));
        rewrites.add(new Pair<>(seqCtrlNoDfs, GraphixRuleCollections.buildLoopHandlingRuleCollection()));
        rewrites.add(new Pair<>(seqCtrlNoDfs, GraphixRuleCollections.buildConsolidationRuleCollection()));
        rewrites.add(new Pair<>(seqCtrlNoDfs, GraphixRuleCollections.buildAccessMethodRuleCollection()));
        rewrites.add(new Pair<>(seqCtrlNoDfs, GraphixRuleCollections.buildAfterAccessMethodRuleCollection()));
        rewrites.add(new Pair<>(seqCtrlNoDfs, GraphixRuleCollections.buildPlanCleanupRuleCollection()));
        return rewrites;
    }

    // TODO (GLENN): When we fully integrate CBO, update this rule-set.
    @Override
    public List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> getLogicalRewrites(IRuleSetKind ruleSetKind,
            ICcApplicationContext appCtx) {
        if (ruleSetKind != RuleSetKind.SAMPLING) {
            throw new IllegalArgumentException(String.valueOf(ruleSetKind));
        }
        return DefaultRuleSetFactory.buildLogicalSampling();
    }

    @Override
    public List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> getPhysicalRewrites(
            ICcApplicationContext appCtx) {
        List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> rewrites = new ArrayList<>();
        SequentialOnceRuleController seqOnceCtrl = new SequentialOnceRuleController(true);
        SequentialOnceRuleController seqOnceTopLevel = new SequentialOnceRuleController(false);
        rewrites.add(new Pair<>(seqOnceCtrl, RuleCollections.buildPhysicalRewritesAllLevelsRuleCollection()));
        rewrites.add(new Pair<>(seqOnceTopLevel, GraphixRuleCollections.buildPhysicalRuleCollection()));
        rewrites.add(new Pair<>(seqOnceTopLevel, RuleCollections.buildPhysicalRewritesTopLevelRuleCollection(appCtx)));
        rewrites.add(new Pair<>(seqOnceCtrl, GraphixRuleCollections.buildBeforeJobGenRuleCollection()));

        // We want to replace all instances Asterix-specific physical rules with Graphix-specific physical rules.
        for (Pair<AbstractRuleController, List<IAlgebraicRewriteRule>> controllerPair : rewrites) {
            for (ListIterator<IAlgebraicRewriteRule> it = controllerPair.getSecond().listIterator(); it.hasNext();) {
                IAlgebraicRewriteRule rewriteRule = it.next();
                if (rewriteRule instanceof SetAsterixMemoryRequirementsRule) {
                    it.set(new SetGraphixMemoryRequirementsRule());

                } else if (rewriteRule instanceof SetAsterixPhysicalOperatorsRule) {
                    it.set(new SetGraphixPhysicalOperatorsRule());
                }
            }
        }
        return rewrites;
    }
}
