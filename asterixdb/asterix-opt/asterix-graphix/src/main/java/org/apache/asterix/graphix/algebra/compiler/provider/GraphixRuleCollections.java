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
import org.apache.asterix.graphix.algebra.optimizer.cleanup.CancelPrimaryKeyEqualityRule;
import org.apache.asterix.graphix.algebra.optimizer.cleanup.MinimizeOperatorsFromHeadToTailRule;
import org.apache.asterix.graphix.algebra.optimizer.cleanup.RemoveAlwaysTrueSelectConditionRule;
import org.apache.asterix.graphix.algebra.optimizer.cleanup.RemoveConstantSwitchCaseRule;
import org.apache.asterix.graphix.algebra.optimizer.cleanup.RemoveRedundantExpressionsRule;
import org.apache.asterix.graphix.algebra.optimizer.cleanup.RemoveRedundantGraphixVariablesRule;
import org.apache.asterix.graphix.algebra.optimizer.cleanup.RemoveUnusedFixedPointOutputRule;
import org.apache.asterix.graphix.algebra.optimizer.cleanup.RemoveUnusedSubplanInFixedPointRule;
import org.apache.asterix.graphix.algebra.optimizer.cleanup.ReplaceExtraneousEqualityRule;
import org.apache.asterix.graphix.algebra.optimizer.correction.CancelSubplanListifyForLoopRule;
import org.apache.asterix.graphix.algebra.optimizer.correction.FixMarkerSinkOperatorPlacementRule;
import org.apache.asterix.graphix.algebra.optimizer.correction.FixReferencedLoopVariableContextRule;
import org.apache.asterix.graphix.algebra.optimizer.correction.ReplaceReplicateOperatorWithETSRule;
import org.apache.asterix.graphix.algebra.optimizer.function.ReplacePathFunctionArgumentsRule;
import org.apache.asterix.graphix.algebra.optimizer.function.RewriteGraphixFunctionsRule;
import org.apache.asterix.graphix.algebra.optimizer.navigation.AnnotateJoinOperatorsInLoopRule;
import org.apache.asterix.graphix.algebra.optimizer.navigation.FinalizeJoinOperatorTreeInLoopRule;
import org.apache.asterix.graphix.algebra.optimizer.navigation.PromoteSelectInLoopToJoinRule;
import org.apache.asterix.graphix.algebra.optimizer.navigation.TransformJoinInLoopToINLJRule;
import org.apache.asterix.graphix.algebra.optimizer.normalize.ExtractFromRecordConstructorRule;
import org.apache.asterix.graphix.algebra.optimizer.normalize.PushCycleConstraintBelowAppendRule;
import org.apache.asterix.graphix.algebra.optimizer.normalize.PushProperJoinThroughLoopRule;
import org.apache.asterix.graphix.algebra.optimizer.physical.IntroduceExchangeInFixedPointRule;
import org.apache.asterix.graphix.algebra.optimizer.physical.IntroduceProjectBeforeFixedPointRule;
import org.apache.asterix.graphix.algebra.optimizer.physical.IsolateHyracksOperatorsWithLoopRule;
import org.apache.asterix.graphix.algebra.optimizer.semantics.CheckUnboundedAllPathSemanticsRule;
import org.apache.asterix.graphix.algebra.optimizer.semantics.IntroduceCheapestPathSemanticsRule;
import org.apache.asterix.graphix.algebra.optimizer.semantics.IntroduceFirstPathSemanticsRule;
import org.apache.asterix.graphix.algebra.optimizer.typing.InjectTypeCastForFixedPointRule;
import org.apache.asterix.graphix.algebra.optimizer.typing.SetTranslatePathTypeInformationRule;
import org.apache.asterix.optimizer.base.RuleCollections;
import org.apache.asterix.optimizer.rules.RemoveRedundantSelectRule;
import org.apache.asterix.optimizer.rules.am.IntroduceJoinAccessMethodRule;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.rewriter.base.HeuristicOptimizer;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.rewriter.rules.IsolateHyracksOperatorsRule;
import org.apache.hyracks.algebricks.rewriter.rules.RemoveRedundantVariablesRule;
import org.apache.hyracks.algebricks.rewriter.rules.RemoveUnusedAssignAndAggregateRule;

public final class GraphixRuleCollections {
    private final static PhysicalOperatorTag[] DISABLED_OPS_JOB_GEN =
            HeuristicOptimizer.hyraxOperatorsBelowWhichJobGenIsDisabled;

    public static List<IAlgebraicRewriteRule> buildTranslationCleanupRuleCollection() {
        List<IAlgebraicRewriteRule> ruleCollection = RuleCollections.buildInitialTranslationRuleCollection();
        ruleCollection.add(new ReplaceReplicateOperatorWithETSRule());
        return ruleCollection;
    }

    public static List<IAlgebraicRewriteRule> buildInitialGraphixRuleCollection() {
        List<IAlgebraicRewriteRule> ruleCollection = new ArrayList<>();
        ruleCollection.add(new SetTranslatePathTypeInformationRule());
        ruleCollection.add(new ReplaceReplicateOperatorWithETSRule());
        ruleCollection.add(new ReplacePathFunctionArgumentsRule());
        ruleCollection.add(new CancelPrimaryKeyEqualityRule());
        ruleCollection.add(new RemoveRedundantSelectRule());
        ruleCollection.add(new ReplaceExtraneousEqualityRule());
        ruleCollection.add(new ExtractFromRecordConstructorRule());
        ruleCollection.add(new RemoveRedundantGraphixVariablesRule());
        ruleCollection.add(new RemoveUnusedAssignAndAggregateRule());
        ruleCollection.add(new FixReferencedLoopVariableContextRule());
        ruleCollection.add(new RemoveUnusedFixedPointOutputRule());
        ruleCollection.add(new RewriteGraphixFunctionsRule());
        return ruleCollection;
    }

    public static List<IAlgebraicRewriteRule> buildJoinInferenceRuleCollection() {
        List<IAlgebraicRewriteRule> ruleCollection = RuleCollections.buildCondPushDownAndJoinInferenceRuleCollection();
        for (ListIterator<IAlgebraicRewriteRule> it = ruleCollection.listIterator(); it.hasNext();) {
            IAlgebraicRewriteRule rewriteRule = it.next();
            if (rewriteRule instanceof RemoveRedundantVariablesRule) {
                it.set(new RemoveRedundantGraphixVariablesRule());
            }
        }
        return ruleCollection;
    }

    public static List<IAlgebraicRewriteRule> buildLoadFieldsRuleCollection(ICcApplicationContext context) {
        List<IAlgebraicRewriteRule> ruleCollection = RuleCollections.buildLoadFieldsRuleCollection(context);
        ruleCollection.add(0, new CancelSubplanListifyForLoopRule());
        for (ListIterator<IAlgebraicRewriteRule> it = ruleCollection.listIterator(); it.hasNext();) {
            IAlgebraicRewriteRule rewriteRule = it.next();
            if (rewriteRule instanceof RemoveRedundantVariablesRule) {
                it.set(new RemoveRedundantGraphixVariablesRule());
            }
        }
        return ruleCollection;
    }

    public static List<IAlgebraicRewriteRule> buildLoopHandlingRuleCollection() {
        List<IAlgebraicRewriteRule> ruleCollection = new ArrayList<>();
        ruleCollection.add(new PushCycleConstraintBelowAppendRule());
        ruleCollection.add(new PromoteSelectInLoopToJoinRule());
        ruleCollection.add(new IntroduceCheapestPathSemanticsRule());
        ruleCollection.add(new IntroduceFirstPathSemanticsRule());
        return ruleCollection;
    }

    public static List<IAlgebraicRewriteRule> buildConsolidationRuleCollection() {
        List<IAlgebraicRewriteRule> ruleCollection = RuleCollections.buildConsolidationRuleCollection();
        for (ListIterator<IAlgebraicRewriteRule> it = ruleCollection.listIterator(); it.hasNext();) {
            IAlgebraicRewriteRule rewriteRule = it.next();
            if (rewriteRule instanceof RemoveRedundantVariablesRule) {
                it.set(new RemoveRedundantGraphixVariablesRule());
            }
        }
        ruleCollection.add(new RemoveConstantSwitchCaseRule());
        ruleCollection.add(new PushProperJoinThroughLoopRule());
        ruleCollection.add(new RemoveRedundantGraphixVariablesRule());
        ruleCollection.add(new RemoveUnusedSubplanInFixedPointRule());
        return ruleCollection;
    }

    public static List<IAlgebraicRewriteRule> buildAfterAccessMethodRuleCollection() {
        List<IAlgebraicRewriteRule> ruleCollection = new ArrayList<>();
        ruleCollection.add(new PushCycleConstraintBelowAppendRule());
        ruleCollection.add(new AnnotateJoinOperatorsInLoopRule());
        ruleCollection.add(new FinalizeJoinOperatorTreeInLoopRule());
        ruleCollection.add(new RemoveRedundantExpressionsRule());
        return ruleCollection;
    }

    public static List<IAlgebraicRewriteRule> buildAccessMethodRuleCollection() {
        List<IAlgebraicRewriteRule> ruleCollection = RuleCollections.buildAccessMethodRuleCollection();
        for (ListIterator<IAlgebraicRewriteRule> it = ruleCollection.listIterator(); it.hasNext();) {
            IAlgebraicRewriteRule rewriteRule = it.next();
            if (rewriteRule instanceof IntroduceJoinAccessMethodRule) {
                // We'll let the normal INLJ rule fire first, then apply our INLJ rules immediately afterward.
                it.add(new PushCycleConstraintBelowAppendRule());
                it.add(new TransformJoinInLoopToINLJRule());
                break;
            }
        }
        return ruleCollection;
    }

    public static List<IAlgebraicRewriteRule> buildPlanCleanupRuleCollection() {
        List<IAlgebraicRewriteRule> ruleCollection = RuleCollections.buildPlanCleanupRuleCollection();
        for (ListIterator<IAlgebraicRewriteRule> it = ruleCollection.listIterator(); it.hasNext();) {
            IAlgebraicRewriteRule rewriteRule = it.next();
            if (rewriteRule instanceof RemoveRedundantVariablesRule) {
                it.set(new RemoveRedundantGraphixVariablesRule());
            }
        }
        ruleCollection.add(new RemoveAlwaysTrueSelectConditionRule());
        ruleCollection.add(new InjectTypeCastForFixedPointRule());
        ruleCollection.add(new SetTranslatePathTypeInformationRule());
        ruleCollection.add(new MinimizeOperatorsFromHeadToTailRule());
        return ruleCollection;
    }

    public static List<IAlgebraicRewriteRule> buildPhysicalRuleCollection() {
        List<IAlgebraicRewriteRule> ruleCollection = new ArrayList<>();
        ruleCollection.add(new IntroduceExchangeInFixedPointRule());
        return ruleCollection;
    }

    public static List<IAlgebraicRewriteRule> buildBeforeJobGenRuleCollection() {
        List<IAlgebraicRewriteRule> ruleCollection = RuleCollections.prepareForJobGenRuleCollection();
        for (ListIterator<IAlgebraicRewriteRule> it = ruleCollection.listIterator(); it.hasNext();) {
            IAlgebraicRewriteRule rewriteRule = it.next();
            if (rewriteRule instanceof IsolateHyracksOperatorsRule) {
                it.set(new IsolateHyracksOperatorsWithLoopRule(DISABLED_OPS_JOB_GEN));
                it.add(new IntroduceProjectBeforeFixedPointRule());
            }
        }
        ruleCollection.add(new FixMarkerSinkOperatorPlacementRule());
        ruleCollection.add(new CheckUnboundedAllPathSemanticsRule());
        return ruleCollection;
    }
}
