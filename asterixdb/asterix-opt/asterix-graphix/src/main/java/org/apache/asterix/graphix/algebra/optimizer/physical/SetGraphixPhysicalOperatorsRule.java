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
package org.apache.asterix.graphix.algebra.optimizer.physical;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.asterix.algebra.operators.physical.BTreeSearchPOperator;
import org.apache.asterix.graphix.algebra.annotations.JoinInLoopOperatorAnnotations;
import org.apache.asterix.graphix.algebra.operator.logical.PathSemanticsReductionOperator;
import org.apache.asterix.graphix.algebra.operator.physical.PathSemanticsReductionPOperator;
import org.apache.asterix.graphix.algebra.operator.physical.PersistentBuildJoinPOperator;
import org.apache.asterix.graphix.algebra.operator.physical.UnsortedBTreeSearchPOperator;
import org.apache.asterix.graphix.algebra.optimizer.navigation.PromoteSelectInLoopToJoinRule;
import org.apache.asterix.optimizer.rules.SetAsterixPhysicalOperatorsRule;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.FixedPointOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.RecursiveHeadOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.RecursiveTailOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.FixedPointPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.HybridHashJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.RecursiveHeadPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.RecursiveTailPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.StateReleasePOperator;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;
import org.apache.hyracks.api.exceptions.ErrorCode;

public class SetGraphixPhysicalOperatorsRule extends SetAsterixPhysicalOperatorsRule {
    private final Map<ILogicalOperator, Byte> stateReleaseOperatorMap = new LinkedHashMap<>();

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperatorVisitor<IPhysicalOperator, Boolean> visitor = createPhysicalOperatorFactoryVisitor(context);
        setPhysicalOperator((AbstractLogicalOperator) opRef.getValue(), null, visitor, context);

        // Note: we expect the rule controller using this rule to **not** be a fixed-point controller.
        return true;
    }

    private void setPhysicalOperator(AbstractLogicalOperator op, ILogicalOperator containerOp,
            ILogicalOperatorVisitor<IPhysicalOperator, Boolean> opFactory, IOptimizationContext context)
            throws AlgebricksException {
        if (op.getPhysicalOperator() == null) {
            // If the physical operator has not been set by other rules, set it here.
            IPhysicalOperator physicalOp = op.accept(opFactory, isTopLevelOp(containerOp));
            if (physicalOp == null) {
                ErrorCode errorCode = ErrorCode.PHYS_OPERATOR_NOT_SET;
                throw AlgebricksException.create(errorCode, op.getSourceLocation(), op.getOperatorTag());
            }

            // PBJ contains its own state-release logic, there's no need to decorate it here.
            PhysicalOperatorTag pOpTag = physicalOp.getOperatorTag();
            if (!stateReleaseOperatorMap.containsKey(op) || pOpTag == PhysicalOperatorTag.PERSISTENT_BUILD_JOIN) {
                op.setPhysicalOperator(physicalOp);

            } else {
                Byte designatorByte = stateReleaseOperatorMap.get(op);
                op.setPhysicalOperator(new StateReleasePOperator(physicalOp, designatorByte));
            }

        } else if (stateReleaseOperatorMap.containsKey(op)) {
            // If we encounter operators in-the-loop that have had their physical operators set, decorate them here.
            IPhysicalOperator physicalOp = op.getPhysicalOperator();
            switch (physicalOp.getOperatorTag()) {
                case PERSISTENT_BUILD_JOIN:
                case FIXED_POINT:
                case RECURSIVE_HEAD:
                case RECURSIVE_TAIL:
                case STATE_RELEASE:
                    break;

                case HYBRID_HASH_JOIN:
                case BTREE_SEARCH:
                    op.accept(opFactory, isTopLevelOp(containerOp));
                    physicalOp = op.getPhysicalOperator();

                default:
                    Byte designatorByte = stateReleaseOperatorMap.get(op);
                    op.setPhysicalOperator(new StateReleasePOperator(physicalOp, designatorByte));
            }
        }

        // We have encountered a loop. Find all operators in the loop.
        if (op.getOperatorTag() == LogicalOperatorTag.RECURSIVE_HEAD) {
            RecursiveHeadOperator headOp = (RecursiveHeadOperator) op;
            byte markerByte = context.getDesignationByteFor(headOp);

            // Walk to our fixed-point operator.
            Mutable<ILogicalOperator> workingOpRef = headOp.getInputs().get(0);
            while (workingOpRef.getValue().getOperatorTag() != LogicalOperatorTag.FIXED_POINT) {
                stateReleaseOperatorMap.put(workingOpRef.getValue(), markerByte);
                if (workingOpRef.getValue().getInputs().size() != 1) {
                    throw new AlgebricksException(ErrorCode.ILLEGAL_STATE, "Encountered op with more than one input!");
                }
                workingOpRef = workingOpRef.getValue().getInputs().get(0);
            }

            // We have found our fixed-point operator. Mark all operators in our nested plan.
            FixedPointOperator fpOp = (FixedPointOperator) workingOpRef.getValue();
            for (ILogicalPlan nestedPlan : fpOp.getNestedPlans()) {
                for (Mutable<ILogicalOperator> rootOpRef : nestedPlan.getRoots()) {
                    markOperator(rootOpRef, markerByte);
                }
            }
        }

        // Explore our nested plans, then our inputs recursively.
        if (op.hasNestedPlans()) {
            for (ILogicalPlan plan : ((AbstractOperatorWithNestedPlans) op).getNestedPlans()) {
                for (Mutable<ILogicalOperator> rootRef : plan.getRoots()) {
                    setPhysicalOperator((AbstractLogicalOperator) rootRef.getValue(), op, opFactory, context);
                }
            }
        }
        for (Mutable<ILogicalOperator> opRef : op.getInputs()) {
            setPhysicalOperator((AbstractLogicalOperator) opRef.getValue(), containerOp, opFactory, context);
        }
    }

    private boolean isTopLevelOp(ILogicalOperator containerOp) {
        if (containerOp == null) {
            // Case #1: we are not in a nested plan.
            return true;

        } else if (containerOp.getOperatorTag() != LogicalOperatorTag.FIXED_POINT) {
            // Case #2: we are in a nested plan that is not a part of a fixed-point operator.
            return false;

        } else if (containerOp.getOperatorTag() == LogicalOperatorTag.FIXED_POINT) {
            // Case #3: we are in a fixed-point nested plan.
            return true;

        } else {
            throw new IllegalArgumentException();
        }
    }

    private void markOperator(Mutable<ILogicalOperator> opRef, byte markerDesignation) {
        switch (opRef.getValue().getOperatorTag()) {
            case RECURSIVE_TAIL:
                return;

            case INNERJOIN:
                stateReleaseOperatorMap.put(opRef.getValue(), markerDesignation);
                InnerJoinOperator joinOp = (InnerJoinOperator) opRef.getValue();
                if (JoinInLoopOperatorAnnotations.isJoinInLoop(joinOp)) {
                    // We will not decorate the non-recursive branch of our JOIN in the loop.
                    int transientInputIndex = PromoteSelectInLoopToJoinRule.JOIN_TRANSIENT_INPUT_INDEX;
                    markOperator(joinOp.getInputs().get(transientInputIndex), markerDesignation);
                    break;
                }

            default:
                stateReleaseOperatorMap.put(opRef.getValue(), markerDesignation);
                for (Mutable<ILogicalOperator> inputRef : opRef.getValue().getInputs()) {
                    markOperator(inputRef, markerDesignation);
                }
        }
    }

    @Override
    protected ILogicalOperatorVisitor<IPhysicalOperator, Boolean> createPhysicalOperatorFactoryVisitor(
            IOptimizationContext context) {
        return new AsterixPhysicalOperatorFactoryVisitor(context) {
            final class PhysicalLoopBuilder {
                private FixedPointOperator fpOp = null;
                private FixedPointPOperator fpPOp = null;
                private RecursiveHeadOperator headOp = null;
                private RecursiveHeadPOperator headPOp = null;
                private RecursiveTailOperator tailOp = null;
                private RecursiveTailPOperator tailPOp = null;
                private byte designatorByte;

                private void accept(RecursiveHeadOperator headOp, RecursiveHeadPOperator headPOp) {
                    if (this.headOp != null || this.fpOp != null || this.tailOp != null) {
                        throw new IllegalStateException();
                    }
                    this.headOp = headOp;
                    this.headPOp = headPOp;
                }

                private void accept(FixedPointOperator fpOp, FixedPointPOperator fpPOp) {
                    if (this.fpOp != null || this.tailOp != null) {
                        throw new IllegalStateException();
                    }
                    this.fpOp = fpOp;
                    this.fpPOp = fpPOp;
                }

                private void accept(RecursiveTailOperator tailOp, RecursiveTailPOperator tailPOp) {
                    if (this.tailOp != null) {
                        throw new IllegalStateException();
                    }
                    this.tailOp = tailOp;
                    this.tailPOp = tailPOp;

                    // This "accept" should be our last call.
                    this.headPOp.setFixedPointOp(this.fpOp);
                }

                @Override
                public String toString() {
                    StringBuilder sb = new StringBuilder();
                    if (fpOp != null) {
                        sb.append("Designator Byte: ").append(designatorByte).append("\n");
                        sb.append("Fixed-Point (Logical): ").append(fpOp).append("\n");
                        sb.append("Fixed-Point (Physical): ").append(fpPOp).append("\n");
                    }
                    if (headOp != null) {
                        sb.append("Recursive-Head (Logical): ").append(headOp).append("\n");
                        sb.append("Recursive-Head (Physical): ").append(headPOp).append("\n");
                    }
                    if (tailOp != null) {
                        sb.append("Recursive-Tail (Logical): ").append(tailOp).append("\n");
                        sb.append("Recursive-Tail (Physical): ").append(tailPOp).append("\n");
                    }
                    return sb.toString();
                }
            }

            private final Deque<PhysicalLoopBuilder> builderStack = new ArrayDeque<>();

            // We expect to first visit our head operator...
            @Override
            public IPhysicalOperator visitRecursiveHeadOperator(RecursiveHeadOperator op, Boolean topLevelOp) {
                PhysicalLoopBuilder builder = new PhysicalLoopBuilder();
                RecursiveHeadPOperator headPOp = new RecursiveHeadPOperator();
                builder.designatorByte = context.getDesignationByteFor(op);
                builder.accept(op, headPOp);
                builderStack.addLast(builder);
                return headPOp;
            }

            // ...next, our fixed-point operator...
            @Override
            public IPhysicalOperator visitFixedPointOperator(FixedPointOperator op, Boolean topLevelOp) {
                PhysicalLoopBuilder builder = builderStack.getLast();
                FixedPointPOperator fpPOp = new FixedPointPOperator(builder.designatorByte);
                builder.accept(op, fpPOp);
                return fpPOp;
            }

            // ...and last, our tail operator.
            @Override
            public IPhysicalOperator visitRecursiveTailOperator(RecursiveTailOperator op, Boolean topLevelOp) {
                PhysicalLoopBuilder builder = builderStack.removeLast();
                RecursiveTailPOperator tailPOp = new RecursiveTailPOperator();
                builder.accept(op, tailPOp);
                return tailPOp;
            }

            @Override
            public IPhysicalOperator visitDistinctOperator(DistinctOperator op, Boolean topLevelOp) {
                if (op instanceof PathSemanticsReductionOperator) {
                    return new PathSemanticsReductionPOperator();
                }
                return super.visitDistinctOperator(op, topLevelOp);
            }

            @Override
            public IPhysicalOperator visitUnnestMapOperator(UnnestMapOperator op, Boolean topLevelOp)
                    throws AlgebricksException {
                if (!stateReleaseOperatorMap.containsKey(op)) {
                    return super.visitUnnestMapOperator(op, topLevelOp);
                }

                // B+ tree searches must be unordered (our plan in-the-loop cannot have blocking operators).
                IPhysicalOperator unnestMapPOp = super.visitUnnestMapOperator(op, topLevelOp);
                if (unnestMapPOp.getOperatorTag() == PhysicalOperatorTag.BTREE_SEARCH) {
                    return new UnsortedBTreeSearchPOperator((BTreeSearchPOperator) unnestMapPOp);
                }
                return unnestMapPOp;
            }

            @Override
            public IPhysicalOperator visitInnerJoinOperator(InnerJoinOperator op, Boolean topLevelOp)
                    throws AlgebricksException {
                if (!stateReleaseOperatorMap.containsKey(op)) {
                    return super.visitInnerJoinOperator(op, topLevelOp);
                }
                PhysicalLoopBuilder builder = builderStack.getLast();

                // Hybrid-hash-JOINs should become PBJs.
                AbstractJoinPOperator joinPOp = (AbstractJoinPOperator) super.visitInnerJoinOperator(op, topLevelOp);
                switch (joinPOp.getOperatorTag()) {
                    case HYBRID_HASH_JOIN:
                        HybridHashJoinPOperator hhjPOp = (HybridHashJoinPOperator) joinPOp;
                        return new PersistentBuildJoinPOperator(joinPOp.getKind(), joinPOp.getPartitioningType(),
                                hhjPOp.getKeysLeftBranch(), hhjPOp.getKeysRightBranch(),
                                context.getPhysicalOptimizationConfig().getMaxFramesForJoinLeftInput(),
                                context.getPhysicalOptimizationConfig().getMaxRecordsPerFrame(),
                                context.getPhysicalOptimizationConfig().getFudgeFactor(), builder.designatorByte);

                    default:
                        // We only have two JOIN operators: INLJ and PBJ.
                        throw new AlgebricksException(ErrorCode.OPERATOR_NOT_IMPLEMENTED, op.getSourceLocation(),
                                joinPOp + " (within loop)");
                }
            }
        };
    }
}
