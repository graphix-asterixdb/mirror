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

import org.apache.asterix.graphix.algebra.operator.logical.PathSemanticsReductionOperator;
import org.apache.asterix.optimizer.base.AsterixOptimizationContext;
import org.apache.asterix.optimizer.rules.SetAsterixMemoryRequirementsRule;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalMemoryRequirements;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.control.common.config.OptionTypes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SetGraphixMemoryRequirementsRule extends SetAsterixMemoryRequirementsRule {
    private static final Logger LOGGER = LogManager.getLogger();

    // The following is set in the configuration file or with a SET statement.
    public static final String LMK_OPTION_KEY_NAME = "graphix.compiler.lmkmemory";

    @Override
    protected ILogicalOperatorVisitor<Void, Void> createMemoryRequirementsConfigurator(IOptimizationContext context) {
        return forceMinMemoryBudget((AsterixOptimizationContext) context) ? null
                : new GraphixMemoryRequirementsConfigurator(context);
    }

    private static final class GraphixMemoryRequirementsConfigurator extends MemoryRequirementsConfigurator {
        private static final long _32MB = (long) 32 * 1048576;

        private GraphixMemoryRequirementsConfigurator(IOptimizationContext context) {
            super(context);
        }

        @Override
        public Void visitDistinctOperator(DistinctOperator op, Void arg) throws AlgebricksException {
            IPhysicalOperator physicalOperator = op.getPhysicalOperator();
            if (op instanceof PathSemanticsReductionOperator) {
                Object lmkOptionPropertyValue = physConfig.getExtensionProperty(LMK_OPTION_KEY_NAME);
                long memBudget = (lmkOptionPropertyValue == null) ? _32MB
                        : OptionTypes.LONG_BYTE_UNIT.parse((String) lmkOptionPropertyValue);
                int memBudgetInFrames = (int) (memBudget / physConfig.getFrameSize());

                // Make sure we aren't under-allocating.
                LocalMemoryRequirements memoryReqs = physicalOperator.getLocalMemoryRequirements();
                int minBudgetInFrames = memoryReqs.getMinMemoryBudgetInFrames();
                if (memBudgetInFrames < minBudgetInFrames) {
                    throw AlgebricksException.create(ErrorCode.ILLEGAL_MEMORY_BUDGET, op.getSourceLocation(),
                            op.getOperatorTag().toString(), memBudgetInFrames * physConfig.getFrameSize(),
                            minBudgetInFrames * physConfig.getFrameSize());
                }
                LOGGER.debug("LMK operator has been assigned a budget of {} frames.", memBudgetInFrames);
                memoryReqs.setMemoryBudgetInFrames(memBudgetInFrames);
                return null;
            }
            return super.visitDistinctOperator(op, arg);
        }
    }
}
