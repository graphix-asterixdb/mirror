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
package org.apache.asterix.graphix.algebra.optimizer.correction;

import org.apache.asterix.graphix.algebra.variable.LoopVariableCopyCallback;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.FixedPointOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class FixReferencedLoopVariableContextRule implements IAlgebraicRewriteRule {
    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (opRef.getValue().getOperatorTag() != LogicalOperatorTag.FIXED_POINT) {
            return false;
        }
        FixedPointOperator fpOp = (FixedPointOperator) opRef.getValue();
        if (fpOp.getOnIsomorphicCopyCallback() == null) {
            return false;
        }

        // All of this work is handled by our callback, we just need to invoke it.
        if (!(fpOp.getOnIsomorphicCopyCallback() instanceof LoopVariableCopyCallback)) {
            return false;
        }
        return ((LoopVariableCopyCallback) fpOp.getOnIsomorphicCopyCallback()).propagate();
    }
}
