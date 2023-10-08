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
package org.apache.asterix.graphix.function;

import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.asterix.graphix.runtime.evaluator.compare.IsDistinctEdgeDescriptor;
import org.apache.asterix.graphix.runtime.evaluator.compare.IsDistinctEverythingDescriptor;
import org.apache.asterix.graphix.runtime.evaluator.compare.IsDistinctVertexDescriptor;
import org.apache.asterix.graphix.runtime.evaluator.navigate.AppendToExistingPathDescriptor;
import org.apache.asterix.graphix.runtime.evaluator.navigate.CreateNewOneHopDescriptor;
import org.apache.asterix.graphix.runtime.evaluator.navigate.CreateNewZeroHopDescriptor;
import org.apache.asterix.graphix.runtime.evaluator.navigate.OptimizedEdgeCountDescriptor;
import org.apache.asterix.graphix.runtime.evaluator.navigate.TranslateForwardPathDescriptor;
import org.apache.asterix.graphix.runtime.evaluator.navigate.TranslateReversePathDescriptor;
import org.apache.asterix.om.functions.IFunctionCollection;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionRegistrant;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;

public class GraphixFunctionRegistrant implements IFunctionRegistrant {
    private static final long serialVersionUID = 1L;

    @Override
    public void register(IFunctionCollection fc) {
        fc.add(IsDistinctVertexDescriptor::new);
        fc.add(IsDistinctEdgeDescriptor::new);
        fc.add(IsDistinctEverythingDescriptor::new);
        fc.add(CreateNewOneHopDescriptor::new);
        fc.add(CreateNewZeroHopDescriptor::new);
        fc.add(AppendToExistingPathDescriptor::new);
        fc.add(OptimizedEdgeCountDescriptor::new);

        // We have types we need to pass to our TRANSLATE-PATH descriptor evaluator-factories.
        Function<Supplier<IFunctionDescriptor>, IFunctionDescriptorFactory> translatePathFactoryFactory =
                fdSupplier -> new IFunctionDescriptorFactory() {
                    @Override
                    public IFunctionTypeInferer createFunctionTypeInferer() {
                        return (expr, fd, context, compilerProps) -> {
                            AbstractFunctionCallExpression funcCallExpr = (AbstractFunctionCallExpression) expr;
                            IAType vertexItemType = (IAType) funcCallExpr.getOpaqueParameters()[0];
                            IAType edgeItemType = (IAType) funcCallExpr.getOpaqueParameters()[1];
                            fd.setImmutableStates(vertexItemType, edgeItemType);
                        };
                    }

                    @Override
                    public IFunctionDescriptor createFunctionDescriptor() {
                        return fdSupplier.get();
                    }
                };
        fc.add(translatePathFactoryFactory.apply(TranslateForwardPathDescriptor::new));
        fc.add(translatePathFactoryFactory.apply(TranslateReversePathDescriptor::new));
    }
}
