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
package org.apache.asterix.graphix.runtime.evaluator.compare;

import org.apache.asterix.graphix.function.GraphixFunctionIdentifiers;
import org.apache.asterix.graphix.runtime.pointable.GraphixPathPointable;
import org.apache.asterix.graphix.runtime.pointable.SinglyLinkedListPointable;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * For a given tuple (edge, path) pair, return true if there are no duplicate edges in the given path (and false
 * otherwise). This function is used internally to enforce no-repeated-edge navigation semantics.
 */
public class IsDistinctEdgeDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new AbstractElementCompareEvaluator() {
                    private final IScalarEvaluator edgeArgEval = args[0].createScalarEvaluator(ctx);
                    private final IScalarEvaluator pathArgEval = args[1].createScalarEvaluator(ctx);
                    private final IPointable edgeArgPtr = new VoidPointable();
                    private final IPointable pathArgPtr = new VoidPointable();

                    @Override
                    protected boolean readTuple(IFrameTupleReference tuple, IPointable result)
                            throws HyracksDataException {
                        edgeArgEval.evaluate(tuple, edgeArgPtr);
                        pathArgEval.evaluate(tuple, pathArgPtr);

                        // If our edge or path is NULL or MISSING, then our result is NULL / MISSING.
                        if (PointableHelper.checkAndSetMissingOrNull(result, edgeArgPtr, pathArgPtr)) {
                            return false;
                        }

                        // Ensure that we have a path (i.e. bit-array) as our second argument.
                        byte typeTagByte = pathArgPtr.getByteArray()[pathArgPtr.getStartOffset()];
                        if (typeTagByte != GraphixPathPointable.PATH_SERIALIZED_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 1, typeTagByte,
                                    GraphixPathPointable.PATH_SERIALIZED_TYPE_TAG);
                        }

                        edgeListItemCallback.set(edgeArgPtr);
                        pathPtr.set(pathArgPtr);
                        return true;
                    }

                    @Override
                    protected boolean compare() throws HyracksDataException {
                        SinglyLinkedListPointable edgeListPointable = pathPtr.getEdgeListPointable();
                        while (edgeListPointable.hasNext()) {
                            edgeListPointable.next();
                            if (edgeListItemCallback.isMatchFound()) {
                                return true;
                            }
                        }
                        return false;
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return GraphixFunctionIdentifiers.IS_DISTINCT_EDGE;
    }
}
