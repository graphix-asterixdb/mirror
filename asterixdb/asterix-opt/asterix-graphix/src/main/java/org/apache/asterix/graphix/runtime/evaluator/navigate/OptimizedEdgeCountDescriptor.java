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
package org.apache.asterix.graphix.runtime.evaluator.navigate;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.graphix.function.GraphixFunctionIdentifiers;
import org.apache.asterix.graphix.runtime.pointable.GraphixPathPointable;
import org.apache.asterix.om.types.ATypeTag;
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
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * Given an existing path, return the number of edges in the path itself. This function operates on the internal path
 * representation, <b>not</b> the materialized representation. We expect the following as arguments:
 * <ol>
 *   <li>A path, whose edge count we will directly access.</li>
 * </ol>
 */
public class OptimizedEdgeCountDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {
                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final DataOutput dataOutput = resultStorage.getDataOutput();
                    private final IPointable pathPointable = new VoidPointable();
                    private final IScalarEvaluator pathArgEval = args[0].createScalarEvaluator(ctx);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        pathArgEval.evaluate(tuple, pathPointable);
                        if (PointableHelper.checkAndSetMissingOrNull(result, pathPointable)) {
                            return;
                        }

                        // Ensure that we have a path (i.e. bit-array) as our only argument.
                        byte typeTagByte = pathPointable.getByteArray()[pathPointable.getStartOffset()];
                        if (typeTagByte != GraphixPathPointable.PATH_SERIALIZED_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, typeTagByte,
                                    GraphixPathPointable.PATH_SERIALIZED_TYPE_TAG);
                        }

                        // Access and return our edge count.
                        resultStorage.reset();
                        try {
                            int edgeCountValue = AInt32SerializerDeserializer.getInt(pathPointable.getByteArray(),
                                    pathPointable.getStartOffset() + GraphixPathPointable.HEADER_EDGE_ITEM_COUNT);
                            dataOutput.writeByte(ATypeTag.SERIALIZED_INT32_TYPE_TAG);
                            dataOutput.writeInt(edgeCountValue);
                            result.set(resultStorage);

                        } catch (IOException e) {
                            throw HyracksDataException.create(e);
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return GraphixFunctionIdentifiers.EDGE_COUNT_FROM_HEADER;
    }
}
