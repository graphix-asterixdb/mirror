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

import org.apache.asterix.graphix.function.GraphixFunctionIdentifiers;
import org.apache.asterix.graphix.runtime.pointable.GraphixPathPointable;
import org.apache.asterix.graphix.runtime.pointable.SinglyLinkedListPointable;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
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
 * Create an initial path, which consists of a single hop (one edge, two vertices). We expect the following as
 * arguments:
 * <ol>
 *   <li>A vertex field, which will act as the source vertex.</li>
 *   <li>A vertex field, which will act as the destination vertex (for this hop).</li>
 *   <li>An edge field, which will connect the source vertex and the destination.</li>
 * </ol>
 */
public class CreateNewOneHopDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {
                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final DataOutput dataOutput = resultStorage.getDataOutput();
                    private final IScalarEvaluator sourceArgEval = args[0].createScalarEvaluator(ctx);
                    private final IScalarEvaluator destArgEval = args[1].createScalarEvaluator(ctx);
                    private final IScalarEvaluator edgeArgEval = args[2].createScalarEvaluator(ctx);
                    private final VoidPointable sourceArgPtr = new VoidPointable();
                    private final VoidPointable destArgPtr = new VoidPointable();
                    private final VoidPointable edgeArgPtr = new VoidPointable();

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        // If any argument is NULL or MISSING, then our output path is NULL or MISSING.
                        sourceArgEval.evaluate(tuple, sourceArgPtr);
                        destArgEval.evaluate(tuple, destArgPtr);
                        edgeArgEval.evaluate(tuple, edgeArgPtr);
                        if (PointableHelper.checkAndSetMissingOrNull(result, sourceArgPtr, destArgPtr, edgeArgPtr)) {
                            return;
                        }

                        // Build our path header. We start with our type tag.
                        resultStorage.reset();
                        try {
                            dataOutput.writeByte(GraphixPathPointable.PATH_SERIALIZED_TYPE_TAG);

                            // Our offsets are relative to the entire path object. Write our vertex list size.
                            int itemHeaderSize = SinglyLinkedListPointable.LIST_ITEM_LENGTH_SIZE;
                            int pathHeaderSize = GraphixPathPointable.PATH_HEADER_LENGTH;
                            int sourceItemSize = sourceArgPtr.getLength() + itemHeaderSize;
                            int destItemSize = destArgPtr.getLength() + itemHeaderSize;
                            int startOfEdgeList = pathHeaderSize + sourceItemSize + destItemSize;
                            dataOutput.writeInt(startOfEdgeList);

                            // Write our edge list size.
                            int edgeItemSize = edgeArgPtr.getLength() + itemHeaderSize;
                            int endOfEdgeList = startOfEdgeList + edgeItemSize;
                            dataOutput.writeInt(endOfEdgeList);

                            // Write the number of edges (just 1).
                            dataOutput.writeInt(1);

                            // Write our source vertex and our destination vertex.
                            byte[] sourceData = sourceArgPtr.getByteArray();
                            byte[] destData = destArgPtr.getByteArray();
                            dataOutput.writeInt(sourceArgPtr.getLength());
                            dataOutput.write(sourceData, sourceArgPtr.getStartOffset(), sourceArgPtr.getLength());
                            dataOutput.writeInt(destArgPtr.getLength());
                            dataOutput.write(destData, destArgPtr.getStartOffset(), destArgPtr.getLength());

                            // Write our edge.
                            byte[] edgeData = edgeArgPtr.getByteArray();
                            dataOutput.writeInt(edgeArgPtr.getLength());
                            dataOutput.write(edgeData, edgeArgPtr.getStartOffset(), edgeArgPtr.getLength());
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
        return GraphixFunctionIdentifiers.CREATE_NEW_ONE_HOP_PATH;
    }
}
