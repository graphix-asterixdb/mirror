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

import static org.apache.asterix.graphix.runtime.pointable.GraphixPathPointable.HEADER_EDGE_ITEM_COUNT;
import static org.apache.asterix.graphix.runtime.pointable.GraphixPathPointable.HEADER_EDGE_LIST_END;
import static org.apache.asterix.graphix.runtime.pointable.GraphixPathPointable.HEADER_VERTEX_LIST_END;
import static org.apache.asterix.graphix.runtime.pointable.GraphixPathPointable.PATH_HEADER_LENGTH;
import static org.apache.asterix.graphix.runtime.pointable.GraphixPathPointable.PATH_SERIALIZED_TYPE_TAG;
import static org.apache.asterix.graphix.runtime.pointable.SinglyLinkedListPointable.LIST_ITEM_LENGTH_SIZE;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.graphix.function.GraphixFunctionIdentifiers;
import org.apache.asterix.graphix.runtime.pointable.GraphixPathPointable;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Create a new path that includes our given vertices and edge. This action is append-only, so we do not peer inside the
 * existing vertex or edge lists. We expect the following as arguments:
 * <ol>
 *   <li>A vertex field, which will act as the destination vertex.</li>
 *   <li>An edge field, which will connect the source vertex and the destination.</li>
 *   <li>An existing path, which our vertices and edge will be added to.</li>
 * </ol>
 */
public class AppendToExistingPathDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final Logger LOGGER = LogManager.getLogger();
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

                    private final IScalarEvaluator vertexArgEval = args[0].createScalarEvaluator(ctx);
                    private final IScalarEvaluator edgeArgEval = args[1].createScalarEvaluator(ctx);
                    private final IScalarEvaluator pathArgEval = args[2].createScalarEvaluator(ctx);
                    private final IPointable vertexArgPtr = new VoidPointable();
                    private final IPointable edgeArgPtr = new VoidPointable();
                    private final IPointable pathArgPtr = new VoidPointable();

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        vertexArgEval.evaluate(tuple, vertexArgPtr);
                        edgeArgEval.evaluate(tuple, edgeArgPtr);
                        pathArgEval.evaluate(tuple, pathArgPtr);
                        if (PointableHelper.checkAndSetMissingOrNull(result, vertexArgPtr, edgeArgPtr, pathArgPtr)) {
                            return;
                        }
                        resultStorage.reset();

                        // Ensure that we have a path (i.e. bit-array) as our third argument.
                        byte typeTagByte = pathArgPtr.getByteArray()[pathArgPtr.getStartOffset()];
                        if (typeTagByte != GraphixPathPointable.PATH_SERIALIZED_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 2, typeTagByte,
                                    GraphixPathPointable.PATH_SERIALIZED_TYPE_TAG);
                        }

                        try {
                            // Build our path header. We start with our type tag.
                            dataOutput.writeByte(PATH_SERIALIZED_TYPE_TAG);
                            byte[] pathData = pathArgPtr.getByteArray();

                            // Write the end offset of our new vertex list.
                            int oldVertexLocalListEnd = IntegerPointable.getInteger(pathData,
                                    pathArgPtr.getStartOffset() + HEADER_VERTEX_LIST_END);
                            int vertexItemSize = vertexArgPtr.getLength() + LIST_ITEM_LENGTH_SIZE;
                            dataOutput.writeInt(oldVertexLocalListEnd + vertexItemSize);

                            // Write the end offset of our new edge list.
                            int oldEdgeLocalListEnd = IntegerPointable.getInteger(pathData,
                                    pathArgPtr.getStartOffset() + HEADER_EDGE_LIST_END);
                            int edgeItemSize = edgeArgPtr.getLength() + LIST_ITEM_LENGTH_SIZE;
                            dataOutput.writeInt(oldEdgeLocalListEnd + edgeItemSize + vertexItemSize);

                            // Increment our edge list item counter.
                            int oldEdgeItemCount = IntegerPointable.getInteger(pathData,
                                    pathArgPtr.getStartOffset() + HEADER_EDGE_ITEM_COUNT);
                            LOGGER.trace("Path of length {} found at the APPEND_TO_PATH call.", oldEdgeItemCount);
                            int edgeItemCount = oldEdgeItemCount + 1;
                            dataOutput.writeInt(edgeItemCount);

                            // Copy all of our old vertices.
                            int oldVertexListAbsoluteStart = pathArgPtr.getStartOffset() + PATH_HEADER_LENGTH;
                            int oldVertexListAbsoluteEnd = oldVertexLocalListEnd + pathArgPtr.getStartOffset();
                            int oldVertexListAbsoluteLength = oldVertexListAbsoluteEnd - oldVertexListAbsoluteStart;
                            dataOutput.write(pathData, oldVertexListAbsoluteStart, oldVertexListAbsoluteLength);

                            // Copy our new vertex.
                            byte[] vertexData = vertexArgPtr.getByteArray();
                            dataOutput.writeInt(vertexArgPtr.getLength());
                            dataOutput.write(vertexData, vertexArgPtr.getStartOffset(), vertexArgPtr.getLength());

                            // Copy all of our old edges.
                            int oldEdgeListAbsoluteStart = oldVertexLocalListEnd + pathArgPtr.getStartOffset();
                            int oldEdgeListAbsoluteEnd = oldEdgeLocalListEnd + pathArgPtr.getStartOffset();
                            int oldEdgeListAbsoluteLength = oldEdgeListAbsoluteEnd - oldEdgeListAbsoluteStart;
                            dataOutput.write(pathData, oldEdgeListAbsoluteStart, oldEdgeListAbsoluteLength);

                            // Copy our new edge.
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
        return GraphixFunctionIdentifiers.APPEND_TO_EXISTING_PATH;
    }
}
