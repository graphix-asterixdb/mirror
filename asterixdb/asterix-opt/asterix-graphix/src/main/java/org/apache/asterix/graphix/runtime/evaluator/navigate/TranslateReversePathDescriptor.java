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

import static org.apache.asterix.graphix.type.TranslatePathTypeComputer.EDGES_FIELD_NAME;
import static org.apache.asterix.graphix.type.TranslatePathTypeComputer.VERTICES_FIELD_NAME;

import java.io.IOException;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.graphix.function.GraphixFunctionIdentifiers;
import org.apache.asterix.graphix.runtime.pointable.GraphixPathPointable;
import org.apache.asterix.graphix.runtime.pointable.SinglyLinkedListPointable;
import org.apache.asterix.graphix.runtime.pointable.consumer.IPointableConsumer;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class TranslateReversePathDescriptor extends TranslateForwardPathDescriptor {
    private static final long serialVersionUID = 1L;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            // TODO (GLENN): There is a definitely a better way to do this with less memory (+ object creation). :-)
            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {
                    // A path record consists of an edge list and a vertex list.
                    private final ArrayBackedValueStorage forwardItemAccessStorage = new ArrayBackedValueStorage();
                    private final ArrayBackedValueStorage forwardEdgeListStorage = new ArrayBackedValueStorage();
                    private final ArrayBackedValueStorage forwardVertexListStorage = new ArrayBackedValueStorage();
                    private final OrderedListBuilder forwardEdgeListBuilder = new OrderedListBuilder();
                    private final OrderedListBuilder forwardVertexListBuilder = new OrderedListBuilder();
                    private final ListAccessor forwardListAccessor = new ListAccessor();
                    private final AbstractPointable forwardListItem = new VoidPointable();

                    // We will iterate through our singly-linked-list and add to the lists above as we traverse.
                    private final IScalarEvaluator pathArgEval = args[0].createScalarEvaluator(ctx);
                    private final IPointableConsumer edgeCallback = forwardEdgeListBuilder::addItem;
                    private final IPointableConsumer vertexCallback = forwardVertexListBuilder::addItem;
                    private final GraphixPathPointable pathPointable =
                            new GraphixPathPointable(vertexCallback, edgeCallback);

                    // Because we have a *SLL*, we need to buffer our list and reverse to generate R2L lists.
                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final ArrayBackedValueStorage reverseEdgeListStorage = new ArrayBackedValueStorage();
                    private final ArrayBackedValueStorage reverseVertexListStorage = new ArrayBackedValueStorage();
                    private final OrderedListBuilder reverseEdgeListBuilder = new OrderedListBuilder();
                    private final OrderedListBuilder reverseVertexListBuilder = new OrderedListBuilder();
                    private final RecordBuilder recBuilder = new RecordBuilder();

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

                        // Iterate through our path vertices from L2R first...
                        forwardVertexListStorage.reset();
                        forwardVertexListBuilder.reset(vertexListType);
                        SinglyLinkedListPointable vertexListPointable = pathPointable.getVertexListPointable();
                        while (vertexListPointable.hasNext()) {
                            vertexListPointable.next();
                        }
                        forwardVertexListBuilder.write(forwardVertexListStorage.getDataOutput(), true);

                        // ...then reverse our path vertices list...
                        reverseVertexListStorage.reset();
                        reverseVertexListBuilder.reset(vertexListType);
                        forwardListAccessor.reset(forwardVertexListStorage.getByteArray(),
                                forwardVertexListStorage.getStartOffset());
                        for (int i = forwardListAccessor.size() - 1; i >= 0; i--) {
                            try {
                                forwardListAccessor.getOrWriteItem(i, forwardListItem, forwardItemAccessStorage);
                                reverseVertexListBuilder.addItem(forwardListItem);

                            } catch (IOException e) {
                                throw HyracksDataException.create(e);
                            }
                        }
                        reverseVertexListBuilder.write(reverseVertexListStorage.getDataOutput(), true);

                        // ...now iterate through our path edges...
                        forwardEdgeListStorage.reset();
                        forwardEdgeListBuilder.reset(edgeListType);
                        SinglyLinkedListPointable edgeListPointable = pathPointable.getEdgeListPointable();
                        while (edgeListPointable.hasNext()) {
                            edgeListPointable.next();
                        }
                        forwardEdgeListBuilder.write(forwardEdgeListStorage.getDataOutput(), true);

                        // ...and again in reverse...
                        reverseEdgeListStorage.reset();
                        reverseEdgeListBuilder.reset(edgeListType);
                        forwardListAccessor.reset(forwardEdgeListStorage.getByteArray(),
                                forwardEdgeListStorage.getStartOffset());
                        for (int i = forwardListAccessor.size() - 1; i >= 0; i--) {
                            try {
                                forwardListAccessor.getOrWriteItem(i, forwardListItem, forwardItemAccessStorage);
                                reverseEdgeListBuilder.addItem(forwardListItem);

                            } catch (IOException e) {
                                throw HyracksDataException.create(e);
                            }
                        }
                        reverseEdgeListBuilder.write(reverseEdgeListStorage.getDataOutput(), true);

                        // ...and finally, build the path record.
                        int verticesFieldIndex = pathRecordType.getFieldIndex(VERTICES_FIELD_NAME);
                        int edgesFieldIndex = pathRecordType.getFieldIndex(EDGES_FIELD_NAME);
                        resultStorage.reset();
                        recBuilder.reset(pathRecordType);
                        recBuilder.init();
                        recBuilder.addField(verticesFieldIndex, reverseVertexListStorage);
                        recBuilder.addField(edgesFieldIndex, reverseEdgeListStorage);
                        recBuilder.write(resultStorage.getDataOutput(), true);
                        result.set(resultStorage);
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return GraphixFunctionIdentifiers.TRANSLATE_REVERSE_PATH;
    }
}
