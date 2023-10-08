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

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.graphix.function.GraphixFunctionIdentifiers;
import org.apache.asterix.graphix.runtime.pointable.GraphixPathPointable;
import org.apache.asterix.graphix.runtime.pointable.SinglyLinkedListPointable;
import org.apache.asterix.graphix.runtime.pointable.consumer.IPointableConsumer;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class TranslateForwardPathDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    protected AOrderedListType vertexListType;
    protected AOrderedListType edgeListType;
    protected ARecordType pathRecordType;

    @Override
    public void setImmutableStates(Object... states) {
        vertexListType = new AOrderedListType((IAType) states[0], null);
        edgeListType = new AOrderedListType((IAType) states[1], null);
        pathRecordType = new ARecordType(null, new String[] { VERTICES_FIELD_NAME, EDGES_FIELD_NAME },
                new IAType[] { vertexListType, edgeListType }, false);
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {
                    // A path record consists of an edge list and a vertex list.
                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final ArrayBackedValueStorage edgeListStorage = new ArrayBackedValueStorage();
                    private final ArrayBackedValueStorage vertexListStorage = new ArrayBackedValueStorage();
                    private final OrderedListBuilder edgeListBuilder = new OrderedListBuilder();
                    private final OrderedListBuilder vertexListBuilder = new OrderedListBuilder();
                    private final RecordBuilder recBuilder = new RecordBuilder();

                    // We will iterate through our singly-linked-list and add to the lists above as we traverse.
                    private final IScalarEvaluator pathArgEval = args[0].createScalarEvaluator(ctx);
                    private final IPointableConsumer edgeCallback = edgeListBuilder::addItem;
                    private final IPointableConsumer vertexCallback = vertexListBuilder::addItem;
                    private final GraphixPathPointable pathPointable =
                            new GraphixPathPointable(vertexCallback, edgeCallback);

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

                        // Iterate through our path vertices...
                        vertexListStorage.reset();
                        vertexListBuilder.reset(vertexListType);
                        SinglyLinkedListPointable vertexListPointable = pathPointable.getVertexListPointable();
                        while (vertexListPointable.hasNext()) {
                            vertexListPointable.next();
                        }
                        vertexListBuilder.write(vertexListStorage.getDataOutput(), true);

                        // ...and our path edges...
                        edgeListStorage.reset();
                        edgeListBuilder.reset(edgeListType);
                        SinglyLinkedListPointable edgeListPointable = pathPointable.getEdgeListPointable();
                        while (edgeListPointable.hasNext()) {
                            edgeListPointable.next();
                        }
                        edgeListBuilder.write(edgeListStorage.getDataOutput(), true);

                        // ...and finally, build the path record.
                        resultStorage.reset();
                        recBuilder.reset(pathRecordType);
                        recBuilder.init();
                        recBuilder.addField(pathRecordType.getFieldIndex(VERTICES_FIELD_NAME), vertexListStorage);
                        recBuilder.addField(pathRecordType.getFieldIndex(EDGES_FIELD_NAME), edgeListStorage);
                        recBuilder.write(resultStorage.getDataOutput(), true);
                        result.set(resultStorage);
                    }
                };
            }

        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return GraphixFunctionIdentifiers.TRANSLATE_FORWARD_PATH;
    }
}
