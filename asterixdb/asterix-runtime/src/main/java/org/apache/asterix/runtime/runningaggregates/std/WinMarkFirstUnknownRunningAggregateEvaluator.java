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

package org.apache.asterix.runtime.runningaggregates.std;

import java.io.DataOutput;

import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IWindowAggregateEvaluator;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public final class WinMarkFirstUnknownRunningAggregateEvaluator implements IWindowAggregateEvaluator {

    @SuppressWarnings({ "rawtypes" })
    private final ISerializerDeserializer boolSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN);

    private final IScalarEvaluator[] argEvals;

    private final byte unknownTypeTag;

    private final TaggedValuePointable argValue;

    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();

    private final DataOutput dataOutput = resultStorage.getDataOutput();

    private boolean first;

    private boolean firstAllUnknown;

    WinMarkFirstUnknownRunningAggregateEvaluator(ATypeTag unknownTypeTag, IScalarEvaluator[] argEvals) {
        this.argEvals = argEvals;
        this.unknownTypeTag = unknownTypeTag.serialize();
        argValue = TaggedValuePointable.FACTORY.createPointable();
    }

    @Override
    public void init() {
    }

    @Override
    public void initPartition(long partitionLength) {
        first = true;
    }

    @Override
    public void step(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        boolean value = compute(tuple);
        resultStorage.reset();
        boolSerde.serialize(ABoolean.valueOf(value), dataOutput);
        result.set(resultStorage);
    }

    private boolean compute(IFrameTupleReference tuple) throws HyracksDataException {
        if (first) {
            firstAllUnknown = everyArgIsUnknown(tuple);
            first = false;
            return true;
        } else {
            boolean thisAllMissing = firstAllUnknown || everyArgIsUnknown(tuple);
            return !thisAllMissing;
        }
    }

    private boolean everyArgIsUnknown(IFrameTupleReference tuple) throws HyracksDataException {
        for (IScalarEvaluator argEval : argEvals) {
            argEval.evaluate(tuple, argValue);
            if (argValue.getTag() != unknownTypeTag) {
                return false;
            }
        }
        return true;
    }
}
