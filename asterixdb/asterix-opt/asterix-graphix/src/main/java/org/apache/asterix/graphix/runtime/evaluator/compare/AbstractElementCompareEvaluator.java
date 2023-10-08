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

import java.io.DataOutput;

import org.apache.asterix.dataflow.data.nontagged.serde.AObjectSerializerDeserializer;
import org.apache.asterix.graphix.runtime.pointable.GraphixPathPointable;
import org.apache.asterix.graphix.runtime.pointable.consumer.IPointableConsumer;
import org.apache.asterix.om.base.ABoolean;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.DataUtils;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractElementCompareEvaluator implements IScalarEvaluator {
    protected final AObjectSerializerDeserializer serde = AObjectSerializerDeserializer.INSTANCE;
    protected final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    protected final DataOutput dataOutput = resultStorage.getDataOutput();

    protected final ListItemCompareCallback vertexListItemCallback = new ListItemCompareCallback();
    protected final ListItemCompareCallback edgeListItemCallback = new ListItemCompareCallback();
    protected final GraphixPathPointable pathPtr =
            new GraphixPathPointable(vertexListItemCallback, edgeListItemCallback);

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        if (!readTuple(tuple, result)) {
            return;
        }

        resultStorage.reset();
        if (compare()) {
            // We have found a duplicate item. Exit and return false.
            serde.serialize(ABoolean.FALSE, dataOutput);

        } else {
            // No duplicate items have been found. Return true.
            serde.serialize(ABoolean.TRUE, dataOutput);
        }
        result.set(resultStorage);
    }

    /**
     * @return True if we should proceed (and result has been set). False otherwise.
     */
    protected abstract boolean readTuple(IFrameTupleReference tuple, IPointable result) throws HyracksDataException;

    protected abstract boolean compare() throws HyracksDataException;

    // We treat vertices / edges as black-boxes, we do not know their contents. We compare blindly.
    public static final class ListItemCompareCallback implements IPointableConsumer {
        private final VoidPointable inputItemPtr = new VoidPointable();
        private boolean isMatchFound = false;

        public void set(IPointable elementPointable) {
            isMatchFound = false;
            inputItemPtr.set(elementPointable);
        }

        public boolean isMatchFound() {
            return isMatchFound;
        }

        @Override
        public void accept(IPointable listItemPtr) {
            isMatchFound |= DataUtils.equals(inputItemPtr, listItemPtr);
        }
    }
}
