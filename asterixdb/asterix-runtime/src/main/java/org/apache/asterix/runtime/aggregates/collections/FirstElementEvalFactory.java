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
package org.apache.asterix.runtime.aggregates.collections;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.aggregates.std.AbstractAggregateFunction;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class FirstElementEvalFactory implements IAggregateEvaluatorFactory {

    private static final long serialVersionUID = 1L;
    private final IScalarEvaluatorFactory[] args;
    private final boolean isLocal;
    private final SourceLocation sourceLoc;

    public FirstElementEvalFactory(IScalarEvaluatorFactory[] args, boolean isLocal, SourceLocation sourceLoc) {
        this.args = args;
        this.isLocal = isLocal;
        this.sourceLoc = sourceLoc;
    }

    @Override
    public IAggregateEvaluator createAggregateEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
        return new AbstractAggregateFunction(sourceLoc) {

            private boolean first = true;
            // Needs to copy the bytes from inputVal to outputVal because the byte space of inputVal could be re-used
            // by consequent tuples.
            private ArrayBackedValueStorage outputVal = new ArrayBackedValueStorage();
            private IPointable inputVal = new VoidPointable();
            private IScalarEvaluator eval = args[0].createScalarEvaluator(ctx);
            private final byte[] nullBytes = new byte[] { ATypeTag.SERIALIZED_NULL_TYPE_TAG };
            private final byte[] systemNullBytes = new byte[] { ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG };

            @Override
            public void init() throws HyracksDataException {
                first = true;
            }

            @Override
            public void step(IFrameTupleReference tuple) throws HyracksDataException {
                if (!first) {
                    return;
                }
                eval.evaluate(tuple, inputVal);
                byte typeTagByte = inputVal.getByteArray()[inputVal.getStartOffset()];
                if (typeTagByte == ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG) {
                    // Ignores SYSTEM_NULLs generated by local-first-element.
                    return;
                }
                outputVal.assign(inputVal);
                first = false;
            }

            @Override
            public void finish(IPointable result) throws HyracksDataException {
                if (first) {
                    result.set(isLocal ? systemNullBytes : nullBytes, 0, 1);
                    return;
                }
                result.set(outputVal);
            }

            @Override
            public void finishPartial(IPointable result) throws HyracksDataException {
                finish(result);
            }

        };
    }

}
