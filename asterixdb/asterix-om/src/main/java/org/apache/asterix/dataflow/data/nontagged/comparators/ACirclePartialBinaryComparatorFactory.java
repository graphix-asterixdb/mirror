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
package org.apache.asterix.dataflow.data.nontagged.comparators;

import org.apache.asterix.dataflow.data.nontagged.Coordinate;
import org.apache.asterix.dataflow.data.nontagged.serde.ACircleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;

import com.fasterxml.jackson.databind.JsonNode;

public class ACirclePartialBinaryComparatorFactory implements IBinaryComparatorFactory {

    private static final long serialVersionUID = 1L;
    public static final ACirclePartialBinaryComparatorFactory INSTANCE = new ACirclePartialBinaryComparatorFactory();

    private ACirclePartialBinaryComparatorFactory() {
    }

    @Override
    public IBinaryComparator createBinaryComparator() {
        return ACirclePartialBinaryComparatorFactory::compare;
    }

    @SuppressWarnings("squid:S1172") // unused parameter
    public static int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) throws HyracksDataException {
        // center.x
        int c = Double.compare(
                ADoubleSerializerDeserializer.getDouble(b1,
                        s1 + ACircleSerializerDeserializer.getCenterPointCoordinateOffset(Coordinate.X)),
                ADoubleSerializerDeserializer.getDouble(b2,
                        s2 + ACircleSerializerDeserializer.getCenterPointCoordinateOffset(Coordinate.X)));
        if (c == 0) {
            // center.y
            c = Double.compare(
                    ADoubleSerializerDeserializer.getDouble(b1,
                            s1 + ACircleSerializerDeserializer.getCenterPointCoordinateOffset(Coordinate.Y)),
                    ADoubleSerializerDeserializer.getDouble(b2,
                            s2 + ACircleSerializerDeserializer.getCenterPointCoordinateOffset(Coordinate.Y)));
            if (c == 0) {
                // radius
                return Double.compare(
                        ADoubleSerializerDeserializer.getDouble(b1,
                                s1 + ACircleSerializerDeserializer.getRadiusOffset()),
                        ADoubleSerializerDeserializer.getDouble(b2,
                                s2 + ACircleSerializerDeserializer.getRadiusOffset()));
            }
        }
        return c;
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        return registry.getClassIdentifier(getClass(), serialVersionUID);
    }

    @SuppressWarnings("squid:S1172") // unused parameter
    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json) {
        return INSTANCE;
    }
}
