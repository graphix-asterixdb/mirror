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
package org.apache.asterix.external.library.java.base;

import java.io.DataOutput;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

import org.apache.asterix.dataflow.data.nontagged.serde.ADurationSerializerDeserializer;
import org.apache.asterix.om.base.AMutableDuration;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public final class JDuration extends JObject<Duration> {

    public JDuration(int months, long milliseconds) {
        super(new AMutableDuration(months, milliseconds));
    }

    public void setValue(int months, long milliseconds) {
        ((AMutableDuration) value).setValue(months, milliseconds);
    }

    public Duration getValueGeneric() {
        int months = ((AMutableDuration) value).getMonths();
        long millis = ((AMutableDuration) value).getMilliseconds();
        return Duration.of(months, ChronoUnit.MONTHS).plusMillis(millis);
    }

    @Override
    public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
        serializeTypeTag(writeTypeTag, dataOutput, ATypeTag.DURATION);
        ADurationSerializerDeserializer.INSTANCE.serialize((AMutableDuration) value, dataOutput);
    }

    @Override
    public void reset() {
        ((AMutableDuration) value).setValue(0, 0);
    }

    @Override
    public IAType getIAType() {
        return BuiltinType.ADURATION;
    }

    @Override
    public void setValueGeneric(Duration o) {
        long months = o.get(ChronoUnit.MONTHS);
        if (months > Integer.MAX_VALUE) {
            throw new ArithmeticException("Overflow");
        }
        long ms = o.minus(Duration.of(months, ChronoUnit.MONTHS)).get(ChronoUnit.MILLIS);
        setValue((int) months, ms);
    }
}
