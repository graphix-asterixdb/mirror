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

import org.apache.asterix.dataflow.data.nontagged.serde.ABooleanSerializerDeserializer;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public final class JBoolean extends JObject<Boolean> {

    private boolean aBoolean;

    public JBoolean(boolean value) {
        this.aBoolean = value;
    }

    public void setValue(boolean value) {
        this.aBoolean = value;
    }

    public boolean getValue() {
        return aBoolean;
    }

    public Boolean getValueGeneric() {
        return aBoolean;
    }

    @Override
    public IAType getIAType() {
        return BuiltinType.ABOOLEAN;
    }

    @Override
    public IAObject getIAObject() {
        return aBoolean ? ABoolean.TRUE : ABoolean.FALSE;
    }

    @Override
    public void setValueGeneric(Boolean b) {
        setValue(b);
    }

    @Override
    public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
        serializeTypeTag(writeTypeTag, dataOutput, ATypeTag.BOOLEAN);
        ABooleanSerializerDeserializer.INSTANCE.serialize((ABoolean) getIAObject(), dataOutput);
    }

    @Override
    public void reset() {
        aBoolean = false;
    }

}
