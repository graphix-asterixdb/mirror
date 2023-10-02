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

package org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.variablesize;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.tuples.TypeAwareTupleReference;
import org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.AbstractInvertedListTupleReference;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexUtils;

public class VariableSizeInvertedListTupleReference extends AbstractInvertedListTupleReference {

    private final ITreeIndexTupleReference tupleReference;

    @Override
    protected void verifyTypeTrait() throws HyracksDataException {
        InvertedIndexUtils.verifyHasVarSizeTypeTrait(typeTraits);
    }

    public VariableSizeInvertedListTupleReference(ITypeTraits[] typeTraits, ITypeTraits nullTypeTraits)
            throws HyracksDataException {
        super(typeTraits);

        this.tupleReference = new TypeAwareTupleReference(typeTraits, nullTypeTraits);
    }

    @Override
    protected void calculateFieldStartOffsets() {
        tupleReference.resetByTupleOffset(data, startOff);
    }

    @Override
    public int getFieldCount() {
        return typeTraits.length;
    }

    @Override
    public byte[] getFieldData(int fIdx) {
        return data;
    }

    @Override
    public int getFieldLength(int fIdx) {
        return tupleReference.getFieldLength(fIdx);
    }

    @Override
    public int getFieldStart(int fIdx) {
        return tupleReference.getFieldStart(fIdx);
    }
}
