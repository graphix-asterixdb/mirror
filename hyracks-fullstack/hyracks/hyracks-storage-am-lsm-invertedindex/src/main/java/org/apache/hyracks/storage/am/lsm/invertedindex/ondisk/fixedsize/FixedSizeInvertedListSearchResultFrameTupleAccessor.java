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

package org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.fixedsize;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.AbstractInvertedListSearchResultFrameTupleAccessor;
import org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.InvertedListSearchResultFrameTupleAppender;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexUtils;

/**
 * This is a fixed-size tuple accessor class to get one inverted list.
 * The frame structure: [4 bytes for minimum Hyracks frame count] [fixed-size tuple 1] ... [fixed-size tuple n] ...
 * [4 bytes for the tuple count in a frame]
 */
public class FixedSizeInvertedListSearchResultFrameTupleAccessor
        extends AbstractInvertedListSearchResultFrameTupleAccessor {

    private final int tupleSize;
    private final int[] fieldStartOffsets;

    public FixedSizeInvertedListSearchResultFrameTupleAccessor(int frameSize, ITypeTraits[] fields)
            throws HyracksDataException {
        super(frameSize, fields);

        this.fieldStartOffsets = new int[fields.length];
        this.fieldStartOffsets[0] = 0;
        for (int i = 1; i < fields.length; i++) {
            fieldStartOffsets[i] = fieldStartOffsets[i - 1] + fields[i - 1].getFixedLength();
        }

        int tmp = 0;
        for (int i = 0; i < fields.length; i++) {
            tmp += fields[i].getFixedLength();
        }
        tupleSize = tmp;
    }

    @Override
    protected void verifyTypeTraits() throws HyracksDataException {
        InvertedIndexUtils.verifyAllFixedSizeTypeTrait(fields);

        if (InvertedIndexUtils.checkTypeTraitsAllFixed(fields) == false) {
            throw HyracksDataException.create(ErrorCode.INVALID_INVERTED_LIST_TYPE_TRAITS,
                    InvertedIndexUtils.EXPECT_ALL_FIX_GET_VAR_SIZE);
        }
    }

    @Override
    public int getFieldEndOffset(int tupleIndex, int fIdx) {
        return getTupleStartOffset(tupleIndex) + fieldStartOffsets[fIdx] + fields[fIdx].getFixedLength();
    }

    @Override
    public int getFieldLength(int tupleIndex, int fIdx) {
        return fields[fIdx].getFixedLength();
    }

    @Override
    public int getTupleLength(int tupleIndex) {
        return getTupleEndOffset(tupleIndex) - getTupleStartOffset(tupleIndex);
    }

    @Override
    public int getFieldSlotsLength() {
        return 0;
    }

    @Override
    public int getFieldStartOffset(int tupleIndex, int fIdx) {
        return getTupleStartOffset(tupleIndex) + fieldStartOffsets[fIdx];
    }

    @Override
    public int getTupleEndOffset(int tupleIndex) {
        return getFieldEndOffset(tupleIndex, fields.length - 1);
    }

    @Override
    public int getTupleStartOffset(int tupleIndex) {
        return InvertedListSearchResultFrameTupleAppender.MINFRAME_COUNT_SIZE + tupleIndex * tupleSize;
    }

    @Override
    public int getAbsoluteFieldStartOffset(int tupleIndex, int fIdx) {
        return getTupleStartOffset(tupleIndex) + getFieldSlotsLength() + getFieldStartOffset(tupleIndex, fIdx);
    }
}
