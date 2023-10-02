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

package org.apache.hyracks.storage.am.lsm.invertedindex.ondisk;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedListSearchResultFrameTupleAppender;

/**
 * This class is mainly to write the intermediate results in the inverted-index **search** operation.
 * For the inverted list building phrase, please refer to IInvertedListBuilder.
 *
 * An appender class for an inverted list. Each frame has two integer values at the beginning and at the end.
 * The first represents the number of minimum Hyracks frames in a frame. Currently, we use 1 for this value.
 * The latter represents the number of tuples in a frame. This design is required since we may need to use
 * RunFileWriter and RunFileReader class during the inverted-index-search operation.
 *
 * Note that this appender is not aware of the tuple element type, and the length of the tuple is given by the caller
 * at run time.
 */
public class InvertedListSearchResultFrameTupleAppender implements IInvertedListSearchResultFrameTupleAppender {

    // At the end of a frame, an integer value is written to keep the tuple count in this frame.
    public static final int TUPLE_COUNT_SIZE = 4;
    // At the beginning of a frame, an integer value is written to keep the number of minimum frames in this frame.
    // For this class, the frame size is equal to the minimum frame size in Hyracks.
    public static final int MINFRAME_COUNT_SIZE = 4;

    private final int frameSize;
    private ByteBuffer buffer;
    private int tupleCount;
    private int tupleDataEndOffset;

    public InvertedListSearchResultFrameTupleAppender(int frameSize) {
        this.frameSize = frameSize;
    }

    public void reset(ByteBuffer buffer) {
        reset(buffer, true, 0, MINFRAME_COUNT_SIZE);
    }

    public void reset(ByteBuffer buffer, boolean clear, int tupleCount, int tupleDataEndOffset) {
        this.buffer = buffer;
        if (clear) {
            Arrays.fill(this.buffer.array(), (byte) 0);
            this.buffer.clear();
            // the number of minimum frames in a frame - it's one.
            FrameHelper.serializeFrameSize(this.buffer, 1);
        }
        // tuple count
        this.buffer.putInt(FrameHelper.getTupleCountOffset(frameSize), tupleCount);
        this.tupleCount = tupleCount;
        this.tupleDataEndOffset = tupleDataEndOffset;
    }

    public boolean append(byte[] bytes, int offset, int length) {
        if (tupleDataEndOffset + length + TUPLE_COUNT_SIZE <= frameSize) {
            System.arraycopy(bytes, offset, buffer.array(), tupleDataEndOffset, length);
            tupleDataEndOffset += length;
            return true;
        }
        return false;
    }

    public boolean append(int fieldValue) {
        if (tupleDataEndOffset + 4 + TUPLE_COUNT_SIZE <= frameSize) {
            buffer.putInt(tupleDataEndOffset, fieldValue);
            tupleDataEndOffset += 4;
            return true;
        }
        return false;
    }

    public boolean append(long fieldValue) {
        if (tupleDataEndOffset + 8 + TUPLE_COUNT_SIZE <= frameSize) {
            buffer.putLong(tupleDataEndOffset, fieldValue);
            tupleDataEndOffset += 8;
            return true;
        }
        return false;
    }

    public boolean append(char fieldValue) {
        if (tupleDataEndOffset + 2 + TUPLE_COUNT_SIZE <= frameSize) {
            buffer.putLong(tupleDataEndOffset, fieldValue);
            tupleDataEndOffset += 2;
            return true;
        }
        return false;
    }

    public boolean append(byte fieldValue) {
        if (tupleDataEndOffset + 1 + TUPLE_COUNT_SIZE <= frameSize) {
            buffer.put(tupleDataEndOffset, fieldValue);
            tupleDataEndOffset += 1;
            return true;
        }
        return false;
    }

    public boolean hasSpace(int length) {
        return tupleDataEndOffset + length + TUPLE_COUNT_SIZE <= frameSize;
    }

    public void incrementTupleCount(int count) {
        int tupleCountOffset = FrameHelper.getTupleCountOffset(frameSize);
        int currentCount = buffer.getInt(tupleCountOffset);
        int newCount = currentCount + count;
        buffer.putInt(tupleCountOffset, newCount);

        tupleCount += count;
        assert tupleCount == newCount;
    }

    public int getTupleCount() {
        return tupleCount;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

}
