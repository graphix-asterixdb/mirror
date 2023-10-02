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
package org.apache.hyracks.dataflow.std.structures;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.context.IHyracksFrameMgrContext;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.buffermanager.ISimpleFrameBufferManager;

/**
 * A child of {@link SerializableHashTable} that can be moved in and out of disk.
 */
public class PersistentSerializableHashTable extends SerializableHashTable {
    private final TableEnvironment tableEnvironment = new TableEnvironment();
    private final RunFileFrameBuffer runFileFrameBuffer = new RunFileFrameBuffer();

    public PersistentSerializableHashTable(int tableSize, final IHyracksFrameMgrContext ctx,
            ISimpleFrameBufferManager bufferManager) throws HyracksDataException {
        super(tableSize, ctx, bufferManager);
    }

    public void save(RunFileWriter runFileWriter) throws HyracksDataException {
        // Save our headers first, then our content frames.
        for (IntSerDeBuffer header : headers) {
            runFileWriter.nextFrame(header.getByteBuffer());
        }
        for (IntSerDeBuffer content : contents) {
            runFileWriter.nextFrame(content.getByteBuffer());
        }
        tableEnvironment.accept(this);
    }

    public void restore(RunFileReader runFileReader) throws HyracksDataException {
        // Restore our environment.
        tableEnvironment.apply(this);

        // Restore our headers, then our contents. We will re-acquire the memory we released.
        for (int i = 0; i < tableEnvironment.headerFrameSize; i++) {
            runFileFrameBuffer.byteBuffer = getFrame(frameSize);
            runFileReader.nextFrame(runFileFrameBuffer);
            headers[i] = new IntSerDeBuffer(runFileFrameBuffer.getBuffer(), false);
        }
        for (int i = 0; i < tableEnvironment.contentFrameSize; i++) {
            runFileFrameBuffer.byteBuffer = getFrame(frameSize);
            runFileReader.nextFrame(runFileFrameBuffer);
            contents.add(new IntSerDeBuffer(runFileFrameBuffer.getBuffer(), false));
        }
        assert currentByteSize == tableEnvironment.currentByteSize;
    }

    // We use the following the 'freeze' the non-frame contents of a table at a given time.
    protected static class TableEnvironment {
        // Data pertaining to header frames.
        private int headerFrameSize;

        // Data pertaining to the content frames.
        private int contentFrameSize;
        private List<Integer> currentOffsetInEachFrameList;
        private TuplePointer tempTuplePointer;
        private int frameCapacity;
        private int currentLargestFrameNumber;
        private int tupleCount;

        // Data pertaining to both header and content frames.
        private int currentByteSize;

        public void accept(SimpleSerializableHashTable inputTable) {
            this.headerFrameSize = inputTable.headers.length;
            this.contentFrameSize = inputTable.contents.size();
            this.frameCapacity = inputTable.frameCapacity;
            this.currentLargestFrameNumber = inputTable.currentLargestFrameNumber;
            this.tupleCount = inputTable.tupleCount;
            this.currentByteSize = inputTable.currentByteSize;
            this.tempTuplePointer = inputTable.tempTuplePointer;

            // The following must be deep-copy.
            this.currentOffsetInEachFrameList = new ArrayList<>(inputTable.currentOffsetInEachFrameList);
        }

        protected void apply(SimpleSerializableHashTable targetTable) {
            // We **do not** reset of currentByteSize (we have a sanity check above).
            targetTable.headers = new IntSerDeBuffer[this.headerFrameSize];
            targetTable.currentOffsetInEachFrameList = this.currentOffsetInEachFrameList;
            targetTable.frameCapacity = this.frameCapacity;
            targetTable.currentLargestFrameNumber = this.currentLargestFrameNumber;
            targetTable.tupleCount = this.tupleCount;
            targetTable.tempTuplePointer = this.tempTuplePointer;
        }
    }

    protected static class RunFileFrameBuffer implements IFrame {
        private ByteBuffer byteBuffer;

        @Override
        public ByteBuffer getBuffer() {
            return byteBuffer;
        }

        @Override
        public void ensureFrameSize(int frameSize) throws HyracksDataException {
            // The following method is called by RunFileReader, but we shouldn't do anything here. This is a NO-OP.
        }

        @Override
        public void resize(int frameSize) throws HyracksDataException {
            throw new HyracksDataException(ErrorCode.ILLEGAL_STATE, "RunFileFrameBuffer cannot be resized!");
        }

        @Override
        public int getFrameSize() {
            return byteBuffer.capacity();
        }

        @Override
        public int getMinSize() {
            return byteBuffer.capacity();
        }

        @Override
        public void reset() throws HyracksDataException {
            byteBuffer.clear();
        }
    }
}
