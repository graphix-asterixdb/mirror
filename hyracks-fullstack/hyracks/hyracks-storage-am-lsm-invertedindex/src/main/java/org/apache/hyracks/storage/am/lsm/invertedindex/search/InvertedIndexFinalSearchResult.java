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

package org.apache.hyracks.storage.am.lsm.invertedindex.search;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.std.buffermanager.ISimpleFrameBufferManager;
import org.apache.hyracks.storage.am.common.api.INullIntrospector;

/**
 * This is an in-memory based storage for final results of inverted-index searches.
 * Only one frame is used at a time. The same frame will be used multiple times.
 */
public class InvertedIndexFinalSearchResult extends InvertedIndexSearchResult {

    public InvertedIndexFinalSearchResult(ITypeTraits[] invListFields, IHyracksTaskContext ctx,
            ISimpleFrameBufferManager bufferManager, ITypeTraits nullTypeTraits, INullIntrospector nullIntrospector)
            throws HyracksDataException {
        super(invListFields, ctx, bufferManager, nullTypeTraits, nullIntrospector);
    }

    /**
     * The final search result only needs to keep the inverted list fields, not its count.
     */
    @Override
    protected void initTypeTraits(ITypeTraits[] invListFields) {
        typeTraits = new ITypeTraits[invListFields.length];
        for (int i = 0; i < invListFields.length; i++) {
            typeTraits[i] = invListFields[i];
        }
    }

    /**
     * Prepares the write operation. A result of the final search result will be always in memory.
     */
    @Override
    public void prepareWrite(int numExpectedPages) throws HyracksDataException {
        // Final search result: we will use the ioBuffer and we will not create any file.
        // This method can be called multiple times in case of the partitioned T-Occurrence search.
        // For those cases, if the write process has already begun, we should not clear the buffer.
        isInMemoryOpMode = true;
        isFileOpened = false;
        resetAppenderLocation(IO_BUFFER_IDX);
        isWriteFinished = false;
    }

    /**
     * Appends an element to the frame of this result. When processing the final list,
     * it does not create an additional frame when a frame becomes full to let the caller consume the frame.
     *
     * @return false if the current frame for the final result is full.
     *         true otherwise.
     */
    @Override
    public boolean append(ITupleReference invListElement, int count) throws HyracksDataException {
        int numBytesRequired = getNumBytesRequired(invListElement);
        if (!appender.hasSpace(numBytesRequired)) {
            return false;
        }

        if (!appendInvertedListElement(invListElement)) {
            return false;
        }
        appender.incrementTupleCount(1);
        numResults++;

        return true;
    }

    /**
     * Finalizes the write operation.
     */
    @Override
    public void finalizeWrite() throws HyracksDataException {
        if (isWriteFinished) {
            return;
        }
        isWriteFinished = true;
    }

    /**
     * Prepares a read operation.
     */
    @Override
    public void prepareResultRead() throws HyracksDataException {
        if (isInReadMode) {
            return;
        }
        currentReaderBufIdx = 0;
        isInReadMode = true;
    }

    /**
     * Gets the next frame of the current result file.
     */
    @Override
    public ByteBuffer getNextFrame() throws HyracksDataException {
        return buffers.get(IO_BUFFER_IDX);
    }

    /**
     * Finishes reading the result and frees the buffer.
     */
    @Override
    public void closeResultRead(boolean deallocateIOBufferNeeded) throws HyracksDataException {
        // Deallocates I/O buffer if requested.
        if (deallocateIOBufferNeeded) {
            deallocateIOBuffer();
        }
    }

    /**
     * Deallocates the buffer.
     */
    @Override
    public void close() throws HyracksDataException {
        deallocateIOBuffer();
    }

    @Override
    public void reset() throws HyracksDataException {
        // Resets the I/O buffer.
        clearBuffer(ioBuffer);

        searchResultWriter = null;
        searchResultReader = null;
        isInReadMode = false;
        isWriteFinished = false;
        isInMemoryOpMode = false;
        isFileOpened = false;
        currentWriterBufIdx = 0;
        currentReaderBufIdx = 0;
        numResults = 0;
    }

    /**
     * Deallocates the I/O buffer (one frame). This should be the last operation.
     */
    @Override
    protected void deallocateIOBuffer() throws HyracksDataException {
        if (ioBufferFrame != null) {
            bufferManager.releaseFrame(ioBuffer);
            buffers.clear();
            ioBufferFrame = null;
            ioBuffer = null;
        }
    }

    /**
     * Resets the buffer.
     */
    public void resetBuffer() {
        appender.reset(buffers.get(IO_BUFFER_IDX));
    }

}
