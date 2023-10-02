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
package org.apache.asterix.column.values.writer;

import static org.apache.asterix.column.values.writer.filters.AbstractColumnFilterWriter.FILTER_SIZE;

import java.nio.ByteBuffer;
import java.util.PriorityQueue;

import org.apache.asterix.column.bytes.stream.out.ByteBufferOutputStream;
import org.apache.asterix.column.bytes.stream.out.MultiPersistentBufferBytesOutputStream;
import org.apache.asterix.column.bytes.stream.out.pointer.IReservedPointer;
import org.apache.asterix.column.values.IColumnBatchWriter;
import org.apache.asterix.column.values.IColumnValuesWriter;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;

/**
 * A writer for a batch columns' values
 */
public final class ColumnBatchWriter implements IColumnBatchWriter {
    private final ByteBufferOutputStream primaryKeys;
    private final MultiPersistentBufferBytesOutputStream columns;
    private final int pageSize;
    private final double tolerance;
    private final IReservedPointer columnLengthPointer;

    private ByteBuffer pageZero;
    private int columnsOffset;
    private int filtersOffset;
    private int primaryKeysOffset;
    private int nonKeyColumnStartOffset;

    public ColumnBatchWriter(Mutable<IColumnWriteMultiPageOp> multiPageOpRef, int pageSize, double tolerance) {
        this.pageSize = pageSize;
        this.tolerance = tolerance;
        primaryKeys = new ByteBufferOutputStream();
        columns = new MultiPersistentBufferBytesOutputStream(multiPageOpRef);
        columnLengthPointer = columns.createPointer();
    }

    @Override
    public void setPageZeroBuffer(ByteBuffer pageZero, int numberOfColumns, int numberOfPrimaryKeys) {
        this.pageZero = pageZero;
        int offset = pageZero.position();

        columnsOffset = offset;
        offset += numberOfColumns * Integer.BYTES;

        filtersOffset = offset;
        offset += numberOfColumns * FILTER_SIZE;

        pageZero.position(offset);
        primaryKeysOffset = offset;
        primaryKeys.reset(pageZero);
        nonKeyColumnStartOffset = pageZero.capacity();
    }

    @Override
    public int writePrimaryKeyColumns(IColumnValuesWriter[] primaryKeyWriters) throws HyracksDataException {
        int allocatedSpace = 0;
        for (int i = 0; i < primaryKeyWriters.length; i++) {
            IColumnValuesWriter writer = primaryKeyWriters[i];
            setColumnOffset(i, primaryKeysOffset + primaryKeys.size());
            writer.flush(primaryKeys);
            allocatedSpace += writer.getAllocatedSpace();
        }
        return allocatedSpace;
    }

    @Override
    public int writeColumns(PriorityQueue<IColumnValuesWriter> nonKeysColumnWriters) throws HyracksDataException {
        int allocatedSpace = 0;
        columns.reset();
        while (!nonKeysColumnWriters.isEmpty()) {
            IColumnValuesWriter writer = nonKeysColumnWriters.poll();
            writeColumn(writer);
            allocatedSpace += writer.getAllocatedSpace();
        }
        return allocatedSpace;
    }

    private void writeColumn(IColumnValuesWriter writer) throws HyracksDataException {
        if (!hasEnoughSpace(columns.getCurrentBufferPosition(), writer)) {
            /*
             * We reset the columns stream to write all pages and confiscate a new buffer to minimize splitting
             * the columns value into multiple pages.
             */
            nonKeyColumnStartOffset += columns.capacity();
            columns.reset();
        }

        int columnRelativeOffset = columns.size();
        columns.reserveInteger(columnLengthPointer);
        setColumnOffset(writer.getColumnIndex(), nonKeyColumnStartOffset + columnRelativeOffset);

        writeFilter(writer);
        writer.flush(columns);

        int length = columns.size() - columnRelativeOffset;
        columnLengthPointer.setInteger(length);
    }

    private boolean hasEnoughSpace(int bufferPosition, IColumnValuesWriter columnWriter) {
        //Estimated size mostly overestimate the size
        int columnSize = columnWriter.getEstimatedSize();
        float remainingPercentage = (pageSize - bufferPosition) / (float) pageSize;
        if (columnSize > pageSize) {
            /*
             * If the column size is larger than the page size, we check whether the remaining space is less than
             * the tolerance percentage
             * - true  --> allocate new buffer and tolerate empty space
             * - false --> we split the column into two pages
             */
            return remainingPercentage >= tolerance;
        }

        int freeSpace = pageSize - (bufferPosition + columnSize);

        /*
         * Check if the free space is enough to fit the column or the free space is less that the tolerance percentage
         * - true  --> we allocate new buffer and tolerate empty space
         * - false --> we split the column into two pages
         */
        return freeSpace > columnSize || remainingPercentage >= tolerance;
    }

    private void setColumnOffset(int columnIndex, int offset) {
        pageZero.putInt(columnsOffset + Integer.BYTES * columnIndex, offset);
    }

    private void writeFilter(IColumnValuesWriter writer) {
        int offset = filtersOffset + writer.getColumnIndex() * FILTER_SIZE;
        pageZero.putLong(offset, writer.getNormalizedMinValue());
        pageZero.putLong(offset + Long.BYTES, writer.getNormalizedMaxValue());
    }
}
