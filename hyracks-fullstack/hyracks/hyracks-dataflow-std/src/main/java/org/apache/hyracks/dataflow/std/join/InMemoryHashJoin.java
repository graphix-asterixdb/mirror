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
package org.apache.hyracks.dataflow.std.join;

import java.io.DataOutput;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksFrameMgrContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriter;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.buffermanager.ISimpleFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.TupleInFrameListAccessor;
import org.apache.hyracks.dataflow.std.structures.ISerializableTable;
import org.apache.hyracks.dataflow.std.structures.PersistentSerializableHashTable;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class InMemoryHashJoin {

    private final List<ByteBuffer> buffers;
    private final FrameTupleAccessor accessorBuild;
    private final ITuplePartitionComputer tpcBuild;
    private final IFrameTupleAccessor accessorProbe;
    private final ITuplePartitionComputer tpcProbe;
    private final FrameTupleAppender appender;
    private ITuplePairComparator tpComparator;
    private final boolean isLeftOuter;
    private final ArrayTupleBuilder missingTupleBuild;
    private final ISerializableTable table;
    private final TuplePointer storedTuplePointer;
    private final boolean reverseOutputOrder; //Should we reverse the order of tuples, we are writing in output
    private final TupleInFrameListAccessor tupleAccessor;
    // To release frames
    private final ISimpleFrameBufferManager bufferManager;
    private final boolean isTableCapacityNotZero;
    private final RecordDescriptor rDBuild;
    private final IMissingWriter[] missingWritersBuild;

    private static final Logger LOGGER = LogManager.getLogger();

    public InMemoryHashJoin(IHyracksFrameMgrContext ctx, FrameTupleAccessor accessorProbe,
            ITuplePartitionComputer tpcProbe, FrameTupleAccessor accessorBuild, RecordDescriptor rDBuild,
            ITuplePartitionComputer tpcBuild, boolean isLeftOuter, IMissingWriter[] missingWritersBuild,
            ISerializableTable table, ISimpleFrameBufferManager bufferManager) throws HyracksDataException {
        this(ctx, accessorProbe, tpcProbe, accessorBuild, rDBuild, tpcBuild, isLeftOuter, missingWritersBuild, table,
                false, bufferManager);
    }

    public InMemoryHashJoin(IHyracksFrameMgrContext ctx, FrameTupleAccessor accessorProbe,
            ITuplePartitionComputer tpcProbe, FrameTupleAccessor accessorBuild, RecordDescriptor rDBuild,
            ITuplePartitionComputer tpcBuild, boolean isLeftOuter, IMissingWriter[] missingWritersBuild,
            ISerializableTable table, boolean reverse, ISimpleFrameBufferManager bufferManager)
            throws HyracksDataException {
        this.table = table;
        storedTuplePointer = new TuplePointer();
        buffers = new ArrayList<>();
        this.accessorBuild = accessorBuild;
        this.tpcBuild = tpcBuild;
        this.accessorProbe = accessorProbe;
        this.tpcProbe = tpcProbe;
        appender = new FrameTupleAppender(new VSizeFrame(ctx));
        this.isLeftOuter = isLeftOuter;
        if (isLeftOuter) {
            int fieldCountOuter = accessorBuild.getFieldCount();
            missingTupleBuild = new ArrayTupleBuilder(fieldCountOuter);
            DataOutput out = missingTupleBuild.getDataOutput();
            for (int i = 0; i < fieldCountOuter; i++) {
                missingWritersBuild[i].writeMissing(out);
                missingTupleBuild.addFieldEndOffset();
            }
        } else {
            missingTupleBuild = null;
        }
        this.missingWritersBuild = missingWritersBuild;
        this.rDBuild = rDBuild;
        reverseOutputOrder = reverse;
        this.tupleAccessor = new TupleInFrameListAccessor(rDBuild, buffers);
        this.bufferManager = bufferManager;
        this.isTableCapacityNotZero = table.getTableSize() != 0;
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("InMemoryHashJoin has been created for a table size of " + table.getTableSize()
                    + " for Thread ID " + Thread.currentThread().getId() + ".");
        }
    }

    public InMemoryHashJoin(IHyracksFrameMgrContext ctx, InMemoryHashJoin inputIMHJ, RecordDescriptor rDProbe)
            throws HyracksDataException {
        this.tpcBuild = inputIMHJ.tpcBuild;
        this.tpcProbe = inputIMHJ.tpcProbe;
        this.buffers = inputIMHJ.buffers;
        this.table = inputIMHJ.table;
        this.rDBuild = inputIMHJ.rDBuild;
        this.bufferManager = inputIMHJ.bufferManager;
        this.isTableCapacityNotZero = inputIMHJ.isTableCapacityNotZero;
        this.isLeftOuter = inputIMHJ.isLeftOuter;
        this.reverseOutputOrder = inputIMHJ.reverseOutputOrder;
        this.missingWritersBuild = inputIMHJ.missingWritersBuild;
        this.tpComparator = inputIMHJ.tpComparator;

        // We do NOT copy the fields below, we require new instances.
        this.accessorBuild = new FrameTupleAccessor(inputIMHJ.rDBuild);
        this.accessorProbe = new FrameTupleAccessor(rDProbe);
        this.tupleAccessor = new TupleInFrameListAccessor(this.rDBuild, this.buffers);
        this.appender = new FrameTupleAppender(new VSizeFrame(ctx));
        this.storedTuplePointer = new TuplePointer();
        if (this.isLeftOuter) {
            int fieldCountOuter = this.accessorBuild.getFieldCount();
            this.missingTupleBuild = new ArrayTupleBuilder(fieldCountOuter);
            DataOutput out = this.missingTupleBuild.getDataOutput();
            for (int i = 0; i < fieldCountOuter; i++) {
                this.missingWritersBuild[i].writeMissing(out);
                this.missingTupleBuild.addFieldEndOffset();
            }
        } else {
            missingTupleBuild = null;
        }
    }

    public void build(ByteBuffer buffer) throws HyracksDataException {
        accessorBuild.reset(buffer);
        int tCount = accessorBuild.getTupleCount();
        if (tCount <= 0) {
            return;
        }
        buffers.add(buffer);
        int bIndex = buffers.size() - 1;
        for (int i = 0; i < tCount; ++i) {
            int entry = tpcBuild.partition(accessorBuild, i, table.getTableSize());
            storedTuplePointer.reset(bIndex, i);
            // If an insertion fails, then tries to insert the same tuple pointer again after compacting the table.
            if (!table.insert(entry, storedTuplePointer)) {
                if (!compactTableAndInsertAgain(entry, storedTuplePointer)) {
                    throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                            "Record insertion failed in in-memory hash join even after compaction.");
                }
            }
        }
    }

    public boolean compactTableAndInsertAgain(int entry, TuplePointer tPointer) throws HyracksDataException {
        boolean oneMoreTry = false;
        if (compactHashTable() >= 0) {
            oneMoreTry = table.insert(entry, tPointer);
        }
        return oneMoreTry;
    }

    /**
     * Tries to compact the table to make some space.
     *
     * @return the number of frames that have been reclaimed. If no compaction has happened, the value -1 is returned.
     */
    public int compactHashTable() throws HyracksDataException {
        if (table.isGarbageCollectionNeeded()) {
            return table.collectGarbage(tupleAccessor, tpcBuild);
        }
        return -1;
    }

    /**
     * Must be called before starting to join to set the right comparator with the right context.
     *
     * @param comparator the comparator to use for comparing the probe tuples against the build tuples
     */
    void setComparator(ITuplePairComparator comparator) {
        tpComparator = comparator;
    }

    /**
     * Reads the given tuple from the probe side and joins it with tuples from the build side.
     * This method assumes that the accessorProbe is already set to the current probe frame.
     */
    void join(int tid, IFrameWriter writer) throws HyracksDataException {
        boolean matchFound = false;
        if (isTableCapacityNotZero) {
            int entry = tpcProbe.partition(accessorProbe, tid, table.getTableSize());
            int tupleCount = table.getTupleCount(entry);
            for (int i = 0; i < tupleCount; i++) {
                table.getTuplePointer(entry, i, storedTuplePointer);
                int bIndex = storedTuplePointer.getFrameIndex();
                int tIndex = storedTuplePointer.getTupleIndex();
                accessorBuild.reset(buffers.get(bIndex));
                int c = tpComparator.compare(accessorProbe, tid, accessorBuild, tIndex);
                if (c == 0) {
                    matchFound = true;
                    appendToResult(tid, tIndex, writer);
                }
            }
        }
        if (!matchFound && isLeftOuter) {
            FrameUtils.appendConcatToWriter(writer, appender, accessorProbe, tid,
                    missingTupleBuild.getFieldEndOffsets(), missingTupleBuild.getByteArray(), 0,
                    missingTupleBuild.getSize());
        }
    }

    public void join(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        accessorProbe.reset(buffer);
        int tupleCount0 = accessorProbe.getTupleCount();
        for (int i = 0; i < tupleCount0; ++i) {
            join(i, writer);
        }
    }

    public void resetAccessorProbe(IFrameTupleAccessor newAccessorProbe) {
        accessorProbe.reset(newAccessorProbe.getBuffer());
    }

    public void completeJoin(IFrameWriter writer) throws HyracksDataException {
        appender.write(writer, true);
    }

    public void releaseMemory() throws HyracksDataException {
        int nFrames = buffers.size();
        // Frames assigned to the data table will be released here.
        if (bufferManager != null) {
            for (int i = 0; i < nFrames; i++) {
                bufferManager.releaseFrame(buffers.get(i));
            }
        }
        buffers.clear();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("InMemoryHashJoin has finished using " + nFrames + " frames for Thread ID "
                    + Thread.currentThread().getId() + ".");
        }
    }

    public void closeTable() throws HyracksDataException {
        table.close();
    }

    public void saveTable(RunFileWriter runFileWriter) throws HyracksDataException {
        PersistentSerializableHashTable hashTable = (PersistentSerializableHashTable) table;
        hashTable.save(runFileWriter);
    }

    public void restoreTable(RunFileReader runFileReader) throws HyracksDataException {
        PersistentSerializableHashTable hashTable = (PersistentSerializableHashTable) table;
        hashTable.restore(runFileReader);
    }

    private void appendToResult(int probeSidetIx, int buildSidetIx, IFrameWriter writer) throws HyracksDataException {
        if (reverseOutputOrder) {
            FrameUtils.appendConcatToWriter(writer, appender, accessorBuild, buildSidetIx, accessorProbe, probeSidetIx);
        } else {
            FrameUtils.appendConcatToWriter(writer, appender, accessorProbe, probeSidetIx, accessorBuild, buildSidetIx);
        }
    }
}
