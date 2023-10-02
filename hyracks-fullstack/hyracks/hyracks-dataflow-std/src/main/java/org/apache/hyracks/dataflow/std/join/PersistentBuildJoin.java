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

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedMemoryConstrain;
import org.apache.hyracks.dataflow.std.buffermanager.PreferToSpillFullyOccupiedFramePolicy;
import org.apache.hyracks.dataflow.std.buffermanager.VPartitionTupleBufferManager;

/**
 * A specialized instance of {@link OptimizedHybridHashJoin} that assumes the build-phase artifacts
 * (i.e. the hash table) of another {@link OptimizedHybridHashJoin}.
 */
public class PersistentBuildJoin extends OptimizedHybridHashJoin {
    public static void closeSpilledProbePartitions(OptimizedHybridHashJoin inputOHHJ) throws HyracksDataException {
        inputOHHJ.closeAllSpilledPartitions(inputOHHJ.probeRFWriters, inputOHHJ.probeRelName);
    }

    public PersistentBuildJoin(OptimizedHybridHashJoin inputOOHJ) throws HyracksDataException {
        super(inputOOHJ.jobletCtx, inputOOHJ.memSizeInFrames, inputOOHJ.numOfPartitions, inputOOHJ.probeRelName,
                inputOOHJ.buildRelName, inputOOHJ.probeRd, inputOOHJ.buildRd, inputOOHJ.probeHpc, inputOOHJ.buildHpc,
                inputOOHJ.probePredEval, inputOOHJ.buildPredEval, inputOOHJ.isLeftOuter, inputOOHJ.nullWriterFactories,
                inputOOHJ.tableFactory);

        // Keep all of our build-side artifacts.
        System.arraycopy(inputOOHJ.buildRFWriters, 0, this.buildRFWriters, 0, inputOOHJ.buildRFWriters.length);
        this.buildPSizeInTups = inputOOHJ.buildPSizeInTups;
        this.bigFrameAppender = inputOOHJ.bigFrameAppender;
        this.framePool = inputOOHJ.framePool;
        this.table = inputOOHJ.table;
        this.spilledStatus.or(inputOOHJ.spilledStatus);

        // A new in-memory-JOIN is required, to avoid stepping on toes when closing our input.
        IPartitionedMemoryConstrain constrain = VPartitionTupleBufferManager.NO_CONSTRAIN;
        this.bufferManager = new VPartitionTupleBufferManager(constrain, inputOOHJ.numOfPartitions, this.framePool);
        this.spillPolicy = new PreferToSpillFullyOccupiedFramePolicy(bufferManager, this.spilledStatus);
        this.inMemJoiner = new InMemoryHashJoin(inputOOHJ.jobletCtx, inputOOHJ.inMemJoiner, inputOOHJ.probeRd);

        // Initialize our probe.
        this.probePSizeInTups = new int[inputOOHJ.probePSizeInTups.length];
        Arrays.fill(this.probePSizeInTups, 0);
    }

    // We do not want to delete our build run-files.
    @Override
    public RunFileReader getBuildRFReader(int pid) throws HyracksDataException {
        return buildRFWriters[pid] == null ? null : buildRFWriters[pid].createReader();
    }

    // We disallow any build-methods to be called from this point forward.
    @Override
    public final void initBuild() throws HyracksDataException {
        throw new HyracksDataException(ErrorCode.ILLEGAL_STATE, "Build phase cannot be invoked PersistentBuildJoin!");
    }

    @Override
    public final void build(ByteBuffer buffer) throws HyracksDataException {
        throw new HyracksDataException(ErrorCode.ILLEGAL_STATE, "Build phase cannot be invoked PersistentBuildJoin!");
    }

    @Override
    public void closeBuild() throws HyracksDataException {
        throw new HyracksDataException(ErrorCode.ILLEGAL_STATE, "Build phase cannot be invoked PersistentBuildJoin!");
    }
}
