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

package org.apache.asterix.transaction.management.runtime;

import org.apache.asterix.common.api.IJobEventListenerFactory;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractPushRuntimeFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionerFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IJobletEventListenerFactory;

public class CommitRuntimeFactory extends AbstractPushRuntimeFactory {

    private static final long serialVersionUID = 2L;

    protected final ITuplePartitionerFactory partitionerFactory;
    protected final int datasetId;
    protected final int[] primaryKeyFields;
    protected final boolean isWriteTransaction;
    protected int[] datasetPartitions;
    protected final boolean isSink;

    public CommitRuntimeFactory(int datasetId, int[] primaryKeyFields, boolean isWriteTransaction,
            int[] datasetPartitions, boolean isSink, ITuplePartitionerFactory partitionerFactory) {
        this.partitionerFactory = partitionerFactory;
        this.datasetId = datasetId;
        this.primaryKeyFields = primaryKeyFields;
        this.isWriteTransaction = isWriteTransaction;
        this.datasetPartitions = datasetPartitions;
        this.isSink = isSink;
    }

    @Override
    public String toString() {
        return "commit";
    }

    @Override
    public IPushRuntime[] createPushRuntime(IHyracksTaskContext ctx) throws HyracksDataException {
        IJobletEventListenerFactory fact = ctx.getJobletContext().getJobletEventListenerFactory();
        return new IPushRuntime[] { new CommitRuntime(ctx, ((IJobEventListenerFactory) fact).getTxnId(datasetId),
                datasetId, primaryKeyFields, isWriteTransaction,
                datasetPartitions[ctx.getTaskAttemptId().getTaskId().getPartition()], isSink, partitionerFactory,
                datasetPartitions) };
    }
}
