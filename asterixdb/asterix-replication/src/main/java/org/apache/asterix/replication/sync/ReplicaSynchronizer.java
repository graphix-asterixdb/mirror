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
package org.apache.asterix.replication.sync;

import java.io.IOException;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.replication.IReplicationStrategy;
import org.apache.asterix.common.transactions.ICheckpointManager;
import org.apache.asterix.replication.api.PartitionReplica;
import org.apache.asterix.replication.messaging.CheckpointPartitionIndexesTask;
import org.apache.asterix.replication.messaging.ReplicationProtocol;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Performs the steps required to ensure any newly added replica
 * will be in-sync with master
 */
public class ReplicaSynchronizer {

    private static final Logger LOGGER = LogManager.getLogger();
    private final INcApplicationContext appCtx;
    private final PartitionReplica replica;

    public ReplicaSynchronizer(INcApplicationContext appCtx, PartitionReplica replica) {
        this.appCtx = appCtx;
        this.replica = replica;
    }

    public void sync(boolean register, boolean deltaRecovery) throws IOException {
        LOGGER.debug("starting replica sync process for replica {}", replica);
        Object partitionLock = appCtx.getReplicaManager().getPartitionSyncLock(replica.getIdentifier().getPartition());
        synchronized (partitionLock) {
            LOGGER.trace("acquired partition replica lock");
            final ICheckpointManager checkpointManager = appCtx.getTransactionSubsystem().getCheckpointManager();
            try {
                // suspend checkpointing datasets to prevent async IO operations while sync'ing replicas
                checkpointManager.suspend();
                LOGGER.debug("starting replica files sync");
                syncFiles(deltaRecovery);
                LOGGER.debug("completed replica files sync");
                checkpointReplicaIndexes();
                LOGGER.debug("replica indexes checkpoint completed");
                if (register) {
                    LOGGER.debug("registering replica");
                    appCtx.getReplicationManager().register(replica);
                    LOGGER.debug("replica registered");
                }
            } finally {
                checkpointManager.resume();
            }
        }
    }

    private void syncFiles(boolean deltaRecovery) throws IOException {
        final ReplicaFilesSynchronizer fileSync = new ReplicaFilesSynchronizer(appCtx, replica, deltaRecovery);
        // flush replicated dataset to generate disk component for any remaining in-memory components
        final IReplicationStrategy replStrategy = appCtx.getReplicationManager().getReplicationStrategy();
        appCtx.getDatasetLifecycleManager().flushDataset(replStrategy,
                p -> p == replica.getIdentifier().getPartition());
        waitForReplicatedDatasetsIO();
        LOGGER.debug("flushed partition datasets");
        fileSync.sync();
    }

    private void checkpointReplicaIndexes() throws IOException {
        final int partition = replica.getIdentifier().getPartition();
        String masterNode =
                appCtx.getReplicaManager().isPartitionOrigin(partition) ? appCtx.getServiceContext().getNodeId() : null;
        CheckpointPartitionIndexesTask task =
                new CheckpointPartitionIndexesTask(partition, getPartitionMaxComponentId(partition), masterNode);
        LOGGER.debug("asking replica to checkpoint indexes");
        ReplicationProtocol.sendTo(replica, task);
        ReplicationProtocol.waitForAck(replica);
    }

    private long getPartitionMaxComponentId(int partition) throws HyracksDataException {
        final IReplicationStrategy replStrategy = appCtx.getReplicationManager().getReplicationStrategy();
        final PersistentLocalResourceRepository localResourceRepository =
                (PersistentLocalResourceRepository) appCtx.getLocalResourceRepository();
        return localResourceRepository.getReplicatedIndexesMaxComponentId(partition, replStrategy);
    }

    private void waitForReplicatedDatasetsIO() throws HyracksDataException {
        // wait for IO operations to ensure replicated datasets files won't change during replica sync
        final IReplicationStrategy replStrategy = appCtx.getReplicationManager().getReplicationStrategy();
        appCtx.getDatasetLifecycleManager().waitForIO(replStrategy, replica.getIdentifier().getPartition());
    }
}
