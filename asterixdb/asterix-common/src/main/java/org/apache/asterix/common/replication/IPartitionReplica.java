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
package org.apache.asterix.common.replication;

import org.apache.asterix.common.storage.ReplicaIdentifier;

public interface IPartitionReplica {

    enum PartitionReplicaStatus {
        /* replica is in-sync with master */
        IN_SYNC,
        /* replica is still catching up with master */
        CATCHING_UP,
        /* replica is not connected with master */
        DISCONNECTED
    }

    /**
     * Gets the status of a replica.
     *
     * @return The status
     */
    PartitionReplicaStatus getStatus();

    /**
     * Gets the identifier of a replica
     *
     * @return The identifier
     */
    ReplicaIdentifier getIdentifier();

    /**
     * Notifies that failure {@code failure} occurred on this replica
     *
     * @param failure
     */
    void notifyFailure(Exception failure);

    /**
     * Gets the current sync progress
     *
     * @return the current sync progress
     */
    double getSyncProgress();

    /**
     * Gets the last progress time of this replica based on System.nanoTime
     *
     * @return the last progress time
     */
    long getLastProgressTime();
}
