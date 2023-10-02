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

import java.net.InetSocketAddress;
import java.util.Set;

public interface IReplicationDestination {

    /**
     * Adds {@code replica} to this {@link IReplicationDestination}
     *
     * @param replica
     */
    void add(IPartitionReplica replica);

    /**
     * Removes {@code replica} from this {@link IReplicationDestination}
     *
     * @param replica
     */
    void remove(IPartitionReplica replica);

    /**
     * Notifies that failure {@code failure} occurred on this {@link IReplicationDestination}
     *
     * @param failure
     */
    void notifyFailure(Exception failure);

    /**
     * Gets the list of replicas on this {@link IReplicationDestination}
     *
     * @return the list of replicas
     */
    Set<IPartitionReplica> getReplicas();

    /**
     * Gets the (resolved) location of this {@link IReplicationDestination}
     *
     * @return the (resolved) location
     */
    InetSocketAddress getLocation();
}
