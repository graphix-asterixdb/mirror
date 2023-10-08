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
package org.apache.asterix.graphix.test.runtime.reachability;

import java.io.Serializable;

import org.apache.hyracks.api.io.FileSplit;

public interface ISSReachabilityTestFixture extends Serializable, AutoCloseable {
    /**
     * @return All node names in the working cluster.
     */
    String[] getNodeNames();

    /**
     * @return Locations of all PERSONS CSV files (in particular, which NCs contain which files).
     */
    FileSplit[] getPersonsFileSplits();

    /**
     * @return Locations of all KNOWS CSV files (in particular, which NCs contain which files).
     */
    FileSplit[] getKnowsFileSplits();

    /**
     * @return ID of the source person.
     */
    long getSourcePerson();

    /**
     * @return IP address of the cluster controller. By default, we use our localhost.
     */
    default String getClusterControllerHostIP() {
        return "127.0.0.1";
    }

    /**
     * @return Port to use to communicate with the cluster controller. By default, we use 39000.
     */
    default int getClusterControllerHostPort() {
        return 39000;
    }

    default String getDescription() {
        StringBuilder sb = new StringBuilder();
        sb.append("\nTest Name: ").append(this.getClass().getName()).append('\n');
        sb.append("Working Nodes: \n");
        for (String nodeName : getNodeNames()) {
            sb.append('\t').append(nodeName).append('\n');
        }
        sb.append("Person Files: \n");
        for (FileSplit fileSplit : getPersonsFileSplits()) {
            sb.append('\t').append(fileSplit.getPath());
            sb.append(" on node ").append(fileSplit.getNodeName()).append('\n');
        }
        sb.append("Knows Files: \n");
        for (FileSplit fileSplit : getKnowsFileSplits()) {
            sb.append('\t').append(fileSplit.getPath());
            sb.append(" on node ").append(fileSplit.getNodeName()).append('\n');
        }
        sb.append("Source Person: ").append(getSourcePerson()).append('\n');
        sb.append("Cluster Controller: ").append(getClusterControllerHostIP());
        sb.append(':').append(getClusterControllerHostPort());
        return sb.toString();
    }
}
