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
package org.apache.asterix.app.message;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;

/**
 * Message sent from NC to CC on successful local commit of an atomic statement/job.
 */
public class AtomicJobCompletionMessage implements ICcAddressedMessage {

    private static final long serialVersionUID = 1L;
    private final String nodeId;
    private final JobId jobId;

    public AtomicJobCompletionMessage(JobId jobId, String nodeId) {
        this.jobId = jobId;
        this.nodeId = nodeId;
    }

    @Override
    public void handle(ICcApplicationContext appCtx) throws HyracksDataException, InterruptedException {
        appCtx.getGlobalTxManager().handleJobCompletionMessage(jobId, nodeId);
    }

    @Override
    public String toString() {
        return "AtomicJobCompletionMessage{" + "jobId=" + jobId + ", nodeId='" + nodeId + '\'' + '}';
    }
}
