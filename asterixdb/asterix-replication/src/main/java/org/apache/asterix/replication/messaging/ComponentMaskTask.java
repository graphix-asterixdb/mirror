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
package org.apache.asterix.replication.messaging;

import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.exceptions.ReplicationException;
import org.apache.asterix.common.storage.ResourceReference;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.asterix.replication.api.IReplicaTask;
import org.apache.asterix.replication.api.IReplicationWorker;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;

/**
 * A task to create a mask file for an incoming lsm component from master
 */
public class ComponentMaskTask implements IReplicaTask {

    private final String file;

    public ComponentMaskTask(String file) {
        this.file = file;
    }

    @Override
    public void perform(INcApplicationContext appCtx, IReplicationWorker worker) {
        try {
            IIOManager ioManager = appCtx.getIoManager();
            // create mask
            final FileReference maskPath = getComponentMaskPath(ioManager, file);
            ioManager.create(maskPath);
            ReplicationProtocol.sendAck(worker.getChannel(), worker.getReusableBuffer());
        } catch (IOException e) {
            throw new ReplicationException(e);
        }
    }

    public static FileReference getComponentMaskPath(IIOManager ioManager, String componentFile) throws IOException {
        final FileReference localPath = ioManager.resolve(componentFile);
        final FileReference resourceDir = localPath.getParent();
        ioManager.makeDirectories(resourceDir);
        final String componentSequence = ResourceReference.getComponentSequence(componentFile);
        return resourceDir.getChild(StorageConstants.COMPONENT_MASK_FILE_PREFIX + componentSequence);
    }

    @Override
    public ReplicationProtocol.ReplicationRequestType getMessageType() {
        return ReplicationProtocol.ReplicationRequestType.LSM_COMPONENT_MASK;
    }

    @Override
    public void serialize(OutputStream out) throws HyracksDataException {
        try {
            final DataOutputStream dos = new DataOutputStream(out);
            dos.writeUTF(file);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    public static ComponentMaskTask create(DataInput input) throws IOException {
        String indexFile = input.readUTF();
        return new ComponentMaskTask(indexFile);
    }
}
