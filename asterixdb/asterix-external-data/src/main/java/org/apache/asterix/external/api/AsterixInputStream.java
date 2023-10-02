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
package org.apache.asterix.external.api;

import java.io.InputStream;

import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.asterix.external.util.IFeedLogManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public abstract class AsterixInputStream extends InputStream {

    protected AbstractFeedDataFlowController controller;
    protected IFeedLogManager logManager;
    protected IStreamNotificationHandler notificationHandler;

    public abstract boolean stop() throws Exception;

    public abstract boolean handleException(Throwable th);

    // TODO: Find a better way to send notifications
    public void setController(AbstractFeedDataFlowController controller) {
        this.controller = controller;
    }

    // TODO: Find a better way to send notifications
    public void setFeedLogManager(IFeedLogManager logManager) throws HyracksDataException {
        this.logManager = logManager;
    }

    public void setNotificationHandler(IStreamNotificationHandler notificationHandler) {
        this.notificationHandler = notificationHandler;
    }

    public String getStreamName() {
        return "";
    }

    public String getPreviousStreamName() {
        return "";
    }
}
