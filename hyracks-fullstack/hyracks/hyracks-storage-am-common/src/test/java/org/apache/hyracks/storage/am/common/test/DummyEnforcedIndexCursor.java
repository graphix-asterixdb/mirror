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
package org.apache.hyracks.storage.am.common.test;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.common.EnforcedIndexCursor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.ISearchPredicate;

public class DummyEnforcedIndexCursor extends EnforcedIndexCursor {
    @Override
    protected void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        // Dummy
    }

    @Override
    protected boolean doHasNext() throws HyracksDataException {
        // Dummy
        return false;
    }

    @Override
    protected void doNext() throws HyracksDataException {
        // Dummy
    }

    @Override
    protected void doDestroy() throws HyracksDataException {
        // Dummy
    }

    @Override
    protected void doClose() throws HyracksDataException {
        // Dummy
    }

    @Override
    protected ITupleReference doGetTuple() {
        return null;
    }

}
