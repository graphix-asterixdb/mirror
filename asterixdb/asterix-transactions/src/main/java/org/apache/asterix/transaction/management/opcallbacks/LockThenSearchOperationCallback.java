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
package org.apache.asterix.transaction.management.opcallbacks;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.AbstractOperationCallback;
import org.apache.asterix.common.transactions.DatasetId;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexFrameWriter;
import org.apache.hyracks.storage.common.ISearchOperationCallback;

public class LockThenSearchOperationCallback extends AbstractOperationCallback implements ISearchOperationCallback {

    /**
     * variables used for deadlock-free locking protocol
     */
    private final ILSMIndexFrameWriter operatorNodePushable;
    private int pkHash;

    public LockThenSearchOperationCallback(DatasetId datasetId, long resourceId, int[] entityIdFields,
            ITransactionSubsystem txnSubsystem, ITransactionContext txnCtx,
            IOperatorNodePushable operatorNodePushable) {
        super(datasetId, resourceId, entityIdFields, txnCtx, txnSubsystem.getLockManager());
        this.operatorNodePushable = (ILSMIndexFrameWriter) operatorNodePushable;
    }

    @Override
    public boolean proceed(ITupleReference tuple) throws HyracksDataException {
        return true;
    }

    @Override
    public void reconcile(ITupleReference tuple) throws HyracksDataException {
    }

    @Override
    public void cancel(ITupleReference tuple) throws HyracksDataException {
    }

    @Override
    public void complete(ITupleReference tuple) throws HyracksDataException {
    }

    @Override
    public void before(ITupleReference tuple) throws HyracksDataException {
        pkHash = computePrimaryKeyHashValue(tuple, primaryKeyFields);
        try {
            if (operatorNodePushable != null) {

                /**********************************************************************************
                 * In order to achieve deadlock-free locking protocol during any write (insert/delete/upsert)
                 * operations, the following logic is implemented.
                 * See https://cwiki.apache.org/confluence/display/ASTERIXDB/Deadlock-Free+Locking+Protocol for
                 * : more details.
                 * 1. for each entry in a frame
                 * 2. returnValue = tryLock() for an entry
                 * 3. if returnValue == false
                 * 3-1. flush all entries (which already acquired locks) to the next operator
                 * : this will make all those entries reach commit operator so that corresponding commit logs will
                 * : be created.
                 * 3-2. acquire lock using lock() instead of tryLock() for the failed entry
                 * : we know for sure this lock call will not cause deadlock since the transactor doesn't hold any
                 * : other locks.
                 * 4. create an update log and insert the entry
                 * From the above logic, step 2 and 3 are implemented in this before() method.
                 **********************/

                //release all locks held by this actor (which is a thread) by flushing partial frame.
                boolean tryLockSucceed = lockManager.tryLock(datasetId, pkHash, LockMode.X, txnCtx);
                if (!tryLockSucceed) {
                    //flush entries which have been inserted already to release locks hold by them
                    operatorNodePushable.flushPartialFrame();

                    //acquire lock
                    lockManager.lock(datasetId, pkHash, LockMode.X, txnCtx);
                }

            } else {
                //operatorNodePushable can be null when metadata node operation is executed
                lockManager.lock(datasetId, pkHash, LockMode.X, txnCtx);
            }
        } catch (ACIDException e) {
            throw HyracksDataException.create(e);
        }
    }

    public void release() throws ACIDException {
        lockManager.unlock(datasetId, pkHash, LockMode.X, txnCtx);
    }
}
