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
package org.apache.asterix.transaction.management.service.transaction;

import static org.apache.asterix.common.transactions.ITransactionManager.AtomicityLevel;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.TransactionOptions;
import org.apache.asterix.common.transactions.TxnId;

public class TransactionContextFactory {

    private TransactionContextFactory() {
    }

    public static ITransactionContext create(TxnId txnId, TransactionOptions options, INcApplicationContext appCtx) {
        final AtomicityLevel atomicityLevel = options.getAtomicityLevel();
        switch (atomicityLevel) {
            case ATOMIC:
                return new AtomicTransactionContext(txnId);
            case ATOMIC_NO_WAL:
                return new AtomicNoWALTransactionContext(txnId, appCtx);
            case ENTITY_LEVEL:
                return new EntityLevelTransactionContext(txnId);
            default:
                throw new IllegalStateException("Unknown transaction context type: " + atomicityLevel);
        }
    }
}
