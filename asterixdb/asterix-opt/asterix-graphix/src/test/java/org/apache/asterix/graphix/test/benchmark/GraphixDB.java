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
package org.apache.asterix.graphix.test.benchmark;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.ldbcouncil.snb.driver.Db;
import org.ldbcouncil.snb.driver.DbConnectionState;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.OperationHandler;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.control.LoggingService;

public abstract class GraphixDB extends Db {
    private GraphixConnectionState connectionState;

    @Override
    protected void onInit(Map<String, String> map, LoggingService loggingService) throws DbException {
        connectionState = new GraphixConnectionState(map);
    }

    @Override
    protected void onClose() throws IOException {
        connectionState.close();
    }

    @Override
    protected DbConnectionState getConnectionState() {
        return connectionState;
    }

    protected abstract static class BaseQuery<T extends Operation<S>, S> {
        private final SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

        protected String getQueryText(T operation, GraphixConnectionState connectionState) throws DbException {
            String queryText = connectionState.getQuery(operation.type());
            for (Map.Entry<String, Object> parameterEntry : operation.parameterMap().entrySet()) {
                String parameterValue;
                if (parameterEntry.getValue() instanceof String) {
                    parameterValue = String.format("\"%s\"", parameterEntry.getValue());

                } else if (parameterEntry.getValue() instanceof Date) {
                    Date parameterAsDate = (Date) parameterEntry.getValue();
                    parameterValue = String.format("DATETIME(\"%s\")", datetimeFormat.format(parameterAsDate));

                } else if (parameterEntry.getValue() instanceof Number) {
                    parameterValue = parameterEntry.getValue().toString();

                } else {
                    throw new DbException(new IllegalStateException("Unexpected parameter encountered!"));
                }
                queryText = queryText.replace("$" + parameterEntry.getKey(), parameterValue);
            }
            return queryText;
        }
    }

    protected abstract static class SingleResultQuery<T extends Operation<S>, S> extends BaseQuery<T, S>
            implements OperationHandler<T, GraphixConnectionState> {
        protected abstract Class<S> getResultType();

        @Override
        public void executeOperation(T operation, GraphixConnectionState connectionState, ResultReporter resultReporter)
                throws DbException {
            String queryText = getQueryText(operation, connectionState);
            List<S> results = connectionState.executeStatement(queryText, getResultType());
            resultReporter.report(0, results.get(0), operation);
        }
    }

    protected abstract static class MultipleResultQuery<T extends Operation<List<S>>, S> extends BaseQuery<T, List<S>>
            implements OperationHandler<T, GraphixConnectionState> {
        protected abstract Class<S> getResultType();

        @Override
        public void executeOperation(T operation, GraphixConnectionState connectionState, ResultReporter resultReporter)
                throws DbException {
            String queryText = getQueryText(operation, connectionState);
            List<S> results = connectionState.executeStatement(queryText, getResultType());
            resultReporter.report(0, results, operation);
        }
    }
}
