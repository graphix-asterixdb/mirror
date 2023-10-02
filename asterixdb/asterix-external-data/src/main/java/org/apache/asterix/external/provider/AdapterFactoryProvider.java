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
package org.apache.asterix.external.provider;

import java.util.Map;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.external.IExternalFilterEvaluatorFactory;
import org.apache.asterix.external.api.ITypedAdapterFactory;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;

/**
 * This class represents the entry point to all things adapters
 */
public class AdapterFactoryProvider {

    private AdapterFactoryProvider() {
    }

    // get adapter factory. this method has the side effect of modifying the configuration as necessary
    public static ITypedAdapterFactory getAdapterFactory(ICCServiceContext serviceCtx, String adapterName,
            Map<String, String> configuration, ARecordType itemType, ARecordType metaType,
            IWarningCollector warningCollector, IExternalFilterEvaluatorFactory filterEvaluatorFactory)
            throws HyracksDataException, AlgebricksException {
        ExternalDataUtils.defaultConfiguration(configuration);
        ExternalDataUtils.prepare(adapterName, configuration);
        ICcApplicationContext context = (ICcApplicationContext) serviceCtx.getApplicationContext();
        ITypedAdapterFactory adapterFactory =
                (ITypedAdapterFactory) context.getAdapterFactoryService().createAdapterFactory();
        adapterFactory.setOutputType(itemType);
        adapterFactory.setMetaType(metaType);
        adapterFactory.configure(serviceCtx, configuration, warningCollector, filterEvaluatorFactory);
        return adapterFactory;
    }
}
