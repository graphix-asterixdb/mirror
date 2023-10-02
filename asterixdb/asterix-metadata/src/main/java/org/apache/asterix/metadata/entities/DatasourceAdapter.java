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
package org.apache.asterix.metadata.entities;

import org.apache.asterix.common.external.IDataSourceAdapter.AdapterType;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.external.dataset.adapter.AdapterIdentifier;
import org.apache.asterix.metadata.MetadataCache;
import org.apache.asterix.metadata.api.IMetadataEntity;

public class DatasourceAdapter implements IMetadataEntity<DatasourceAdapter> {

    private static final long serialVersionUID = 3L;

    private final AdapterIdentifier adapterIdentifier;
    private final String classname;
    private final AdapterType type;
    private final DataverseName libraryDataverseName;
    private final String libraryName;

    public DatasourceAdapter(AdapterIdentifier adapterIdentifier, AdapterType type, String classname,
            DataverseName libraryDataverseName, String libraryName) {
        this.adapterIdentifier = adapterIdentifier;
        this.type = type;
        this.classname = classname;
        this.libraryDataverseName = libraryDataverseName;
        this.libraryName = libraryName;
    }

    @Override
    public DatasourceAdapter addToCache(MetadataCache cache) {
        return cache.addAdapterIfNotExists(this);
    }

    @Override
    public DatasourceAdapter dropFromCache(MetadataCache cache) {
        return cache.dropAdapterIfExists(this);
    }

    public AdapterIdentifier getAdapterIdentifier() {
        return adapterIdentifier;
    }

    public String getClassname() {
        return classname;
    }

    public AdapterType getType() {
        return type;
    }

    public DataverseName getLibraryDataverseName() {
        return libraryDataverseName;
    }

    public String getLibraryName() {
        return libraryName;
    }
}
