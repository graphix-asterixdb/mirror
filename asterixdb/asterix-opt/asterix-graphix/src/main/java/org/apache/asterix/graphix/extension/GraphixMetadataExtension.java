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
package org.apache.asterix.graphix.extension;

import static org.apache.asterix.metadata.bootstrap.MetadataBootstrap.enlistMetadataDataset;

import java.rmi.RemoteException;
import java.util.List;

import org.apache.asterix.common.api.ExtensionId;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.metadata.bootstrap.GraphixIndexDetailProvider;
import org.apache.asterix.graphix.metadata.bootstrap.GraphixRecordDetailProvider;
import org.apache.asterix.graphix.metadata.bootstrap.IGraphixIndexDetail;
import org.apache.asterix.graphix.metadata.bootstrap.IRecordTypeDetail;
import org.apache.asterix.graphix.metadata.entity.dependency.IEntityRequirements;
import org.apache.asterix.graphix.metadata.entity.schema.Graph;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataNode;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.api.ExtensionMetadataDataset;
import org.apache.asterix.metadata.api.ExtensionMetadataDatasetId;
import org.apache.asterix.metadata.api.IExtensionMetadataSearchKey;
import org.apache.asterix.metadata.api.IMetadataExtension;
import org.apache.asterix.metadata.api.IMetadataIndex;
import org.apache.asterix.metadata.bootstrap.MetadataBootstrap;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.entitytupletranslators.MetadataTupleTranslatorProvider;
import org.apache.asterix.metadata.utils.MetadataConstants;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public class GraphixMetadataExtension implements IMetadataExtension {
    public static final ExtensionId GRAPHIX_METADATA_EXTENSION_ID =
            new ExtensionId(GraphixMetadataExtension.class.getName(), 0);

    public static Graph getGraph(MetadataTransactionContext mdTxnCtx, DataverseName dataverseName, String graphName)
            throws AlgebricksException {
        IExtensionMetadataSearchKey searchKey = new IExtensionMetadataSearchKey() {
            private static final long serialVersionUID = 1L;

            @Override
            public ExtensionMetadataDatasetId getDatasetId() {
                return GraphixIndexDetailProvider.getGraphIndexDetail().getExtensionDatasetID();
            }

            @Override
            public ITupleReference getSearchKey() {
                return MetadataNode.createTuple(dataverseName, graphName);
            }
        };
        List<Graph> graphs = MetadataManager.INSTANCE.getEntities(mdTxnCtx, searchKey);
        return (graphs.isEmpty()) ? null : graphs.get(0);
    }

    public static List<Graph> getAllGraphs(MetadataTransactionContext mdTxnTtx, DataverseName dataverseName)
            throws AlgebricksException {
        IExtensionMetadataSearchKey dataverseSearchKey = new IExtensionMetadataSearchKey() {
            private static final long serialVersionUID = 1L;

            @Override
            public ExtensionMetadataDatasetId getDatasetId() {
                return GraphixIndexDetailProvider.getGraphIndexDetail().getExtensionDatasetID();
            }

            @Override
            public ITupleReference getSearchKey() {
                return (dataverseName == null) ? null : MetadataNode.createTuple(dataverseName);
            }
        };
        return MetadataManager.INSTANCE.getEntities(mdTxnTtx, dataverseSearchKey);
    }

    public static List<IEntityRequirements> getAllEntityRequirements(MetadataTransactionContext mdTxnTtx)
            throws AlgebricksException {
        IExtensionMetadataSearchKey searchKey = new IExtensionMetadataSearchKey() {
            private static final long serialVersionUID = 1L;

            @Override
            public ExtensionMetadataDatasetId getDatasetId() {
                return GraphixIndexDetailProvider.getGraphDependencyIndexDetail().getExtensionDatasetID();
            }

            @Override
            public ITupleReference getSearchKey() {
                return null;
            }
        };
        return MetadataManager.INSTANCE.getEntities(mdTxnTtx, searchKey);
    }

    @Override
    public ExtensionId getId() {
        return GRAPHIX_METADATA_EXTENSION_ID;
    }

    @Override
    public void configure(List<Pair<String, String>> args) {
        // No (extra) configuration needed.
    }

    @Override
    public MetadataTupleTranslatorProvider getMetadataTupleTranslatorProvider() {
        return new MetadataTupleTranslatorProvider();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public List<ExtensionMetadataDataset> getExtensionIndexes() {
        try {
            return List.of(GraphixIndexDetailProvider.getGraphIndexDetail().getExtensionDataset(),
                    GraphixIndexDetailProvider.getGraphDependencyIndexDetail().getExtensionDataset());

        } catch (Throwable th) {
            th.printStackTrace();
            throw th;
        }
    }

    @Override
    public void initializeMetadata(INCServiceContext appCtx)
            throws HyracksDataException, RemoteException, ACIDException, AsterixException {
        // Enlist our datasets.
        IGraphixIndexDetail<?> graphIndexDetail = GraphixIndexDetailProvider.getGraphIndexDetail();
        IGraphixIndexDetail<?> dependencyIndexDetail = GraphixIndexDetailProvider.getGraphDependencyIndexDetail();
        ExtensionMetadataDataset<?> graphIndexDataset = graphIndexDetail.getExtensionDataset();
        ExtensionMetadataDataset<?> dependencyIndexDataset = dependencyIndexDetail.getExtensionDataset();
        enlistMetadataDataset(appCtx, graphIndexDataset, GraphixMetadataExtension::isCatalogUpgradeLegal);
        enlistMetadataDataset(appCtx, dependencyIndexDataset, GraphixMetadataExtension::isCatalogUpgradeLegal);

        // If this is a new universe, insert our extension datasets.
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        try {
            if (MetadataBootstrap.isNewUniverse()) {
                insertGraphDataset(mdTxnCtx);
                insertDependencyDataset(mdTxnCtx);

            } else {
                // If we are migrating from an existing non-Graphix install, then we need to update our metadata.
                if (MetadataManager.INSTANCE.getDataset(mdTxnCtx, MetadataConstants.METADATA_DATAVERSE_NAME,
                        graphIndexDataset.getIndexedDatasetName()) == null) {
                    insertGraphDataset(mdTxnCtx);
                }
                if (MetadataManager.INSTANCE.getDataset(mdTxnCtx, MetadataConstants.METADATA_DATAVERSE_NAME,
                        dependencyIndexDataset.getIndexedDatasetName()) == null) {
                    insertDependencyDataset(mdTxnCtx);
                }
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);

        } catch (Exception e) {
            MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            throw HyracksDataException.create(e);
        }

        // Expose our functions (both private and public).
        //        GraphixFunctionSupplier.INSTANCE.addToBuiltInFunctions();
    }

    private static void insertGraphDataset(MetadataTransactionContext mdTxnCtx) throws AlgebricksException {
        IGraphixIndexDetail<?> graphIndexDetail = GraphixIndexDetailProvider.getGraphIndexDetail();
        IRecordTypeDetail graphRecordDetail = GraphixRecordDetailProvider.getGraphRecordDetail();
        ExtensionMetadataDataset<?> extensionDataset = graphIndexDetail.getExtensionDataset();
        MetadataBootstrap.insertMetadataDatasets(mdTxnCtx, new IMetadataIndex[] { extensionDataset });
        MetadataManager.INSTANCE.addDatatype(mdTxnCtx, new Datatype(MetadataConstants.METADATA_DATAVERSE_NAME,
                graphRecordDetail.getRecordType().getTypeName(), graphRecordDetail.getRecordType(), false));
    }

    private static void insertDependencyDataset(MetadataTransactionContext mdTxnCtx) throws AlgebricksException {
        IGraphixIndexDetail<?> dependencyIndexDetail = GraphixIndexDetailProvider.getGraphDependencyIndexDetail();
        IRecordTypeDetail dependencyRecordDetail = GraphixRecordDetailProvider.getGraphDependencyRecordDetail();
        ExtensionMetadataDataset<?> extensionDataset = dependencyIndexDetail.getExtensionDataset();
        MetadataBootstrap.insertMetadataDatasets(mdTxnCtx, new IMetadataIndex[] { extensionDataset });
        MetadataManager.INSTANCE.addDatatype(mdTxnCtx, new Datatype(MetadataConstants.METADATA_DATAVERSE_NAME,
                dependencyRecordDetail.getRecordType().getTypeName(), dependencyRecordDetail.getRecordType(), false));
    }

    private static boolean isCatalogUpgradeLegal(IMetadataIndex index) {
        IGraphixIndexDetail<?> graphIndexDetail = GraphixIndexDetailProvider.getGraphIndexDetail();
        IGraphixIndexDetail<?> dependencyIndexDetail = GraphixIndexDetailProvider.getGraphDependencyIndexDetail();
        if (!MetadataBootstrap.isCatalogUpgradeLegal(index)) {
            return index.getIndexName().equals(graphIndexDetail.getDatasetName())
                    || index.getIndexName().equals(dependencyIndexDetail.getDatasetName());
        }
        return true;
    }

}
