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
package org.apache.asterix.graphix.algebra.operator.physical;

import org.apache.asterix.algebra.operators.physical.BTreeSearchPOperator;
import org.apache.asterix.common.cluster.PartitioningProperties;
import org.apache.asterix.metadata.declared.DataSourceIndex;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.properties.BroadcastPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.UnorderedPartitionedProperty;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;

public class UnsortedBTreeSearchPOperator extends BTreeSearchPOperator {
    public UnsortedBTreeSearchPOperator(BTreeSearchPOperator bTreeSearchPOp) {
        super(bTreeSearchPOp);
    }

    protected boolean useBatchPointSearch(ILogicalOperator op, PhysicalOptimizationConfig config) {
        // Batch point search requires a sort, which we are explicitly disabling here.
        return false;
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) throws AlgebricksException {
        if (requiresBroadcast) {
            if (isPrimaryIndex && isEqCondition) {
                Index searchIndex = ((DataSourceIndex) idx).getIndex();
                Index.ValueIndexDetails indexDetails = (Index.ValueIndexDetails) searchIndex.getIndexDetails();
                int numberOfKeyFields = indexDetails.getKeyFieldNames().size();
                boolean areLowKeysProvided = lowKeyVarList.size() == numberOfKeyFields;
                boolean areHighKeysProvided = highKeyVarList.size() == numberOfKeyFields;
                if (numberOfKeyFields < 2 || (areLowKeysProvided && areHighKeysProvided)) {
                    StructuralPropertiesVector[] pv = new StructuralPropertiesVector[1];
                    ListSet<LogicalVariable> searchKeyVars = new ListSet<>();
                    searchKeyVars.addAll(lowKeyVarList);
                    searchKeyVars.addAll(highKeyVarList);
                    MetadataProvider mp = (MetadataProvider) context.getMetadataProvider();
                    Dataset dataset = mp.findDataset(searchIndex.getDataverseName(), searchIndex.getDatasetName());
                    PartitioningProperties partitioningProperties = mp.getPartitioningProperties(dataset);
                    pv[0] = new StructuralPropertiesVector(UnorderedPartitionedProperty.ofPartitionsMap(searchKeyVars,
                            domain, partitioningProperties.getComputeStorageMap()), null);
                    return new PhysicalRequirements(pv, IPartitioningRequirementsCoordinator.NO_COORDINATION);
                }
            }
            StructuralPropertiesVector[] pv = new StructuralPropertiesVector[1];
            pv[0] = new StructuralPropertiesVector(new BroadcastPartitioningProperty(domain), null);
            return new PhysicalRequirements(pv, IPartitioningRequirementsCoordinator.NO_COORDINATION);

        } else {
            return super.getRequiredPropertiesForChildren(op, reqdByParent, context);
        }
    }
}
