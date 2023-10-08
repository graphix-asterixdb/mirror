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
package org.apache.asterix.graphix.test.runtime.reachability;

import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.algebricks.runtime.operators.std.AssignRuntimeFactory;
import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.accessors.LongBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.MurmurHash3BinaryHashFunctionFamily;
import org.apache.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import org.apache.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.join.JoinComparatorFactory;
import org.apache.hyracks.dataflow.std.join.OptimizedHybridHashJoinOperatorDescriptor;
import org.apache.hyracks.dataflow.std.misc.ReplicateOperatorDescriptor;
import org.apache.hyracks.dataflow.std.union.UnionAllOperatorDescriptor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * <i>UN</i>directed K-source reachability test harness, for finding all vertices that are reachable from
 * K-source vertices.
 */
@SuppressWarnings("ALL")
public class SSRUndirectedTestExecutor extends SSRDirectedTestExecutor {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final long serialVersionUID = 1L;

    protected static final RecordDescriptor afterGroupRecDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            // Person1Id | Person2Id | creationDate
            Integer64SerializerDeserializer.INSTANCE, Integer64SerializerDeserializer.INSTANCE,
            new UTF8StringSerializerDeserializer() });

    @Override
    protected IOperatorDescriptor buildPBJBuildInputOperator(JobSpecification spec, ISSReachabilityTestFixture test) {
        // Build our SCAN operator for PERSONS.
        FileSplit[] personsFileSplits = test.getPersonsFileSplits();
        IFileSplitProvider personsSplitProvider = new ConstantFileSplitProvider(personsFileSplits);
        IOperatorDescriptor personsScanOp = new FileScanOperatorDescriptor(spec, personsSplitProvider,
                new DelimitedDataTupleParserFactory(personsParserFactories, '|'), personsRecDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, personsScanOp, test.getNodeNames());
        IConnectorDescriptor personsScannerConn = new MToNPartitioningConnectorDescriptor(spec,
                FieldHashPartitionComputerFactory.of(new int[] { 1 }, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(LongPointable.FACTORY) }));

        // Build our SCAN operator for KNOWS.
        FileSplit[] knowsFileSplits = test.getKnowsFileSplits();
        IFileSplitProvider knowsSplitProvider = new ConstantFileSplitProvider(knowsFileSplits);
        IOperatorDescriptor knowsScanOp = new FileScanOperatorDescriptor(spec, knowsSplitProvider,
                new DelimitedDataTupleParserFactory(knowsParserFactories, '|'), knowsRecDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, knowsScanOp, test.getNodeNames());
        IConnectorDescriptor knowsScannerConn = new OneToOneConnectorDescriptor(spec);

        // Build our REPLICATE operator for KNOWS.
        IOperatorDescriptor knowsReplicateOp = new ReplicateOperatorDescriptor(spec, knowsRecDesc, 2);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, knowsReplicateOp, test.getNodeNames());
        spec.connect(knowsScannerConn, knowsScanOp, 0, knowsReplicateOp, 0);
        IConnectorDescriptor knowsReplicateConn1 = new OneToOneConnectorDescriptor(spec);
        IConnectorDescriptor knowsReplicateConn2 = new OneToOneConnectorDescriptor(spec);

        // We want to flip Person1Id and Person2Id of the second replicate branch.
        IPushRuntimeFactory flipFactory = new AssignRuntimeFactory(new int[] { 3 },
                new IScalarEvaluatorFactory[] { new ColumnAccessEvalFactory(1) }, new int[] { 0, 2, 3 });
        IOperatorDescriptor flipOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 1,
                new IPushRuntimeFactory[] { flipFactory }, new RecordDescriptor[] { knowsRecDesc });
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, flipOp, test.getNodeNames());
        spec.connect(knowsReplicateConn2, knowsReplicateOp, 1, flipOp, 0);
        IConnectorDescriptor flipConn = new OneToOneConnectorDescriptor(spec);

        // Now we want to connect both branches back together...
        IOperatorDescriptor unionAllOp = new UnionAllOperatorDescriptor(spec, 2, knowsRecDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, unionAllOp, test.getNodeNames());
        spec.connect(knowsReplicateConn1, knowsReplicateOp, 0, unionAllOp, 0);
        spec.connect(flipConn, flipOp, 0, unionAllOp, 1);
        IConnectorDescriptor unionAllConn = new MToNPartitioningConnectorDescriptor(spec,
                FieldHashPartitionComputerFactory.of(new int[] { 2 }, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(LongPointable.FACTORY) }));

        // Join PERSONS (build) and KNOWS (probe) together (through Person2Id of KNOWS).
        int knowsFieldCount = knowsRecDesc.getFieldCount();
        int personsFieldCount = personsRecDesc.getFieldCount();
        ISerializerDeserializer[] joinSerde = new ISerializerDeserializer[knowsFieldCount + personsFieldCount];
        System.arraycopy(knowsRecDesc.getFields(), 0, joinSerde, 0, knowsFieldCount);
        System.arraycopy(personsRecDesc.getFields(), 0, joinSerde, knowsFieldCount, personsFieldCount);
        RecordDescriptor personKnowsRecDesc = new RecordDescriptor(joinSerde);
        IOperatorDescriptor personsKnowsJoinOp =
                new OptimizedHybridHashJoinOperatorDescriptor(spec, 32, -1, 1.3, new int[] { 2 }, new int[] { 1 },
                        new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE },
                        new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE },
                        personKnowsRecDesc, new JoinComparatorFactory(LongBinaryComparatorFactory.INSTANCE, 2, 1),
                        new JoinComparatorFactory(LongBinaryComparatorFactory.INSTANCE, 1, 2), null, null, false, null);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, personsKnowsJoinOp, test.getNodeNames());
        spec.connect(personsScannerConn, personsScanOp, 0, personsKnowsJoinOp, 1);
        spec.connect(unionAllConn, unionAllOp, 0, personsKnowsJoinOp, 0);
        return personsKnowsJoinOp;
    }
}
