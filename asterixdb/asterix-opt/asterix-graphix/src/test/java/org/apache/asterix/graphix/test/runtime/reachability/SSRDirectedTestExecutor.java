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

import static org.apache.hyracks.util.StorageUtil.StorageUnit.KILOBYTE;

import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.asterix.dataflow.data.nontagged.printers.adm.ARecordPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import org.apache.asterix.formats.nontagged.BinaryBooleanInspector;
import org.apache.asterix.graphix.runtime.evaluator.compare.IsDistinctVertexDescriptor;
import org.apache.asterix.graphix.runtime.evaluator.navigate.AppendToExistingPathDescriptor;
import org.apache.asterix.graphix.runtime.evaluator.navigate.CreateNewOneHopDescriptor;
import org.apache.asterix.graphix.runtime.evaluator.navigate.TranslateForwardPathDescriptor;
import org.apache.asterix.graphix.runtime.operator.LocalDistinctRuntimeFactory;
import org.apache.asterix.graphix.runtime.operator.buffer.IBufferCacheSupplier;
import org.apache.asterix.graphix.type.TranslatePathTypeComputer;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.comparisons.EqualsDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ClosedRecordConstructorDescriptor;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ConstantEvalFactory;
import org.apache.hyracks.algebricks.runtime.operators.message.MessageSinkRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.message.StateReleaseRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.algebricks.runtime.operators.std.AssignRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.std.StreamSelectRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.serializer.ResultSerializerFactoryProvider;
import org.apache.hyracks.algebricks.runtime.writers.PrinterBasedWriterFactory;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.result.IResultSet;
import org.apache.hyracks.api.result.IResultSetReader;
import org.apache.hyracks.api.result.ResultSetId;
import org.apache.hyracks.client.result.ResultSet;
import org.apache.hyracks.control.nc.resources.memory.FrameManager;
import org.apache.hyracks.data.std.accessors.LongBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.MurmurHash3BinaryHashFunctionFamily;
import org.apache.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ResultFrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.LongParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import org.apache.hyracks.dataflow.std.connectors.MToNBroadcastConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.StateReleaseConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import org.apache.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.iteration.FixedPointOperatorDescriptor;
import org.apache.hyracks.dataflow.std.join.JoinComparatorFactory;
import org.apache.hyracks.dataflow.std.join.OptimizedHybridHashJoinOperatorDescriptor;
import org.apache.hyracks.dataflow.std.join.PersistentBuildJoinOperatorDescriptor;
import org.apache.hyracks.dataflow.std.misc.ReplicateOperatorDescriptor;
import org.apache.hyracks.dataflow.std.result.ResultWriterOperatorDescriptor;
import org.apache.hyracks.ipc.impl.HyracksConnection;
import org.apache.hyracks.ipc.sockets.PlainSocketChannelFactory;
import org.apache.hyracks.test.support.TestStorageManagerComponentHolder;
import org.apache.hyracks.util.StorageUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Directed K-source reachability test harness, for finding all vertices that are reachable from K-source vertices.
 */
@SuppressWarnings("rawtypes")
public class SSRDirectedTestExecutor implements Serializable {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final long serialVersionUID = 1L;

    protected static final ARecordType personsRecType = new ARecordType("personsRecordType",
            new String[] { "creationDate", "id", "firstName", "lastName", "gender", "birthday", "locationIP",
                    "browserUsed", "language", "email" },
            new IAType[] { BuiltinType.ASTRING, BuiltinType.AINT64, BuiltinType.ASTRING, BuiltinType.ASTRING,
                    BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                    BuiltinType.ASTRING, BuiltinType.ASTRING },
            false);

    protected static final ARecordType pathPersonsRecType = new ARecordType("pathPersonsRecordType",
            new String[] { "id", "firstName" }, new IAType[] { BuiltinType.AINT64, BuiltinType.ASTRING }, false);

    protected static final ARecordType knowsRecType =
            new ARecordType("knowsRecordType", new String[] { "creationDate", "Person1Id", "Person2Id" },
                    new IAType[] { BuiltinType.ASTRING, BuiltinType.AINT64, BuiltinType.AINT64 }, false);

    protected static final ARecordType pathKnowsRecType = new ARecordType("pathKnowsRecordType",
            new String[] { "Person1Id", "Person2Id" }, new IAType[] { BuiltinType.AINT64, BuiltinType.AINT64 }, false);

    protected static final ARecordType pathRecType =
            new ARecordType("pathRecordType",
                    new String[] { TranslatePathTypeComputer.VERTICES_FIELD_NAME,
                            TranslatePathTypeComputer.EDGES_FIELD_NAME },
                    new IAType[] { new AOrderedListType(pathPersonsRecType, null),
                            new AOrderedListType(pathKnowsRecType, null) },
                    false);

    protected static final IValueParserFactory[] personsParserFactories = new IValueParserFactory[] {
            UTF8StringParserFactory.INSTANCE, LongParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
            UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
            UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
            UTF8StringParserFactory.INSTANCE };

    protected static final IValueParserFactory[] knowsParserFactories = new IValueParserFactory[] {
            UTF8StringParserFactory.INSTANCE, LongParserFactory.INSTANCE, LongParserFactory.INSTANCE };

    protected static final RecordDescriptor personsRecDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            // creationDate | id | firstName | lastName | gender | birthday
            new UTF8StringSerializerDeserializer(), Integer64SerializerDeserializer.INSTANCE,
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),

            // locationIP | browserUsed | language | email
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() });

    protected static final RecordDescriptor knowsRecDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            // creationDate | Person1Id | Person2Id
            new UTF8StringSerializerDeserializer(), Integer64SerializerDeserializer.INSTANCE,
            Integer64SerializerDeserializer.INSTANCE });

    protected static final RecordDescriptor fpRecDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            // id | workingPath
            Integer64SerializerDeserializer.INSTANCE, null });

    protected static final RecordDescriptor joinRecDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            // id | workingPath | creationDate | Person1ID | Person2ID
            Integer64SerializerDeserializer.INSTANCE, null, new UTF8StringSerializerDeserializer(),
            Integer64SerializerDeserializer.INSTANCE, Integer64SerializerDeserializer.INSTANCE,

            // creationDate | id | firstName | lastName | gender | birthday
            new UTF8StringSerializerDeserializer(), Integer64SerializerDeserializer.INSTANCE,
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),

            // locationIP | browserUsed | language | email
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() });

    static {
        TestStorageManagerComponentHolder.init(8192, 256, 256);
    }

    protected IOperatorDescriptor buildSourceOperator(JobSpecification spec, ISSReachabilityTestFixture test)
            throws Exception {
        // Build our SCAN operator for PERSONS (our source).
        FileSplit[] fileSplits = test.getPersonsFileSplits();
        IFileSplitProvider splitProvider = new ConstantFileSplitProvider(fileSplits);
        IOperatorDescriptor sourcePersonsScanOp = new FileScanOperatorDescriptor(spec, splitProvider,
                new DelimitedDataTupleParserFactory(personsParserFactories, '|'), personsRecDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sourcePersonsScanOp, test.getNodeNames());
        IConnectorDescriptor sourcePersonsConn = new OneToOneConnectorDescriptor(spec);

        // Filter only our source PERSON.
        IFunctionDescriptor equalFunction = new EqualsDescriptor();
        equalFunction.setImmutableStates(BuiltinType.AINT64, BuiltinType.AINT64);
        byte[] sourceIDInBytes = ArrayUtils.addAll(new byte[] { BuiltinType.AINT64.getTypeTag().serialize() },
                LongPointable.toByteArray(test.getSourcePerson()));
        IScalarEvaluatorFactory equalFactory = equalFunction.createEvaluatorFactory(new IScalarEvaluatorFactory[] {
                new TypedColumnAccessEvaluatorFactory(1, ATypeTag.BIGINT), new ConstantEvalFactory(sourceIDInBytes) });
        int[] noProjection = IntStream.range(0, personsRecDesc.getFields().length).toArray();
        IPushRuntimeFactory sourceSelectRuntime = new StreamSelectRuntimeFactory(equalFactory, noProjection,
                BinaryBooleanInspector.FACTORY, false, -1, null);
        IOperatorDescriptor sourceMetaOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 1,
                new IPushRuntimeFactory[] { sourceSelectRuntime }, new RecordDescriptor[] { personsRecDesc });
        spec.connect(sourcePersonsConn, sourcePersonsScanOp, 0, sourceMetaOp, 0);
        IConnectorDescriptor filterPersonsConn = new MToNPartitioningConnectorDescriptor(spec,
                FieldHashPartitionComputerFactory.of(new int[] { 1 }, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(LongPointable.FACTORY) }));

        // Build our SCAN operator for KNOWS.
        FileSplit[] knowsFileSplits = test.getKnowsFileSplits();
        IFileSplitProvider knowsSplitProvider = new ConstantFileSplitProvider(knowsFileSplits);
        IOperatorDescriptor knowsScanOp = new FileScanOperatorDescriptor(spec, knowsSplitProvider,
                new DelimitedDataTupleParserFactory(knowsParserFactories, '|'), knowsRecDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, knowsScanOp, test.getNodeNames());
        IConnectorDescriptor knowsScannerConn = new MToNPartitioningConnectorDescriptor(spec,
                FieldHashPartitionComputerFactory.of(new int[] { 1 }, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(LongPointable.FACTORY) }));

        // Join PERSONS (build) and KNOWS (probe) together (through Person1Id of KNOWS).
        int knowsFieldCount = knowsRecDesc.getFieldCount();
        int personsFieldCount = personsRecDesc.getFieldCount();
        ISerializerDeserializer[] join1Serde = new ISerializerDeserializer[knowsFieldCount + personsFieldCount];
        System.arraycopy(knowsRecDesc.getFields(), 0, join1Serde, 0, knowsFieldCount);
        System.arraycopy(personsRecDesc.getFields(), 0, join1Serde, knowsFieldCount, personsFieldCount);
        RecordDescriptor join1RecDesc = new RecordDescriptor(join1Serde);
        IOperatorDescriptor personsKnowsJoinOp =
                new OptimizedHybridHashJoinOperatorDescriptor(spec, 32, -1, 1.3, new int[] { 1 }, new int[] { 1 },
                        new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE },
                        new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE }, join1RecDesc,
                        new JoinComparatorFactory(LongBinaryComparatorFactory.INSTANCE, 1, 1),
                        new JoinComparatorFactory(LongBinaryComparatorFactory.INSTANCE, 1, 1), null, null, false, null);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, personsKnowsJoinOp, test.getNodeNames());
        spec.connect(filterPersonsConn, sourceMetaOp, 0, personsKnowsJoinOp, 1);
        spec.connect(knowsScannerConn, knowsScanOp, 0, personsKnowsJoinOp, 0);
        IConnectorDescriptor personKnowsJoinConn = new MToNPartitioningConnectorDescriptor(spec,
                FieldHashPartitionComputerFactory.of(new int[] { 2 }, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(LongPointable.FACTORY) }));

        // Build the SCAN operator for our dest PERSON.
        IOperatorDescriptor destPersonsScanOp = new FileScanOperatorDescriptor(spec, splitProvider,
                new DelimitedDataTupleParserFactory(personsParserFactories, '|'), personsRecDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, destPersonsScanOp, test.getNodeNames());
        IConnectorDescriptor destPersonsConn = new MToNPartitioningConnectorDescriptor(spec,
                FieldHashPartitionComputerFactory.of(new int[] { 1 }, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(LongPointable.FACTORY) }));

        // ...and perform another JOIN with our KNOWS output.
        ISerializerDeserializer[] join2Serde = new ISerializerDeserializer[knowsFieldCount + (personsFieldCount * 2)];
        System.arraycopy(personsRecDesc.getFields(), 0, join2Serde, 0, personsFieldCount);
        System.arraycopy(knowsRecDesc.getFields(), 0, join2Serde, personsFieldCount, knowsFieldCount);
        System.arraycopy(personsRecDesc.getFields(), 0, join2Serde, join1Serde.length, personsFieldCount);
        RecordDescriptor join2RecDesc = new RecordDescriptor(join2Serde);
        IOperatorDescriptor personKnowsPersonJoinOp =
                new OptimizedHybridHashJoinOperatorDescriptor(spec, 32, -1, 1.3, new int[] { 1 }, new int[] { 2 },
                        new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE },
                        new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE }, join2RecDesc,
                        new JoinComparatorFactory(LongBinaryComparatorFactory.INSTANCE, 1, 2),
                        new JoinComparatorFactory(LongBinaryComparatorFactory.INSTANCE, 2, 1), null, null, false, null);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, personKnowsPersonJoinOp, test.getNodeNames());
        spec.connect(personKnowsJoinConn, personsKnowsJoinOp, 0, personKnowsPersonJoinOp, 1);
        spec.connect(destPersonsConn, destPersonsScanOp, 0, personKnowsPersonJoinOp, 0);
        IConnectorDescriptor firstHopConn = new OneToOneConnectorDescriptor(spec);

        // Serialize our source PERSONS tuple into a record...
        int pathPersonsFieldCount = pathPersonsRecType.getFieldNames().length;
        IScalarEvaluatorFactory[] serializeSourceArgs = new IScalarEvaluatorFactory[pathPersonsFieldCount * 2];
        int[] sourceFieldIndices = new int[] { 14, 15 };
        for (int i = 0; i < pathPersonsFieldCount; i++) {
            final int fieldIndex = sourceFieldIndices[i];
            final ATypeTag typeTag = pathPersonsRecType.getFieldTypes()[i].getTypeTag();
            serializeSourceArgs[(2 * i) + 1] = new TypedColumnAccessEvaluatorFactory(fieldIndex, typeTag);
        }
        IFunctionDescriptor sourceConstructorFunction = new ClosedRecordConstructorDescriptor();
        sourceConstructorFunction.setImmutableStates(pathPersonsRecType);

        // ...our KNOWS tuple into a record...
        int pathKnowsFieldCount = pathKnowsRecType.getFieldNames().length;
        IScalarEvaluatorFactory[] serializeKnowsArgs = new IScalarEvaluatorFactory[pathKnowsFieldCount * 2];
        int[] knowsFieldIndices = new int[] { 11, 12 };
        for (int i = 0; i < pathKnowsFieldCount; i++) {
            final int fieldIndex = knowsFieldIndices[i];
            final ATypeTag typeTag = knowsRecType.getFieldTypes()[i].getTypeTag();
            serializeKnowsArgs[(2 * i) + 1] = new TypedColumnAccessEvaluatorFactory(fieldIndex, typeTag);
        }
        IFunctionDescriptor knowsConstructorFunction = new ClosedRecordConstructorDescriptor();
        knowsConstructorFunction.setImmutableStates(pathPersonsRecType);

        // ...and our dest PERSONS tuple into a record...
        IScalarEvaluatorFactory[] serializeDestArgs = new IScalarEvaluatorFactory[pathPersonsFieldCount * 2];
        int[] destFieldIndices = new int[] { 1, 2 };
        for (int i = 0; i < pathPersonsFieldCount; i++) {
            final int fieldIndex = destFieldIndices[i];
            final ATypeTag typeTag = pathPersonsRecType.getFieldTypes()[i].getTypeTag();
            serializeDestArgs[(2 * i) + 1] = new TypedColumnAccessEvaluatorFactory(fieldIndex, typeTag);
        }
        IFunctionDescriptor destConstructorFunction = new ClosedRecordConstructorDescriptor();
        destConstructorFunction.setImmutableStates(pathPersonsRecType);

        // ...and project away everything but the destination ID and our new records.
        int workingRecordSize = personsFieldCount * 2 + knowsFieldCount;
        IPushRuntimeFactory serializeRecordRuntime = new AssignRuntimeFactory(
                new int[] { workingRecordSize, workingRecordSize + 1, workingRecordSize + 2 },
                new IScalarEvaluatorFactory[] { sourceConstructorFunction.createEvaluatorFactory(serializeSourceArgs),
                        knowsConstructorFunction.createEvaluatorFactory(serializeKnowsArgs),
                        destConstructorFunction.createEvaluatorFactory(serializeDestArgs) },
                new int[] { 1, workingRecordSize, workingRecordSize + 1, workingRecordSize + 2 });
        ISerializerDeserializer[] serializeRecordSerde = new ISerializerDeserializer[] {
                Integer64SerializerDeserializer.INSTANCE, new ARecordSerializerDeserializer(pathPersonsRecType),
                new ARecordSerializerDeserializer(knowsRecType),
                new ARecordSerializerDeserializer(pathPersonsRecType) };
        RecordDescriptor serializeOutRecDesc = new RecordDescriptor(serializeRecordSerde);

        // Create a path using our serialized person. Project away the record we previously built.
        IScalarEvaluatorFactory[] createPathArgs = new IScalarEvaluatorFactory[] { new ColumnAccessEvalFactory(1),
                new ColumnAccessEvalFactory(3), new ColumnAccessEvalFactory(2) };
        IFunctionDescriptor createPathFunction = new CreateNewOneHopDescriptor();
        int[] createPathProjection = new int[] { 0, 2 };
        IPushRuntimeFactory createPathRuntime = new AssignRuntimeFactory(new int[] { 2 },
                new IScalarEvaluatorFactory[] { createPathFunction.createEvaluatorFactory(createPathArgs) },
                createPathProjection);

        // We want to return a meta op that captures the runtimes above.
        IPushRuntimeFactory[] metaOpRuntimes = new IPushRuntimeFactory[] { serializeRecordRuntime, createPathRuntime };
        RecordDescriptor[] metaOpRecDescs = new RecordDescriptor[] { serializeOutRecDesc, fpRecDesc };
        IOperatorDescriptor metaOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 1, metaOpRuntimes, metaOpRecDescs);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, metaOp, test.getNodeNames());
        spec.connect(firstHopConn, personKnowsPersonJoinOp, 0, metaOp, 0);
        return metaOp;
    }

    protected IOperatorDescriptor buildPBJBuildInputOperator(JobSpecification spec, ISSReachabilityTestFixture test) {
        // Build our SCAN operator for PERSONS (our destination).
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
        IConnectorDescriptor knowsScannerConn = new MToNPartitioningConnectorDescriptor(spec,
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
        spec.connect(knowsScannerConn, knowsScanOp, 0, personsKnowsJoinOp, 0);
        return personsKnowsJoinOp;
    }

    protected IOperatorDescriptor buildAfterFixedPointOp(JobSpecification spec, ISSReachabilityTestFixture test) {
        // Filter out any endpoints that we have already visited.
        int bufferPageSize = StorageUtil.getIntSizeInBytes(32, KILOBYTE);
        IBufferCacheSupplier cacheSupplier = TestStorageManagerComponentHolder::getBufferCache;
        IPushRuntimeFactory selectEndpointRuntime = new StateReleaseRuntimeFactory(
                new LocalDistinctRuntimeFactory(new int[] { 0, 1 }, 0.01, bufferPageSize, 4,
                        new ITypeTraits[] { LongPointable.TYPE_TRAITS },
                        new IScalarEvaluatorFactory[] { new ColumnAccessEvalFactory(0) },
                        new IBinaryComparatorFactory[] { LongBinaryComparatorFactory.INSTANCE }, cacheSupplier),
                (byte) 1);

        // Wrap this up in a single meta op.
        IOperatorDescriptor metaOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 1,
                new IPushRuntimeFactory[] { selectEndpointRuntime }, new RecordDescriptor[] { fpRecDesc });
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, metaOp, test.getNodeNames());
        return metaOp;
    }

    protected IOperatorDescriptor buildRecursiveInputOperator(JobSpecification spec, ISSReachabilityTestFixture test)
            throws Exception {
        int personsFieldCount = personsRecDesc.getFieldCount();
        int knowsFieldCount = knowsRecDesc.getFieldCount();
        int fpFieldCount = fpRecDesc.getFieldCount();

        // Serialize our vertex into a record...
        int pathPersonsFieldCount = pathPersonsRecType.getFieldNames().length;
        IScalarEvaluatorFactory[] serializeVertexArgs = new IScalarEvaluatorFactory[pathPersonsFieldCount * 2];
        int[] personsFieldIndices =
                new int[] { 1 + fpFieldCount + knowsFieldCount, 2 + fpFieldCount + knowsFieldCount };
        for (int i = 0; i < pathPersonsFieldCount; i++) {
            final int fieldIndex = personsFieldIndices[i];
            final ATypeTag typeTag = pathPersonsRecType.getFieldTypes()[i].getTypeTag();
            serializeVertexArgs[(2 * i) + 1] = new TypedColumnAccessEvaluatorFactory(fieldIndex, typeTag);
        }
        IFunctionDescriptor vertexConstructorFunction = new ClosedRecordConstructorDescriptor();
        vertexConstructorFunction.setImmutableStates(pathPersonsRecType);

        // ...and build our projection. We will remove all the old vertex fields.
        int[] vertexProjection = new int[knowsFieldCount + fpFieldCount + 1];
        ISerializerDeserializer[] vertexRecordSerde = new ISerializerDeserializer[vertexProjection.length];
        for (int i = 0; i < fpFieldCount; i++) {
            vertexProjection[i] = i;
            vertexRecordSerde[i] = fpRecDesc.getFields()[i];
        }
        for (int i = 0; i < knowsFieldCount; i++) {
            vertexProjection[i + fpFieldCount] = i + fpFieldCount;
            vertexRecordSerde[i + fpFieldCount] = knowsRecDesc.getFields()[i];
        }
        vertexProjection[knowsFieldCount + fpFieldCount] = personsFieldCount + knowsFieldCount + fpFieldCount;
        vertexRecordSerde[knowsFieldCount + fpFieldCount] = new ARecordSerializerDeserializer(personsRecType);
        IPushRuntimeFactory serializeVertexRuntime = new StateReleaseRuntimeFactory(
                new AssignRuntimeFactory(new int[] { personsFieldCount + knowsFieldCount + fpFieldCount },
                        new IScalarEvaluatorFactory[] {
                                vertexConstructorFunction.createEvaluatorFactory(serializeVertexArgs) },
                        vertexProjection),
                (byte) 1);
        RecordDescriptor vertexOutRecDesc = new RecordDescriptor(vertexRecordSerde);

        // Serialize our edge into a record...
        int pathKnowsFieldCount = pathKnowsRecType.getFieldNames().length;
        IScalarEvaluatorFactory[] serializeEdgeArgs = new IScalarEvaluatorFactory[pathKnowsFieldCount * 2];
        int[] knowsFieldIndices = new int[] { 1 + fpFieldCount, 2 + fpFieldCount };
        for (int i = 0; i < pathKnowsFieldCount; i++) {
            final int fieldIndex = knowsFieldIndices[i];
            final ATypeTag typeTag = pathKnowsRecType.getFieldTypes()[i].getTypeTag();
            serializeEdgeArgs[(2 * i) + 1] = new TypedColumnAccessEvaluatorFactory(fieldIndex, typeTag);
        }
        IFunctionDescriptor edgeConstructorFunction = new ClosedRecordConstructorDescriptor();
        edgeConstructorFunction.setImmutableStates(pathKnowsRecType);

        // ...and build our projection. We will keep our P2ID, our vertex record, our edge record, and our working path.
        int[] edgeProjection =
                new int[] { fpFieldCount + 2, knowsFieldCount + fpFieldCount, knowsFieldCount + fpFieldCount + 1, 1 };
        ISerializerDeserializer[] edgeRecordSerde =
                new ISerializerDeserializer[] { vertexOutRecDesc.getFields()[fpFieldCount + 2],
                        vertexOutRecDesc.getFields()[knowsFieldCount + fpFieldCount],
                        new ARecordSerializerDeserializer(knowsRecType), vertexOutRecDesc.getFields()[1] };
        IPushRuntimeFactory serializeEdgeRuntime = new StateReleaseRuntimeFactory(
                new AssignRuntimeFactory(new int[] { knowsFieldCount + fpFieldCount + 1 },
                        new IScalarEvaluatorFactory[] {
                                edgeConstructorFunction.createEvaluatorFactory(serializeEdgeArgs) },
                        edgeProjection),
                (byte) 1);
        RecordDescriptor edgeOutRecDesc = new RecordDescriptor(edgeRecordSerde);

        // Filter out any tuples that would introduce cycles. We will not project away any fields.
        IScalarEvaluatorFactory isDistinctVertexFactory = new IsDistinctVertexDescriptor().createEvaluatorFactory(
                new IScalarEvaluatorFactory[] { new ColumnAccessEvalFactory(1), new ColumnAccessEvalFactory(3) });
        IPushRuntimeFactory selectCyclesRuntime =
                new StateReleaseRuntimeFactory(new StreamSelectRuntimeFactory(isDistinctVertexFactory,
                        new int[] { 0, 1, 2, 3 }, BinaryBooleanInspector.FACTORY, false, -1, null), (byte) 1);

        // Finally, append to our path and project away the records.
        IScalarEvaluatorFactory appendPathEvalFactory = new AppendToExistingPathDescriptor()
                .createEvaluatorFactory(new IScalarEvaluatorFactory[] { new ColumnAccessEvalFactory(1),
                        new ColumnAccessEvalFactory(2), new ColumnAccessEvalFactory(3) });
        IPushRuntimeFactory assignPathRuntime = new StateReleaseRuntimeFactory(new AssignRuntimeFactory(new int[] { 4 },
                new IScalarEvaluatorFactory[] { appendPathEvalFactory }, new int[] { 0, 4 }), (byte) 1);
        IOperatorDescriptor metaOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 1,
                new IPushRuntimeFactory[] { serializeVertexRuntime, serializeEdgeRuntime, selectCyclesRuntime,
                        assignPathRuntime },
                new RecordDescriptor[] { vertexOutRecDesc, edgeOutRecDesc, edgeOutRecDesc, fpRecDesc });
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, metaOp, test.getNodeNames());
        return metaOp;
    }

    protected IOperatorDescriptor buildMarkerSinkOp(JobSpecification spec, ISSReachabilityTestFixture test)
            throws Exception {
        // We do not want markers arriving past this point.
        IPushRuntimeFactory markerSinkRuntime = new MessageSinkRuntimeFactory();

        // Convert our path into a path record before printing.
        IFunctionDescriptor yieldPathFunction = new TranslateForwardPathDescriptor();
        yieldPathFunction.setImmutableStates(pathPersonsRecType, pathKnowsRecType);
        IScalarEvaluatorFactory[] yieldPathRecordArgs =
                new IScalarEvaluatorFactory[] { new ColumnAccessEvalFactory(1) };
        IPushRuntimeFactory yieldPathRecordRuntime = new AssignRuntimeFactory(new int[] { 2 },
                new IScalarEvaluatorFactory[] { yieldPathFunction.createEvaluatorFactory(yieldPathRecordArgs) },
                new int[] { 2 });
        RecordDescriptor yieldPathRecDesc =
                new RecordDescriptor(new ISerializerDeserializer[] { new ARecordSerializerDeserializer(pathRecType) });
        IOperatorDescriptor metaOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 1,
                new IPushRuntimeFactory[] { markerSinkRuntime, yieldPathRecordRuntime },
                new RecordDescriptor[] { fpRecDesc, yieldPathRecDesc });
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, metaOp, test.getNodeNames());
        return metaOp;
    }

    protected IOperatorDescriptor buildReplicateOp(JobSpecification spec, ISSReachabilityTestFixture test) {
        IOperatorDescriptor replicateOp = new ReplicateOperatorDescriptor(spec, fpRecDesc, 2);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, replicateOp, test.getNodeNames());
        return replicateOp;
    }

    protected IOperatorDescriptor buildResultOp(JobSpecification spec, ISSReachabilityTestFixture test)
            throws Exception {
        ResultSetId resultSetId = new ResultSetId(0);
        spec.addResultSetId(resultSetId);
        IOperatorDescriptor resultOp = new ResultWriterOperatorDescriptor(spec, resultSetId, null, false,
                ResultSerializerFactoryProvider.INSTANCE.getResultSerializerFactoryProvider(new int[] { 0 },
                        new IPrinterFactory[] { new ARecordPrinterFactory(pathRecType) },
                        PrinterBasedWriterFactory.INSTANCE),
                1);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, resultOp, test.getNodeNames()[0]);
        return resultOp;
    }

    public Pair<JobId, ResultSetId> executeTest(ISSReachabilityTestFixture test) throws Exception {
        String ccHost = test.getClusterControllerHostIP();
        int ccPort = test.getClusterControllerHostPort();
        try (HyracksConnection hcc = new HyracksConnection(ccHost, ccPort)) {
            JobSpecification spec = new JobSpecification(32768);

            // 1a. Read the PERSONS CSV files.
            // 1b. Read the KNOWS CSV files.
            // 2. JOIN both PERSONS and KNOWS, using PERSONS as our build.
            IOperatorDescriptor pbjBuildInputOp = buildPBJBuildInputOperator(spec, test);
            IConnectorDescriptor pbjBuildConn = new MToNPartitioningConnectorDescriptor(spec,
                    FieldHashPartitionComputerFactory.of(new int[] { 1 }, new IBinaryHashFunctionFactory[] {
                            PointableBinaryHashFunctionFactory.of(LongPointable.FACTORY) }));

            // 3a. (we get tuples to use for our probe)
            // 3b. JOIN our PERSON-KNOWS with our FIXED-POINT output.
            IOperatorDescriptor pbjOp = new PersistentBuildJoinOperatorDescriptor(spec, 8, -1, 1.2, new int[] { 0 },
                    new int[] { 1 }, new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE },
                    new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE }, joinRecDesc,
                    new JoinComparatorFactory(LongBinaryComparatorFactory.INSTANCE, 0, 1),
                    new JoinComparatorFactory(LongBinaryComparatorFactory.INSTANCE, 1, 0), null, null, (byte) 1, false,
                    null);
            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, pbjOp, test.getNodeNames());
            spec.connect(pbjBuildConn, pbjBuildInputOp, 0, pbjOp, 1);
            IConnectorDescriptor pbjConn = new MToNPartitioningConnectorDescriptor(spec,
                    FieldHashPartitionComputerFactory.of(new int[] { 4 }, new IBinaryHashFunctionFactory[] {
                            PointableBinaryHashFunctionFactory.of(LongPointable.FACTORY) }));

            // 4. Locally, filter out tuples that contain duplicate PERSONS from the step (3b) JOIN.
            // 5. Serialize our PERSONS into a record.
            // 6. Serialize our KNOWS into a record.
            // 7. Filter out tuples whose addition of the PERSONS from (3b) would induce a cycle.
            // 8. Append the serialized PERSONS and KNOWS onto our path.
            IOperatorDescriptor recursiveInputOp = buildRecursiveInputOperator(spec, test);
            IConnectorDescriptor pbjStateReleaseConn = new StateReleaseConnectorDescriptor(spec, pbjConn, (byte) 1);
            spec.connect(pbjStateReleaseConn, pbjOp, 0, recursiveInputOp, 0);
            spec.getConnectorMap().remove(pbjConn.getConnectorId());
            IConnectorDescriptor recursiveInputConn = new OneToOneConnectorDescriptor(spec);

            // 3a[1]. Read the PERSONS CSV files.
            // 3a[2]. Limit the PERSONS read by some constant number.
            // 3a[3]. READ the KNOWS CSV files.
            // 3a[4]. JOIN the 3a[1] PERSONS with the KNOWS dataset.
            // 3a[5]. Read the PERSONS CSV files again.
            // 3a[6]. JOIN the 3a[4] output with the other PERSONS dataset.
            // 3a[7]. Serialize our PERSONS, KNOWS, PERSONS into a record.
            // 3a[8]. Create a 1-hop-edge path from our serialized records.
            IOperatorDescriptor anchorInputOp = buildSourceOperator(spec, test);
            IConnectorDescriptor anchorInputConn = new MToNPartitioningConnectorDescriptor(spec,
                    FieldHashPartitionComputerFactory.of(new int[] { 0 }, new IBinaryHashFunctionFactory[] {
                            PointableBinaryHashFunctionFactory.of(LongPointable.FACTORY) }));

            // 9. (manage our fixed-point :-))
            // 10. Filter out duplicate endpoints.
            // 11. Replicate the tuples from our fixed point. We have two outputs.
            // 12a. Project out only the path from the fixed-point output.
            // 12b. Write the previous projection to a file.
            IOperatorDescriptor fpOp = new FixedPointOperatorDescriptor(spec, fpRecDesc, 2, (byte) 1);
            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, fpOp, test.getNodeNames());
            IOperatorDescriptor afterFpOp = buildAfterFixedPointOp(spec, test);
            IOperatorDescriptor replicateOp = buildReplicateOp(spec, test);
            IOperatorDescriptor markerSinkOp = buildMarkerSinkOp(spec, test);
            IOperatorDescriptor resultWriterOp = buildResultOp(spec, test);
            spec.connect(anchorInputConn, anchorInputOp, 0, fpOp, 0);
            spec.connect(recursiveInputConn, recursiveInputOp, 0, fpOp, 1);
            spec.connect(new OneToOneConnectorDescriptor(spec), fpOp, 0, afterFpOp, 0);
            spec.connect(new OneToOneConnectorDescriptor(spec), afterFpOp, 0, replicateOp, 0);
            spec.connect(new OneToOneConnectorDescriptor(spec), replicateOp, 0, markerSinkOp, 0);
            spec.connect(new MToNBroadcastConnectorDescriptor(spec), markerSinkOp, 0, resultWriterOp, 0);

            // 13c / 3a. Send the fixed-point output back to our PBJ operator.
            IConnectorDescriptor replicateConn = new OneToOneConnectorDescriptor(spec);
            spec.connect(replicateConn, replicateOp, 1, pbjOp, 0);
            spec.addRoot(resultWriterOp);

            // Execute our job!
            JobId jobId = hcc.startJob(spec);
            hcc.waitForCompletion(jobId);
            LOGGER.info("LDBC reachability test has finished!");
            return new Pair<>(jobId, spec.getResultSetIds().get(0));
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public List<SSReachabilityTestResult.Path> readResults(JobId jobId, ResultSetId resultSetId,
            ISSReachabilityTestFixture test) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        // Grab our result.
        String ccHost = test.getClusterControllerHostIP();
        int ccPort = test.getClusterControllerHostPort();
        List<SSReachabilityTestResult.Path> resultRecords = new ArrayList<>();
        try (HyracksConnection hcc = new HyracksConnection(ccHost, ccPort)) {
            ByteBufferInputStream bbis = new ByteBufferInputStream();
            FrameManager resultDisplayFrameMgr = new FrameManager(32768);
            VSizeFrame frame = new VSizeFrame(resultDisplayFrameMgr);
            IFrameTupleAccessor fta = new ResultFrameTupleAccessor();

            // Iterate through each result frame.
            IResultSet resultSet = new ResultSet(hcc, PlainSocketChannelFactory.INSTANCE, 32768, 1);
            IResultSetReader reader = resultSet.createReader(jobId, resultSetId);
            int readSize = reader.read(frame);
            while (readSize > 0) {
                try {
                    fta.reset(frame.getBuffer());
                    for (int tIndex = 0; tIndex < fta.getTupleCount(); tIndex++) {
                        int start = fta.getTupleStartOffset(tIndex);
                        int length = fta.getTupleEndOffset(tIndex) - start;
                        bbis.setByteBuffer(frame.getBuffer(), start);
                        byte[] recordBytes = new byte[length];
                        bbis.read(recordBytes, 0, length);

                        // We'll parse our record as a JSON string.
                        String recordAsString = new String(recordBytes, 0, length);
                        resultRecords.add(objectMapper.readValue(recordAsString, SSReachabilityTestResult.Path.class));
                    }
                } finally {
                    bbis.close();
                }
                readSize = reader.read(frame);
            }
        }
        return resultRecords;
    }

    protected static class TypedColumnAccessEvaluatorFactory implements IScalarEvaluatorFactory {
        private static final long serialVersionUID = 1L;

        private final int fieldIndex;
        private final ATypeTag typeTag;

        public TypedColumnAccessEvaluatorFactory(int fieldIndex, ATypeTag typeTag) {
            this.fieldIndex = fieldIndex;
            this.typeTag = typeTag;
        }

        @Override
        public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) {
            return new IScalarEvaluator() {
                private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                private final DataOutput dataOutput = resultStorage.getDataOutput();

                @Override
                public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                    try {
                        resultStorage.reset();
                        dataOutput.writeByte(typeTag.serialize());
                        dataOutput.write(tuple.getFieldData(fieldIndex), tuple.getFieldStart(fieldIndex),
                                tuple.getFieldLength(fieldIndex));
                        result.set(resultStorage);

                    } catch (IOException e) {
                        throw HyracksDataException.create(e);
                    }
                }
            };
        }
    }
}
