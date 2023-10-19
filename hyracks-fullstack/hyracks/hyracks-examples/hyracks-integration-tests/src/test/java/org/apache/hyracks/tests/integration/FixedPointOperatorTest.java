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
package org.apache.hyracks.tests.integration;

import static org.apache.hyracks.dataflow.std.iteration.FixedPointOperatorDescriptor.ANCHOR_INPUT_INDEX;
import static org.apache.hyracks.dataflow.std.iteration.FixedPointOperatorDescriptor.FIXED_POINT_OUTPUT_INDEX;
import static org.apache.hyracks.dataflow.std.iteration.FixedPointOperatorDescriptor.RECURSIVE_INPUT_INDEX;
import static org.apache.hyracks.util.file.FileUtil.joinPath;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.MurmurHash3BinaryHashFunctionFamily;
import org.apache.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.FrameFixedFieldAppender;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractDelegateFrameWriter;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.connectors.MToNBroadcastConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.StateReleaseConnectorDescriptor;
import org.apache.hyracks.dataflow.std.iteration.FixedPointOperatorDescriptor;
import org.apache.hyracks.dataflow.std.join.JoinComparatorFactory;
import org.apache.hyracks.dataflow.std.join.PersistentBuildJoinOperatorDescriptor;
import org.apache.hyracks.dataflow.std.message.FrameTupleListener;
import org.apache.hyracks.dataflow.std.message.consumer.MarkerMessageConsumer;
import org.apache.hyracks.dataflow.std.misc.MessageSinkOperatorDescriptor;
import org.apache.hyracks.dataflow.std.misc.ReplicateOperatorDescriptor;
import org.apache.hyracks.ipc.impl.HyracksConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("rawtypes")
public class FixedPointOperatorTest implements Serializable {
    private static final Logger LOGGER = LogManager.getLogger();

    private static final long serialVersionUID = 1L;

    private final ISerializerDeserializer[] serdeArray1 = { IntegerSerializerDeserializer.INSTANCE };
    private final ISerializerDeserializer[] serdeArray2 =
            { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };

    private static final int NUMBER_OF_NCS = 8;
    private static ClusterControllerService cc;
    private static NodeControllerService[] ncs;
    private static IHyracksClientConnection hcc;
    private static String[] ncNames;

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @BeforeClass
    public static void init() throws Exception {
        CCConfig ccConfig = new CCConfig();
        ccConfig.setClientListenAddress("127.0.0.1");
        ccConfig.setClientListenPort(39000);
        ccConfig.setClusterListenAddress("127.0.0.1");
        ccConfig.setClusterListenPort(39001);
        ccConfig.setProfileDumpPeriod(10000);

        // Start our CC.
        FileUtils.deleteQuietly(new File("target" + File.separator + "data"));
        FileUtils.copyDirectory(new File("data"), new File(joinPath("target", "data")));
        File outDir = new File("target" + File.separator + "ClusterController");
        outDir.mkdirs();
        File ccRoot = File.createTempFile(AbstractIntegrationTest.class.getName(), ".data", outDir);
        ccRoot.delete();
        ccRoot.mkdir();
        ccConfig.setRootDir(ccRoot.getAbsolutePath());
        cc = new ClusterControllerService(ccConfig);
        cc.start();

        // Create our NCs.
        ncs = new NodeControllerService[NUMBER_OF_NCS];
        ncNames = new String[NUMBER_OF_NCS];
        for (int i = 0; i < NUMBER_OF_NCS; i++) {
            String ncName = "NC_" + i;
            NCConfig ncConfig = new NCConfig(ncName);
            ncConfig.setClusterAddress("localhost");
            ncConfig.setClusterPort(39001);
            ncConfig.setClusterListenAddress("127.0.0.1");
            ncConfig.setDataListenAddress("127.0.0.1");
            ncConfig.setResultListenAddress("127.0.0.1");
            ncConfig.setIODevices(
                    new String[] { joinPath(System.getProperty("user.dir"), "target", "data", "device" + i) });
            ncs[i] = new NodeControllerService(ncConfig);
            ncNames[i] = ncName;
            ncs[i].start();
        }

        // Establish a Hyracks connection.
        hcc = new HyracksConnection(ccConfig.getClientListenAddress(), ccConfig.getClientListenPort());
    }

    @AfterClass
    public static void deinit() throws Exception {
        for (NodeControllerService nc : ncs) {
            nc.stop();
        }
        cc.stop();
    }

    public void runTest(JobSpecification spec) throws Exception {
        JobId jobId = hcc.startJob(spec);
        hcc.waitForCompletion(jobId);
    }

    /**
     * <ul>
     *   <li>Single partition.</li>
     *   <li>No PBJ operators.</li>
     *   <li>One iteration.</li>
     * </ul>
     */
    @Test
    public void RecursionTestCase1() throws Exception {
        JobSpecification spec = new JobSpecification();
        RecordDescriptor recDesc = new RecordDescriptor(serdeArray1);

        IOperatorDescriptor oddNumberGenerator = new TestGeneratorOperatorDescriptor(spec, recDesc, true);
        IOperatorDescriptor fixedPoint = new FixedPointOperatorDescriptor(spec, recDesc, 2, (byte) 1);
        IOperatorDescriptor replicate = new ReplicateOperatorDescriptor(spec, recDesc, 2);
        IOperatorDescriptor select = new TestSelectOperatorDescriptor(spec, recDesc, 0);
        IOperatorDescriptor resultSink = new TestResultSinkOperatorDescriptor(spec, recDesc, 2500);
        IOperatorDescriptor messageSink = new MessageSinkOperatorDescriptor(spec, recDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, oddNumberGenerator, ncNames[0]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, fixedPoint, ncNames[0]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, replicate, ncNames[0]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, select, ncNames[0]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, resultSink, ncNames[0]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, messageSink, ncNames[0]);

        spec.connect(new OneToOneConnectorDescriptor(spec), oddNumberGenerator, 0, fixedPoint, ANCHOR_INPUT_INDEX);
        spec.connect(new OneToOneConnectorDescriptor(spec), fixedPoint, FIXED_POINT_OUTPUT_INDEX, replicate, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), replicate, 0, messageSink, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), messageSink, 0, resultSink, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), replicate, 1, select, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), select, 0, fixedPoint, RECURSIVE_INPUT_INDEX);
        spec.addRoot(resultSink);
        runTest(spec);
    }

    /**
     * <ul>
     *   <li>Single partition.</li>
     *   <li>With PBJ operator (all tuples are spilled).</li>
     *   <li>One iteration.</li>
     * </ul>
     */
    @Test
    public void RecursionTestCase2() throws Exception {
        JobSpecification spec = new JobSpecification();
        RecordDescriptor recDesc2 = new RecordDescriptor(serdeArray2);
        RecordDescriptor recDesc1 = new RecordDescriptor(serdeArray1);

        IOperatorDescriptor oddNumberGenerator1 = new TestGeneratorOperatorDescriptor(spec, recDesc1, true);
        IOperatorDescriptor oddNumberGenerator2 = new TestGeneratorOperatorDescriptor(spec, recDesc1, true);
        IOperatorDescriptor join = new PersistentBuildJoinOperatorDescriptor(spec, 8, 20, 1.2, new int[] { 0 },
                new int[] { 0 }, new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE },
                new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE }, recDesc2,
                new JoinComparatorFactory(IntegerBinaryComparatorFactory.INSTANCE, 0, 0),
                new JoinComparatorFactory(IntegerBinaryComparatorFactory.INSTANCE, 0, 0), null, null, (byte) 1, false,
                null);
        IOperatorDescriptor fixedPoint = new FixedPointOperatorDescriptor(spec, recDesc1, 2, (byte) 1);
        IOperatorDescriptor replicate = new ReplicateOperatorDescriptor(spec, recDesc1, 2);
        IOperatorDescriptor select = new TestSelectOperatorDescriptor(spec, recDesc1, 0);
        IOperatorDescriptor resultSink = new TestResultSinkOperatorDescriptor(spec, recDesc1, 2500);
        IOperatorDescriptor markerSink = new MessageSinkOperatorDescriptor(spec, recDesc1);
        IOperatorDescriptor project = new TestProjectOperatorDescriptor(spec, recDesc2, recDesc1);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, oddNumberGenerator1, ncNames[0]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, oddNumberGenerator2, ncNames[0]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, ncNames[0]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, fixedPoint, ncNames[0]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, replicate, ncNames[0]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, select, ncNames[0]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, resultSink, ncNames[0]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, markerSink, ncNames[0]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, project, ncNames[0]);

        spec.connect(new OneToOneConnectorDescriptor(spec), oddNumberGenerator1, 0, join, 1);
        spec.connect(new OneToOneConnectorDescriptor(spec), oddNumberGenerator2, 0, fixedPoint, ANCHOR_INPUT_INDEX);
        spec.connect(new OneToOneConnectorDescriptor(spec), fixedPoint, FIXED_POINT_OUTPUT_INDEX, join, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), join, 0, project, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), project, 0, replicate, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), replicate, 0, markerSink, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), markerSink, 0, resultSink, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), replicate, 1, select, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), select, 0, fixedPoint, RECURSIVE_INPUT_INDEX);
        spec.addRoot(resultSink);
        runTest(spec);
    }

    /**
     * <ul>
     *   <li>Single partition.</li>
     *   <li>With PBJ operator (all tuples are resident).</li>
     *   <li>Two iterations.</li>
     * </ul>
     */
    @Test
    public void RecursionTestCase3() throws Exception {
        JobSpecification spec = new JobSpecification();
        RecordDescriptor recDesc2 = new RecordDescriptor(serdeArray2);
        RecordDescriptor recDesc1 = new RecordDescriptor(serdeArray1);

        IOperatorDescriptor oddNumberGenerator1 = new TestGeneratorOperatorDescriptor(spec, recDesc1, true);
        IOperatorDescriptor oddNumberGenerator2 = new TestGeneratorOperatorDescriptor(spec, recDesc1, true);
        IOperatorDescriptor join = new PersistentBuildJoinOperatorDescriptor(spec, 100, 20, 1.2, new int[] { 0 },
                new int[] { 0 }, new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE },
                new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE }, recDesc2,
                new JoinComparatorFactory(IntegerBinaryComparatorFactory.INSTANCE, 0, 0),
                new JoinComparatorFactory(IntegerBinaryComparatorFactory.INSTANCE, 0, 0), null, null, (byte) 1, false,
                null);
        IOperatorDescriptor fixedPoint = new FixedPointOperatorDescriptor(spec, recDesc1, 2, (byte) 1);
        IOperatorDescriptor replicate = new ReplicateOperatorDescriptor(spec, recDesc1, 2);
        IOperatorDescriptor select = new TestSelectOperatorDescriptor(spec, recDesc1, 1);
        IOperatorDescriptor resultSink = new TestResultSinkOperatorDescriptor(spec, recDesc1, 5000);
        IOperatorDescriptor markerSink = new MessageSinkOperatorDescriptor(spec, recDesc1);
        IOperatorDescriptor project = new TestProjectOperatorDescriptor(spec, recDesc2, recDesc1);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, oddNumberGenerator1, ncNames[0]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, oddNumberGenerator2, ncNames[0]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, ncNames[0]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, fixedPoint, ncNames[0]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, replicate, ncNames[0]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, select, ncNames[0]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, resultSink, ncNames[0]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, markerSink, ncNames[0]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, project, ncNames[0]);

        spec.connect(new OneToOneConnectorDescriptor(spec), oddNumberGenerator1, 0, join, 1);
        spec.connect(new OneToOneConnectorDescriptor(spec), oddNumberGenerator2, 0, fixedPoint, ANCHOR_INPUT_INDEX);
        spec.connect(new OneToOneConnectorDescriptor(spec), fixedPoint, FIXED_POINT_OUTPUT_INDEX, join, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), join, 0, project, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), project, 0, replicate, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), replicate, 0, markerSink, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), markerSink, 0, resultSink, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), replicate, 1, select, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), select, 0, fixedPoint, RECURSIVE_INPUT_INDEX);
        spec.addRoot(resultSink);
        runTest(spec);
    }

    /**
     * <ul>
     *   <li>Single partition.</li>
     *   <li>With PBJ operator (all tuples are spilled).</li>
     *   <li>Three iterations.</li>
     * </ul>
     */
    @Test
    public void RecursionTestCase4() throws Exception {
        JobSpecification spec = new JobSpecification();
        RecordDescriptor recDesc2 = new RecordDescriptor(serdeArray2);
        RecordDescriptor recDesc1 = new RecordDescriptor(serdeArray1);

        IOperatorDescriptor oddNumberGenerator1 = new TestGeneratorOperatorDescriptor(spec, recDesc1, true);
        IOperatorDescriptor oddNumberGenerator2 = new TestGeneratorOperatorDescriptor(spec, recDesc1, true);
        IOperatorDescriptor join = new PersistentBuildJoinOperatorDescriptor(spec, 8, 20, 1.2, new int[] { 0 },
                new int[] { 0 }, new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE },
                new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE }, recDesc2,
                new JoinComparatorFactory(IntegerBinaryComparatorFactory.INSTANCE, 0, 0),
                new JoinComparatorFactory(IntegerBinaryComparatorFactory.INSTANCE, 0, 0), null, null, (byte) 1, false,
                null);
        IOperatorDescriptor fixedPoint = new FixedPointOperatorDescriptor(spec, recDesc1, 2, (byte) 1);
        IOperatorDescriptor replicate = new ReplicateOperatorDescriptor(spec, recDesc1, 2);
        IOperatorDescriptor select = new TestSelectOperatorDescriptor(spec, recDesc1, 2);
        IOperatorDescriptor resultSink = new TestResultSinkOperatorDescriptor(spec, recDesc1, 7500);
        IOperatorDescriptor markerSink = new MessageSinkOperatorDescriptor(spec, recDesc1);
        IOperatorDescriptor project = new TestProjectOperatorDescriptor(spec, recDesc2, recDesc1);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, oddNumberGenerator1, ncNames[0]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, oddNumberGenerator2, ncNames[0]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, ncNames[0]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, fixedPoint, ncNames[0]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, replicate, ncNames[0]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, select, ncNames[0]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, resultSink, ncNames[0]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, markerSink, ncNames[0]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, project, ncNames[0]);

        spec.connect(new OneToOneConnectorDescriptor(spec), oddNumberGenerator1, 0, join, 1);
        spec.connect(new OneToOneConnectorDescriptor(spec), oddNumberGenerator2, 0, fixedPoint, ANCHOR_INPUT_INDEX);
        spec.connect(new OneToOneConnectorDescriptor(spec), fixedPoint, FIXED_POINT_OUTPUT_INDEX, join, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), join, 0, project, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), project, 0, replicate, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), replicate, 0, markerSink, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), markerSink, 0, resultSink, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), replicate, 1, select, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), select, 0, fixedPoint, RECURSIVE_INPUT_INDEX);
        spec.addRoot(resultSink);
        runTest(spec);
    }

    /**
     * <ul>
     *   <li>Two partitions.</li>
     *   <li>With PBJ operator (all tuples are spilled).</li>
     *   <li>Three iterations.</li>
     * </ul>
     */
    @Test
    public void RecursionTestCase5() throws Exception {
        JobSpecification spec = new JobSpecification();
        RecordDescriptor recDesc2 = new RecordDescriptor(serdeArray2);
        RecordDescriptor recDesc1 = new RecordDescriptor(serdeArray1);

        IOperatorDescriptor oddNumberGenerator1 = new TestGeneratorOperatorDescriptor(spec, recDesc1, true);
        IOperatorDescriptor oddNumberGenerator2 = new TestGeneratorOperatorDescriptor(spec, recDesc1, true);
        IOperatorDescriptor join = new PersistentBuildJoinOperatorDescriptor(spec, 8, 20, 1.2, new int[] { 0 },
                new int[] { 0 }, new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE },
                new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE }, recDesc2,
                new JoinComparatorFactory(IntegerBinaryComparatorFactory.INSTANCE, 0, 0),
                new JoinComparatorFactory(IntegerBinaryComparatorFactory.INSTANCE, 0, 0), null, null, (byte) 1, false,
                null);
        IOperatorDescriptor fixedPoint = new FixedPointOperatorDescriptor(spec, recDesc1, 2, (byte) 1);
        IOperatorDescriptor replicate = new ReplicateOperatorDescriptor(spec, recDesc1, 2);
        IOperatorDescriptor select = new TestSelectOperatorDescriptor(spec, recDesc1, 2);
        IOperatorDescriptor resultSink = new TestResultSinkOperatorDescriptor(spec, recDesc1, 10000);
        IOperatorDescriptor markerSink = new MessageSinkOperatorDescriptor(spec, recDesc1);
        IOperatorDescriptor project = new TestProjectOperatorDescriptor(spec, recDesc2, recDesc1);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, oddNumberGenerator1, ncNames[0], ncNames[1]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, oddNumberGenerator2, ncNames[0], ncNames[1]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, ncNames[0], ncNames[1]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, fixedPoint, ncNames[0], ncNames[1]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, replicate, ncNames[0], ncNames[1]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, select, ncNames[0], ncNames[1]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, resultSink, ncNames[0], ncNames[1]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, markerSink, ncNames[0], ncNames[1]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, project, ncNames[0], ncNames[1]);

        spec.connect(new OneToOneConnectorDescriptor(spec), oddNumberGenerator1, 0, join, 1);
        spec.connect(new OneToOneConnectorDescriptor(spec), oddNumberGenerator2, 0, fixedPoint, ANCHOR_INPUT_INDEX);
        IConnectorDescriptor fpJoinConn = new MToNPartitioningConnectorDescriptor(spec,
                FieldHashPartitionComputerFactory.of(new int[] { 0 }, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(IntegerPointable.FACTORY) }));
        IConnectorDescriptor fpJoinStateReleaseConn = new StateReleaseConnectorDescriptor(spec, fpJoinConn, (byte) 1);
        spec.connect(fpJoinStateReleaseConn, fixedPoint, FIXED_POINT_OUTPUT_INDEX, join, 0);
        spec.getConnectorMap().remove(fpJoinConn.getConnectorId());
        spec.connect(new OneToOneConnectorDescriptor(spec), join, 0, project, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), project, 0, replicate, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), replicate, 0, markerSink, 0);
        spec.connect(new MToNBroadcastConnectorDescriptor(spec), markerSink, 0, resultSink, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), replicate, 1, select, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), select, 0, fixedPoint, RECURSIVE_INPUT_INDEX);
        spec.addRoot(resultSink);
        runTest(spec);
    }

    /**
     * <ul>
     *   <li>8 partitions.</li>
     *   <li>With PBJ operator (all tuples are spilled).</li>
     *   <li>Three iterations.</li>
     * </ul>
     */
    @Test
    public void RecursionTestCase6() throws Exception {
        JobSpecification spec = new JobSpecification();
        RecordDescriptor recDesc2 = new RecordDescriptor(serdeArray2);
        RecordDescriptor recDesc1 = new RecordDescriptor(serdeArray1);

        IOperatorDescriptor oddNumberGenerator1 = new TestGeneratorOperatorDescriptor(spec, recDesc1, true);
        IOperatorDescriptor oddNumberGenerator2 = new TestGeneratorOperatorDescriptor(spec, recDesc1, true);
        IOperatorDescriptor join = new PersistentBuildJoinOperatorDescriptor(spec, 8, 20, 1.2, new int[] { 0 },
                new int[] { 0 }, new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE },
                new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE }, recDesc2,
                new JoinComparatorFactory(IntegerBinaryComparatorFactory.INSTANCE, 0, 0),
                new JoinComparatorFactory(IntegerBinaryComparatorFactory.INSTANCE, 0, 0), null, null, (byte) 1, false,
                null);
        IOperatorDescriptor fixedPoint = new FixedPointOperatorDescriptor(spec, recDesc1, 2, (byte) 1);
        IOperatorDescriptor replicate = new ReplicateOperatorDescriptor(spec, recDesc1, 2);
        IOperatorDescriptor select = new TestSelectOperatorDescriptor(spec, recDesc1, 2);
        IOperatorDescriptor resultSink = new TestResultSinkOperatorDescriptor(spec, recDesc1, 25000);
        IOperatorDescriptor markerSink = new MessageSinkOperatorDescriptor(spec, recDesc1);
        IOperatorDescriptor project = new TestProjectOperatorDescriptor(spec, recDesc2, recDesc1);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, oddNumberGenerator1, ncNames);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, oddNumberGenerator2, ncNames);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, ncNames);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, fixedPoint, ncNames);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, replicate, ncNames);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, select, ncNames);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, resultSink, ncNames);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, markerSink, ncNames);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, project, ncNames);

        spec.connect(new OneToOneConnectorDescriptor(spec), oddNumberGenerator1, 0, join, 1);
        spec.connect(new OneToOneConnectorDescriptor(spec), oddNumberGenerator2, 0, fixedPoint, ANCHOR_INPUT_INDEX);
        IConnectorDescriptor fpJoinConn = new MToNPartitioningConnectorDescriptor(spec,
                FieldHashPartitionComputerFactory.of(new int[] { 0 }, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(IntegerPointable.FACTORY) }));
        IConnectorDescriptor fpJoinStateReleaseConn = new StateReleaseConnectorDescriptor(spec, fpJoinConn, (byte) 1);
        spec.connect(fpJoinStateReleaseConn, fixedPoint, FIXED_POINT_OUTPUT_INDEX, join, 0);
        spec.getConnectorMap().remove(fpJoinConn.getConnectorId());
        spec.connect(new OneToOneConnectorDescriptor(spec), join, 0, project, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), project, 0, replicate, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), replicate, 0, markerSink, 0);
        spec.connect(new MToNBroadcastConnectorDescriptor(spec), markerSink, 0, resultSink, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), replicate, 1, select, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), select, 0, fixedPoint, RECURSIVE_INPUT_INDEX);
        spec.addRoot(resultSink);
        runTest(spec);
    }

    private static class TestProjectOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
        private static final long serialVersionUID = 1L;
        private final RecordDescriptor inRecDesc;

        public TestProjectOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor inRecDesc,
                RecordDescriptor outRecDesc) {
            super(spec, 1, 1);
            this.outRecDescs[0] = outRecDesc;
            this.inRecDesc = inRecDesc;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
                throws HyracksDataException {
            return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
                private final FrameTupleAccessor frameTupleAccessor = new FrameTupleAccessor(inRecDesc);
                private final FrameTupleListener frameTupleListener = new FrameTupleListener(outRecDescs[0]);
                private final FrameTupleAppender frameTupleAppender = new FrameTupleAppender();
                private final MarkerMessageConsumer markerMessageConsumer = new MarkerMessageConsumer((byte) 1);
                private final IFrame outputBuffer = new VSizeFrame(ctx);
                private final int[] projection = new int[] { 0 };
                private IFrameWriter decoratedWriter;

                @Override
                public void open() throws HyracksDataException {
                    decoratedWriter = new AbstractDelegateFrameWriter(writer) {
                        @Override
                        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                            if (!frameTupleListener.acceptFrame(buffer)) {
                                return;
                            }
                            super.nextFrame(buffer);
                        }
                    };
                    writer.open();
                    frameTupleAppender.reset(outputBuffer, true);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    if (FrameHelper.isMessageFrame(buffer)) {
                        if (markerMessageConsumer.accept(buffer)) {
                            frameTupleListener.startListening();
                            frameTupleAppender.flush(decoratedWriter);
                            if (frameTupleListener.hasTuples()) {
                                MarkerMessageConsumer.raiseLiveFlag(buffer);
                            }
                            frameTupleListener.stopListening();
                        }
                        writer.nextFrame(buffer);

                    } else {
                        frameTupleAccessor.reset(buffer);
                        int tupleCount = frameTupleAccessor.getTupleCount();
                        for (int i = 0; i < tupleCount; i++) {
                            if (!frameTupleAppender.appendProjection(frameTupleAccessor, i, projection)) {
                                LOGGER.trace("Buffer at PROJECT (" + partition + ") is full. Flushing.");
                                frameTupleAppender.flush(writer);
                                frameTupleAppender.appendProjection(frameTupleAccessor, i, projection);
                            }
                        }
                    }
                }

                @Override
                public void fail() throws HyracksDataException {
                    writer.fail();
                }

                @Override
                public void flush() throws HyracksDataException {
                    LOGGER.trace("Flushing at PROJECT (" + partition + ").");
                    frameTupleAppender.flush(writer);
                }

                @Override
                public void close() throws HyracksDataException {
                    writer.close();
                }
            };
        }
    }

    private static class TestSelectOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
        private static final long serialVersionUID = 1L;
        private final int occurrencesUntilDrop;

        public TestSelectOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor recordDesc,
                int occurrencesUntilDrop) {
            super(spec, 1, 1);
            this.occurrencesUntilDrop = occurrencesUntilDrop;
            this.outRecDescs[0] = recordDesc;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
                throws HyracksDataException {
            return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
                private final Map<Integer, Integer> tupleEncounteredMap = new HashMap<>();
                private final FrameTupleAccessor frameTupleAccessor = new FrameTupleAccessor(outRecDescs[0]);
                private final FrameTupleListener frameTupleListener = new FrameTupleListener(outRecDescs[0]);
                private final MarkerMessageConsumer markerMessageConsumer = new MarkerMessageConsumer((byte) 1);
                private final FrameTupleAppender frameTupleAppender = new FrameTupleAppender();
                private final IFrame outFrame = new VSizeFrame(ctx);
                private IFrameWriter decoratedWriter;

                @Override
                public void open() throws HyracksDataException {
                    decoratedWriter = new AbstractDelegateFrameWriter(writer) {
                        @Override
                        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                            if (!frameTupleListener.acceptFrame(buffer)) {
                                return;
                            }
                            super.nextFrame(buffer);
                        }
                    };
                    writer.open();
                    frameTupleAppender.reset(outFrame, true);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    if (FrameHelper.isMessageFrame(buffer)) {
                        LOGGER.trace("Found message frame at SELECT (" + partition + ").");
                        if (markerMessageConsumer.accept(buffer)) {
                            frameTupleListener.startListening();
                            frameTupleAppender.flush(decoratedWriter);
                            if (frameTupleListener.hasTuples()) {
                                MarkerMessageConsumer.raiseLiveFlag(buffer);
                            }
                            frameTupleListener.stopListening();
                        }
                        writer.nextFrame(buffer);

                    } else {
                        frameTupleAccessor.reset(buffer);
                        int tupleCount = frameTupleAccessor.getTupleCount();
                        for (int i = 0; i < tupleCount; i++) {
                            int tupleStart = frameTupleAccessor.getAbsoluteFieldStartOffset(i, 0);
                            int s = IntegerPointable.getInteger(buffer.array(), tupleStart);
                            LOGGER.trace("Found value " + s + " at SELECT (" + partition + ").");
                            tupleEncounteredMap.putIfAbsent(s, 0);
                            if (tupleEncounteredMap.get(s) < occurrencesUntilDrop) {
                                if (!frameTupleAppender.append(frameTupleAccessor, i)) {
                                    LOGGER.trace("Buffer at SELECT (" + partition + ") is full. Flushing.");
                                    frameTupleAppender.flush(writer);
                                    frameTupleAppender.append(frameTupleAccessor, i);
                                }
                                tupleEncounteredMap.put(s, tupleEncounteredMap.get(s) + 1);
                            } else {
                                LOGGER.trace("Dropping value " + s + " at partition " + partition + ".");
                            }
                        }
                    }
                }

                @Override
                public void fail() throws HyracksDataException {
                    writer.fail();
                }

                @Override
                public void flush() throws HyracksDataException {
                    LOGGER.trace("Flushing at SELECT (" + partition + ").");
                    frameTupleAppender.flush(writer);
                }

                @Override
                public void close() throws HyracksDataException {
                    writer.close();
                }
            };
        }
    }

    private static class TestResultSinkOperatorDescriptor extends AbstractOperatorDescriptor {
        private static final long serialVersionUID = 1L;
        private final RecordDescriptor recDesc;
        private final int expectedSum;

        public TestResultSinkOperatorDescriptor(JobSpecification spec, RecordDescriptor recDesc, int expectedSum) {
            super(spec, 1, 0);
            this.recDesc = recDesc;
            this.expectedSum = expectedSum;
        }

        @Override
        public void contributeActivities(IActivityGraphBuilder builder) {
            ActivityId resultActivityId = new ActivityId(getOperatorId(), 0);
            IActivity counterActivityNode = new CounterActivity(resultActivityId);
            builder.addActivity(this, counterActivityNode);
            builder.addSourceEdge(0, counterActivityNode, 0);
        }

        private class CounterActivity extends AbstractActivityNode {
            private static final long serialVersionUID = 1L;

            public CounterActivity(ActivityId id) {
                super(id);
            }

            @Override
            public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                    IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
                return new AbstractUnaryInputSinkOperatorNodePushable() {
                    private final FrameTupleAccessor fta = new FrameTupleAccessor(recDesc);
                    private int sum;

                    @Override
                    public void open() {
                        sum = 0;
                    }

                    @Override
                    public void nextFrame(ByteBuffer buffer) {
                        fta.reset(buffer);
                        int tupleCount = fta.getTupleCount();
                        for (int i = 0; i < tupleCount; i++) {
                            int tupleStart = fta.getAbsoluteFieldStartOffset(i, 0);
                            int s = IntegerPointable.getInteger(buffer.array(), tupleStart);
                            LOGGER.trace("Found value " + s + " at COUNTER (" + partition + ").");
                            sum += s;
                        }
                    }

                    @Override
                    public void fail() {

                    }

                    @Override
                    public void close() {
                        Assert.assertEquals(expectedSum, sum);
                    }
                };
            }
        }
    }

    private static class TestGeneratorOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
        private static final long serialVersionUID = 1L;
        private final boolean isOdd;

        public TestGeneratorOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor recordDesc,
                boolean isOdd) {
            super(spec, 0, 1);
            this.isOdd = isOdd;
            this.outRecDescs[0] = recordDesc;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            return new AbstractUnaryOutputSourceOperatorNodePushable() {
                private final ByteArrayOutputStream baos = new ByteArrayOutputStream(ctx.getInitialFrameSize());
                private final DataOutputStream dos = new DataOutputStream(baos);
                private final FrameFixedFieldAppender appender = new FrameFixedFieldAppender(1);

                @Override
                public void initialize() throws HyracksDataException {
                    VSizeFrame outputFrame = new VSizeFrame(ctx);
                    writer.open();
                    appender.reset(outputFrame, true);
                    for (int i = (isOdd ? 1 : 2); i < 100; i += 2) {
                        LOGGER.trace("Pushing value " + i + " out from GENERATOR (" + partition + ").");
                        baos.reset();
                        IntegerSerializerDeserializer.INSTANCE.serialize(i, dos);
                        appender.appendField(baos.toByteArray(), 0, baos.size());
                        appender.write(writer, false);
                        outputFrame = new VSizeFrame(ctx);
                        appender.reset(outputFrame, true);
                    }
                    writer.close();
                }
            };
        }
    }
}
