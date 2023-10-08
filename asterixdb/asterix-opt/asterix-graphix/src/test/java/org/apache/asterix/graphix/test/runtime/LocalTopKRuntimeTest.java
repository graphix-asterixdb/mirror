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
package org.apache.asterix.graphix.test.runtime;

import static org.apache.hyracks.util.StorageUtil.StorageUnit.KILOBYTE;
import static org.apache.hyracks.util.file.FileUtil.joinPath;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.asterix.graphix.runtime.operator.LocalDistinctRuntimeFactory;
import org.apache.asterix.graphix.runtime.operator.LocalMinimumKRuntimeFactory;
import org.apache.asterix.graphix.runtime.operator.LocalMinimumRuntimeFactory;
import org.apache.asterix.graphix.runtime.operator.buffer.IBufferCacheSupplier;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.data.std.accessors.DoubleBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.FrameFixedFieldAppender;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.ipc.impl.HyracksConnection;
import org.apache.hyracks.test.support.TestStorageManagerComponentHolder;
import org.apache.hyracks.util.StorageUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("rawtypes")
public class LocalTopKRuntimeTest implements Serializable {
    private static final long serialVersionUID = 1L;

    private static IHyracksClientConnection hcc;
    private static ClusterControllerService cc;
    private static NodeControllerService nc;

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
        File ccDir = new File(joinPath("target", "cluster-controller"));
        ccDir.mkdirs();
        File ccRoot = File.createTempFile(LocalTopKRuntimeTest.class.getName(), ".data", ccDir);
        ccRoot.delete();
        ccRoot.mkdir();
        ccConfig.setRootDir(ccRoot.getAbsolutePath());
        cc = new ClusterControllerService(ccConfig);
        cc.start();

        // Create our NC.
        File ncDir = new File(joinPath(System.getProperty("user.dir"), "target", "data"));
        FileUtils.deleteQuietly(ncDir);
        ncDir.delete();
        ncDir.mkdir();
        NCConfig ncConfig = new NCConfig("nc1");
        ncConfig.setClusterAddress("localhost");
        ncConfig.setClusterPort(39001);
        ncConfig.setClusterListenAddress("127.0.0.1");
        ncConfig.setDataListenAddress("127.0.0.1");
        ncConfig.setResultListenAddress("127.0.0.1");
        ncConfig.setIODevices(new String[] { ncDir.getPath() });
        nc = new NodeControllerService(ncConfig);
        nc.start();

        // Establish a Hyracks connection.
        hcc = new HyracksConnection(ccConfig.getClientListenAddress(), ccConfig.getClientListenPort());
    }

    @AfterClass
    public static void deinit() throws Exception {
        nc.stop();
        cc.stop();
    }

    @Test
    public void testLocalDistinct() throws Exception {
        int bufferPageSize = StorageUtil.getIntSizeInBytes(32, KILOBYTE);
        IBufferCacheSupplier cacheSupplier = TestStorageManagerComponentHolder::getBufferCache;
        executeTest(1,
                () -> new LocalDistinctRuntimeFactory(new int[] { 0, 1 }, 0.01, bufferPageSize, 4,
                        new ITypeTraits[] { IntegerPointable.TYPE_TRAITS },
                        new IScalarEvaluatorFactory[] { new ColumnAccessEvalFactory(0) },
                        new IBinaryComparatorFactory[] { IntegerBinaryComparatorFactory.INSTANCE }, cacheSupplier));
    }

    @Test
    public void testLocalMinimum() throws Exception {
        int bufferPageSize = StorageUtil.getIntSizeInBytes(32, KILOBYTE);
        IBufferCacheSupplier cacheSupplier = TestStorageManagerComponentHolder::getBufferCache;
        executeTest(1,
                () -> new LocalMinimumRuntimeFactory(new int[] { 0, 1 }, 0.01, bufferPageSize, 4, 4,
                        new ITypeTraits[] { IntegerPointable.TYPE_TRAITS }, DoublePointable.TYPE_TRAITS,
                        new IScalarEvaluatorFactory[] { new ColumnAccessEvalFactory(0) },
                        new ColumnAccessEvalFactory(1),
                        new IBinaryComparatorFactory[] { IntegerBinaryComparatorFactory.INSTANCE },
                        DoubleBinaryComparatorFactory.INSTANCE, cacheSupplier));
    }

    @Test
    public void testLocalMinimumK() throws Exception {
        int bufferPageSize = StorageUtil.getIntSizeInBytes(32, KILOBYTE);
        IBufferCacheSupplier cacheSupplier = TestStorageManagerComponentHolder::getBufferCache;
        executeTest(3,
                () -> new LocalMinimumKRuntimeFactory(new int[] { 0, 1 }, 0.01, bufferPageSize, 4, 4,
                        new ITypeTraits[] { IntegerPointable.TYPE_TRAITS }, DoublePointable.TYPE_TRAITS,
                        new IScalarEvaluatorFactory[] { new ColumnAccessEvalFactory(0) },
                        new ColumnAccessEvalFactory(1),
                        new IBinaryComparatorFactory[] { IntegerBinaryComparatorFactory.INSTANCE },
                        DoubleBinaryComparatorFactory.INSTANCE, cacheSupplier, 2));
    }

    private void executeTest(int kValue, Supplier<IPushRuntimeFactory> runtimeFactorySupplier) throws Exception {
        JobSpecification spec = new JobSpecification(32678);
        ISerializerDeserializer[] serDe = new ISerializerDeserializer[2];
        serDe[0] = IntegerSerializerDeserializer.INSTANCE;
        serDe[1] = DoubleSerializerDeserializer.INSTANCE;
        RecordDescriptor recDesc = new RecordDescriptor(serDe);
        TestStorageManagerComponentHolder.init(8192, 256, 256);

        int generatorOpIntegerRange = 10000;
        IOperatorDescriptor generatorOp = new AbstractSingleActivityOperatorDescriptor(spec, 0, 1) {
            {
                outRecDescs[0] = recDesc;
            }

            @Override
            public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                    IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
                final int duplicationFactor = 10;
                return new AbstractUnaryOutputSourceOperatorNodePushable() {
                    private final ByteArrayOutputStream baos1 = new ByteArrayOutputStream(ctx.getInitialFrameSize());
                    private final ByteArrayOutputStream baos2 = new ByteArrayOutputStream(ctx.getInitialFrameSize());
                    private final DataOutputStream dos1 = new DataOutputStream(baos1);
                    private final DataOutputStream dos2 = new DataOutputStream(baos2);
                    private final FrameFixedFieldAppender appender = new FrameFixedFieldAppender(2);

                    @Override
                    public void initialize() throws HyracksDataException {
                        VSizeFrame outputFrame = new VSizeFrame(ctx);
                        writer.open();
                        appender.reset(outputFrame, true);

                        // Not trying to be efficient here... :-)
                        List<Integer> domainValues = IntStream.range(0, generatorOpIntegerRange * duplicationFactor)
                                .boxed().map(i -> i / duplicationFactor).collect(Collectors.toList());
                        Collections.shuffle(domainValues);
                        for (Integer domainValue : domainValues) {
                            baos1.reset();
                            baos2.reset();
                            IntegerSerializerDeserializer.INSTANCE.serialize(domainValue, dos1);
                            DoubleSerializerDeserializer.INSTANCE.serialize(Math.random(), dos2);
                            appender.appendField(baos1.toByteArray(), 0, baos1.size());
                            appender.appendField(baos2.toByteArray(), 0, baos2.size());
                            if (domainValue % 10 == 0) {
                                appender.write(writer, true);
                                appender.resetWithLeftOverData(outputFrame);
                                outputFrame = new VSizeFrame(ctx);
                            }
                        }
                        writer.close();
                    }
                };
            }
        };
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, generatorOp, "nc1");
        IPushRuntimeFactory runtimeFactory = runtimeFactorySupplier.get();
        IOperatorDescriptor metaOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 1,
                new IPushRuntimeFactory[] { runtimeFactory }, new RecordDescriptor[] { recDesc });
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, metaOp, "nc1");
        spec.connect(new OneToOneConnectorDescriptor(spec), generatorOp, 0, metaOp, 0);

        IOperatorDescriptor resultSinkOp = new AbstractSingleActivityOperatorDescriptor(spec, 1, 0) {
            @Override
            public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                    IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
                return new AbstractUnaryInputSinkOperatorNodePushable() {
                    private final FrameTupleAccessor fta = new FrameTupleAccessor(recDesc);
                    private final Map<Integer, Pair<Integer, List<Double>>> visitedSet = new HashMap<>();

                    @Override
                    public void open() {
                    }

                    @Override
                    public void nextFrame(ByteBuffer buffer) {
                        fta.reset(buffer);
                        byte[] ftaBuffer = fta.getBuffer().array();
                        for (int i = 0; i < fta.getTupleCount(); i++) {
                            int sortKeyStartOffset = fta.getAbsoluteFieldStartOffset(i, 0);
                            int weightValueStartOffset = fta.getAbsoluteFieldStartOffset(i, 1);
                            int value = IntegerPointable.getInteger(ftaBuffer, sortKeyStartOffset);
                            double weight = DoublePointable.getDouble(ftaBuffer, weightValueStartOffset);
                            if (!visitedSet.containsKey(value)) {
                                visitedSet.put(value, new Pair<>(1, new ArrayList<>(List.of(weight))));

                            } else {
                                if (visitedSet.get(value).first > kValue) {
                                    Optional<Double> max = visitedSet.get(value).second.stream().max(Double::compare);
                                    Assert.assertTrue(max.isPresent());
                                    Assert.assertTrue(weight < max.get());
                                }
                                Pair<Integer, List<Double>> integerListPair = visitedSet.get(value);
                                integerListPair.setFirst(integerListPair.first + 1);
                                integerListPair.getSecond().add(weight);
                            }
                        }
                    }

                    @Override
                    public void fail() {
                    }

                    @Override
                    public void close() {
                    }
                };
            }
        };
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, resultSinkOp, "nc1");
        spec.connect(new OneToOneConnectorDescriptor(spec), metaOp, 0, resultSinkOp, 0);

        // Run our job.
        spec.addRoot(resultSinkOp);
        JobId jobId = hcc.startJob(spec);
        hcc.waitForCompletion(jobId);
    }
}
