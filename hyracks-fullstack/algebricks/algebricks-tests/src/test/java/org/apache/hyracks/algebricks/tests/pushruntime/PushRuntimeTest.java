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
package org.apache.hyracks.algebricks.tests.pushruntime;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.algebricks.data.impl.BinaryBooleanInspectorImpl;
import org.apache.hyracks.algebricks.data.impl.BinaryIntegerInspectorImpl;
import org.apache.hyracks.algebricks.data.impl.IntegerPrinterFactory;
import org.apache.hyracks.algebricks.data.impl.NoopMissingWriterFactory;
import org.apache.hyracks.algebricks.data.impl.UTF8StringPrinterFactory;
import org.apache.hyracks.algebricks.runtime.aggregators.TupleCountAggregateFunctionFactory;
import org.apache.hyracks.algebricks.runtime.aggregators.TupleCountRunningAggregateFunctionFactory;
import org.apache.hyracks.algebricks.runtime.base.AlgebricksPipeline;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.TupleFieldEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.aggreg.AggregateRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.aggreg.NestedPlansAccumulatingAggregatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.aggrun.RunningAggregateRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputPushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.group.MicroPreClusteredGroupRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.message.MessageSinkRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.message.StateReleaseRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.algebricks.runtime.operators.meta.SubplanRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.sort.MicroSortRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.std.AssignRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.std.EmptyTupleSourceRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.std.NestedTupleSourceRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.std.PrinterRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.std.SinkWriterRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.std.SplitOperatorDescriptor;
import org.apache.hyracks.algebricks.runtime.operators.std.StreamLimitRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.std.StreamProjectRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.std.StreamSelectRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.std.StringStreamingRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.std.SwitchOperatorDescriptor;
import org.apache.hyracks.algebricks.runtime.operators.std.UnnestRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.writers.PrinterBasedWriterFactory;
import org.apache.hyracks.algebricks.tests.util.AlgebricksHyracksIntegrationUtil;
import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.io.ManagedFileSplit;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryComparatorFactory;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.parsers.FloatParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import org.apache.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.LineFileWriteOperatorDescriptor;
import org.apache.hyracks.dataflow.std.group.preclustered.PreclusteredGroupOperatorDescriptor;
import org.apache.hyracks.dataflow.std.message.producer.MarkerMessageProducer;
import org.apache.hyracks.dataflow.std.misc.ReplicateOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sort.InMemorySortOperatorDescriptor;
import org.apache.hyracks.util.IntSerDeUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class PushRuntimeTest {

    private static final String SEPARATOR = System.getProperty("file.separator");
    private static final String PATH_ACTUAL = "rttest";
    private static final String PATH_BASE = "src" + SEPARATOR + "test" + SEPARATOR + "resources";
    private static final String PATH_EXPECTED = PATH_BASE + SEPARATOR + "results";
    private static final int FRAME_SIZE = 32768;

    private static final String[] DEFAULT_NODES = new String[] { AlgebricksHyracksIntegrationUtil.NC1_ID };
    private static final AtomicInteger aInteger = new AtomicInteger(0);

    @BeforeClass
    public static void setUp() throws Exception {
        File outdir = new File(PATH_ACTUAL);
        outdir.mkdirs();
        AlgebricksHyracksIntegrationUtil.init();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        AlgebricksHyracksIntegrationUtil.deinit();
        File outdir = new File(PATH_ACTUAL);
        File[] files = outdir.listFiles();
        if (files == null || files.length == 0) {
            outdir.delete();
        }
    }

    @Test
    public void etsAssignPrint() throws Exception {
        JobSpecification spec = new JobSpecification(FRAME_SIZE);
        IntegerConstantEvalFactory const1 = new IntegerConstantEvalFactory(400);
        IntegerConstantEvalFactory const2 = new IntegerConstantEvalFactory(3);

        EmptyTupleSourceRuntimeFactory ets = new EmptyTupleSourceRuntimeFactory();
        RecordDescriptor etsDesc = new RecordDescriptor(new ISerializerDeserializer[] {});
        AssignRuntimeFactory assign = new AssignRuntimeFactory(new int[] { 0, 1 },
                new IScalarEvaluatorFactory[] { const1, const2 }, new int[] { 0, 1 });
        RecordDescriptor assignDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        PrinterRuntimeFactory printer = new PrinterRuntimeFactory(new int[] { 0, 1 },
                new IPrinterFactory[] { IntegerPrinterFactory.INSTANCE, IntegerPrinterFactory.INSTANCE }, assignDesc);

        AlgebricksMetaOperatorDescriptor algebricksOp =
                new AlgebricksMetaOperatorDescriptor(spec, 0, 0, new IPushRuntimeFactory[] { ets, assign, printer },
                        new RecordDescriptor[] { etsDesc, assignDesc, null });
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, algebricksOp, DEFAULT_NODES);
        spec.addRoot(algebricksOp);
        AlgebricksHyracksIntegrationUtil.runJob(spec);
    }

    @Test
    public void etsAssignWrite() throws Exception {
        JobSpecification spec = new JobSpecification(FRAME_SIZE);
        IntegerConstantEvalFactory const1 = new IntegerConstantEvalFactory(400);
        IntegerConstantEvalFactory const2 = new IntegerConstantEvalFactory(3);

        EmptyTupleSourceRuntimeFactory ets = new EmptyTupleSourceRuntimeFactory();
        RecordDescriptor etsDesc = new RecordDescriptor(new ISerializerDeserializer[] {});
        AssignRuntimeFactory assign = new AssignRuntimeFactory(new int[] { 0, 1 },
                new IScalarEvaluatorFactory[] { const1, const2 }, new int[] { 0, 1 });
        RecordDescriptor assignDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        String filePath = PATH_ACTUAL + SEPARATOR + "etsAssignWrite.out";
        File outFile = new File(filePath);
        SinkWriterRuntimeFactory writer = new SinkWriterRuntimeFactory(new int[] { 0, 1 },
                new IPrinterFactory[] { IntegerPrinterFactory.INSTANCE, IntegerPrinterFactory.INSTANCE }, outFile,
                PrinterBasedWriterFactory.INSTANCE, assignDesc);

        AlgebricksMetaOperatorDescriptor algebricksOp =
                new AlgebricksMetaOperatorDescriptor(spec, 0, 0, new IPushRuntimeFactory[] { ets, assign, writer },
                        new RecordDescriptor[] { etsDesc, assignDesc, null });
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, algebricksOp, DEFAULT_NODES);
        spec.addRoot(algebricksOp);
        AlgebricksHyracksIntegrationUtil.runJob(spec);

        StringBuilder buf = new StringBuilder();
        readFileToString(outFile, buf);
        Assert.assertEquals("400; 3", buf.toString());
        outFile.delete();
    }

    @Test
    public void scanSelectWrite() throws Exception {
        JobSpecification spec = new JobSpecification(FRAME_SIZE);

        // the scanner
        FileSplit[] intFileSplits = new FileSplit[1];
        intFileSplits[0] = new ManagedFileSplit(AlgebricksHyracksIntegrationUtil.NC1_ID,
                "data" + File.separator + "simple" + File.separator + "int-part1.tbl");
        IFileSplitProvider intSplitProvider = new ConstantFileSplitProvider(intFileSplits);
        RecordDescriptor intScannerDesc =
                new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE });
        IValueParserFactory[] valueParsers = new IValueParserFactory[] { IntegerParserFactory.INSTANCE };
        FileScanOperatorDescriptor intScanner = new FileScanOperatorDescriptor(spec, intSplitProvider,
                new DelimitedDataTupleParserFactory(valueParsers, '|'), intScannerDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, intScanner, DEFAULT_NODES);

        // the algebricks op.
        IScalarEvaluatorFactory cond =
                new IntegerGreaterThanEvalFactory(new IntegerConstantEvalFactory(2), new TupleFieldEvaluatorFactory(0));
        StreamSelectRuntimeFactory select = new StreamSelectRuntimeFactory(cond, new int[] { 0 },
                BinaryBooleanInspectorImpl.FACTORY, false, -1, null);
        RecordDescriptor selectDesc = intScannerDesc;

        String filePath = PATH_ACTUAL + SEPARATOR + "scanSelectWrite.out";
        File outFile = new File(filePath);
        SinkWriterRuntimeFactory writer =
                new SinkWriterRuntimeFactory(new int[] { 0 }, new IPrinterFactory[] { IntegerPrinterFactory.INSTANCE },
                        outFile, PrinterBasedWriterFactory.INSTANCE, selectDesc);

        AlgebricksMetaOperatorDescriptor algebricksOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 0,
                new IPushRuntimeFactory[] { select, writer }, new RecordDescriptor[] { selectDesc, null });

        PartitionConstraintHelper.addPartitionCountConstraint(spec, algebricksOp, 1);

        spec.connect(new OneToOneConnectorDescriptor(spec), intScanner, 0, algebricksOp, 0);

        spec.addRoot(algebricksOp);
        AlgebricksHyracksIntegrationUtil.runJob(spec);

        StringBuilder buf = new StringBuilder();
        readFileToString(outFile, buf);
        Assert.assertEquals("0", buf.toString());
        outFile.delete();
    }

    @Test
    public void scanMessageSelectWrite() throws Exception {
        JobSpecification spec = new JobSpecification(FRAME_SIZE);

        // Build our scanner (no marker decoration here).
        FileSplit[] intFileSplits = new FileSplit[1];
        intFileSplits[0] = new ManagedFileSplit(AlgebricksHyracksIntegrationUtil.NC1_ID,
                "data" + File.separator + "simple" + File.separator + "int-part1.tbl");
        IFileSplitProvider intSplitProvider = new ConstantFileSplitProvider(intFileSplits);
        RecordDescriptor intScannerDesc =
                new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE });
        IValueParserFactory[] valueParsers = new IValueParserFactory[] { IntegerParserFactory.INSTANCE };
        FileScanOperatorDescriptor intScanner = new FileScanOperatorDescriptor(spec, intSplitProvider,
                new DelimitedDataTupleParserFactory(valueParsers, '|'), intScannerDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, intScanner, DEFAULT_NODES);

        // Build our marker generator.
        IOperatorDescriptor markerGenerator = new MessageInjectorOperatorDescriptor(spec, intScannerDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, markerGenerator, DEFAULT_NODES);

        // Build our SELECT runtime, decorated with marker handling.
        IScalarEvaluatorFactory cond =
                new IntegerGreaterThanEvalFactory(new IntegerConstantEvalFactory(5), new TupleFieldEvaluatorFactory(0));
        StreamSelectRuntimeFactory select = new StreamSelectRuntimeFactory(cond, new int[] { 0 },
                BinaryBooleanInspectorImpl.FACTORY, false, -1, null);
        IPushRuntimeFactory selectMarker = new StateReleaseRuntimeFactory(select, (byte) 1);

        // Build our addition action, triggered by a marker.
        AddOneOnMarkerRuntimeFactory addMarker = new AddOneOnMarkerRuntimeFactory(new int[] { 0 }, intScannerDesc);

        // Build our WRITER runtime, preceded by a marker sink.
        String filePath = PATH_ACTUAL + SEPARATOR + "scanMarkSelectWrite.out";
        File outFile = new File(filePath);
        IPushRuntimeFactory markerSink = new MessageSinkRuntimeFactory();
        SinkWriterRuntimeFactory writer =
                new SinkWriterRuntimeFactory(new int[] { 0 }, new IPrinterFactory[] { IntegerPrinterFactory.INSTANCE },
                        outFile, PrinterBasedWriterFactory.INSTANCE, intScannerDesc);

        // Assemble our Algebricks operator and connectors.
        AlgebricksMetaOperatorDescriptor algebricksOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 0,
                new IPushRuntimeFactory[] { selectMarker, addMarker, markerSink, writer },
                new RecordDescriptor[] { intScannerDesc, intScannerDesc, intScannerDesc, null });
        PartitionConstraintHelper.addPartitionCountConstraint(spec, algebricksOp, 1);
        spec.connect(new OneToOneConnectorDescriptor(spec), intScanner, 0, markerGenerator, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), markerGenerator, 0, algebricksOp, 0);
        spec.addRoot(algebricksOp);

        // 0 + marker frame trigger = 0, 2+1, 4+1.
        AlgebricksHyracksIntegrationUtil.runJob(spec);
        StringBuilder buf = new StringBuilder();
        readFileToString(outFile, buf);
        Assert.assertEquals("035", buf.toString());
        // noinspection ResultOfMethodCallIgnored
        outFile.delete();
    }

    @Test
    public void etsAssignProjectWrite() throws Exception {

        JobSpecification spec = new JobSpecification(FRAME_SIZE);
        IntegerConstantEvalFactory const1 = new IntegerConstantEvalFactory(400);
        IntegerConstantEvalFactory const2 = new IntegerConstantEvalFactory(3);

        EmptyTupleSourceRuntimeFactory ets = new EmptyTupleSourceRuntimeFactory();
        RecordDescriptor etsDesc = new RecordDescriptor(new ISerializerDeserializer[] {});
        AssignRuntimeFactory assign = new AssignRuntimeFactory(new int[] { 0, 1 },
                new IScalarEvaluatorFactory[] { const1, const2 }, new int[] { 0, 1 });
        RecordDescriptor assignDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });
        StreamProjectRuntimeFactory project = new StreamProjectRuntimeFactory(new int[] { 1 });
        RecordDescriptor projectDesc =
                new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE });

        String filePath = PATH_ACTUAL + SEPARATOR + "etsAssignProjectWrite.out";
        File outFile = new File(filePath);
        SinkWriterRuntimeFactory writer =
                new SinkWriterRuntimeFactory(new int[] { 0 }, new IPrinterFactory[] { IntegerPrinterFactory.INSTANCE },
                        outFile, PrinterBasedWriterFactory.INSTANCE, projectDesc);

        AlgebricksMetaOperatorDescriptor algebricksOp = new AlgebricksMetaOperatorDescriptor(spec, 0, 0,
                new IPushRuntimeFactory[] { ets, assign, project, writer },
                new RecordDescriptor[] { etsDesc, assignDesc, projectDesc, null });

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, algebricksOp, DEFAULT_NODES);

        spec.addRoot(algebricksOp);
        AlgebricksHyracksIntegrationUtil.runJob(spec);

        StringBuilder buf = new StringBuilder();
        readFileToString(outFile, buf);
        Assert.assertEquals("3", buf.toString());
        outFile.delete();
    }

    @Test
    public void scanLimitWrite() throws Exception {
        JobSpecification spec = new JobSpecification(FRAME_SIZE);

        // the scanner
        FileSplit[] fileSplits = new FileSplit[1];
        fileSplits[0] = new ManagedFileSplit(AlgebricksHyracksIntegrationUtil.NC1_ID,
                "data" + File.separator + "tpch0.001" + File.separator + "customer.tbl");
        IFileSplitProvider splitProvider = new ConstantFileSplitProvider(fileSplits);

        RecordDescriptor scannerDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), IntegerSerializerDeserializer.INSTANCE,
                new UTF8StringSerializerDeserializer(), FloatSerializerDeserializer.INSTANCE,
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() });
        IValueParserFactory[] valueParsers = new IValueParserFactory[] { IntegerParserFactory.INSTANCE,
                UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                UTF8StringParserFactory.INSTANCE, FloatParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                UTF8StringParserFactory.INSTANCE };
        FileScanOperatorDescriptor scanner = new FileScanOperatorDescriptor(spec, splitProvider,
                new DelimitedDataTupleParserFactory(valueParsers, '|'), scannerDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, scanner, DEFAULT_NODES);

        // the algebricks op.
        StreamLimitRuntimeFactory limit = new StreamLimitRuntimeFactory(new IntegerConstantEvalFactory(2), null,
                new int[] { 0 }, BinaryIntegerInspectorImpl.FACTORY);
        RecordDescriptor limitDesc =
                new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE });

        String filePath = PATH_ACTUAL + SEPARATOR + "scanLimitWrite.out";
        File outFile = new File(filePath);
        SinkWriterRuntimeFactory writer =
                new SinkWriterRuntimeFactory(new int[] { 0 }, new IPrinterFactory[] { IntegerPrinterFactory.INSTANCE },
                        outFile, PrinterBasedWriterFactory.INSTANCE, limitDesc);

        AlgebricksMetaOperatorDescriptor algebricksOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 0,
                new IPushRuntimeFactory[] { limit, writer }, new RecordDescriptor[] { limitDesc, null });
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, algebricksOp,
                new String[] { AlgebricksHyracksIntegrationUtil.NC1_ID });

        spec.connect(new OneToOneConnectorDescriptor(spec), scanner, 0, algebricksOp, 0);

        spec.addRoot(algebricksOp);
        AlgebricksHyracksIntegrationUtil.runJob(spec);

        StringBuilder buf = new StringBuilder();
        readFileToString(outFile, buf);
        Assert.assertEquals("12", buf.toString());
        outFile.delete();
    }

    @Test
    public void etsUnnestWrite() throws Exception {
        JobSpecification spec = new JobSpecification(FRAME_SIZE);

        EmptyTupleSourceRuntimeFactory ets = new EmptyTupleSourceRuntimeFactory();
        RecordDescriptor etsDesc = new RecordDescriptor(new ISerializerDeserializer[] {});
        IUnnestingEvaluatorFactory aggregFactory = new IntArrayUnnester(new int[] { 100, 200, 300 });
        UnnestRuntimeFactory unnest = new UnnestRuntimeFactory(0, aggregFactory, new int[] { 0 }, false, null);
        RecordDescriptor unnestDesc =
                new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE });

        String filePath = PATH_ACTUAL + SEPARATOR + "etsUnnestWrite.out";
        File outFile = new File(filePath);
        SinkWriterRuntimeFactory writer =
                new SinkWriterRuntimeFactory(new int[] { 0 }, new IPrinterFactory[] { IntegerPrinterFactory.INSTANCE },
                        outFile, PrinterBasedWriterFactory.INSTANCE, unnestDesc);

        AlgebricksMetaOperatorDescriptor algebricksOp =
                new AlgebricksMetaOperatorDescriptor(spec, 0, 0, new IPushRuntimeFactory[] { ets, unnest, writer },
                        new RecordDescriptor[] { etsDesc, unnestDesc, null });
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, algebricksOp,
                new String[] { AlgebricksHyracksIntegrationUtil.NC1_ID });
        spec.addRoot(algebricksOp);
        AlgebricksHyracksIntegrationUtil.runJob(spec);

        StringBuilder buf = new StringBuilder();
        readFileToString(outFile, buf);
        Assert.assertEquals("100200300", buf.toString());
        outFile.delete();
    }

    @Test
    public void scanAggregateWrite() throws Exception {
        JobSpecification spec = new JobSpecification(FRAME_SIZE);

        // the scanner
        FileSplit[] fileSplits = new FileSplit[1];
        fileSplits[0] = new ManagedFileSplit(AlgebricksHyracksIntegrationUtil.NC1_ID,
                "data" + File.separator + "tpch0.001" + File.separator + "customer-part1.tbl");
        IFileSplitProvider splitProvider = new ConstantFileSplitProvider(fileSplits);
        RecordDescriptor scannerDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), IntegerSerializerDeserializer.INSTANCE,
                new UTF8StringSerializerDeserializer(), FloatSerializerDeserializer.INSTANCE,
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() });
        IValueParserFactory[] valueParsers = new IValueParserFactory[] { IntegerParserFactory.INSTANCE,
                UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                UTF8StringParserFactory.INSTANCE, FloatParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                UTF8StringParserFactory.INSTANCE };
        FileScanOperatorDescriptor scanner = new FileScanOperatorDescriptor(spec, splitProvider,
                new DelimitedDataTupleParserFactory(valueParsers, '|'), scannerDesc);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, scanner,
                new String[] { AlgebricksHyracksIntegrationUtil.NC1_ID });

        // the algebricks op.
        AggregateRuntimeFactory agg = new AggregateRuntimeFactory(
                new IAggregateEvaluatorFactory[] { new TupleCountAggregateFunctionFactory() });
        RecordDescriptor aggDesc =
                new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE });

        String filePath = PATH_ACTUAL + SEPARATOR + "scanAggregateWrite.out";
        File outFile = new File(filePath);
        SinkWriterRuntimeFactory writer =
                new SinkWriterRuntimeFactory(new int[] { 0 }, new IPrinterFactory[] { IntegerPrinterFactory.INSTANCE },
                        outFile, PrinterBasedWriterFactory.INSTANCE, aggDesc);

        AlgebricksMetaOperatorDescriptor algebricksOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 0,
                new IPushRuntimeFactory[] { agg, writer }, new RecordDescriptor[] { aggDesc, null });

        PartitionConstraintHelper.addPartitionCountConstraint(spec, algebricksOp, 1);

        spec.connect(new OneToOneConnectorDescriptor(spec), scanner, 0, algebricksOp, 0);

        spec.addRoot(algebricksOp);
        AlgebricksHyracksIntegrationUtil.runJob(spec);

        StringBuilder buf = new StringBuilder();
        readFileToString(outFile, buf);
        Assert.assertEquals("75", buf.toString());
        outFile.delete();
    }

    @Test
    public void scanSortGbySelectWrite() throws Exception {
        JobSpecification spec = new JobSpecification(FRAME_SIZE);

        // the scanner
        FileSplit[] fileSplits = new FileSplit[1];
        fileSplits[0] = new ManagedFileSplit(AlgebricksHyracksIntegrationUtil.NC1_ID,
                "data" + File.separator + "tpch0.001" + File.separator + "customer.tbl");
        IFileSplitProvider splitProvider = new ConstantFileSplitProvider(fileSplits);
        RecordDescriptor scannerDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), IntegerSerializerDeserializer.INSTANCE,
                new UTF8StringSerializerDeserializer(), FloatSerializerDeserializer.INSTANCE,
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() });
        IValueParserFactory[] valueParsers = new IValueParserFactory[] { IntegerParserFactory.INSTANCE,
                UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                UTF8StringParserFactory.INSTANCE, FloatParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                UTF8StringParserFactory.INSTANCE };
        FileScanOperatorDescriptor scanner = new FileScanOperatorDescriptor(spec, splitProvider,
                new DelimitedDataTupleParserFactory(valueParsers, '|'), scannerDesc);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, scanner,
                new String[] { AlgebricksHyracksIntegrationUtil.NC1_ID });

        // the sort (by nation id)
        RecordDescriptor sortDesc = scannerDesc;
        InMemorySortOperatorDescriptor sort = new InMemorySortOperatorDescriptor(spec, new int[] { 3 },
                new IBinaryComparatorFactory[] { IntegerBinaryComparatorFactory.INSTANCE }, sortDesc);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sort,
                new String[] { AlgebricksHyracksIntegrationUtil.NC1_ID });

        // the group-by
        NestedTupleSourceRuntimeFactory nts = new NestedTupleSourceRuntimeFactory();
        RecordDescriptor ntsDesc = sortDesc;
        AggregateRuntimeFactory agg = new AggregateRuntimeFactory(
                new IAggregateEvaluatorFactory[] { new TupleCountAggregateFunctionFactory() });
        RecordDescriptor aggDesc =
                new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE });
        AlgebricksPipeline pipeline = new AlgebricksPipeline(new IPushRuntimeFactory[] { nts, agg },
                new RecordDescriptor[] { ntsDesc, aggDesc }, null, null);
        NestedPlansAccumulatingAggregatorFactory npaaf = new NestedPlansAccumulatingAggregatorFactory(
                new AlgebricksPipeline[] { pipeline }, new int[] { 3 }, new int[] {});
        RecordDescriptor gbyDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });
        PreclusteredGroupOperatorDescriptor gby = new PreclusteredGroupOperatorDescriptor(spec, new int[] { 3 },
                new IBinaryComparatorFactory[] { IntegerBinaryComparatorFactory.INSTANCE }, npaaf, gbyDesc, false, -1);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, gby,
                new String[] { AlgebricksHyracksIntegrationUtil.NC1_ID });

        // the algebricks op.
        IScalarEvaluatorFactory cond =
                new IntegerEqualsEvalFactory(new IntegerConstantEvalFactory(3), new TupleFieldEvaluatorFactory(0)); // Canadian customers
        StreamSelectRuntimeFactory select = new StreamSelectRuntimeFactory(cond, new int[] { 1 },
                BinaryBooleanInspectorImpl.FACTORY, false, -1, null);
        RecordDescriptor selectDesc =
                new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE });

        String filePath = PATH_ACTUAL + SEPARATOR + "scanSortGbySelectWrite.out";
        File outFile = new File(filePath);
        SinkWriterRuntimeFactory writer =
                new SinkWriterRuntimeFactory(new int[] { 0 }, new IPrinterFactory[] { IntegerPrinterFactory.INSTANCE },
                        outFile, PrinterBasedWriterFactory.INSTANCE, selectDesc);

        AlgebricksMetaOperatorDescriptor algebricksOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 0,
                new IPushRuntimeFactory[] { select, writer }, new RecordDescriptor[] { selectDesc, null });

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, algebricksOp,
                new String[] { AlgebricksHyracksIntegrationUtil.NC1_ID });

        spec.connect(new OneToOneConnectorDescriptor(spec), scanner, 0, sort, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), sort, 0, gby, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), gby, 0, algebricksOp, 0);
        spec.addRoot(algebricksOp);

        AlgebricksHyracksIntegrationUtil.runJob(spec);
        StringBuilder buf = new StringBuilder();
        readFileToString(outFile, buf);
        Assert.assertEquals("9", buf.toString());
        outFile.delete();
    }

    @Test
    public void etsUnnestRunningaggregateWrite() throws Exception {
        JobSpecification spec = new JobSpecification(FRAME_SIZE);

        EmptyTupleSourceRuntimeFactory ets = new EmptyTupleSourceRuntimeFactory();
        RecordDescriptor etsDesc = new RecordDescriptor(new ISerializerDeserializer[] {});
        IUnnestingEvaluatorFactory aggregFactory = new IntArrayUnnester(new int[] { 100, 200, 300 });
        UnnestRuntimeFactory unnest = new UnnestRuntimeFactory(0, aggregFactory, new int[] { 0 }, false, null);
        RecordDescriptor unnestDesc =
                new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE });

        RunningAggregateRuntimeFactory ragg = new RunningAggregateRuntimeFactory(new int[] { 0, 1 }, new int[] { 1 },
                new IRunningAggregateEvaluatorFactory[] { new TupleCountRunningAggregateFunctionFactory() });
        RecordDescriptor raggDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        String filePath = PATH_ACTUAL + SEPARATOR + "etsUnnestRunningaggregateWrite.out";
        File outFile = new File(filePath);
        SinkWriterRuntimeFactory writer =
                new SinkWriterRuntimeFactory(new int[] { 1 }, new IPrinterFactory[] { IntegerPrinterFactory.INSTANCE },
                        outFile, PrinterBasedWriterFactory.INSTANCE, raggDesc);

        AlgebricksMetaOperatorDescriptor algebricksOp = new AlgebricksMetaOperatorDescriptor(spec, 0, 0,
                new IPushRuntimeFactory[] { ets, unnest, ragg, writer },
                new RecordDescriptor[] { etsDesc, unnestDesc, raggDesc, null });

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, algebricksOp,
                new String[] { AlgebricksHyracksIntegrationUtil.NC1_ID });

        spec.addRoot(algebricksOp);
        AlgebricksHyracksIntegrationUtil.runJob(spec);

        StringBuilder buf = new StringBuilder();
        readFileToString(outFile, buf);
        Assert.assertEquals("123", buf.toString());
        outFile.delete();
    }

    @Test
    public void etsAssignScriptWrite() throws Exception {
        JobSpecification spec = new JobSpecification(FRAME_SIZE);
        IntegerConstantEvalFactory const1 = new IntegerConstantEvalFactory(400);
        IntegerConstantEvalFactory const2 = new IntegerConstantEvalFactory(3);

        EmptyTupleSourceRuntimeFactory ets = new EmptyTupleSourceRuntimeFactory();
        RecordDescriptor etsDesc = new RecordDescriptor(new ISerializerDeserializer[] {});
        AssignRuntimeFactory assign = new AssignRuntimeFactory(new int[] { 0, 1 },
                new IScalarEvaluatorFactory[] { const1, const2 }, new int[] { 0, 1 });
        RecordDescriptor assignDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        IValueParserFactory[] valueParsers = { IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE };

        String osname = System.getProperty("os.name");
        String command;
        if (osname.equals("Linux")) {
            command = "bash target/testscripts/idscript";
        } else if (osname.startsWith("Windows")) {
            command = "target\\testscripts\\idscript.cmd";
        } else {
            // don't know how to test
            return;
        }

        StringStreamingRuntimeFactory script = new StringStreamingRuntimeFactory(command,
                new IPrinterFactory[] { IntegerPrinterFactory.INSTANCE, IntegerPrinterFactory.INSTANCE }, ' ',
                new DelimitedDataTupleParserFactory(valueParsers, ' '));
        RecordDescriptor scriptDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        String filePath = PATH_ACTUAL + SEPARATOR + "etsAssignScriptWrite.out";
        File outFile = new File(filePath);
        SinkWriterRuntimeFactory writer = new SinkWriterRuntimeFactory(new int[] { 0, 1 },
                new IPrinterFactory[] { IntegerPrinterFactory.INSTANCE, IntegerPrinterFactory.INSTANCE }, outFile,
                PrinterBasedWriterFactory.INSTANCE, scriptDesc);

        AlgebricksMetaOperatorDescriptor algebricksOp = new AlgebricksMetaOperatorDescriptor(spec, 0, 0,
                new IPushRuntimeFactory[] { ets, assign, script, writer },
                new RecordDescriptor[] { etsDesc, assignDesc, scriptDesc, null });

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, algebricksOp,
                new String[] { AlgebricksHyracksIntegrationUtil.NC1_ID });

        spec.addRoot(algebricksOp);
        AlgebricksHyracksIntegrationUtil.runJob(spec);

        StringBuilder buf = new StringBuilder();
        readFileToString(outFile, buf);
        Assert.assertEquals("400; 3", buf.toString());
        outFile.delete();
    }

    @Test
    public void scanReplicateWrite() throws Exception {
        final int outputArity = 2;

        JobSpecification spec = new JobSpecification(FRAME_SIZE);

        String inputFileName = "data" + File.separator + "tpch0.001" + File.separator + "customer.tbl";

        FileSplit[] inputSplits =
                new FileSplit[] { new ManagedFileSplit(AlgebricksHyracksIntegrationUtil.NC1_ID, inputFileName) };

        DelimitedDataTupleParserFactory stringParser = new DelimitedDataTupleParserFactory(
                new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE }, '\u0000');
        RecordDescriptor stringRec =
                new RecordDescriptor(new ISerializerDeserializer[] { new UTF8StringSerializerDeserializer(), });

        FileScanOperatorDescriptor scanOp = new FileScanOperatorDescriptor(spec,
                new ConstantFileSplitProvider(inputSplits), stringParser, stringRec);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, scanOp,
                new String[] { AlgebricksHyracksIntegrationUtil.NC1_ID });

        ReplicateOperatorDescriptor replicateOp = new ReplicateOperatorDescriptor(spec, stringRec, outputArity);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, replicateOp,
                new String[] { AlgebricksHyracksIntegrationUtil.NC1_ID });

        IOperatorDescriptor outputOp[] = new IOperatorDescriptor[outputArity];
        File[] outputFile = new File[outputArity];
        for (int i = 0; i < outputArity; i++) {
            FileSplit fileSplit = createFile(AlgebricksHyracksIntegrationUtil.nc1);
            outputFile[i] = fileSplit.getFile(AlgebricksHyracksIntegrationUtil.nc1.getIoManager());
            outputOp[i] = new LineFileWriteOperatorDescriptor(spec, new FileSplit[] { fileSplit });
            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, outputOp[i],
                    new String[] { AlgebricksHyracksIntegrationUtil.NC1_ID });
        }

        spec.connect(new OneToOneConnectorDescriptor(spec), scanOp, 0, replicateOp, 0);
        for (int i = 0; i < outputArity; i++) {
            spec.connect(new OneToOneConnectorDescriptor(spec), replicateOp, i, outputOp[i], 0);
        }

        for (int i = 0; i < outputArity; i++) {
            spec.addRoot(outputOp[i]);
        }
        AlgebricksHyracksIntegrationUtil.runJob(spec);

        for (int i = 0; i < outputArity; i++) {
            compareFiles("data" + File.separator + "device0" + File.separator + inputFileName,
                    outputFile[i].getAbsolutePath());
        }
    }

    public FileSplit createFile(NodeControllerService ncs) throws IOException {
        String fileName = "f" + aInteger.getAndIncrement() + ".tmp";
        FileReference fileRef = ncs.getIoManager().getFileReference(0, fileName);
        FileUtils.deleteQuietly(fileRef.getFile());
        fileRef.getFile().createNewFile();
        return new ManagedFileSplit(ncs.getId(), fileName);
    }

    @Test
    public void scanSplitWrite() throws Exception {
        final int outputArity = 2;

        JobSpecification spec = new JobSpecification(FRAME_SIZE);

        String inputFileName[] = { "data" + File.separator + "simple" + File.separator + "int-string-part1.tbl",
                "data" + File.separator + "simple" + File.separator + "int-string-part1-split-0.tbl",
                "data" + File.separator + "simple" + File.separator + "int-string-part1-split-1.tbl" };
        File[] inputFiles = new File[inputFileName.length];
        for (int i = 0; i < inputFileName.length; i++) {
            inputFiles[i] = new File(inputFileName[i]);
        }
        File[] outputFile = new File[outputArity];
        FileSplit[] outputFileSplit = new FileSplit[outputArity];
        for (int i = 0; i < outputArity; i++) {
            outputFileSplit[i] = createFile(AlgebricksHyracksIntegrationUtil.nc1);
            outputFile[i] = outputFileSplit[i].getFile(AlgebricksHyracksIntegrationUtil.nc1.getIoManager());
        }

        FileSplit[] inputSplits =
                new FileSplit[] { new ManagedFileSplit(AlgebricksHyracksIntegrationUtil.NC1_ID, inputFileName[0]) };
        IFileSplitProvider intSplitProvider = new ConstantFileSplitProvider(inputSplits);

        RecordDescriptor scannerDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() });

        IValueParserFactory[] valueParsers =
                new IValueParserFactory[] { IntegerParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE };

        FileScanOperatorDescriptor intScanner = new FileScanOperatorDescriptor(spec, intSplitProvider,
                new DelimitedDataTupleParserFactory(valueParsers, '|'), scannerDesc);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, intScanner, DEFAULT_NODES);

        SplitOperatorDescriptor splitOp = new SplitOperatorDescriptor(spec, scannerDesc, outputArity,
                new TupleFieldEvaluatorFactory(0), BinaryIntegerInspectorImpl.FACTORY);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, splitOp,
                new String[] { AlgebricksHyracksIntegrationUtil.NC1_ID });

        IOperatorDescriptor outputOp[] = new IOperatorDescriptor[outputFile.length];
        for (int i = 0; i < outputArity; i++) {
            outputOp[i] = new LineFileWriteOperatorDescriptor(spec, new FileSplit[] { outputFileSplit[i] });
            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, outputOp[i],
                    new String[] { AlgebricksHyracksIntegrationUtil.NC1_ID });
        }

        spec.connect(new OneToOneConnectorDescriptor(spec), intScanner, 0, splitOp, 0);
        for (int i = 0; i < outputArity; i++) {
            spec.connect(new OneToOneConnectorDescriptor(spec), splitOp, i, outputOp[i], 0);
        }

        for (int i = 0; i < outputArity; i++) {
            spec.addRoot(outputOp[i]);
        }
        AlgebricksHyracksIntegrationUtil.runJob(spec);

        for (int i = 0; i < outputArity; i++) {
            compareFiles("data" + File.separator + "device0" + File.separator + inputFileName[i + 1],
                    outputFile[i].getAbsolutePath());
        }
    }

    @Test
    public void scanSwitchWrite() throws Exception {
        final int outputArity = 3;

        JobSpecification spec = new JobSpecification(FRAME_SIZE);

        String inputFileName[] = { "data" + File.separator + "simple" + File.separator + "int-string-part2.tbl",
                "data" + File.separator + "simple" + File.separator + "int-string-part2-switch-0.tbl",
                "data" + File.separator + "simple" + File.separator + "int-string-part2-switch-1.tbl",
                "data" + File.separator + "simple" + File.separator + "int-string-part2-switch-2.tbl" };
        File[] inputFiles = new File[inputFileName.length];
        for (int i = 0; i < inputFileName.length; i++) {
            inputFiles[i] = new File(inputFileName[i]);
        }
        File[] outputFile = new File[outputArity];
        FileSplit[] outputFileSplit = new FileSplit[outputArity];
        for (int i = 0; i < outputArity; i++) {
            outputFileSplit[i] = createFile(AlgebricksHyracksIntegrationUtil.nc1);
            outputFile[i] = outputFileSplit[i].getFile(AlgebricksHyracksIntegrationUtil.nc1.getIoManager());
        }

        FileSplit[] inputSplits =
                new FileSplit[] { new ManagedFileSplit(AlgebricksHyracksIntegrationUtil.NC1_ID, inputFileName[0]) };
        IFileSplitProvider intSplitProvider = new ConstantFileSplitProvider(inputSplits);

        RecordDescriptor scannerDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() });

        IValueParserFactory[] valueParsers =
                new IValueParserFactory[] { IntegerParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE };

        FileScanOperatorDescriptor intScanner = new FileScanOperatorDescriptor(spec, intSplitProvider,
                new DelimitedDataTupleParserFactory(valueParsers, '|'), scannerDesc);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, intScanner, DEFAULT_NODES);

        HashMap<Integer, int[]> outputMapping = new HashMap<>();
        outputMapping.put(0, new int[] { 1, 2 });
        outputMapping.put(1, new int[] { 0, 2 });
        outputMapping.put(2, new int[] { 0, 1 });

        SwitchOperatorDescriptor switchOp = new SwitchOperatorDescriptor(spec, scannerDesc, outputArity,
                new TupleFieldEvaluatorFactory(0), BinaryIntegerInspectorImpl.FACTORY, outputMapping);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, switchOp,
                new String[] { AlgebricksHyracksIntegrationUtil.NC1_ID });

        IOperatorDescriptor outputOp[] = new IOperatorDescriptor[outputFile.length];
        for (int i = 0; i < outputArity; i++) {
            outputOp[i] = new LineFileWriteOperatorDescriptor(spec, new FileSplit[] { outputFileSplit[i] });
            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, outputOp[i],
                    new String[] { AlgebricksHyracksIntegrationUtil.NC1_ID });
        }

        spec.connect(new OneToOneConnectorDescriptor(spec), intScanner, 0, switchOp, 0);
        for (int i = 0; i < outputArity; i++) {
            spec.connect(new OneToOneConnectorDescriptor(spec), switchOp, i, outputOp[i], 0);
        }

        for (int i = 0; i < outputArity; i++) {
            spec.addRoot(outputOp[i]);
        }
        AlgebricksHyracksIntegrationUtil.runJob(spec);

        for (int i = 0; i < outputArity; i++) {
            compareFiles("data" + File.separator + "device0" + File.separator + inputFileName[i + 1],
                    outputFile[i].getAbsolutePath());
        }
    }

    @Test
    public void scanMicroSortWrite() throws Exception {
        JobSpecification spec = new JobSpecification(FRAME_SIZE);

        // the scanner
        FileSplit[] fileSplits = new FileSplit[1];
        fileSplits[0] = new ManagedFileSplit(AlgebricksHyracksIntegrationUtil.NC1_ID,
                "data" + File.separator + "tpch0.001" + File.separator + "nation.tbl");
        IFileSplitProvider splitProvider = new ConstantFileSplitProvider(fileSplits);
        RecordDescriptor scannerDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer(),
                IntegerSerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() });
        IValueParserFactory[] valueParsers = new IValueParserFactory[] { IntegerParserFactory.INSTANCE,
                UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE };
        FileScanOperatorDescriptor scanner = new FileScanOperatorDescriptor(spec, splitProvider,
                new DelimitedDataTupleParserFactory(valueParsers, '|'), scannerDesc);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, scanner,
                new String[] { AlgebricksHyracksIntegrationUtil.NC1_ID });

        // the algebricks op.
        MicroSortRuntimeFactory sort =
                new MicroSortRuntimeFactory(new int[] { 1 }, (INormalizedKeyComputerFactory) null,
                        new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE }, null, 50);
        RecordDescriptor sortDesc = scannerDesc;

        String fileName = "scanMicroSortWrite.out";
        String filePath = PATH_ACTUAL + SEPARATOR + fileName;
        String resultFilePath = PATH_EXPECTED + SEPARATOR + fileName;
        File outFile = new File(filePath);
        SinkWriterRuntimeFactory writer = new SinkWriterRuntimeFactory(new int[] { 0, 1, 2, 3 },
                new IPrinterFactory[] { IntegerPrinterFactory.INSTANCE, UTF8StringPrinterFactory.INSTANCE,
                        IntegerPrinterFactory.INSTANCE, UTF8StringPrinterFactory.INSTANCE },
                outFile, PrinterBasedWriterFactory.INSTANCE, sortDesc);

        AlgebricksMetaOperatorDescriptor algebricksOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 0,
                new IPushRuntimeFactory[] { sort, writer }, new RecordDescriptor[] { sortDesc, null });

        PartitionConstraintHelper.addPartitionCountConstraint(spec, algebricksOp, 1);

        spec.connect(new OneToOneConnectorDescriptor(spec), scanner, 0, algebricksOp, 0);

        spec.addRoot(algebricksOp);
        AlgebricksHyracksIntegrationUtil.runJob(spec);

        compareFiles(filePath, resultFilePath);
        outFile.delete();
    }

    @Test
    public void etsAssignSubplanProjectWrite() throws Exception {
        JobSpecification spec = new JobSpecification(FRAME_SIZE);
        IntegerConstantEvalFactory const1 = new IntegerConstantEvalFactory(400);
        IntegerConstantEvalFactory const2 = new IntegerConstantEvalFactory(3);

        EmptyTupleSourceRuntimeFactory ets = new EmptyTupleSourceRuntimeFactory();
        RecordDescriptor etsDesc = new RecordDescriptor(new ISerializerDeserializer[] {});

        AssignRuntimeFactory assign1 =
                new AssignRuntimeFactory(new int[] { 0 }, new IScalarEvaluatorFactory[] { const1 }, new int[] { 0 });
        RecordDescriptor assign1Desc =
                new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE });

        NestedTupleSourceRuntimeFactory nts = new NestedTupleSourceRuntimeFactory();

        AssignRuntimeFactory assign2 =
                new AssignRuntimeFactory(new int[] { 1 },
                        new IScalarEvaluatorFactory[] {
                                new IntegerAddEvalFactory(new TupleFieldEvaluatorFactory(0), const2) },
                        new int[] { 0, 1 });
        RecordDescriptor assign2Desc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        StreamProjectRuntimeFactory project1 = new StreamProjectRuntimeFactory(new int[] { 1 });
        RecordDescriptor project1Desc =
                new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE });

        AlgebricksPipeline pipeline = new AlgebricksPipeline(new IPushRuntimeFactory[] { nts, assign2, project1 },
                new RecordDescriptor[] { assign1Desc, assign2Desc, project1Desc }, null, null);

        SubplanRuntimeFactory subplan = new SubplanRuntimeFactory(Collections.singletonList(pipeline),
                new IMissingWriterFactory[] { NoopMissingWriterFactory.INSTANCE }, assign1Desc, null, null);

        RecordDescriptor subplanDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        StreamProjectRuntimeFactory project2 = new StreamProjectRuntimeFactory(new int[] { 1 });
        RecordDescriptor project2Desc =
                new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE });

        String filePath = PATH_ACTUAL + SEPARATOR + "etsAssignSubplanProjectWrite.out";
        File outFile = new File(filePath);
        SinkWriterRuntimeFactory writer =
                new SinkWriterRuntimeFactory(new int[] { 0 }, new IPrinterFactory[] { IntegerPrinterFactory.INSTANCE },
                        outFile, PrinterBasedWriterFactory.INSTANCE, project2Desc);

        AlgebricksMetaOperatorDescriptor algebricksOp = new AlgebricksMetaOperatorDescriptor(spec, 0, 0,
                new IPushRuntimeFactory[] { ets, assign1, subplan, project2, writer },
                new RecordDescriptor[] { etsDesc, assign1Desc, subplanDesc, project2Desc, null });

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, algebricksOp, DEFAULT_NODES);

        spec.addRoot(algebricksOp);
        AlgebricksHyracksIntegrationUtil.runJob(spec);

        StringBuilder buf = new StringBuilder();
        readFileToString(outFile, buf);
        Assert.assertEquals("403", buf.toString());
        outFile.delete();
    }

    @Test
    public void scanMicroSortGbySelectWrite() throws Exception {
        JobSpecification spec = new JobSpecification(FRAME_SIZE);

        // the scanner
        FileSplit[] fileSplits = new FileSplit[1];
        fileSplits[0] = new ManagedFileSplit(AlgebricksHyracksIntegrationUtil.NC1_ID,
                "data" + File.separator + "tpch0.001" + File.separator + "customer.tbl");
        IFileSplitProvider splitProvider = new ConstantFileSplitProvider(fileSplits);
        RecordDescriptor scannerDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), IntegerSerializerDeserializer.INSTANCE,
                new UTF8StringSerializerDeserializer(), FloatSerializerDeserializer.INSTANCE,
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() });
        IValueParserFactory[] valueParsers = new IValueParserFactory[] { IntegerParserFactory.INSTANCE,
                UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                UTF8StringParserFactory.INSTANCE, FloatParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                UTF8StringParserFactory.INSTANCE };
        FileScanOperatorDescriptor scanner = new FileScanOperatorDescriptor(spec, splitProvider,
                new DelimitedDataTupleParserFactory(valueParsers, '|'), scannerDesc);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, scanner,
                new String[] { AlgebricksHyracksIntegrationUtil.NC1_ID });

        // the sort (by nation id)
        RecordDescriptor sortDesc = scannerDesc;
        MicroSortRuntimeFactory sort =
                new MicroSortRuntimeFactory(new int[] { 3 }, (INormalizedKeyComputerFactory) null,
                        new IBinaryComparatorFactory[] { IntegerBinaryComparatorFactory.INSTANCE }, null, 50);

        // the group-by
        NestedTupleSourceRuntimeFactory nts = new NestedTupleSourceRuntimeFactory();
        RecordDescriptor ntsDesc = sortDesc;
        AggregateRuntimeFactory agg = new AggregateRuntimeFactory(
                new IAggregateEvaluatorFactory[] { new TupleCountAggregateFunctionFactory() });
        RecordDescriptor aggDesc =
                new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE });
        AlgebricksPipeline pipeline = new AlgebricksPipeline(new IPushRuntimeFactory[] { nts, agg },
                new RecordDescriptor[] { ntsDesc, aggDesc }, null, null);
        NestedPlansAccumulatingAggregatorFactory npaaf = new NestedPlansAccumulatingAggregatorFactory(
                new AlgebricksPipeline[] { pipeline }, new int[] { 3 }, new int[] {});
        RecordDescriptor gbyDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });
        MicroPreClusteredGroupRuntimeFactory gby = new MicroPreClusteredGroupRuntimeFactory(new int[] { 3 },
                new IBinaryComparatorFactory[] { IntegerBinaryComparatorFactory.INSTANCE }, npaaf, sortDesc, gbyDesc,
                null, -1);

        // the algebricks op.
        IScalarEvaluatorFactory cond =
                new IntegerEqualsEvalFactory(new IntegerConstantEvalFactory(3), new TupleFieldEvaluatorFactory(0)); // Canadian customers
        StreamSelectRuntimeFactory select = new StreamSelectRuntimeFactory(cond, new int[] { 1 },
                BinaryBooleanInspectorImpl.FACTORY, false, -1, null);
        RecordDescriptor selectDesc =
                new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE });

        String filePath = PATH_ACTUAL + SEPARATOR + "scanSortGbySelectWrite.out";
        File outFile = new File(filePath);
        SinkWriterRuntimeFactory writer =
                new SinkWriterRuntimeFactory(new int[] { 0 }, new IPrinterFactory[] { IntegerPrinterFactory.INSTANCE },
                        outFile, PrinterBasedWriterFactory.INSTANCE, selectDesc);

        AlgebricksMetaOperatorDescriptor algebricksOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 0,
                new IPushRuntimeFactory[] { sort, gby, select, writer },
                new RecordDescriptor[] { sortDesc, gbyDesc, selectDesc, null });

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, algebricksOp,
                new String[] { AlgebricksHyracksIntegrationUtil.NC1_ID });

        spec.connect(new OneToOneConnectorDescriptor(spec), scanner, 0, algebricksOp, 0);
        spec.addRoot(algebricksOp);

        AlgebricksHyracksIntegrationUtil.runJob(spec);
        StringBuilder buf = new StringBuilder();
        readFileToString(outFile, buf);
        Assert.assertEquals("9", buf.toString());
        outFile.delete();
    }

    private static void readFileToString(File file, StringBuilder buf) throws Exception {
        BufferedReader result = new BufferedReader(new FileReader(file));
        boolean first = true;
        while (true) {
            String s = result.readLine();
            if (s == null) {
                break;
            } else {
                if (!first) {
                    first = false;
                    buf.append('\n');
                }
                buf.append(s);
            }
        }
        result.close();
    }

    public void compareFiles(String fileNameA, String fileNameB) throws IOException {
        BufferedReader fileA = new BufferedReader(new FileReader(fileNameA));
        BufferedReader fileB = new BufferedReader(new FileReader(fileNameB));

        String lineA, lineB;
        while ((lineA = fileA.readLine()) != null) {
            lineB = fileB.readLine();
            Assert.assertEquals(lineA, lineB);
        }
        Assert.assertNull(fileB.readLine());
    }

    private static class MessageInjectorOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
        private static final long serialVersionUID = 1L;
        private final RecordDescriptor recordDescriptor;

        public MessageInjectorOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor recordDescriptor) {
            super(spec, 1, 1);
            this.recordDescriptor = recordDescriptor;
            this.outRecDescs[0] = recordDescriptor;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
                private MarkerMessageProducer markerMessageProducer;
                private FrameTupleAccessor frameTupleAccessor;

                @Override
                public void open() throws HyracksDataException {
                    frameTupleAccessor = new FrameTupleAccessor(recordDescriptor);
                    markerMessageProducer = new MarkerMessageProducer((byte) 1);
                    writer.open();
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    frameTupleAccessor.reset(buffer);
                    int tupleCount = frameTupleAccessor.getTupleCount();
                    for (int t = 0; t < tupleCount; t++) {
                        // After every tuple, we flush our single-tuple frame...
                        FrameTupleAppender frameTupleAppender = new FrameTupleAppender(new VSizeFrame(ctx));
                        FrameUtils.appendToWriter(writer, frameTupleAppender, frameTupleAccessor, t);
                        frameTupleAppender.flush(writer);

                        // ...and send a marker frame per partition.
                        for (short i = 0; i < nPartitions; i++) {
                            VSizeFrame outputFrame = new VSizeFrame(ctx);
                            outputFrame.getBuffer().clear();
                            markerMessageProducer.apply(outputFrame.getBuffer(), (short) partition, i, writer);
                        }
                    }
                }

                @Override
                public void fail() throws HyracksDataException {
                    writer.fail();
                }

                @Override
                public void close() throws HyracksDataException {
                    writer.close();
                }

                @Override
                public void flush() throws HyracksDataException {
                    writer.flush();
                }
            };
        }
    }

    private static class AddOneOnMarkerRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {
        private static final long serialVersionUID = 1L;
        private final RecordDescriptor recordDescriptor;

        public AddOneOnMarkerRuntimeFactory(int[] projectionList, RecordDescriptor recordDescriptor) {
            super(projectionList);
            this.recordDescriptor = recordDescriptor;
        }

        @Override
        public AbstractOneInputOneOutputPushRuntime createOneOutputPushRuntime(IHyracksTaskContext ctx) {
            final FrameTupleAccessor tupleAccessor = new FrameTupleAccessor(recordDescriptor);
            return new AbstractOneInputOneOutputPushRuntime() {
                private boolean isMarkerEncountered = false;

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    if (FrameHelper.isMessageFrame(buffer)) {
                        isMarkerEncountered = true;
                        writer.nextFrame(buffer);
                    } else if (!isMarkerEncountered) {
                        writer.nextFrame(buffer);
                    } else {
                        tupleAccessor.reset(buffer);
                        int n = tupleAccessor.getTupleCount();
                        for (int i = 0; i < n; i++) {
                            byte[] bytes = buffer.array();
                            int tupleStartOffset = tupleAccessor.getTupleStartOffset(i);
                            int fieldSlotsLength = tupleAccessor.getFieldSlotsLength();
                            int p = IntSerDeUtils.getInt(bytes, tupleStartOffset + fieldSlotsLength) + 1;
                            IntSerDeUtils.putInt(bytes, tupleStartOffset + fieldSlotsLength, p);
                        }
                        writer.nextFrame(buffer);
                    }
                }

                @Override
                public void close() throws HyracksDataException {
                    writer.close();
                }
            };
        }
    };
}
