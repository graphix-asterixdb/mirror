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

import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.io.ManagedFileSplit;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.result.ResultSetId;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.util.file.FileUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.paukov.combinatorics3.Generator;

import com.fasterxml.jackson.databind.ObjectMapper;

public class SingleSourceReachabilityTest {
    private static final Logger LOGGER = LogManager.getLogger();

    @SuppressWarnings("FieldCanBeLocal")
    private static final boolean DEBUG = true;

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void test() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        // We are going to run the same test for several variations of clusters.
        int[] nodeControllerCounts = new int[] { 1, 2, 4, 8, 16, 32 };
        Map<Integer, Set<Long>> resultEndpoints = new HashMap<>();
        for (int nodeControllerCount : nodeControllerCounts) {
            resultEndpoints.put(nodeControllerCount, new HashSet<>());
        }

        // Execute our tests.
        for (int ncCount : nodeControllerCounts) {
            try (ISSReachabilityTestFixture testFixture = new SSRNNCTestFixture(ncCount)) {
                LOGGER.info(testFixture.getDescription());
                SSRDirectedTestExecutor testHarness = new SSRDirectedTestExecutor();
                Pair<JobId, ResultSetId> resultPair = testHarness.executeTest(testFixture);
                List<SSReachabilityTestResult.Path> results =
                        testHarness.readResults(resultPair.first, resultPair.second, testFixture);
                LOGGER.info("SSR{}NC test finished. {} results found.", ncCount, results.size());

                // We'll store each of endpoints. We should only get endpoints once.
                for (SSReachabilityTestResult.Path path : results) {
                    List<SSReachabilityTestResult.Vertex> vertices = path.Vertices;
                    int verticesLength = vertices.size();
                    long endpoint = vertices.get(verticesLength - 1).id;
                    Assert.assertTrue("Duplicate endpoint found!", resultEndpoints.get(ncCount).add(endpoint));
                }

                // Write our results to a file (if we are debugging).
                if (DEBUG) {
                    File logFile = new File(String.format("target/resultSet%dReachability.json", ncCount));
                    logFile.delete();
                    try (FileWriter fileWriter = new FileWriter(logFile)) {
                        logFile.createNewFile();
                        for (SSReachabilityTestResult.Path p : results) {
                            fileWriter.write(objectMapper.writeValueAsString(p) + '\n');
                        }
                    }
                }
            }
        }

        // All tests should yield the same results.
        Generator.combination(resultEndpoints.values()).simple(2).stream()
                .forEach(e -> Assert.assertEquals("Different endpoints!", e.get(0), e.get(1)));
        LOGGER.info("All NC runs have identical results!");
    }

    private static class SSRNNCTestFixture implements ISSReachabilityTestFixture {
        private static final long serialVersionUID = 1L;

        private final ClusterControllerService cc;
        private final NodeControllerService[] ncs;
        private final int n;

        @SuppressWarnings({ "ResultOfMethodCallIgnored", "resource" })
        public SSRNNCTestFixture(int n) throws Exception {
            CCConfig ccConfig = new CCConfig();
            ccConfig.setClientListenAddress("127.0.0.1");
            ccConfig.setClientListenPort(39000);
            ccConfig.setClusterListenAddress("127.0.0.1");
            ccConfig.setClusterListenPort(39001);
            ccConfig.setProfileDumpPeriod(10000);
            Random random = new Random(0);

            // Copy our data directory over.
            FileUtils.deleteQuietly(new File(FileUtil.joinPath("target", "data")));
            FileUtils.deleteQuietly(new File(FileUtil.joinPath("target", "cluster-controller")));
            FileUtils.copyDirectory(new File("data"), new File(FileUtil.joinPath("target", "data")));
            File ccDir = new File(FileUtil.joinPath("target", "cluster-controller"));
            ccDir.delete();
            ccDir.mkdirs();

            // We will split our CSV files into N parts. We start with Persons...
            String userDir = System.getProperty("user.dir");
            String destPath = FileUtil.joinPath(userDir, "target", "data");
            String sourcePath = FileUtil.joinPath(userDir, "target", "data", "social-network", "dynamic");
            try (LineIterator it = FileUtils.lineIterator(new File(FileUtil.joinPath(sourcePath, "Person",
                    "part-00000-90a0e6d8-dc80-4387-a10e-abd5af50e68d-c000.csv")))) {
                FileWriter[] personFileWriters = new FileWriter[n];
                for (int i = 0; i < n; i++) {
                    String workingPath = FileUtil.joinPath(destPath, "device" + (i + 1));
                    new File(workingPath).mkdirs();
                    File personsFile = new File(FileUtil.joinPath(workingPath, "person.csv"));
                    personsFile.createNewFile();
                    personFileWriters[i] = new FileWriter(personsFile);
                }
                while (it.hasNext()) {
                    int personsIndex = random.nextInt(n);
                    personFileWriters[personsIndex].write(it.nextLine());
                    personFileWriters[personsIndex].write('\n');
                }
                for (int i = 0; i < n; i++) {
                    personFileWriters[i].close();
                }
            }

            // ...and end with Knows.
            try (LineIterator it = FileUtils.lineIterator(new File(FileUtil.joinPath(sourcePath, "Person_knows_Person",
                    "part-00000-3ae0b4dc-39c7-432d-9e2f-96c1f0ff08dd-c000.csv")))) {
                FileWriter[] knowsFileWriters = new FileWriter[n];
                for (int i = 0; i < n; i++) {
                    String workingPath = FileUtil.joinPath(destPath, "device" + (i + 1));
                    File knowsFile = new File(FileUtil.joinPath(workingPath, "knows.csv"));
                    knowsFile.createNewFile();
                    knowsFileWriters[i] = new FileWriter(knowsFile);
                }
                while (it.hasNext()) {
                    int knowsIndex = random.nextInt(n);
                    knowsFileWriters[knowsIndex].write(it.nextLine());
                    knowsFileWriters[knowsIndex].write('\n');
                }
                for (int i = 0; i < n; i++) {
                    knowsFileWriters[i].close();
                }
            }

            // Start our CC.
            File ccRoot = File.createTempFile(SingleSourceReachabilityTest.class.getName(), ".data", ccDir);
            ccRoot.delete();
            ccRoot.mkdir();
            ccConfig.setRootDir(ccRoot.getAbsolutePath());
            this.cc = new ClusterControllerService(ccConfig);
            this.cc.start();

            // Create our NCs.
            this.n = n;
            this.ncs = new NodeControllerService[n];
            String basePath = FileUtil.joinPath(System.getProperty("user.dir"), "target", "data");
            for (int i = 0; i < n; i++) {
                NCConfig ncConfig = new NCConfig("nc" + (i + 1));
                ncConfig.setClusterAddress("localhost");
                ncConfig.setClusterPort(39001);
                ncConfig.setClusterListenAddress("127.0.0.1");
                ncConfig.setDataListenAddress("127.0.0.1");
                ncConfig.setResultListenAddress("127.0.0.1");
                File ncDir = new File(FileUtil.joinPath(basePath, "device" + (i + 1)));
                ncConfig.setIODevices(new String[] { ncDir.getPath() });
                this.ncs[i] = new NodeControllerService(ncConfig);
                this.ncs[i].start();
            }
        }

        @Override
        public String[] getNodeNames() {
            String[] nodeNames = new String[n];
            for (int i = 0; i < n; i++) {
                nodeNames[i] = "nc" + (i + 1);
            }
            return nodeNames;
        }

        @Override
        public FileSplit[] getPersonsFileSplits() {
            FileSplit[] fileSplits = new FileSplit[n];
            for (int i = 0; i < n; i++) {
                fileSplits[i] = new ManagedFileSplit("nc" + (i + 1), "person.csv");
            }
            return fileSplits;
        }

        @Override
        public FileSplit[] getKnowsFileSplits() {
            FileSplit[] fileSplits = new FileSplit[n];
            for (int i = 0; i < n; i++) {
                fileSplits[i] = new ManagedFileSplit("nc" + (i + 1), "knows.csv");
            }
            return fileSplits;
        }

        @Override
        public long getSourcePerson() {
            return 14;
        }

        public void close() throws Exception {
            for (NodeControllerService nc : ncs) {
                nc.stop();
            }
            cc.stop();
        }
    }
}
