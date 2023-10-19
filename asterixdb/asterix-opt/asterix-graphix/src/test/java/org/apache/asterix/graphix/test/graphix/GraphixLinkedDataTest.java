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
package org.apache.asterix.graphix.test.graphix;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.asterix.testframework.context.TestFileContext;
import org.apache.asterix.testframework.xml.TestCase;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.util.file.FileUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class GraphixLinkedDataTest {
    private static final String PATH_ACTUAL = FileUtil.joinPath("target", "ldtest");
    private static final String PATH_BASE = FileUtil.joinPath("src", "test", "resources", "linkeddatats");
    private static final String FILE_TEST_CONFIG = FileUtil.joinPath("src", "test", "resources", "test-1.conf");
    private static final String[] SETUP_FILES;

    // This test runs off the same graph. This is specified with the setup files below.
    static {
        SETUP_FILES = new String[3];
        for (int i = 0; i < 3; i++) {
            String filename = "social-network." + (i + 1) + ".setup.sqlpp";
            SETUP_FILES[i] = FileUtil.joinPath(PATH_BASE, "queries", "graphix", filename);
        }
    }

    private static final GraphixIntegrationUtil integrationUtil = new GraphixIntegrationUtil();
    private static final TestExecutor testExecutor = new TestExecutor() {
        private final List<Pair<File, Exception>> failedTests = new ArrayList<>();

        @Override
        protected void fail(boolean runDiagnostics, TestCaseContext testCaseCtx, TestCase.CompilationUnit cUnit,
                List<TestFileContext> testFileCtxs, ProcessBuilder pb, File testFile, MutableInt queryCount,
                Exception e) {
            // We will not stop on our first error, we want to run all files (easier to debug :-)).
            failedTests.add(new Pair<>(testFile, e));
            queryCount.increment();
        }

        @Override
        public void executeTest(String actualPath, TestCaseContext testCaseCtx, ProcessBuilder pb,
                boolean isDmlRecoveryTest) throws Exception {
            failedTests.clear();
            super.executeTest(actualPath, testCaseCtx, pb, isDmlRecoveryTest);
            if (!failedTests.isEmpty()) {
                StringBuilder sb = new StringBuilder();
                sb.append('\n');
                for (int i = 0; i < failedTests.size(); i++) {
                    Pair<File, Exception> failedTest = failedTests.get(i);
                    sb.append('\t').append(i + 1).append(") ");
                    sb.append(failedTest.first.toString()).append(":\n\t\t");
                    sb.append(failedTest.getSecond().toString().replaceAll("\n", "\n\t\t"));
                    sb.append('\n');
                }
                LOGGER.error(sb.toString());
                throw new Exception("Test has failed! " + failedTests.size() + " bad runs recorded.");
            }
        }
    };

    private final TestCaseContext tcCtx;

    public GraphixLinkedDataTest(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        //noinspection ResultOfMethodCallIgnored
        new File(PATH_ACTUAL).mkdirs();
        integrationUtil.init(true, FILE_TEST_CONFIG);
        for (String setupFile : SETUP_FILES) {
            String setupContent = FileUtils.readFileToString(new File(setupFile), StandardCharsets.UTF_8);
            testExecutor.executeSqlppUpdateOrDdl(setupContent, TestCaseContext.OutputFormat.LOSSLESS_JSON);
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        integrationUtil.deinit(true);
        File outdir = new File(PATH_ACTUAL);
        File[] files = outdir.listFiles();
        if (files == null || files.length == 0) {
            //noinspection ResultOfMethodCallIgnored
            outdir.delete();
        }
    }

    @Parameterized.Parameters(name = "LinkedDataTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        Collection<Object[]> testArgs = new ArrayList<>();
        TestCaseContext.Builder b = new TestCaseContext.Builder();
        for (TestCaseContext ctx : b.build(new File(PATH_BASE))) {
            testArgs.add(new Object[] { ctx });
        }
        return testArgs;
    }

    @Test
    public void test() throws Exception {
        testExecutor.executeTest(PATH_ACTUAL, tcCtx, null, false);
    }
}
