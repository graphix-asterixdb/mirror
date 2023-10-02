/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.asterix.test.server;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.asterix.testframework.context.TestCaseContext;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Runs the runtime test cases under 'asterix-app/src/test/resources/runtimets'.
 */
@RunWith(Parameterized.class)
public class SqlppExecutionNCServiceIT extends AbstractExecutionIT {

    @Parameters(name = "SqlppExecutionNCServiceIT {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        Collection<Object[]> testArgs = buildTestsInXml("only_sqlpp.xml");
        if (testArgs.size() == 0) {
            testArgs = buildTestsInXml("testsuite_sqlpp.xml");
        }
        return testArgs;
    }

    @BeforeClass
    public static void setup() {
        testExecutor.setAvailableCharsets(Stream
                .of("UTF-8", "UTF-16", "UTF-16BE", "UTF-16LE", "UTF-32", "UTF-32BE", "UTF-32LE", "x-UTF-32BE-BOM",
                        "x-UTF-32LE-BOM", "x-UTF-16LE-BOM")
                .filter(Charset::isSupported).map(Charset::forName).collect(Collectors.toList()));
    }

    protected static Collection<Object[]> buildTestsInXml(String xmlfile) throws Exception {
        Collection<Object[]> testArgs = new ArrayList<Object[]>();
        TestCaseContext.Builder b = new TestCaseContext.Builder();
        for (TestCaseContext ctx : b.build(new File(PATH_BASE), xmlfile)) {
            testArgs.add(new Object[] { ctx });
        }
        return testArgs;

    }

    public SqlppExecutionNCServiceIT(TestCaseContext tcCtx) {
        super(tcCtx);
    }
}
