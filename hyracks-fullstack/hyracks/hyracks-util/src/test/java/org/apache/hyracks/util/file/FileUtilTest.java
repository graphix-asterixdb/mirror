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
package org.apache.hyracks.util.file;

import static org.apache.hyracks.util.file.FileUtil.joinPath;

import org.junit.Assert;
import org.junit.Test;

public class FileUtilTest {

    @Test
    public void testUnixPaths() {
        Assert.assertEquals("/tmp/far/baz/lala", joinPath('/', "/tmp/", "/", "far", "baz/", "///lala"));
        Assert.assertEquals("tmp/far/bar", joinPath('/', "tmp//far", "bar"));
        Assert.assertEquals("/tmp/far/bar", joinPath('/', "/tmp/", "far", "bar/"));
        Assert.assertEquals("/tmp/far/bar", joinPath('/', "/" + "/tmp/", "far", "bar/"));
    }

    @Test
    public void testWindozePaths() {
        Assert.assertEquals("C:\\temp\\far\\baz\\lala",
                joinPath('\\', "C:\\", "temp\\", "\\far", "\\baz\\", "\\", "\\", "\\lala"));
        Assert.assertEquals("\\\\myserver\\tmp\\far\\baz\\lala",
                joinPath('\\', "\\\\myserver\\tmp\\\\far\\baz\\\\\\\\lala"));
        Assert.assertEquals("C:\\temp\\far\\baz\\lala", joinPath('\\', "C:\\temp\\\\far\\baz\\\\\\\\lala\\"));
    }

    @Test
    public void testCanonicalize() {
        Assert.assertEquals("bat.txt", FileUtil.canonicalize("foo/../bat.txt"));
        Assert.assertEquals("bat.txt", FileUtil.canonicalize("foo/bar/../../bat.txt"));
        Assert.assertEquals("foo/", FileUtil.canonicalize("foo/bar/../"));
        Assert.assertEquals("foo", FileUtil.canonicalize("foo/bar/.."));
        Assert.assertEquals("../bat.txt", FileUtil.canonicalize("../bat.txt"));
        Assert.assertEquals("/bat.txt", FileUtil.canonicalize("/foo/bar/../../bat.txt"));
        Assert.assertEquals("/bar/bat.txt", FileUtil.canonicalize("/foo/../bar/bat.txt"));
    }

    @Test
    public void testCanonicalizeWindoze() {
        Assert.assertEquals("bat.txt", FileUtil.canonicalize("foo\\..\\bat.txt"));
        Assert.assertEquals("bat.txt", FileUtil.canonicalize("foo\\bar\\..\\..\\bat.txt"));
        Assert.assertEquals("foo\\", FileUtil.canonicalize("foo\\bar\\..\\"));
        Assert.assertEquals("foo", FileUtil.canonicalize("foo\\bar\\.."));
        Assert.assertEquals("..\\bat.txt", FileUtil.canonicalize("..\\bat.txt"));
        Assert.assertEquals("\\bat.txt", FileUtil.canonicalize("\\foo\\bar\\..\\..\\bat.txt"));
        Assert.assertEquals("\\bar\\bat.txt", FileUtil.canonicalize("\\foo\\..\\bar\\bat.txt"));
        Assert.assertEquals("C:\\bar\\bat.txt", FileUtil.canonicalize("C:\\foo\\..\\bar\\bat.txt"));
    }
}
