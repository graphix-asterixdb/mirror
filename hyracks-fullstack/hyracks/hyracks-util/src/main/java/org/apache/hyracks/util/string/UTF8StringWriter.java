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
package org.apache.hyracks.util.string;

import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.lang.ref.SoftReference;

public class UTF8StringWriter implements Serializable {

    private static final long serialVersionUID = 1L;
    transient SoftReference<byte[]> tempBytesRef;

    public final void writeUTF8(CharSequence str, DataOutput out) throws IOException {
        UTF8StringUtil.writeUTF8(str, out, this);
    }

    public final void writeUTF8(char[] buffer, int start, int length, DataOutput out) throws IOException {
        UTF8StringUtil.writeUTF8(buffer, start, length, out, this);
    }

}
