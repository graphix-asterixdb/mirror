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
package org.apache.asterix.external.input.stream;

import java.io.IOException;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.input.record.reader.hdfs.EmptyRecordReader;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class HDFSInputStream extends AsterixInputStream {

    private RecordReader<Object, Text> reader;
    private Text value = null;
    private Object key = null;
    private int currentSplitIndex = 0;
    private boolean read[];
    private InputFormat<?, Text> inputFormat;
    private InputSplit[] inputSplits;
    private String[] readSchedule;
    private String nodeName;
    private JobConf conf;
    private int pos = 0;

    @SuppressWarnings("unchecked")
    public HDFSInputStream(boolean read[], InputSplit[] inputSplits, String[] readSchedule, String nodeName,
            JobConf conf, Map<String, String> configuration) throws IOException, AsterixException {
        this.read = read;
        this.inputSplits = inputSplits;
        this.readSchedule = readSchedule;
        this.nodeName = nodeName;
        this.conf = conf;
        this.inputFormat = conf.getInputFormat();
        this.reader = new EmptyRecordReader<Object, Text>();
        nextInputSplit();
        this.value = new Text();
    }

    @Override
    public int read() throws IOException {
        if (value.getLength() < pos) {
            if (!readMore()) {
                return -1;
            }
        } else if (value.getLength() == pos) {
            pos++;
            return ExternalDataConstants.BYTE_LF;
        }
        return value.getBytes()[pos++];
    }

    private int readRecord(byte[] buffer, int offset, int len) {
        int actualLength = value.getLength() + 1;
        if ((actualLength - pos) > len) {
            //copy partial record
            System.arraycopy(value.getBytes(), pos, buffer, offset, len);
            pos += len;
            return len;
        } else {
            int numBytes = value.getLength() - pos;
            System.arraycopy(value.getBytes(), pos, buffer, offset, numBytes);
            buffer[offset + numBytes] = ExternalDataConstants.LF;
            pos += numBytes;
            numBytes++;
            return numBytes;
        }
    }

    @Override
    public int read(byte[] buffer, int offset, int len) throws IOException {
        if (value.getLength() > pos) {
            return readRecord(buffer, offset, len);
        }
        if (!readMore()) {
            return -1;
        }
        return readRecord(buffer, offset, len);
    }

    private boolean readMore() throws IOException {
        try {
            pos = 0;
            return HDFSInputStream.this.hasNext();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean stop() throws Exception {
        return false;
    }

    @Override
    public boolean handleException(Throwable th) {
        return false;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    private boolean hasNext() throws Exception {
        if (reader.next(key, value)) {
            return true;
        }
        while (nextInputSplit()) {
            if (reader.next(key, value)) {
                return true;
            }
        }
        return false;
    }

    private boolean nextInputSplit() throws IOException {
        for (; currentSplitIndex < inputSplits.length; currentSplitIndex++) {
            /**
             * read all the partitions scheduled to the current node
             */
            if (readSchedule[currentSplitIndex].equals(nodeName)) {
                /**
                 * pick an unread split to read synchronize among
                 * simultaneous partitions in the same machine
                 */
                synchronized (read) {
                    if (read[currentSplitIndex] == false) {
                        read[currentSplitIndex] = true;
                    } else {
                        continue;
                    }
                }
                reader.close();
                reader = getRecordReader(currentSplitIndex);
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private RecordReader<Object, Text> getRecordReader(int splitIndex) throws IOException {
        reader = (RecordReader<Object, Text>) inputFormat.getRecordReader(inputSplits[splitIndex], conf, Reporter.NULL);
        if (key == null) {
            key = reader.createKey();
            value = reader.createValue();
        }
        return reader;
    }
}
