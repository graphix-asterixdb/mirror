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

package org.apache.hyracks.examples.btree.client;

import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.io.ManagedFileSplit;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;

public class JobHelper {
    public static IFileSplitProvider createFileSplitProvider(String[] splitNCs, String btreeFileName) {
        FileSplit[] fileSplits = new FileSplit[splitNCs.length];
        for (int i = 0; i < splitNCs.length; ++i) {
            String fileName = btreeFileName + "." + splitNCs[i];
            fileSplits[i] = new ManagedFileSplit(splitNCs[i], fileName);
        }
        return new ConstantFileSplitProvider(fileSplits);
    }

    public static void createPartitionConstraint(JobSpecification spec, IOperatorDescriptor op, String[] splitNCs) {
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, op, splitNCs);
    }

    public static int[][] getPartitionsMap(int numPartitions) {
        int[][] map = new int[numPartitions][1];
        for (int i = 0; i < numPartitions; i++) {
            map[i] = new int[] { i };
        }
        return map;
    }
}
