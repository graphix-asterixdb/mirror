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

package org.apache.asterix.metadata.entitytupletranslators;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.NodeGroup;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.AUnorderedList;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a NodeGroup metadata entity to an ITupleReference and vice versa.
 */
public class NodeGroupTupleTranslator extends AbstractTupleTranslator<NodeGroup> {

    // Payload field containing serialized NodeGroup.
    private static final int NODEGROUP_PAYLOAD_TUPLE_FIELD_INDEX = 1;

    protected UnorderedListBuilder listBuilder;
    protected ArrayBackedValueStorage itemValue;

    protected NodeGroupTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.NODEGROUP_DATASET, NODEGROUP_PAYLOAD_TUPLE_FIELD_INDEX);
        if (getTuple) {
            listBuilder = new UnorderedListBuilder();
            itemValue = new ArrayBackedValueStorage();
        }
    }

    @Override
    protected NodeGroup createMetadataEntityFromARecord(ARecord nodeGroupRecord) {
        String gpName =
                ((AString) nodeGroupRecord.getValueByPos(MetadataRecordTypes.NODEGROUP_ARECORD_GROUPNAME_FIELD_INDEX))
                        .getStringValue();
        IACursor cursor = ((AUnorderedList) nodeGroupRecord
                .getValueByPos(MetadataRecordTypes.NODEGROUP_ARECORD_NODENAMES_FIELD_INDEX)).getCursor();
        List<String> nodeNames = new ArrayList<>();
        while (cursor.next()) {
            nodeNames.add(((AString) cursor.get()).getStringValue());
        }
        return new NodeGroup(gpName, nodeNames);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(NodeGroup instance) throws HyracksDataException {
        // write the key in the first field of the tuple
        tupleBuilder.reset();
        aString.setValue(instance.getNodeGroupName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // write the payload in the second field of the tuple
        recordBuilder.reset(MetadataRecordTypes.NODEGROUP_RECORDTYPE);
        // write field 0
        fieldValue.reset();
        aString.setValue(instance.getNodeGroupName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.NODEGROUP_ARECORD_GROUPNAME_FIELD_INDEX, fieldValue);

        // write field 1
        listBuilder.reset((AUnorderedListType) MetadataRecordTypes.NODEGROUP_RECORDTYPE
                .getFieldTypes()[MetadataRecordTypes.NODEGROUP_ARECORD_NODENAMES_FIELD_INDEX]);
        List<String> nodeNames = instance.getNodeNames();
        for (String nodeName : nodeNames) {
            itemValue.reset();
            aString.setValue(nodeName);
            stringSerde.serialize(aString, itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(MetadataRecordTypes.NODEGROUP_ARECORD_NODENAMES_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(Calendar.getInstance().getTime().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.NODEGROUP_ARECORD_TIMESTAMP_FIELD_INDEX, fieldValue);

        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }
}
