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
package org.apache.asterix.graphix.runtime.pointable;

import java.util.NoSuchElementException;

import org.apache.asterix.graphix.runtime.pointable.consumer.IPointableConsumer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A lean representation of a singly-linked list, where each item in the list only has a 4-byte length. We can only
 * access this SLL from start to finish, as we do not know the size of each item (nor the number of items) apriori.
 */
public class SinglyLinkedListPointable extends AbstractPointable {
    private static final Logger LOGGER = LogManager.getLogger();
    public static final int LIST_ITEM_LENGTH_SIZE = 4;

    private final IPointableConsumer listItemConsumer;
    private final VoidPointable listItemPointable;

    // This is a **stateful** pointable, whose action is dictated by the given list-item callback.
    private int absoluteCursor;
    private int workingIndex;

    public SinglyLinkedListPointable(IPointableConsumer listItemConsumer) {
        this.listItemPointable = new VoidPointable();
        this.listItemConsumer = listItemConsumer;
    }

    @Override
    public void set(byte[] bytes, int start, int length) {
        super.set(bytes, start, length);
        absoluteCursor = start;
        workingIndex = 0;
    }

    public boolean hasNext() {
        return absoluteCursor < (start + length);
    }

    public void next() throws HyracksDataException {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        // Determine the length of our working item.
        int absoluteLengthStart = absoluteCursor;
        int itemLength = IntegerPointable.getInteger(bytes, absoluteLengthStart);

        // Consume our list item.
        int absoluteItemStart = absoluteCursor + LIST_ITEM_LENGTH_SIZE;
        listItemPointable.set(bytes, absoluteItemStart, itemLength);
        listItemConsumer.accept(listItemPointable);
        LOGGER.trace("Item at index {} has been consumed.", workingIndex);

        // Advance our cursor.
        absoluteCursor += LIST_ITEM_LENGTH_SIZE + itemLength;
        workingIndex++;
    }
}
