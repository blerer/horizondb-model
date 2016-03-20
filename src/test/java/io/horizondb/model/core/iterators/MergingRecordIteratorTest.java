/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.horizondb.model.core.iterators;

import io.horizondb.io.Buffer;
import io.horizondb.io.buffers.Buffers;
import io.horizondb.model.core.DataBlock;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.ResourceIterator;
import io.horizondb.model.core.blocks.DataBlockBuilder;
import io.horizondb.model.core.records.BinaryTimeSeriesRecord;
import io.horizondb.model.core.util.TimeUtils;
import io.horizondb.model.schema.FieldType;
import io.horizondb.model.schema.RecordTypeDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.assertFalse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class MergingRecordIteratorTest {

    /**
     * The time reference.
     */
    private static long TIME_IN_MILLIS = TimeUtils.parseDateTime("2013-11-26 12:00:00.000");

    /**
     * The time reference.
     */
    private static long TIME_IN_NANOS = TimeUnit.MILLISECONDS.toNanos(TIME_IN_MILLIS);

    @Test
    public void testMergeWithOverlappingBlocks() throws Exception {

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        TimeSeriesDefinition def = TimeSeriesDefinition.newBuilder("test")
                                                       .timeUnit(TimeUnit.NANOSECONDS)
                                                       .addRecordType(recordTypeDefinition)
                                                       .blockSize(40)
                                                       .build();

        DataBlock block1 = new DataBlockBuilder(def).newRecord("exchangeState")
                                                    .setTimestampInNanos(0, TIME_IN_NANOS + 12000700)
                                                    .setTimestampInMillis(1, TIME_IN_MILLIS + 12)
                                                    .setByte(2, 3)
                                                    .newRecord("exchangeState")
                                                    .setTimestampInNanos(0, TIME_IN_NANOS + 13000900)
                                                    .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                    .setByte(2, 3)
                                                    .newRecord("exchangeState")
                                                    .setTimestampInNanos(0, TIME_IN_NANOS + 13004400)
                                                    .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                    .setByte(2, 1)
                                                    .build();

        DataBlock block2 = new DataBlockBuilder(def).newRecord("exchangeState")
                                                    .setTimestampInNanos(0, TIME_IN_NANOS + 14000000)
                                                    .setTimestampInMillis(1, TIME_IN_MILLIS + 14)
                                                    .setByte(2, 3)
                                                    .newRecord("exchangeState")
                                                    .setTimestampInNanos(0, TIME_IN_NANOS + 16000000)
                                                    .setTimestampInMillis(1, TIME_IN_MILLIS + 16)
                                                    .setByte(2, 1)
                                                    .build();

        ResourceIterator<BinaryTimeSeriesRecord> firstIterator = 
                new BinaryTimeSeriesRecordIterator(def, BlockIterators.iterator(def, serialize(block1, block2)));

        DataBlock block3 = new DataBlockBuilder(def).newRecord("exchangeState")
                                                    .setTimestampInNanos(0, TIME_IN_NANOS + 15000000)
                                                    .setTimestampInMillis(1, TIME_IN_MILLIS + 15)
                                                    .setByte(2, 3)
                                                    .newRecord("exchangeState")
                                                    .setTimestampInNanos(0, TIME_IN_NANOS + 17000000)
                                                    .setTimestampInMillis(1, TIME_IN_MILLIS + 17)
                                                    .setByte(2, 3)
                                                    .build();

        ResourceIterator<BinaryTimeSeriesRecord> secondIterator = 
                new BinaryTimeSeriesRecordIterator(def, BlockIterators.iterator(def, serialize(block3)));

        validateMergedOutput(def, firstIterator, secondIterator);

        firstIterator = new BinaryTimeSeriesRecordIterator(def, BlockIterators.iterator(def, serialize(block3)));
        secondIterator = new BinaryTimeSeriesRecordIterator(def, BlockIterators.iterator(def, serialize(block1, block2)));
        validateMergedOutput(def, firstIterator, secondIterator);
        firstIterator = new BinaryTimeSeriesRecordIterator(def, BlockIterators.iterator(def, serialize(block1, block3)));
        secondIterator = new BinaryTimeSeriesRecordIterator(def, BlockIterators.iterator(def, serialize(block2)));
        validateMergedOutput(def, firstIterator, secondIterator);
    }

    @Test
    public void testMergeWithoutOverlappingBlocks() throws Exception {

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        TimeSeriesDefinition def = TimeSeriesDefinition.newBuilder("test")
                                                       .timeUnit(TimeUnit.NANOSECONDS)
                                                       .addRecordType(recordTypeDefinition)
                                                       .blockSize(40)
                                                       .build();

        DataBlock block1 = new DataBlockBuilder(def).newRecord("exchangeState")
                                                    .setTimestampInNanos(0, TIME_IN_NANOS + 12000700)
                                                    .setTimestampInMillis(1, TIME_IN_MILLIS + 12)
                                                    .setByte(2, 3)
                                                    .newRecord("exchangeState")
                                                    .setTimestampInNanos(0, TIME_IN_NANOS + 13000900)
                                                    .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                    .setByte(2, 3)
                                                    .newRecord("exchangeState")
                                                    .setTimestampInNanos(0, TIME_IN_NANOS + 13004400)
                                                    .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                    .setByte(2, 1)
                                                    .build();

        DataBlock block2 = new DataBlockBuilder(def).newRecord("exchangeState")
                                                    .setTimestampInNanos(0, TIME_IN_NANOS + 14000000)
                                                    .setTimestampInMillis(1, TIME_IN_MILLIS + 14)
                                                    .setByte(2, 3)
                                                    .newRecord("exchangeState")
                                                    .setTimestampInNanos(0, TIME_IN_NANOS + 15000000)
                                                    .setTimestampInMillis(1, TIME_IN_MILLIS + 15)
                                                    .setByte(2, 3)
                                                    .build();

        ResourceIterator<BinaryTimeSeriesRecord> firstIterator = 
                new BinaryTimeSeriesRecordIterator(def, BlockIterators.iterator(def, serialize(block1, block2)));

        DataBlock block3 = new DataBlockBuilder(def).newRecord("exchangeState")
                                                    .setTimestampInNanos(0, TIME_IN_NANOS + 16000000)
                                                    .setTimestampInMillis(1, TIME_IN_MILLIS + 16)
                                                    .setByte(2, 1)
                                                    .newRecord("exchangeState")
                                                    .setTimestampInNanos(0, TIME_IN_NANOS + 17000000)
                                                    .setTimestampInMillis(1, TIME_IN_MILLIS + 17)
                                                    .setByte(2, 3)
                                                    .build();

        ResourceIterator<BinaryTimeSeriesRecord> secondIterator = 
                new BinaryTimeSeriesRecordIterator(def, BlockIterators.iterator(def, serialize(block3)));

        validateMergedOutput(def, firstIterator, secondIterator);

        firstIterator = new BinaryTimeSeriesRecordIterator(def, BlockIterators.iterator(def, serialize(block3)));
        secondIterator = new BinaryTimeSeriesRecordIterator(def, BlockIterators.iterator(def, serialize(block1, block2)));
        validateMergedOutput(def, firstIterator, secondIterator);

        firstIterator = new BinaryTimeSeriesRecordIterator(def, BlockIterators.iterator(def, serialize(block1, block3)));
        secondIterator = new BinaryTimeSeriesRecordIterator(def, BlockIterators.iterator(def, serialize(block2)));

        validateMergedOutput(def, firstIterator, secondIterator);
    }

    protected void validateMergedOutput(TimeSeriesDefinition def,
                                        ResourceIterator<? extends Record> left,
                                        ResourceIterator<? extends Record> right) throws IOException {

        try (MergingRecordIterator readIterator = new MergingRecordIterator(def, left, right)) {

            assertTrue(readIterator.hasNext());
            Record actual = readIterator.next();

            assertFalse(actual.isDelta());
            assertEquals(TIME_IN_NANOS + 12000700L, actual.getTimestampInNanos(0));
            assertEquals(TIME_IN_MILLIS + 12, actual.getTimestampInMillis(1));
            assertEquals(3, actual.getByte(2));

            assertTrue(readIterator.hasNext());
            actual = readIterator.next();

            assertTrue(actual.isDelta());
            assertEquals(1000200L, actual.getTimestampInNanos(0));
            assertEquals(1, actual.getTimestampInMillis(1));
            assertEquals(0, actual.getByte(2));

            assertTrue(readIterator.hasNext());
            actual = readIterator.next();

            assertTrue(actual.isDelta());
            assertEquals(3500L, actual.getTimestampInNanos(0));
            assertEquals(0, actual.getTimestampInMillis(1));
            assertEquals(-2, actual.getByte(2));

            assertTrue(readIterator.hasNext());
            actual = readIterator.next();

            assertTrue(actual.isDelta());
            assertEquals(995600, actual.getTimestampInNanos(0));
            assertEquals(1, actual.getTimestampInMillis(1));
            assertEquals(2, actual.getByte(2));

            assertTrue(readIterator.hasNext());
            actual = readIterator.next();

            assertTrue(actual.isDelta());
            assertEquals(1000000L, actual.getTimestampInNanos(0));
            assertEquals(1, actual.getTimestampInMillis(1));
            assertEquals(0, actual.getByte(2));

            assertTrue(readIterator.hasNext());
            actual = readIterator.next();

            assertTrue(actual.isDelta());
            assertEquals(1000000L, actual.getTimestampInNanos(0));
            assertEquals(1, actual.getTimestampInMillis(1));
            assertEquals(-2, actual.getByte(2));

            assertTrue(readIterator.hasNext());
            actual = readIterator.next();

            assertTrue(actual.isDelta());
            assertEquals(1000000L, actual.getTimestampInNanos(0));
            assertEquals(1, actual.getTimestampInMillis(1));
            assertEquals(2, actual.getByte(2));

            assertFalse(readIterator.hasNext());
        }
    }

    private static Buffer serialize(DataBlock... blocks) throws IOException {
        Buffer buffer = Buffers.allocate(computeSerializedSize(blocks));

        for (DataBlock block : blocks) {
            block.writeTo(buffer);
        }

        return buffer;
    }

    private static int computeSerializedSize(DataBlock... blocks) throws IOException {
        int size = 0;
        for (DataBlock block : blocks) {
            size += block.computeSerializedSize();
        }
        return size;
    }
}
