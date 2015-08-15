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
import io.horizondb.io.ReadableBuffer;
import io.horizondb.io.buffers.Buffers;
import io.horizondb.io.buffers.CompositeBuffer;
import io.horizondb.io.compression.CompressionType;
import io.horizondb.model.core.DataBlock;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.ResourceIterator;
import io.horizondb.model.core.blocks.DataBlockBuilder;
import io.horizondb.model.core.fields.TimestampField;
import io.horizondb.model.core.filters.Filters;
import io.horizondb.model.core.records.BinaryTimeSeriesRecord;
import io.horizondb.model.core.util.TimeUtils;
import io.horizondb.model.schema.FieldType;
import io.horizondb.model.schema.RecordTypeDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

import static io.horizondb.model.core.iterators.BlockIterators.compress;
import static io.horizondb.model.core.iterators.BlockIterators.iterator;
import static io.horizondb.model.schema.FieldType.NANOSECONDS_TIMESTAMP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Benjamin
 *
 */
public class BinaryTimeSeriesRecordIteratorTest {

    /**
     * The time reference.
     */
    private static long TIME_IN_MILLIS = TimeUtils.parseDateTime("2013-11-26 12:00:00.000");

    /**
     * The time reference.
     */
    private static long TIME_IN_NANOS = TimeUnit.MILLISECONDS.toNanos(TIME_IN_MILLIS);

    @Test
    public void testNextWithBlockHeader() throws Exception {

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        TimeSeriesDefinition def = TimeSeriesDefinition.newBuilder("test")
                                                       .timeUnit(TimeUnit.NANOSECONDS)
                                                       .addRecordType(recordTypeDefinition)
                                                       .build();

        DataBlock block = new DataBlockBuilder(def).newRecord("exchangeState")
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

        Buffer buffer = serialize(block);

        try (ResourceIterator<BinaryTimeSeriesRecord> readIterator = new BinaryTimeSeriesRecordIterator(def, buffer)) {

            assertTrue(readIterator.hasNext());
            Record actual = readIterator.next();
            
            assertFalse(actual.isDelta());
            assertEquals(TIME_IN_NANOS + 12000700L, actual.getTimestampInNanos(0));
            assertEquals(TIME_IN_MILLIS + 12, actual.getTimestampInMillis(1));
            assertEquals(3, actual.getByte(2));

            assertTrue(readIterator.hasNext());
            actual = readIterator.next();

            assertTrue(actual.isDelta());
            assertEquals(1000200, actual.getTimestampInNanos(0));
            assertEquals(1, actual.getTimestampInMillis(1));
            assertEquals(0, actual.getByte(2));

            assertTrue(readIterator.hasNext());
            actual = readIterator.next();

            assertTrue(actual.isDelta());
            assertEquals(3500, actual.getTimestampInNanos(0));
            assertEquals(0, actual.getTimestampInMillis(1));
            assertEquals(-2, actual.getByte(2));

            assertFalse(readIterator.hasNext());
        }
    }

    @Test
    public void testNextWithBlockHeaderAndFilter() throws Exception {

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();
        
        RecordTypeDefinition tradeDefinition = RecordTypeDefinition.newBuilder("trade")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("price", FieldType.DECIMAL)
                                                                        .build();

        TimeSeriesDefinition def = TimeSeriesDefinition.newBuilder("test")
                                                       .timeUnit(TimeUnit.NANOSECONDS)
                                                       .addRecordType(recordTypeDefinition)
                                                       .addRecordType(tradeDefinition)
                                                       .build();

        DataBlock block = new DataBlockBuilder(def).newRecord("exchangeState")
                                                   .setTimestampInNanos(0, TIME_IN_NANOS + 12000700)
                                                   .setTimestampInMillis(1, TIME_IN_MILLIS + 12)
                                                   .setByte(2, 3)
                                                   .newRecord("exchangeState")
                                                   .setTimestampInNanos(0, TIME_IN_NANOS + 13000900)
                                                   .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                   .setByte(2, 3)
                                                   .newRecord("trade")
                                                   .setTimestampInNanos(0, TIME_IN_NANOS + 13001000)
                                                   .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                   .setDouble(2, 10.0)
                                                   .newRecord("exchangeState")
                                                   .setTimestampInNanos(0, TIME_IN_NANOS + 13004400)
                                                   .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                   .setByte(2, 1)
                                                   .newRecord("trade")
                                                   .setTimestampInNanos(0, TIME_IN_NANOS + 13005000)
                                                   .setTimestampInMillis(1, TIME_IN_MILLIS + 13)
                                                   .setDouble(2, 15.0)
                                                   .build();

        Buffer buffer = serialize(block); 

        try (ResourceIterator<BinaryTimeSeriesRecord> readIterator = new BinaryTimeSeriesRecordIterator(def,
                                                                                                        buffer,
                                                                                                        TimestampField.ALL,
                                                                                                        Filters.eq("exchangeState",
                                                                                                                   false))) {

            assertTrue(readIterator.hasNext());
            Record actual = readIterator.next();
            
            assertFalse(actual.isDelta());
            assertEquals(TIME_IN_NANOS + 12000700L, actual.getTimestampInNanos(0));
            assertEquals(TIME_IN_MILLIS + 12, actual.getTimestampInMillis(1));
            assertEquals(3, actual.getByte(2));

            assertTrue(readIterator.hasNext());
            actual = readIterator.next();

            assertTrue(actual.isDelta());
            assertEquals(1000200, actual.getTimestampInNanos(0));
            assertEquals(1, actual.getTimestampInMillis(1));
            assertEquals(0, actual.getByte(2));

            assertTrue(readIterator.hasNext());
            actual = readIterator.next();

            assertTrue(actual.isDelta());
            assertEquals(3500, actual.getTimestampInNanos(0));
            assertEquals(0, actual.getTimestampInMillis(1));
            assertEquals(-2, actual.getByte(2));

            assertFalse(readIterator.hasNext());
        }
    }
    
    @Test
    public void testNextWithTwoBlockHeaders() throws Exception {

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        TimeSeriesDefinition def = TimeSeriesDefinition.newBuilder("test")
                                                       .timeUnit(TimeUnit.NANOSECONDS)
                                                       .addRecordType(recordTypeDefinition)
                                                       .build();

        DataBlock firstBlock = new DataBlockBuilder(def).newRecord("exchangeState")
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

        DataBlock secondBlock = new DataBlockBuilder(def).newRecord("exchangeState")
                                                         .setTimestampInNanos(0, TIME_IN_NANOS + 14000000)
                                                         .setTimestampInMillis(1, TIME_IN_MILLIS + 14)
                                                         .setByte(2, 1)
                                                         .build();

        Buffer buffer = serialize(firstBlock, secondBlock); 

        try (ResourceIterator<BinaryTimeSeriesRecord> readIterator = new BinaryTimeSeriesRecordIterator(def, buffer)) {

            assertTrue(readIterator.hasNext());
            Record actual = readIterator.next();
            
            assertFalse(actual.isDelta());
            assertEquals(TIME_IN_NANOS + 12000700L, actual.getTimestampInNanos(0));
            assertEquals(TIME_IN_MILLIS + 12, actual.getTimestampInMillis(1));
            assertEquals(3, actual.getByte(2));

            assertTrue(readIterator.hasNext());
            actual = readIterator.next();

            assertTrue(actual.isDelta());
            assertEquals(1000200, actual.getTimestampInNanos(0));
            assertEquals(1, actual.getTimestampInMillis(1));
            assertEquals(0, actual.getByte(2));

            assertTrue(readIterator.hasNext());
            actual = readIterator.next();

            assertTrue(actual.isDelta());
            assertEquals(3500, actual.getTimestampInNanos(0));
            assertEquals(0, actual.getTimestampInMillis(1));
            assertEquals(-2, actual.getByte(2));

            assertTrue(readIterator.hasNext());
            actual = readIterator.next();
            
            assertFalse(actual.isDelta());
            assertEquals(TIME_IN_NANOS + 14000000L, actual.getTimestampInNanos(0));
            assertEquals(TIME_IN_MILLIS + 14, actual.getTimestampInMillis(1));
            assertEquals(1, actual.getByte(2));
            
            assertFalse(readIterator.hasNext());
        }
    }
    
    @Test
    public void testNextWithTwoBlockHeadersAndFirstOneNotInRangeSet() throws Exception {

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        TimeSeriesDefinition def = TimeSeriesDefinition.newBuilder("test")
                                                       .timeUnit(TimeUnit.NANOSECONDS)
                                                       .addRecordType(recordTypeDefinition)
                                                       .build();

        DataBlock firstBlock = new DataBlockBuilder(def).newRecord("exchangeState")
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

        DataBlock secondBlock = new DataBlockBuilder(def).newRecord("exchangeState")
                                                                       .setTimestampInNanos(0, TIME_IN_NANOS + 14000000)
                                                                       .setTimestampInMillis(1, TIME_IN_MILLIS + 14)
                                                                       .setByte(2, 1)
                                                                       .build();

        Buffer buffer = Buffers.allocate(firstBlock.computeSerializedSize() 
                                         + secondBlock.computeSerializedSize());

        firstBlock.writeTo(buffer);
        secondBlock.writeTo(buffer);
        
        Field from = NANOSECONDS_TIMESTAMP.newField().setTimestampInNanos(TIME_IN_NANOS + 13005000);
        Field to = NANOSECONDS_TIMESTAMP.newField().setTimestampInNanos(TIME_IN_NANOS + 15000000);
        
        RangeSet<Field> rangeSet = ImmutableRangeSet.of(Range.closed(from, to)); 
        
        try (ResourceIterator<BinaryTimeSeriesRecord> readIterator = new BinaryTimeSeriesRecordIterator(def, buffer, rangeSet)) {

            assertTrue(readIterator.hasNext());
            Record actual = readIterator.next();
                        
            assertFalse(actual.isDelta());
            assertEquals(TIME_IN_NANOS + 14000000L, actual.getTimestampInNanos(0));
            assertEquals(TIME_IN_MILLIS + 14, actual.getTimestampInMillis(1));
            assertEquals(1, actual.getByte(2));
            
            assertFalse(readIterator.hasNext());
        }
    }
    
    @Test
    public void testNextWithTwoBlockHeadersAndSecondOneNotInRangeSet() throws Exception {

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        TimeSeriesDefinition def = TimeSeriesDefinition.newBuilder("test")
                                                       .timeUnit(TimeUnit.NANOSECONDS)
                                                       .addRecordType(recordTypeDefinition)
                                                       .build();

        DataBlock firstBlock = new DataBlockBuilder(def).newRecord("exchangeState")
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

        DataBlock secondBlock = new DataBlockBuilder(def).newRecord("exchangeState")
                                                         .setTimestampInNanos(0, TIME_IN_NANOS + 14000000)
                                                         .setTimestampInMillis(1, TIME_IN_MILLIS + 14)
                                                         .setByte(2, 1)
                                                         .build();

        Buffer buffer = serialize(firstBlock, secondBlock); 
        
        Field from = NANOSECONDS_TIMESTAMP.newField().setTimestampInNanos(TIME_IN_NANOS + 13000900);
        Field to = NANOSECONDS_TIMESTAMP.newField().setTimestampInNanos(TIME_IN_NANOS + 13005000);
        
        RangeSet<Field> rangeSet = ImmutableRangeSet.of(Range.closed(from, to)); 
        
        try (ResourceIterator<BinaryTimeSeriesRecord> readIterator = new BinaryTimeSeriesRecordIterator(def, buffer, rangeSet)) {

            assertTrue(readIterator.hasNext());
            Record actual = readIterator.next();
            
            assertFalse(actual.isDelta());
            assertEquals(TIME_IN_NANOS + 12000700L, actual.getTimestampInNanos(0));
            assertEquals(TIME_IN_MILLIS + 12, actual.getTimestampInMillis(1));
            assertEquals(3, actual.getByte(2));

            assertTrue(readIterator.hasNext());
            actual = readIterator.next();

            assertTrue(actual.isDelta());
            assertEquals(1000200, actual.getTimestampInNanos(0));
            assertEquals(1, actual.getTimestampInMillis(1));
            assertEquals(0, actual.getByte(2));

            assertTrue(readIterator.hasNext());
            actual = readIterator.next();

            assertTrue(actual.isDelta());
            assertEquals(3500, actual.getTimestampInNanos(0));
            assertEquals(0, actual.getTimestampInMillis(1));
            assertEquals(-2, actual.getByte(2));
            
            assertFalse(readIterator.hasNext());
        }
    }
    
    @Test
    public void testNextWithTwoBlockHeadersAndNoneInRangeSet() throws Exception {

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        TimeSeriesDefinition def = TimeSeriesDefinition.newBuilder("test")
                                                       .timeUnit(TimeUnit.NANOSECONDS)
                                                       .addRecordType(recordTypeDefinition)
                                                       .build();

        DataBlock firstBlock = new DataBlockBuilder(def).newRecord("exchangeState")
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

        DataBlock secondBlock = new DataBlockBuilder(def).newRecord("exchangeState")
                                                                       .setTimestampInNanos(0, TIME_IN_NANOS + 14000000)
                                                                       .setTimestampInMillis(1, TIME_IN_MILLIS + 14)
                                                                       .setByte(2, 1)
                                                                       .build();

        Buffer buffer = serialize(firstBlock, secondBlock); 
        
        Field from = NANOSECONDS_TIMESTAMP.newField().setTimestampInNanos(TIME_IN_NANOS + 13004500);
        Field to = NANOSECONDS_TIMESTAMP.newField().setTimestampInNanos(TIME_IN_NANOS + 13005000);
        
        RangeSet<Field> rangeSet = ImmutableRangeSet.of(Range.closed(from, to)); 
        
        try (ResourceIterator<BinaryTimeSeriesRecord> readIterator = new BinaryTimeSeriesRecordIterator(def, buffer, rangeSet)) {
            
            assertFalse(readIterator.hasNext());
        }
    }
    
    @Test
    public void testNextWithTwoBlockCompressedWithLZ4() throws Exception {

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        TimeSeriesDefinition def = TimeSeriesDefinition.newBuilder("test")
                                                       .timeUnit(TimeUnit.NANOSECONDS)
                                                       .addRecordType(recordTypeDefinition)
                                                       .build();

        DataBlock firstBlock = new DataBlockBuilder(def).newRecord("exchangeState")
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
        
        DataBlock secondBlock = new DataBlockBuilder(def).newRecord("exchangeState")
                                                         .setTimestampInNanos(0, TIME_IN_NANOS + 14000000)
                                                         .setTimestampInMillis(1, TIME_IN_MILLIS + 14)
                                                         .setByte(2, 1)
                                                         .build();

        
        ResourceIterator<DataBlock> iterator = iterator(def, serialize(compress(CompressionType.LZ4, iterator(firstBlock, secondBlock))));
        
        while (iterator.hasNext()) {
            iterator.next();
        }
        
//        try (ResourceIterator<BinaryTimeSeriesRecord> readIterator = new BinaryTimeSeriesRecordIterator(def, buffer)) {
//
//            assertTrue(readIterator.hasNext());
//            Record actual = readIterator.next();
//            
//            assertFalse(actual.isDelta());
//            assertEquals(TIME_IN_NANOS + 12000700L, actual.getTimestampInNanos(0));
//            assertEquals(TIME_IN_MILLIS + 12, actual.getTimestampInMillis(1));
//            assertEquals(3, actual.getByte(2));
//
//            assertTrue(readIterator.hasNext());
//            actual = readIterator.next();
//
//            assertTrue(actual.isDelta());
//            assertEquals(1000200, actual.getTimestampInNanos(0));
//            assertEquals(1, actual.getTimestampInMillis(1));
//            assertEquals(0, actual.getByte(2));
//
//            assertTrue(readIterator.hasNext());
//            actual = readIterator.next();
//
//            assertTrue(actual.isDelta());
//            assertEquals(3500, actual.getTimestampInNanos(0));
//            assertEquals(0, actual.getTimestampInMillis(1));
//            assertEquals(-2, actual.getByte(2));
//
//            assertTrue(readIterator.hasNext());
//            actual = readIterator.next();
//            
//            assertFalse(actual.isDelta());
//            assertEquals(TIME_IN_NANOS + 14000000L, actual.getTimestampInNanos(0));
//            assertEquals(TIME_IN_MILLIS + 14, actual.getTimestampInMillis(1));
//            assertEquals(1, actual.getByte(2));
//            
//            assertFalse(readIterator.hasNext());
//        }
    }
    
    @Test
    public void testHasNextWithEmptyStream() throws Exception {

        RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                        .addField("timestampInMillis",
                                                                                  FieldType.MILLISECONDS_TIMESTAMP)
                                                                        .addField("status", FieldType.BYTE)
                                                                        .build();

        TimeSeriesDefinition def = TimeSeriesDefinition.newBuilder("test")
                                                       .timeUnit(TimeUnit.NANOSECONDS)
                                                       .addRecordType(recordTypeDefinition)
                                                       .build();
        
        try (ResourceIterator<BinaryTimeSeriesRecord> readIterator = new BinaryTimeSeriesRecordIterator(def, Buffers.EMPTY_BUFFER)) {

            assertFalse(readIterator.hasNext());
        }
    }

    private static ReadableBuffer serialize(ResourceIterator<DataBlock> iterator) throws IOException {

        CompositeBuffer composite = new CompositeBuffer();

        while (iterator.hasNext()) {
            DataBlock block = iterator.next();
            Buffer buffer = Buffers.allocate(block.computeSerializedSize());
            block.writeTo(buffer);
            composite.addBytes(buffer);
        }

        return composite;
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
