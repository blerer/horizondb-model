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
import io.horizondb.io.compression.CompressionType;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.RecordIterator;
import io.horizondb.model.core.RecordListBuilder;
import io.horizondb.model.core.RecordUtils;
import io.horizondb.model.core.records.BlockHeaderBuilder;
import io.horizondb.model.core.records.TimeSeriesRecord;
import io.horizondb.model.core.util.TimeUtils;
import io.horizondb.model.schema.FieldType;
import io.horizondb.model.schema.RecordTypeDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

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

        List<TimeSeriesRecord> records = new RecordListBuilder(def).newRecord("exchangeState")
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

        int serializedSize = RecordUtils.computeSerializedSize(records);
        
        TimeSeriesRecord blockHeader = new BlockHeaderBuilder(def).firstTimestampInNanos(TIME_IN_NANOS + 12000700)
                                                                  .lastTimestampInNanos(TIME_IN_NANOS + 13004400)
                                                                  .compressedBlockSize(serializedSize)
                                                                  .uncompressedBlockSize(serializedSize)
                                                                  .recordCount(0, 3)
                                                                  .build();
        
        List<Record> list = new ArrayList<>();
        list.add(blockHeader);
        list.addAll(records);
        
        Buffer buffer = Buffers.allocate(RecordUtils.computeSerializedSize(blockHeader) + serializedSize);

        RecordUtils.writeRecords(buffer, list);
        
        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, buffer)) {

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

        List<TimeSeriesRecord> firstBlock = new RecordListBuilder(def).newRecord("exchangeState")
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

        int serializedSizeFirstBlock = RecordUtils.computeSerializedSize(firstBlock);
        
        TimeSeriesRecord firstBlockHeader = new BlockHeaderBuilder(def).firstTimestampInNanos(TIME_IN_NANOS + 12000700)
                                                                       .lastTimestampInNanos(TIME_IN_NANOS + 13004400)
                                                                       .compressedBlockSize(serializedSizeFirstBlock)
                                                                       .uncompressedBlockSize(serializedSizeFirstBlock)
                                                                       .recordCount(0, 3)
                                                                       .build();
        
        List<TimeSeriesRecord> secondBlock = new RecordListBuilder(def).newRecord("exchangeState")
                                                                       .setTimestampInNanos(0, TIME_IN_NANOS + 14000000)
                                                                       .setTimestampInMillis(1, TIME_IN_MILLIS + 14)
                                                                       .setByte(2, 1)
                                                                       .build();

        int serializedSizeSecondBlock = RecordUtils.computeSerializedSize(secondBlock);
        
        TimeSeriesRecord secondBlockHeader = new BlockHeaderBuilder(def).firstTimestampInNanos(TIME_IN_NANOS + 14000000)
                                                                        .lastTimestampInNanos(TIME_IN_NANOS + 14000000)
                                                                        .compressedBlockSize(serializedSizeSecondBlock)
                                                                        .uncompressedBlockSize(serializedSizeSecondBlock)
                                                                        .recordCount(0, 1)
                                                                        .build();
                
        List<Record> list = new ArrayList<>();
        list.add(firstBlockHeader);
        list.addAll(firstBlock);
        list.add(secondBlockHeader);
        list.addAll(secondBlock);
        
        
        Buffer buffer = Buffers.allocate(RecordUtils.computeSerializedSize(firstBlockHeader) 
                                         + serializedSizeFirstBlock 
                                         + RecordUtils.computeSerializedSize(secondBlockHeader)
                                         + serializedSizeSecondBlock);

        RecordUtils.writeRecords(buffer, list);
        
        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, buffer)) {

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

        List<TimeSeriesRecord> firstBlock = new RecordListBuilder(def).newRecord("exchangeState")
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

        int serializedSizeFirstBlock = RecordUtils.computeSerializedSize(firstBlock);
        
        TimeSeriesRecord firstBlockHeader = new BlockHeaderBuilder(def).firstTimestampInNanos(TIME_IN_NANOS + 12000700)
                                                                       .lastTimestampInNanos(TIME_IN_NANOS + 13004400)
                                                                       .compressedBlockSize(serializedSizeFirstBlock)
                                                                       .uncompressedBlockSize(serializedSizeFirstBlock)
                                                                       .recordCount(0, 3)
                                                                       .build();
        
        List<TimeSeriesRecord> secondBlock = new RecordListBuilder(def).newRecord("exchangeState")
                                                                       .setTimestampInNanos(0, TIME_IN_NANOS + 14000000)
                                                                       .setTimestampInMillis(1, TIME_IN_MILLIS + 14)
                                                                       .setByte(2, 1)
                                                                       .build();

        int serializedSizeSecondBlock = RecordUtils.computeSerializedSize(secondBlock);
        
        TimeSeriesRecord secondBlockHeader = new BlockHeaderBuilder(def).firstTimestampInNanos(TIME_IN_NANOS + 14000000)
                                                                        .lastTimestampInNanos(TIME_IN_NANOS + 14000000)
                                                                        .compressedBlockSize(serializedSizeSecondBlock)
                                                                        .uncompressedBlockSize(serializedSizeSecondBlock)
                                                                        .recordCount(0, 1)
                                                                        .build();
                
        List<Record> list = new ArrayList<>();
        list.add(firstBlockHeader);
        list.addAll(firstBlock);
        list.add(secondBlockHeader);
        list.addAll(secondBlock);
        
        
        Buffer buffer = Buffers.allocate(RecordUtils.computeSerializedSize(firstBlockHeader) 
                                         + serializedSizeFirstBlock 
                                         + RecordUtils.computeSerializedSize(secondBlockHeader)
                                         + serializedSizeSecondBlock);

        RecordUtils.writeRecords(buffer, list);
        
        Field from = NANOSECONDS_TIMESTAMP.newField().setTimestampInNanos(TIME_IN_NANOS + 13005000);
        Field to = NANOSECONDS_TIMESTAMP.newField().setTimestampInNanos(TIME_IN_NANOS + 15000000);
        
        RangeSet<Field> rangeSet = ImmutableRangeSet.of(Range.closed(from, to)); 
        
        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, buffer, rangeSet)) {

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

        List<TimeSeriesRecord> firstBlock = new RecordListBuilder(def).newRecord("exchangeState")
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

        int serializedSizeFirstBlock = RecordUtils.computeSerializedSize(firstBlock);
        
        TimeSeriesRecord firstBlockHeader = new BlockHeaderBuilder(def).firstTimestampInNanos(TIME_IN_NANOS + 12000700)
                                                                       .lastTimestampInNanos(TIME_IN_NANOS + 13004400)
                                                                       .compressedBlockSize(serializedSizeFirstBlock)
                                                                       .uncompressedBlockSize(serializedSizeFirstBlock)
                                                                       .recordCount(0, 3)
                                                                       .build();
        
        List<TimeSeriesRecord> secondBlock = new RecordListBuilder(def).newRecord("exchangeState")
                                                                       .setTimestampInNanos(0, TIME_IN_NANOS + 14000000)
                                                                       .setTimestampInMillis(1, TIME_IN_MILLIS + 14)
                                                                       .setByte(2, 1)
                                                                       .build();

        int serializedSizeSecondBlock = RecordUtils.computeSerializedSize(secondBlock);
        
        TimeSeriesRecord secondBlockHeader = new BlockHeaderBuilder(def).firstTimestampInNanos(TIME_IN_NANOS + 14000000)
                                                                        .lastTimestampInNanos(TIME_IN_NANOS + 14000000)
                                                                        .compressedBlockSize(serializedSizeSecondBlock)
                                                                        .uncompressedBlockSize(serializedSizeSecondBlock)
                                                                        .recordCount(0, 1)
                                                                        .build();
                
        List<Record> list = new ArrayList<>();
        list.add(firstBlockHeader);
        list.addAll(firstBlock);
        list.add(secondBlockHeader);
        list.addAll(secondBlock);
        
        
        Buffer buffer = Buffers.allocate(RecordUtils.computeSerializedSize(firstBlockHeader) 
                                         + serializedSizeFirstBlock 
                                         + RecordUtils.computeSerializedSize(secondBlockHeader)
                                         + serializedSizeSecondBlock);

        RecordUtils.writeRecords(buffer, list);
        
        Field from = NANOSECONDS_TIMESTAMP.newField().setTimestampInNanos(TIME_IN_NANOS + 13000900);
        Field to = NANOSECONDS_TIMESTAMP.newField().setTimestampInNanos(TIME_IN_NANOS + 13005000);
        
        RangeSet<Field> rangeSet = ImmutableRangeSet.of(Range.closed(from, to)); 
        
        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, buffer, rangeSet)) {

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

        List<TimeSeriesRecord> firstBlock = new RecordListBuilder(def).newRecord("exchangeState")
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

        int serializedSizeFirstBlock = RecordUtils.computeSerializedSize(firstBlock);
        
        TimeSeriesRecord firstBlockHeader = new BlockHeaderBuilder(def).firstTimestampInNanos(TIME_IN_NANOS + 12000700)
                                                                       .lastTimestampInNanos(TIME_IN_NANOS + 13004400)
                                                                       .compressedBlockSize(serializedSizeFirstBlock)
                                                                       .uncompressedBlockSize(serializedSizeFirstBlock)
                                                                       .recordCount(0, 3)
                                                                       .build();
        
        List<TimeSeriesRecord> secondBlock = new RecordListBuilder(def).newRecord("exchangeState")
                                                                       .setTimestampInNanos(0, TIME_IN_NANOS + 14000000)
                                                                       .setTimestampInMillis(1, TIME_IN_MILLIS + 14)
                                                                       .setByte(2, 1)
                                                                       .build();

        int serializedSizeSecondBlock = RecordUtils.computeSerializedSize(secondBlock);
        
        TimeSeriesRecord secondBlockHeader = new BlockHeaderBuilder(def).firstTimestampInNanos(TIME_IN_NANOS + 14000000)
                                                                        .lastTimestampInNanos(TIME_IN_NANOS + 14000000)
                                                                        .compressedBlockSize(serializedSizeSecondBlock)
                                                                        .uncompressedBlockSize(serializedSizeSecondBlock)
                                                                        .recordCount(0, 1)
                                                                        .build();
                
        List<Record> list = new ArrayList<>();
        list.add(firstBlockHeader);
        list.addAll(firstBlock);
        list.add(secondBlockHeader);
        list.addAll(secondBlock);
        
        
        Buffer buffer = Buffers.allocate(RecordUtils.computeSerializedSize(firstBlockHeader) 
                                         + serializedSizeFirstBlock 
                                         + RecordUtils.computeSerializedSize(secondBlockHeader)
                                         + serializedSizeSecondBlock);

        RecordUtils.writeRecords(buffer, list);
        
        Field from = NANOSECONDS_TIMESTAMP.newField().setTimestampInNanos(TIME_IN_NANOS + 13004500);
        Field to = NANOSECONDS_TIMESTAMP.newField().setTimestampInNanos(TIME_IN_NANOS + 13005000);
        
        RangeSet<Field> rangeSet = ImmutableRangeSet.of(Range.closed(from, to)); 
        
        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, buffer, rangeSet)) {
            
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

        List<TimeSeriesRecord> firstBlock = new RecordListBuilder(def).newRecord("exchangeState")
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

        int serializedSizeFirstBlock = RecordUtils.computeSerializedSize(firstBlock);
        
        Buffer uncompressedFirstBlock = Buffers.allocate(serializedSizeFirstBlock);
        RecordUtils.writeRecords(uncompressedFirstBlock, firstBlock);
        
        ReadableBuffer compressedFirstBlock = CompressionType.LZ4.newCompressor().compress(uncompressedFirstBlock);
        
        TimeSeriesRecord firstBlockHeader = new BlockHeaderBuilder(def).firstTimestampInNanos(TIME_IN_NANOS + 12000700)
                                                                       .lastTimestampInNanos(TIME_IN_NANOS + 13004400)
                                                                       .compressionType(CompressionType.LZ4)
                                                                       .compressedBlockSize(compressedFirstBlock.readableBytes())
                                                                       .uncompressedBlockSize(serializedSizeFirstBlock)
                                                                       .recordCount(0, 3)
                                                                       .build();
        
        List<TimeSeriesRecord> secondBlock = new RecordListBuilder(def).newRecord("exchangeState")
                                                                       .setTimestampInNanos(0, TIME_IN_NANOS + 14000000)
                                                                       .setTimestampInMillis(1, TIME_IN_MILLIS + 14)
                                                                       .setByte(2, 1)
                                                                       .build();

        int serializedSizeSecondBlock = RecordUtils.computeSerializedSize(secondBlock);
        
        Buffer uncompressedSecondBlock = Buffers.allocate(serializedSizeSecondBlock);
        RecordUtils.writeRecords(uncompressedSecondBlock, secondBlock);
        
        ReadableBuffer compressedSecondBlock = CompressionType.LZ4.newCompressor().compress(uncompressedSecondBlock);
        
        TimeSeriesRecord secondBlockHeader = new BlockHeaderBuilder(def).firstTimestampInNanos(TIME_IN_NANOS + 14000000)
                                                                        .lastTimestampInNanos(TIME_IN_NANOS + 14000000)
                                                                        .compressionType(CompressionType.LZ4)
                                                                        .compressedBlockSize(compressedSecondBlock.readableBytes())
                                                                        .uncompressedBlockSize(serializedSizeSecondBlock)
                                                                        .recordCount(0, 1)
                                                                        .build();     
        
        Buffer buffer = Buffers.allocate(compressedFirstBlock.readableBytes()
                                         + serializedSizeFirstBlock 
                                         + compressedSecondBlock.readableBytes()
                                         + serializedSizeSecondBlock);

        RecordUtils.writeRecord(buffer, firstBlockHeader);
        buffer.transfer(compressedFirstBlock);
        RecordUtils.writeRecord(buffer, secondBlockHeader);
        buffer.transfer(compressedSecondBlock);
        
        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, buffer)) {

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
        
        try (RecordIterator readIterator = new BinaryTimeSeriesRecordIterator(def, Buffers.EMPTY_BUFFER)) {

            assertFalse(readIterator.hasNext());
        }
    }
    
}
