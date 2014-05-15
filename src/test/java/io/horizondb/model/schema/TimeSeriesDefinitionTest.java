/**
 * Copyright 2013 Benjamin Lerer
 * 
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
package io.horizondb.model.schema;

import io.horizondb.io.Buffer;
import io.horizondb.io.buffers.Buffers;
import io.horizondb.model.schema.FieldType;
import io.horizondb.model.schema.PartitionType;
import io.horizondb.model.schema.RecordTypeDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;
import io.horizondb.test.AssertCollections;

import java.io.IOException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Range;

import static org.junit.Assert.assertNull;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Benjamin
 * 
 */
public class TimeSeriesDefinitionTest {

    @Test
    public void testComputeSize() throws IOException {

        RecordTypeDefinition quote = RecordTypeDefinition.newBuilder("Quote")
                                                         .addField("bestBid", FieldType.DECIMAL)
                                                         .addField("bestAsk", FieldType.DECIMAL)
                                                         .addField("bidVolume", FieldType.INTEGER)
                                                         .addField("askVolume", FieldType.INTEGER)
                                                         .build();

        RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
                                                         .addField("price", FieldType.DECIMAL)
                                                         .addField("volume", FieldType.DECIMAL)
                                                         .addField("aggressorSide", FieldType.BYTE)
                                                         .build();

        TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("DAX")
                                                              .timeUnit(TimeUnit.NANOSECONDS)
                                                              .addRecordType(quote)
                                                              .addRecordType(trade)
                                                              .build();

        Buffer buffer = Buffers.allocate(200);

        definition.writeTo(buffer);

        Assert.assertEquals(buffer.readableBytes(), definition.computeSerializedSize());
    }

    @Test
    public void testGetRecordTypeIndex() {

        RecordTypeDefinition quote = RecordTypeDefinition.newBuilder("Quote")
                                                         .addField("bestBid", FieldType.DECIMAL)
                                                         .addField("bestAsk", FieldType.DECIMAL)
                                                         .addField("bidVolume", FieldType.INTEGER)
                                                         .addField("askVolume", FieldType.INTEGER)
                                                         .build();

        RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
                                                         .addField("price", FieldType.DECIMAL)
                                                         .addField("volume", FieldType.DECIMAL)
                                                         .addField("aggressorSide", FieldType.BYTE)
                                                         .build();

        TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("DAX")
                                                              .timeUnit(TimeUnit.NANOSECONDS)
                                                              .addRecordType(quote)
                                                              .addRecordType(trade)
                                                              .build();

        assertEquals(0, definition.getRecordTypeIndex("Quote"));
        assertEquals(1, definition.getRecordTypeIndex("Trade"));
        
        try {
            
            definition.getRecordTypeIndex("Unknown");
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }
        
    }
    
    @Test
    public void testGetFieldIndex() {

        RecordTypeDefinition quote = RecordTypeDefinition.newBuilder("Quote")
                                                         .addField("bestBid", FieldType.DECIMAL)
                                                         .addField("bestAsk", FieldType.DECIMAL)
                                                         .addField("bidVolume", FieldType.INTEGER)
                                                         .addField("askVolume", FieldType.INTEGER)
                                                         .build();

        RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
                                                         .addField("price", FieldType.DECIMAL)
                                                         .addField("volume", FieldType.DECIMAL)
                                                         .addField("aggressorSide", FieldType.BYTE)
                                                         .build();

        TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("DAX")
                                                              .timeUnit(TimeUnit.NANOSECONDS)
                                                              .addRecordType(quote)
                                                              .addRecordType(trade)
                                                              .build();

        assertEquals(0, definition.getFieldIndex(0, "timestamp"));
        assertEquals(1, definition.getFieldIndex(0, "bestBid"));
        assertEquals(4, definition.getFieldIndex(0, "askVolume"));
        
        assertEquals(0, definition.getFieldIndex(1, "timestamp"));
        assertEquals(1, definition.getFieldIndex(1, "price"));
        assertEquals(2, definition.getFieldIndex(1, "volume"));
        
        try {
            
            definition.getFieldIndex(2, "bestBid");
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }
        
        assertEquals(-1 , definition.getFieldIndex(0, "unknown"));
    }
    
    @Test
    public void testNewFieldWithFieldName() {

        RecordTypeDefinition quote = RecordTypeDefinition.newBuilder("Quote")
                                                         .addDecimalField("bestBid")
                                                         .addDecimalField("bestAsk")
                                                         .addIntegerField("bidVolume")
                                                         .addIntegerField("askVolume")
                                                         .addByteField("exchangeState")
                                                         .build();

        RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
                                                         .addDecimalField("price")
                                                         .addIntegerField("volume")
                                                         .addByteField("aggressorSide")
                                                         .addByteField("exchangeState")
                                                         .build();

        TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("DAX")
                                                              .timeUnit(TimeUnit.NANOSECONDS)
                                                              .addRecordType(quote)
                                                              .addRecordType(trade)
                                                              .build();
        
        assertNull(definition.newField("test"));
        assertEquals(FieldType.INTEGER.newField(), definition.newField("volume"));
        assertEquals(FieldType.DECIMAL.newField(), definition.newField("bestBid"));
        assertEquals(FieldType.BYTE.newField(), definition.newField("exchangeState"));
    }

    
    @Test
    public void testSplitRangeWithRangeIncludedWithin2DailyPartition() {
        
        RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
                                                         .addField("price", FieldType.DECIMAL)
                                                         .addField("volume", FieldType.DECIMAL)
                                                         .addField("aggressorSide", FieldType.BYTE)
                                                         .build();
        
        TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("DAX")
                                                              .timeZone(TimeZone.getTimeZone("CET"))
                                                              .partitionType(PartitionType.BY_DAY)
                                                              .addRecordType(trade)
                                                              .build();
        
        Range<Long> range = timeRange("2013.11.14 12:00:00.000", "2013.11.15 13:00:00.000");
        List<Range<Long>> ranges = definition.splitRange(range);
        
        AssertCollections.assertListContains(ranges, 
                                             timeRange("2013.11.14 12:00:00.000", "2013.11.15 00:00:00.000"),
                                             timeRange("2013.11.15 00:00:00.000", "2013.11.15 13:00:00.000"));
    }
    
    @Test
    public void testSplitRangeWithRangeIncludedWithin3DailyPartition() {
        
        RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
                                                         .addField("price", FieldType.DECIMAL)
                                                         .addField("volume", FieldType.DECIMAL)
                                                         .addField("aggressorSide", FieldType.BYTE)
                                                         .build();
        
        TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("DAX")
                                                              .timeZone(TimeZone.getTimeZone("CET"))
                                                              .partitionType(PartitionType.BY_DAY)
                                                              .addRecordType(trade)
                                                              .build();
        
        Range<Long> range = timeRange("2013.11.14 12:00:00.000", "2013.11.16 13:00:00.000");
        List<Range<Long>> ranges = definition.splitRange(range);
        
        AssertCollections.assertListContains(ranges, 
                                             timeRange("2013.11.14 12:00:00.000", "2013.11.15 00:00:00.000"),
                                             timeRange("2013.11.15 00:00:00.000", "2013.11.16 00:00:00.000"),
                                             timeRange("2013.11.16 00:00:00.000", "2013.11.16 13:00:00.000"));
    }
    
    @Test
    public void testGetParser() throws IOException {

        RecordTypeDefinition quote = RecordTypeDefinition.newBuilder("Quote")
                                                         .addField("bestBid", FieldType.DECIMAL)
                                                         .addField("bestAsk", FieldType.DECIMAL)
                                                         .addField("bidVolume", FieldType.INTEGER)
                                                         .addField("askVolume", FieldType.INTEGER)
                                                         .build();

        RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
                                                         .addField("price", FieldType.DECIMAL)
                                                         .addField("volume", FieldType.DECIMAL)
                                                         .addField("aggressorSide", FieldType.BYTE)
                                                         .build();

        TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("DAX")
                                                              .timeUnit(TimeUnit.NANOSECONDS)
                                                              .addRecordType(quote)
                                                              .addRecordType(trade)
                                                              .build();

        Buffer buffer = Buffers.allocate(200);

        definition.writeTo(buffer);

        TimeSeriesDefinition deserializedDefinition = TimeSeriesDefinition.getParser().parseFrom(buffer);
        assertEquals(definition, deserializedDefinition);
    }

    @Test
    public void testParseFrom() throws IOException {

        RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
                                                         .addDecimalField("price")
                                                         .addLongField("volume")
                                                         .build();

        TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("DAX")
                                                              .timeUnit(TimeUnit.MILLISECONDS)
                                                              .addRecordType(trade)
                                                              .build();

        Buffer buffer = Buffers.allocate(200);

        definition.writeTo(buffer);

        TimeSeriesDefinition deserializedDefinition = TimeSeriesDefinition.parseFrom(buffer);
        assertEquals(definition, deserializedDefinition);
    }
    
    /**
     * Returns the time in milliseconds corresponding to the specified {@link String} (format:
     * "yyyy.MM.dd HH:mm:ss.SSS").
     * 
     * @param dateAsText the date/time to convert in milliseconds
     * @return the time in milliseconds corresponding to the specified {@link String}.
     */
    public static long getTime(String dateAsText) {

        SimpleDateFormat format = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss.SSS");
        return format.parse(dateAsText, new ParsePosition(0)).getTime();
    }
    
    public static Range<Long> timeRange(String start, String end) {
        
        return Range.closedOpen(Long.valueOf(getTime(start)), Long.valueOf(getTime(end)));
    }
}
