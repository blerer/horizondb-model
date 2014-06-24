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
package io.horizondb.model.core.records;

import io.horizondb.io.Buffer;
import io.horizondb.io.buffers.Buffers;
import io.horizondb.model.schema.FieldType;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Benjamin
 * 
 */
public class BinaryTimeSeriesRecordTest {

    /**
     * The record type used during the tests.
     */
    private static final int TYPE = 0;

    @Test
    public void testGetMethods() throws IOException {

        TimeSeriesRecord record = new TimeSeriesRecord(TYPE,
                                                       TimeUnit.NANOSECONDS,
                                                       FieldType.MILLISECONDS_TIMESTAMP,
                                                       FieldType.DECIMAL,
                                                       FieldType.BYTE);

        record.setTimestampInNanos(0, 10000000);
        record.setTimestampInMillis(1, 10);
        record.setDecimal(2, 145, 1);
        record.setByte(3, 3);

        BinaryTimeSeriesRecord binaryRecord = record.toBinaryTimeSeriesRecord();

        assertFalse(binaryRecord.isDelta());
        assertEquals(10000000, binaryRecord.getTimestampInNanos(0));
        assertEquals(10, binaryRecord.getTimestampInMillis(1));
        assertEquals(145, binaryRecord.getDecimalMantissa(2));
        assertEquals(1, binaryRecord.getDecimalExponent(2));
        assertEquals(3, binaryRecord.getByte(3));
    }

    @Test
    public void testGetMethodsWithDelta() throws IOException {

        TimeSeriesRecord delta = new TimeSeriesRecord(TYPE,
                                                      TimeUnit.NANOSECONDS,
                                                      FieldType.MILLISECONDS_TIMESTAMP,
                                                      FieldType.DECIMAL,
                                                      FieldType.BYTE);

        delta.setDelta(true);
        delta.setTimestampInNanos(0, 10000000);
        delta.setTimestampInMillis(1, 10);
        delta.setDecimal(2, 145, 1);
        delta.setByte(3, 3);

        BinaryTimeSeriesRecord binaryRecord = delta.toBinaryTimeSeriesRecord();

        assertTrue(binaryRecord.isDelta());
        assertEquals(10000000, binaryRecord.getTimestampInNanos(0));
        assertEquals(10, binaryRecord.getTimestampInMillis(1));
        assertEquals(145, binaryRecord.getDecimalMantissa(2));
        assertEquals(1, binaryRecord.getDecimalExponent(2));
        assertEquals(3, binaryRecord.getByte(3));
    }

    @Test
    public void testGetMethodsWithZeroValue() throws IOException {

        TimeSeriesRecord record = new TimeSeriesRecord(TYPE,
                                                       TimeUnit.NANOSECONDS,
                                                       FieldType.MILLISECONDS_TIMESTAMP,
                                                       FieldType.DECIMAL,
                                                       FieldType.BYTE);

        record.setTimestampInNanos(0, 100);
        record.setByte(3, 3);

        BinaryTimeSeriesRecord binaryRecord = record.toBinaryTimeSeriesRecord();

        assertFalse(binaryRecord.isDelta());
        assertEquals(100, binaryRecord.getTimestampInNanos(0));
        assertEquals(0, binaryRecord.getTimestampInMillis(1));
        assertEquals(0, binaryRecord.getDecimalMantissa(2));
        assertEquals(0, binaryRecord.getDecimalExponent(2));
        assertEquals(3, binaryRecord.getByte(3));
    }

    @Test
    public void testGetMethodsWithMultipleFill() throws IOException {

        TimeSeriesRecord record = new TimeSeriesRecord(TYPE,
                                                       TimeUnit.NANOSECONDS,
                                                       FieldType.MILLISECONDS_TIMESTAMP,
                                                       FieldType.DECIMAL,
                                                       FieldType.BYTE);

        record.setTimestampInNanos(0, 10000000);
        record.setTimestampInMillis(1, 10);
        record.setDecimal(2, 145, 1);
        record.setByte(3, 3);

        BinaryTimeSeriesRecord binaryRecord = record.toBinaryTimeSeriesRecord();

        assertFalse(binaryRecord.isDelta());
        assertEquals(10000000, binaryRecord.getTimestampInNanos(0));
        assertEquals(10, binaryRecord.getTimestampInMillis(1));
        assertEquals(145, binaryRecord.getDecimalMantissa(2));
        assertEquals(1, binaryRecord.getDecimalExponent(2));
        assertEquals(3, binaryRecord.getByte(3));

        record = new TimeSeriesRecord(TYPE,
                                      TimeUnit.NANOSECONDS,
                                      FieldType.MILLISECONDS_TIMESTAMP,
                                      FieldType.DECIMAL,
                                      FieldType.BYTE);

        record.setTimestampInNanos(0, 100);
        record.setByte(3, 3);

        binaryRecord = record.toBinaryTimeSeriesRecord();

        assertFalse(binaryRecord.isDelta());
        assertEquals(100, binaryRecord.getTimestampInNanos(0));
        assertEquals(0, binaryRecord.getTimestampInMillis(1));
        assertEquals(0, binaryRecord.getDecimalMantissa(2));
        assertEquals(0, binaryRecord.getDecimalExponent(2));
        assertEquals(3, binaryRecord.getByte(3));
    }

    @Test
    public void testWriteTo() throws IOException {

        TimeSeriesRecord record = new TimeSeriesRecord(TYPE,
                                                       TimeUnit.NANOSECONDS,
                                                       FieldType.MILLISECONDS_TIMESTAMP,
                                                       FieldType.DECIMAL,
                                                       FieldType.BYTE);

        record.setTimestampInNanos(0, 10000000);
        record.setTimestampInMillis(1, 10);
        record.setDecimal(2, 145, 1);
        record.setByte(3, 3);

        Buffer reader = Buffers.allocate(10);

        record.writeTo(reader);

        BinaryTimeSeriesRecord binaryRecord = new BinaryTimeSeriesRecord(TYPE,
                                                                         TimeUnit.NANOSECONDS,
                                                                         FieldType.MILLISECONDS_TIMESTAMP,
                                                                         FieldType.DECIMAL,
                                                                         FieldType.BYTE);
        binaryRecord.fill(reader);

        Buffer writer = Buffers.allocate(10);
        binaryRecord.writeTo(writer);

        byte[] expected = reader.array();
        byte[] actual = writer.array();

        assertArrayEquals(expected, actual);
    }

    @Test
    public void testWriteToWithGetterCallsBefore() throws IOException {

        TimeSeriesRecord record = new TimeSeriesRecord(TYPE,
                                                       TimeUnit.NANOSECONDS,
                                                       FieldType.MILLISECONDS_TIMESTAMP,
                                                       FieldType.DECIMAL,
                                                       FieldType.BYTE);

        record.setTimestampInNanos(0, 10000000);
        record.setTimestampInMillis(1, 10);
        record.setDecimal(2, 145, 1);
        record.setByte(3, 3);

        Buffer reader = Buffers.allocate(10);

        record.writeTo(reader);

        BinaryTimeSeriesRecord binaryRecord = new BinaryTimeSeriesRecord(TYPE,
                                                                         TimeUnit.NANOSECONDS,
                                                                         FieldType.MILLISECONDS_TIMESTAMP,
                                                                         FieldType.DECIMAL,
                                                                         FieldType.BYTE);
        binaryRecord.fill(reader);

        assertFalse(binaryRecord.isDelta());
        assertEquals(10000000, binaryRecord.getTimestampInNanos(0));
        assertEquals(10, binaryRecord.getTimestampInMillis(1));
        assertEquals(145, binaryRecord.getDecimalMantissa(2));
        assertEquals(1, binaryRecord.getDecimalExponent(2));
        assertEquals(3, binaryRecord.getByte(3));

        Buffer writer = Buffers.allocate(10);

        binaryRecord.writeTo(writer);

        byte[] expected = reader.array();
        byte[] actual = writer.array();

        assertArrayEquals(expected, actual);
    }

    @Test
    public void testGetterWithWriteToCallsBefore() throws IOException {

        TimeSeriesRecord record = new TimeSeriesRecord(TYPE,
                                                       TimeUnit.NANOSECONDS,
                                                       FieldType.MILLISECONDS_TIMESTAMP,
                                                       FieldType.DECIMAL,
                                                       FieldType.BYTE);

        record.setTimestampInNanos(0, 10000000);
        record.setTimestampInMillis(1, 10);
        record.setDecimal(2, 145, 1);
        record.setByte(3, 3);

        Buffer reader = Buffers.allocate(10);

        record.writeTo(reader);

        BinaryTimeSeriesRecord binaryRecord = new BinaryTimeSeriesRecord(TYPE,
                                                                         TimeUnit.NANOSECONDS,
                                                                         FieldType.MILLISECONDS_TIMESTAMP,
                                                                         FieldType.DECIMAL,
                                                                         FieldType.BYTE);
        binaryRecord.fill(reader);

        Buffer writer = Buffers.allocate(10);

        binaryRecord.writeTo(writer);

        byte[] expected = reader.array();
        byte[] actual = writer.array();

        assertArrayEquals(expected, actual);

        assertFalse(binaryRecord.isDelta());
        assertEquals(10000000, binaryRecord.getTimestampInNanos(0));
        assertEquals(10, binaryRecord.getTimestampInMillis(1));
        assertEquals(145, binaryRecord.getDecimalMantissa(2));
        assertEquals(1, binaryRecord.getDecimalExponent(2));
        assertEquals(3, binaryRecord.getByte(3));
    }
    
    @Test
    public void testCopyTo() throws IOException {

        TimeSeriesRecord record = new TimeSeriesRecord(TYPE, TimeUnit.MILLISECONDS, FieldType.INTEGER)
            .setTimestampInMillis(0, 1001L)
            .setInt(1, 3);

        BinaryTimeSeriesRecord binaryRecord = record.toBinaryTimeSeriesRecord();

        TimeSeriesRecord empty = new TimeSeriesRecord(TYPE, TimeUnit.MILLISECONDS, FieldType.INTEGER);

        binaryRecord.copyTo(empty);
        
        TimeSeriesRecord expected = new TimeSeriesRecord(TYPE, TimeUnit.MILLISECONDS, FieldType.INTEGER).setDelta(false)
                                                                                                        .setTimestampInMillis(0, 1001L)
                                                                                                        .setInt(1, 3);
        assertEquals(expected, empty);
        
    }
    
    @Test
    public void testCopyToDelta() throws IOException {

        TimeSeriesRecord record = new TimeSeriesRecord(TYPE, TimeUnit.MILLISECONDS, FieldType.INTEGER)
            .setDelta(true)
            .setTimestampInMillis(0, 1001L)
            .setInt(1, 3);

        BinaryTimeSeriesRecord binaryRecord = record.toBinaryTimeSeriesRecord();

        TimeSeriesRecord empty = new TimeSeriesRecord(TYPE, TimeUnit.MILLISECONDS, FieldType.INTEGER);

        binaryRecord.copyTo(empty);
        
        TimeSeriesRecord expected = new TimeSeriesRecord(TYPE, TimeUnit.MILLISECONDS, FieldType.INTEGER).setDelta(true)                                                                                             .setTimestampInMillis(0, 1001L)
                                                                                                        .setInt(1, 3);
        assertEquals(expected, empty);
        
    }
}
