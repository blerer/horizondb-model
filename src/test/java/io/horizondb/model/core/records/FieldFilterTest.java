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
package io.horizondb.model.core.records;

import io.horizondb.io.Buffer;
import io.horizondb.io.buffers.Buffers;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.fields.IntegerField;
import io.horizondb.model.core.fields.TimestampField;
import io.horizondb.model.schema.FieldType;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FieldFilterTest {
    
    /**
     * The record type used during the tests.
     */
    private static final int TYPE = 0;

    @Test
    public void testFilterRecordEndingFields() throws IOException {
        
        TimeSeriesRecord record = new TimeSeriesRecord(TYPE, 
                                                       TimeUnit.MILLISECONDS, 
                                                       FieldType.INTEGER,
                                                       FieldType.INTEGER,
                                                       FieldType.INTEGER);
        record.setTimestampInMillis(0, 1001L);
        record.setInt(1, 1);
        record.setInt(2, 2);
        record.setInt(3, 3);
        
        testRecordEndingFields(record);
        testRecordEndingFields(record.toBinaryTimeSeriesRecord());
    }

    private static void testRecordEndingFields(Record record) throws IOException {

        FieldFilter filteringRecord = new FieldFilter(TYPE, 0, 1).wrap(record);

        assertEquals(2, filteringRecord.getNumberOfFields());

        assertEquals(filteringRecord.getTimestampInMillis(0), 1001L);
        assertEquals(filteringRecord.getInt(1), 1L);

        try {
            filteringRecord.getInt(2);
            fail();
        } catch (ArrayIndexOutOfBoundsException e) {
            assertTrue(true);
        }

        assertArrayEquals(new Field[] { new TimestampField(TimeUnit.MILLISECONDS).setTimestampInMillis(1001L),
                new IntegerField().setInt(1) }, filteringRecord.getFields());

        assertEquals(6L, filteringRecord.getBitSet().toLong());
        assertEquals(4, filteringRecord.computeSerializedSize());
        Buffer buffer = Buffers.allocate(4);
        filteringRecord.writeTo(buffer);
        assertEquals(Buffers.wrap(new byte[] { 6, -46, 15, 2 }), buffer);
    }

    @Test
    public void testFilterRecordDeltaEndingFields() throws IOException {
        
        TimeSeriesRecord record = new TimeSeriesRecord(TYPE, 
                                                       TimeUnit.MILLISECONDS, 
                                                       FieldType.INTEGER,
                                                       FieldType.INTEGER,
                                                       FieldType.INTEGER);
        record.setTimestampInMillis(0, 1001L);
        record.setDelta(true);
        record.setInt(2, 2);
        
        testFilterRecordDeltaEndingFields(record);
        testFilterRecordDeltaEndingFields(record.toBinaryTimeSeriesRecord());
    }

    private static void testFilterRecordDeltaEndingFields(Record record) throws IOException {

        FieldFilter filteringRecord = new FieldFilter(TYPE, 0, 1, 2).wrap(record);

        assertEquals(3, filteringRecord.getNumberOfFields());

        assertEquals(filteringRecord.getTimestampInMillis(0), 1001L);
        assertEquals(filteringRecord.getInt(1), 0);
        assertEquals(filteringRecord.getInt(2), 2);

        try {
            filteringRecord.getInt(3);
            fail();
        } catch (ArrayIndexOutOfBoundsException e) {
            assertTrue(true);
        }

        assertArrayEquals(new Field[]{new TimestampField(TimeUnit.MILLISECONDS).setTimestampInMillis(1001L), 
                                      new IntegerField(),
                                      new IntegerField().setInt(2)}, 
                          filteringRecord.getFields());

        assertEquals(11L, filteringRecord.getBitSet().toLong());
        assertEquals(4, filteringRecord.computeSerializedSize());
        Buffer buffer = Buffers.allocate(4);
        filteringRecord.writeTo(buffer);
        assertEquals(Buffers.wrap(new byte[] { 11, -46, 15, 4 }), buffer);
    }

    @Test
    public void testFilterRecordRandomFields() throws IOException {

        TimeSeriesRecord record = new TimeSeriesRecord(TYPE, 
                                                       TimeUnit.MILLISECONDS, 
                                                       FieldType.INTEGER,
                                                       FieldType.INTEGER,
                                                       FieldType.INTEGER);
        record.setTimestampInMillis(0, 1001L);
        record.setInt(1, 1);
        record.setInt(2, 2);
        record.setInt(3, 3);

        testFilterRecordRandomFields(record);
        testFilterRecordRandomFields(record.toBinaryTimeSeriesRecord());
    }

    private static void testFilterRecordRandomFields(Record record) throws IOException {

        FieldFilter filteringRecord = new FieldFilter(TYPE, 0, 3).wrap(record);

        assertEquals(2, filteringRecord.getNumberOfFields());

        assertEquals(filteringRecord.getTimestampInMillis(0), 1001L);
        assertEquals(filteringRecord.getInt(1), 3);

        try {
            filteringRecord.getInt(2);
            fail();
        } catch (ArrayIndexOutOfBoundsException e) {
            assertTrue(true);
        }

        assertArrayEquals(new Field[]{new TimestampField(TimeUnit.MILLISECONDS).setTimestampInMillis(1001L), 
                                      new IntegerField().setInt(3)}, 
                          filteringRecord.getFields());

        assertEquals(6L, filteringRecord.getBitSet().toLong());
        assertEquals(4, filteringRecord.computeSerializedSize());
        Buffer buffer = Buffers.allocate(4);
        filteringRecord.writeTo(buffer);
        assertEquals(Buffers.wrap(new byte[]{6, -46, 15, 6}), buffer);
    }

    @Test
    public void testFilterRecordDeltaRandomFields() throws IOException {
        
        TimeSeriesRecord record = new TimeSeriesRecord(TYPE, 
                                                       TimeUnit.MILLISECONDS, 
                                                       FieldType.INTEGER,
                                                       FieldType.INTEGER,
                                                       FieldType.INTEGER);
        record.setTimestampInMillis(0, 1001L);
        record.setDelta(true);
        record.setInt(1, 1);
        record.setInt(2, 2);
        
        testFilterRecordDeltaRandomFields(record);
        testFilterRecordDeltaRandomFields(record);
    }

    private static void testFilterRecordDeltaRandomFields(Record record) throws IOException {
        FieldFilter filteringRecord = new FieldFilter(TYPE, 0, 3).wrap(record);

        assertEquals(2, filteringRecord.getNumberOfFields());

        assertEquals(filteringRecord.getTimestampInMillis(0), 1001L);
        assertEquals(filteringRecord.getInt(1), 0);

        try {
            filteringRecord.getInt(2);
            fail();
        } catch (ArrayIndexOutOfBoundsException e) {
            assertTrue(true);
        }
        
        assertArrayEquals(new Field[]{new TimestampField(TimeUnit.MILLISECONDS).setTimestampInMillis(1001L), 
                                      new IntegerField()}, 
                          filteringRecord.getFields());
        
        assertEquals(3L, filteringRecord.getBitSet().toLong());
        assertEquals(3, filteringRecord.computeSerializedSize());
        Buffer buffer = Buffers.allocate(4);
        filteringRecord.writeTo(buffer);
        assertEquals(Buffers.wrap(new byte[]{3, -46, 15}), buffer);
    }

    @Test
    public void testFilterRecordWithUnorderedFields() throws IOException {
        
        TimeSeriesRecord record = new TimeSeriesRecord(TYPE, 
                                                       TimeUnit.MILLISECONDS, 
                                                       FieldType.INTEGER,
                                                       FieldType.INTEGER,
                                                       FieldType.INTEGER);
        record.setTimestampInMillis(0, 1001L);
        record.setInt(1, 1);
        record.setInt(2, 2);
        record.setInt(3, 3);

        testFilterRecordWithUnorderedFields(record);
        testFilterRecordWithUnorderedFields(record);
    }

    private static void testFilterRecordWithUnorderedFields(Record record) throws IOException {

        FieldFilter filteringRecord = new FieldFilter(TYPE, 3, 0, 2).wrap(record);

        assertEquals(3, filteringRecord.getNumberOfFields());

        assertEquals(filteringRecord.getInt(0), 3);
        assertEquals(filteringRecord.getTimestampInMillis(1), 1001L);
        assertEquals(filteringRecord.getInt(2), 2);

        try {
            filteringRecord.getInt(4);
            fail();
        } catch (ArrayIndexOutOfBoundsException e) {
            assertTrue(true);
        }

        assertArrayEquals(new Field[]{new IntegerField().setInt(3),
                                      new TimestampField(TimeUnit.MILLISECONDS).setTimestampInMillis(1001L), 
                                      new IntegerField().setInt(2)}, 
                          filteringRecord.getFields());

        assertEquals(14L, filteringRecord.getBitSet().toLong());
        assertEquals(5, filteringRecord.computeSerializedSize());
        Buffer buffer = Buffers.allocate(5);
        filteringRecord.writeTo(buffer);
        assertEquals(Buffers.wrap(new byte[]{14, 6, -46, 15, 4}), buffer);
    }
    
    @Test
    public void testFilterRecordDeltaWithUnorderedFields() throws IOException {

        TimeSeriesRecord record = new TimeSeriesRecord(TYPE, 
                                                       TimeUnit.MILLISECONDS, 
                                                       FieldType.INTEGER,
                                                       FieldType.INTEGER,
                                                       FieldType.INTEGER);
        record.setTimestampInMillis(0, 1001L);
        record.setDelta(true);
        record.setInt(1, 1);
        record.setInt(2, 2);

        testFilterRecordDeltaWithUnorderedFields(record);
        testFilterRecordDeltaWithUnorderedFields(record.toBinaryTimeSeriesRecord());
    }

    private static void testFilterRecordDeltaWithUnorderedFields(Record record) throws IOException {

        FieldFilter filteringRecord = new FieldFilter(TYPE, 3, 0, 2).wrap(record);

        assertEquals(3, filteringRecord.getNumberOfFields());

        assertEquals(filteringRecord.getInt(0), 0);
        assertEquals(filteringRecord.getTimestampInMillis(1), 1001L);
        assertEquals(filteringRecord.getInt(2), 2);

        try {
            filteringRecord.getInt(4);
            fail();
        } catch (ArrayIndexOutOfBoundsException e) {
            assertTrue(true);
        }

        assertArrayEquals(new Field[]{new IntegerField(),
                                      new TimestampField(TimeUnit.MILLISECONDS).setTimestampInMillis(1001L), 
                                      new IntegerField().setInt(2)}, 
                          filteringRecord.getFields());

        assertEquals(13L, filteringRecord.getBitSet().toLong());
        assertEquals(4, filteringRecord.computeSerializedSize());
        Buffer buffer = Buffers.allocate(5);
        filteringRecord.writeTo(buffer);
        assertEquals(Buffers.wrap(new byte[]{13, -46, 15, 4}), buffer);
    }
}
