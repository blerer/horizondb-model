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
import io.horizondb.model.core.Filter;
import io.horizondb.model.core.Predicate;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.RecordUtils;
import io.horizondb.model.core.records.BlockHeaderBuilder;
import io.horizondb.model.core.records.TimeSeriesRecord;
import io.horizondb.model.core.util.TimeUtils;
import io.horizondb.model.schema.DatabaseDefinition;
import io.horizondb.model.schema.RecordTypeDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.horizondb.model.core.RecordUtils.writeRecord;
import static io.horizondb.model.core.predicates.FieldUtils.toMillisecondField;
import static io.horizondb.model.core.predicates.Predicates.and;
import static io.horizondb.model.core.predicates.Predicates.ge;
import static io.horizondb.model.core.predicates.Predicates.le;
import static io.horizondb.model.core.predicates.Predicates.lt;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FilteringRecordIteratorTest {

    private TimeSeriesDefinition seriesDefinition;

    @Before
    public void setUp() throws Exception {

        RecordTypeDefinition exchangeState = RecordTypeDefinition.newBuilder("exchangeState")
                                                                 .addMillisecondTimestampField("timestampInMillis")
                                                                 .addByteField("status")
                                                                 .build();

        RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("trade")
                                                         .addMillisecondTimestampField("timestampInMillis")
                                                         .addDecimalField("price")
                                                         .addLongField("volume")
                                                         .build();

        DatabaseDefinition databaseDefinition = new DatabaseDefinition("test");

        this.seriesDefinition = databaseDefinition.newTimeSeriesDefinitionBuilder("DAX")
                                                  .addRecordType(exchangeState)
                                                  .addRecordType(trade)
                                                  .build();
    }

    @After
    public void tearDown() throws Exception {

        this.seriesDefinition = null;
    }

    @Test
    public void testWithDeltaAndNoFiltering() throws IOException {

        long timestamp = TimeUtils.parseDateTime("2013-11-14 11:46:00.000");

        ReadableBuffer buffer = createBufferWithDeltas(this.seriesDefinition, timestamp);

        BinaryTimeSeriesRecordIterator iterator = new BinaryTimeSeriesRecordIterator(this.seriesDefinition, buffer);

        Predicate expression = and(ge("timestamp", toMillisecondField((timestamp - 100) + "ms")),
                                   lt("timestamp", toMillisecondField((timestamp + 500) + "ms")));

        Filter<Record> filter = expression.toFilter(this.seriesDefinition);

        try (FilteringRecordIterator filteringIterator = new FilteringRecordIterator(this.seriesDefinition,
                                                                                     iterator,
                                                                                     filter)) {

            assertTrue(filteringIterator.hasNext());

            Record record = filteringIterator.next();

            assertEquals(timestamp, record.getTimestampInMillis(0));
            assertEquals(timestamp, record.getTimestampInMillis(1));
            assertEquals(10, record.getByte(2));

            record = filteringIterator.next();

            assertTrue(record.isDelta());
            assertEquals(100, record.getTimestampInMillis(0));
            assertEquals(100, record.getTimestampInMillis(1));
            assertEquals(-5, record.getByte(2));

            assertTrue(filteringIterator.hasNext());

            record = filteringIterator.next();

            assertTrue(record.isDelta());
            assertEquals(250, record.getTimestampInMillis(0));
            assertEquals(250, record.getTimestampInMillis(1));
            assertEquals(5, record.getByte(2));

            assertTrue(filteringIterator.hasNext());

            record = filteringIterator.next();

            assertTrue(record.isDelta());
            assertEquals(100, record.getTimestampInMillis(0));
            assertEquals(100, record.getTimestampInMillis(1));
            assertEquals(-4, record.getByte(2));

            assertFalse(filteringIterator.hasNext());
        }
    }

    @Test
    public void testWithDeltaAndFiltering() throws IOException {

        long timestamp = TimeUtils.parseDateTime("2013-11-14 11:46:00.000");

        ReadableBuffer buffer = createBufferWithDeltas(this.seriesDefinition, timestamp);

        BinaryTimeSeriesRecordIterator iterator = new BinaryTimeSeriesRecordIterator(this.seriesDefinition, buffer);

        Predicate expression = and(ge("timestamp", toMillisecondField((timestamp + 120) + "ms")),
                                   lt("timestamp", toMillisecondField((timestamp + 600) + "ms")));

        Filter<Record> filter = expression.toFilter(this.seriesDefinition);

        try (FilteringRecordIterator rangeIterator = new FilteringRecordIterator(this.seriesDefinition,
                                                                                 iterator,
                                                                                 filter)) {

            assertTrue(rangeIterator.hasNext());

            Record record = rangeIterator.next();

            assertFalse(record.isDelta());
            assertEquals(timestamp + 350, record.getTimestampInMillis(0));
            assertEquals(timestamp + 350, record.getTimestampInMillis(1));
            assertEquals(10, record.getByte(2));

            assertTrue(rangeIterator.hasNext());

            record = rangeIterator.next();

            assertTrue(record.isDelta());
            assertEquals(100, record.getTimestampInMillis(0));
            assertEquals(100, record.getTimestampInMillis(1));
            assertEquals(-4, record.getByte(2));

            assertFalse(rangeIterator.hasNext());
        }
    }

    @Test
    public void testWithTwoRecordTypeAndNoFiltering() throws IOException {

        long timestamp = TimeUtils.parseDateTime("2013-11-14 11:46:00.000");

        ReadableBuffer buffer = createBufferWithTwoRecordTypes(this.seriesDefinition, timestamp);

        BinaryTimeSeriesRecordIterator iterator = new BinaryTimeSeriesRecordIterator(this.seriesDefinition, buffer);

        Predicate expression = and(ge("timestamp", toMillisecondField((timestamp - 100) + "ms")),
                                   le("timestamp", toMillisecondField((timestamp + 600) + "ms")));

        Filter<Record> filter = expression.toFilter(this.seriesDefinition);

        try (FilteringRecordIterator rangeIterator = new FilteringRecordIterator(this.seriesDefinition,
                                                                                 iterator,
                                                                                 filter)) {

            assertTrue(rangeIterator.hasNext());

            Record record = rangeIterator.next();

            assertFalse(record.isDelta());
            assertEquals(timestamp, record.getTimestampInMillis(0));
            assertEquals(timestamp, record.getTimestampInMillis(1));
            assertEquals(10, record.getByte(2));

            assertTrue(rangeIterator.hasNext());

            record = rangeIterator.next();

            assertFalse(record.isDelta());
            assertEquals(timestamp, record.getTimestampInMillis(0));
            assertEquals(timestamp, record.getTimestampInMillis(1));
            assertEquals(12, record.getDecimalMantissa(2));
            assertEquals(0, record.getDecimalExponent(2));
            assertEquals(6, record.getLong(3));

            record = rangeIterator.next();

            assertTrue(record.isDelta());
            assertEquals(100, record.getTimestampInMillis(0));
            assertEquals(100, record.getTimestampInMillis(1));
            assertEquals(-5, record.getByte(2));

            assertTrue(rangeIterator.hasNext());

            record = rangeIterator.next();

            assertTrue(record.isDelta());
            assertEquals(250, record.getTimestampInMillis(0));
            assertEquals(250, record.getTimestampInMillis(1));
            assertEquals(5, record.getByte(2));

            assertTrue(rangeIterator.hasNext());

            record = rangeIterator.next();

            assertTrue(record.isDelta());
            assertEquals(360, record.getTimestampInMillis(0));
            assertEquals(360, record.getTimestampInMillis(1));
            assertEquals(5, record.getDecimalMantissa(2));
            assertEquals(-1, record.getDecimalExponent(2));
            assertEquals(-2, record.getLong(3));

            assertTrue(rangeIterator.hasNext());

            record = rangeIterator.next();

            assertTrue(record.isDelta());
            assertEquals(100, record.getTimestampInMillis(0));
            assertEquals(100, record.getTimestampInMillis(1));
            assertEquals(-4, record.getByte(2));

            assertTrue(rangeIterator.hasNext());

            record = rangeIterator.next();

            assertTrue(record.isDelta());
            assertEquals(240, record.getTimestampInMillis(0));
            assertEquals(240, record.getTimestampInMillis(1));
            assertEquals(5, record.getDecimalMantissa(2));
            assertEquals(-1, record.getDecimalExponent(2));
            assertEquals(5, record.getLong(3));

            assertFalse(rangeIterator.hasNext());
        }
    }

    @Test
    public void testWithTwoRecordTypeAndFiltering() throws IOException {

        long timestamp = TimeUtils.parseDateTime("2013-11-14 11:46:00.000");

        ReadableBuffer buffer = createBufferWithTwoRecordTypes(this.seriesDefinition, timestamp);

        BinaryTimeSeriesRecordIterator iterator = new BinaryTimeSeriesRecordIterator(this.seriesDefinition, buffer);

        Predicate expression = and(ge("timestamp", toMillisecondField((timestamp + 200) + "ms")),
                                   lt("timestamp", toMillisecondField((timestamp + 400) + "ms")));

        Filter<Record> filter = expression.toFilter(this.seriesDefinition);

        try (FilteringRecordIterator rangeIterator = new FilteringRecordIterator(this.seriesDefinition,
                                                                                 iterator,
                                                                                 filter)) {

            assertTrue(rangeIterator.hasNext());

            Record record = rangeIterator.next();

            assertFalse(record.isDelta());
            assertEquals(timestamp + 350, record.getTimestampInMillis(0));
            assertEquals(timestamp + 350, record.getTimestampInMillis(1));
            assertEquals(10, record.getByte(2));

            assertTrue(rangeIterator.hasNext());

            record = rangeIterator.next();

            assertFalse(record.isDelta());
            assertEquals(timestamp + 360, record.getTimestampInMillis(0));
            assertEquals(timestamp + 360, record.getTimestampInMillis(1));
            assertEquals(125, record.getDecimalMantissa(2));
            assertEquals(-1, record.getDecimalExponent(2));
            assertEquals(4, record.getLong(3));

            assertFalse(rangeIterator.hasNext());
        }
    }

    @Test
    public void testWithDeltasAndFullRecordsAndFiltering() throws IOException {

        long timestamp = TimeUtils.parseDateTime("2013-11-14 11:46:00.000");

        ReadableBuffer buffer = createBufferWithDeltasAndFullRecords(this.seriesDefinition, timestamp);

        BinaryTimeSeriesRecordIterator iterator = new BinaryTimeSeriesRecordIterator(this.seriesDefinition, buffer);

        Predicate expression = and(ge("timestamp", toMillisecondField((timestamp + 80) + "ms")),
                                   lt("timestamp", toMillisecondField((timestamp + 600) + "ms")));

        Filter<Record> filter = expression.toFilter(this.seriesDefinition);

        try (FilteringRecordIterator rangeIterator = new FilteringRecordIterator(this.seriesDefinition,
                                                                                 iterator,
                                                                                 filter)) {

            assertTrue(rangeIterator.hasNext());

            Record record = rangeIterator.next();

            assertFalse(record.isDelta());
            assertEquals(timestamp + 100, record.getTimestampInMillis(0));
            assertEquals(timestamp + 100, record.getTimestampInMillis(1));
            assertEquals(5, record.getByte(2));

            record = rangeIterator.next();

            assertFalse(record.isDelta());
            assertEquals(timestamp + 350, record.getTimestampInMillis(0));
            assertEquals(timestamp + 350, record.getTimestampInMillis(1));
            assertEquals(10, record.getByte(2));

            assertTrue(rangeIterator.hasNext());

            record = rangeIterator.next();

            assertTrue(record.isDelta());
            assertEquals(100, record.getTimestampInMillis(0));
            assertEquals(100, record.getTimestampInMillis(1));
            assertEquals(-4, record.getByte(2));

            assertFalse(rangeIterator.hasNext());
        }
    }

    @Test
    public void testWithNoDeltaAndNoFiltering() throws IOException {

        long timestamp = TimeUtils.parseDateTime("2013-11-14 11:46:00.000");

        ReadableBuffer buffer = createBufferWithOnlyFullRecords(this.seriesDefinition, timestamp);

        BinaryTimeSeriesRecordIterator iterator = new BinaryTimeSeriesRecordIterator(this.seriesDefinition, buffer);

        Predicate expression = and(ge("timestamp", toMillisecondField((timestamp - 100) + "ms")),
                                   lt("timestamp", toMillisecondField((timestamp + 500) + "ms")));

        Filter<Record> filter = expression.toFilter(this.seriesDefinition);

        try (FilteringRecordIterator rangeIterator = new FilteringRecordIterator(this.seriesDefinition,
                                                                                 iterator,
                                                                                 filter)) {

            assertTrue(rangeIterator.hasNext());

            Record record = rangeIterator.next();

            assertEquals(timestamp, record.getTimestampInMillis(0));
            assertEquals(timestamp, record.getTimestampInMillis(1));
            assertEquals(10, record.getByte(2));

            assertTrue(rangeIterator.hasNext());

            record = rangeIterator.next();

            assertEquals(timestamp + 100, record.getTimestampInMillis(0));
            assertEquals(timestamp + 100, record.getTimestampInMillis(1));
            assertEquals(5, record.getByte(2));

            assertTrue(rangeIterator.hasNext());

            record = rangeIterator.next();

            assertEquals(timestamp + 350, record.getTimestampInMillis(0));
            assertEquals(timestamp + 350, record.getTimestampInMillis(1));
            assertEquals(10, record.getByte(2));

            assertTrue(rangeIterator.hasNext());

            record = rangeIterator.next();

            assertEquals(timestamp + 450, record.getTimestampInMillis(0));
            assertEquals(timestamp + 450, record.getTimestampInMillis(1));
            assertEquals(6, record.getByte(2));

            assertFalse(rangeIterator.hasNext());
        }
    }

    @Test
    public void testWithRangeBeforeRecords() throws IOException {

        long timestamp = TimeUtils.parseDateTime("2013-11-14 11:46:00.000");

        ReadableBuffer buffer = createBufferWithOnlyFullRecords(this.seriesDefinition, timestamp);

        BinaryTimeSeriesRecordIterator iterator = new BinaryTimeSeriesRecordIterator(this.seriesDefinition, buffer);

        Predicate expression = and(ge("timestamp", toMillisecondField((timestamp - 100) + "ms")),
                                   lt("timestamp", toMillisecondField((timestamp + 500) + "ms")));

        Filter<Record> filter = expression.toFilter(this.seriesDefinition);

        try (FilteringRecordIterator rangeIterator = new FilteringRecordIterator(this.seriesDefinition,
                                                                                 iterator,
                                                                                 filter)) {

            assertTrue(rangeIterator.hasNext());

            Record record = rangeIterator.next();

            assertEquals(timestamp, record.getTimestampInMillis(0));
            assertEquals(timestamp, record.getTimestampInMillis(1));
            assertEquals(10, record.getByte(2));

            assertTrue(rangeIterator.hasNext());

            record = rangeIterator.next();

            assertEquals(timestamp + 100, record.getTimestampInMillis(0));
            assertEquals(timestamp + 100, record.getTimestampInMillis(1));
            assertEquals(5, record.getByte(2));

            assertTrue(rangeIterator.hasNext());

            record = rangeIterator.next();

            assertEquals(timestamp + 350, record.getTimestampInMillis(0));
            assertEquals(timestamp + 350, record.getTimestampInMillis(1));
            assertEquals(10, record.getByte(2));

            assertTrue(rangeIterator.hasNext());

            record = rangeIterator.next();

            assertEquals(timestamp + 450, record.getTimestampInMillis(0));
            assertEquals(timestamp + 450, record.getTimestampInMillis(1));
            assertEquals(6, record.getByte(2));

            assertFalse(rangeIterator.hasNext());
        }
    }

    @Test
    public void testWithRangeIncludingOnlyFirstRecord() throws IOException {

        long timestamp = TimeUtils.parseDateTime("2013-11-14 11:46:00.000");

        ReadableBuffer buffer = createBufferWithOnlyFullRecords(this.seriesDefinition, timestamp);

        BinaryTimeSeriesRecordIterator iterator = new BinaryTimeSeriesRecordIterator(this.seriesDefinition, buffer);

        Predicate expression = and(ge("timestamp", toMillisecondField((timestamp - 100) + "ms")),
                                   lt("timestamp", toMillisecondField((timestamp + 1) + "ms")));

        Filter<Record> filter = expression.toFilter(this.seriesDefinition);

        assertTrue(iterator.hasNext());
        
        try (FilteringRecordIterator rangeIterator = new FilteringRecordIterator(this.seriesDefinition,
                                                                                 iterator,
                                                                                 filter)) {

            assertTrue(rangeIterator.hasNext());

            Record record = rangeIterator.next();

            assertEquals(timestamp, record.getTimestampInMillis(0));
            assertEquals(timestamp, record.getTimestampInMillis(1));
            assertEquals(10, record.getByte(2));

            assertFalse(rangeIterator.hasNext());
        }
    }

    @Test
    public void testWithNoDeltaAndWithFiltering() throws IOException {

        long timestamp = TimeUtils.parseDateTime("2013-11-14 11:46:00.000");

        ReadableBuffer buffer = createBufferWithOnlyFullRecords(this.seriesDefinition, timestamp);

        BinaryTimeSeriesRecordIterator iterator = new BinaryTimeSeriesRecordIterator(this.seriesDefinition, buffer);

        Predicate expression = and(ge("timestamp", toMillisecondField((timestamp + 50) + "ms")),
                                   lt("timestamp", toMillisecondField((timestamp + 400) + "ms")));

        Filter<Record> filter = expression.toFilter(this.seriesDefinition);

        try (FilteringRecordIterator rangeIterator = new FilteringRecordIterator(this.seriesDefinition,
                                                                                 iterator,
                                                                                 filter)) {

            assertTrue(rangeIterator.hasNext());

            Record record = rangeIterator.next();

            assertEquals(timestamp + 100, record.getTimestampInMillis(0));
            assertEquals(timestamp + 100, record.getTimestampInMillis(1));
            assertEquals(5, record.getByte(2));

            assertTrue(rangeIterator.hasNext());

            record = rangeIterator.next();

            assertEquals(timestamp + 350, record.getTimestampInMillis(0));
            assertEquals(timestamp + 350, record.getTimestampInMillis(1));
            assertEquals(10, record.getByte(2));

            assertFalse(rangeIterator.hasNext());
        }
    }

    /**
     * Creates the buffer containing only full records.
     * 
     * @param this.seriesDefinition the time series definition
     * @param timestamp the time reference
     * @return the buffer containing only full records.
     * @throws IOException if an I/O problem occurs.
     */
    private static ReadableBuffer createBufferWithOnlyFullRecords(TimeSeriesDefinition seriesDefinition,
                                                                  long timestamp) throws IOException {

        TimeSeriesRecord[] records = seriesDefinition.newRecords();

        Buffer buffer = Buffers.allocate(200);

        records[0].setTimestampInMillis(0, timestamp);
        records[0].setTimestampInMillis(1, timestamp);
        records[0].setByte(2, 10);

        writeRecord(buffer, records[0]);

        records[0].setTimestampInMillis(0, timestamp + 100);
        records[0].setTimestampInMillis(1, timestamp + 100);
        records[0].setByte(2, 5);

        writeRecord(buffer, records[0]);

        records[0].setTimestampInMillis(0, timestamp + 350);
        records[0].setTimestampInMillis(1, timestamp + 350);
        records[0].setByte(2, 10);

        writeRecord(buffer, records[0]);

        records[0].setTimestampInMillis(0, timestamp + 450);
        records[0].setTimestampInMillis(1, timestamp + 450);
        records[0].setByte(2, 6);

        writeRecord(buffer, records[0]);

        Record header = new BlockHeaderBuilder(seriesDefinition).firstTimestamp(timestamp)
                                                                .lastTimestamp(timestamp + 450)
                                                                .compressedBlockSize(buffer.readableBytes())
                                                                .uncompressedBlockSize(buffer.readableBytes())
                                                                .recordCount(0, 4)
                                                                .build();

        Buffer headerBuffer = Buffers.allocate(RecordUtils.computeSerializedSize(header));
        writeRecord(headerBuffer, header);

        return Buffers.composite(headerBuffer, buffer);
    }

    /**
     * Creates the buffer containing deltas.
     * 
     * @param seriesDefinition the time series definition
     * @param timestamp the time reference
     * @return the buffer containing deltas.
     * @throws IOException if an I/O problem occurs.
     */
    private static ReadableBuffer createBufferWithDeltas(TimeSeriesDefinition seriesDefinition,
                                                         long timestamp) throws IOException {

        TimeSeriesRecord[] records = seriesDefinition.newRecords();

        Buffer buffer = Buffers.allocate(200);

        records[0].setTimestampInMillis(0, timestamp);
        records[0].setTimestampInMillis(1, timestamp);
        records[0].setByte(2, 10);

        writeRecord(buffer, records[0]);

        records[0].setDelta(true);
        records[0].setTimestampInMillis(0, 100);
        records[0].setTimestampInMillis(1, 100);
        records[0].setByte(2, -5);

        writeRecord(buffer, records[0]);

        records[0].setDelta(true);
        records[0].setTimestampInMillis(0, 250);
        records[0].setTimestampInMillis(1, 250);
        records[0].setByte(2, 5);

        writeRecord(buffer, records[0]);

        records[0].setDelta(true);
        records[0].setTimestampInMillis(0, 100);
        records[0].setTimestampInMillis(1, 100);
        records[0].setByte(2, -4);

        writeRecord(buffer, records[0]);

        Record header = new BlockHeaderBuilder(seriesDefinition).firstTimestamp(timestamp)
                                                                .lastTimestamp(timestamp + 450)
                                                                .compressedBlockSize(buffer.readableBytes())
                                                                .uncompressedBlockSize(buffer.readableBytes())
                                                                .recordCount(0, 4)
                                                                .build();

        Buffer headerBuffer = Buffers.allocate(RecordUtils.computeSerializedSize(header));
        writeRecord(headerBuffer, header);

        return Buffers.composite(headerBuffer, buffer);
    }

    /**
     * Creates the buffer containing deltas and full .
     * 
     * @param seriesDefinition the time series definition
     * @param timestamp the time reference
     * @return the buffer containing deltas.
     * @throws IOException if an I/O problem occurs.
     */
    private static ReadableBuffer createBufferWithDeltasAndFullRecords(TimeSeriesDefinition seriesDefinition,
                                                                       long timestamp) throws IOException {

        TimeSeriesRecord[] records = seriesDefinition.newRecords();

        Buffer buffer = Buffers.allocate(200);

        records[0].setTimestampInMillis(0, timestamp);
        records[0].setTimestampInMillis(1, timestamp);
        records[0].setByte(2, 10);

        writeRecord(buffer, records[0]);

        records[0].setDelta(true);
        records[0].setTimestampInMillis(0, 100);
        records[0].setTimestampInMillis(1, 100);
        records[0].setByte(2, -5);

        writeRecord(buffer, records[0]);

        records[0].setDelta(false);
        records[0].setTimestampInMillis(0, timestamp + 350);
        records[0].setTimestampInMillis(1, timestamp + 350);
        records[0].setByte(2, 10);

        writeRecord(buffer, records[0]);

        records[0].setDelta(true);
        records[0].setTimestampInMillis(0, 100);
        records[0].setTimestampInMillis(1, 100);
        records[0].setByte(2, -4);

        writeRecord(buffer, records[0]);
        Record header = new BlockHeaderBuilder(seriesDefinition).firstTimestamp(timestamp)
                                                                .lastTimestamp(timestamp + 450)
                                                                .compressedBlockSize(buffer.readableBytes())
                                                                .uncompressedBlockSize(buffer.readableBytes())
                                                                .recordCount(0, 4)
                                                                .build();

        Buffer headerBuffer = Buffers.allocate(RecordUtils.computeSerializedSize(header));
        writeRecord(headerBuffer, header);

        return Buffers.composite(headerBuffer, buffer);
    }

    /**
     * Creates the buffer containing 2 records types.
     * 
     * @param seriesDefinition the time series definition
     * @param timestamp the time reference
     * @return the buffer containing deltas.
     * @throws IOException if an I/O problem occurs.
     */
    private static ReadableBuffer createBufferWithTwoRecordTypes(TimeSeriesDefinition seriesDefinition,
                                                                 long timestamp) throws IOException {

        TimeSeriesRecord[] records = seriesDefinition.newRecords();

        Buffer buffer = Buffers.allocate(200);

        records[0].setTimestampInMillis(0, timestamp);
        records[0].setTimestampInMillis(1, timestamp);
        records[0].setByte(2, 10);

        writeRecord(buffer, records[0]);

        records[1].setTimestampInMillis(0, timestamp);
        records[1].setTimestampInMillis(1, timestamp);
        records[1].setDecimal(2, 12, 0);
        records[1].setLong(3, 6);

        writeRecord(buffer, records[1]);

        records[0].setDelta(true);
        records[0].setTimestampInMillis(0, 100);
        records[0].setTimestampInMillis(1, 100);
        records[0].setByte(2, -5);

        writeRecord(buffer, records[0]);

        records[0].setDelta(true);
        records[0].setTimestampInMillis(0, 250);
        records[0].setTimestampInMillis(1, 250);
        records[0].setByte(2, 5);

        writeRecord(buffer, records[0]);

        records[1].setDelta(true);
        records[1].setTimestampInMillis(0, 360);
        records[1].setTimestampInMillis(1, 360);
        records[1].setDecimal(2, 5, -1);
        records[1].setLong(3, -2);

        writeRecord(buffer, records[1]);

        records[0].setDelta(true);
        records[0].setTimestampInMillis(0, 100);
        records[0].setTimestampInMillis(1, 100);
        records[0].setByte(2, -4);

        writeRecord(buffer, records[0]);

        records[1].setDelta(true);
        records[1].setTimestampInMillis(0, 240);
        records[1].setTimestampInMillis(1, 240);
        records[1].setDecimal(2, 5, -1);
        records[1].setLong(3, 5);

        writeRecord(buffer, records[1]);
        Record header = new BlockHeaderBuilder(seriesDefinition).firstTimestamp(timestamp)
                                                                .lastTimestamp(timestamp + 600)
                                                                .compressedBlockSize(buffer.readableBytes())
                                                                .uncompressedBlockSize(buffer.readableBytes())
                                                                .recordCount(0, 4)
                                                                .recordCount(1, 3)
                                                                .build();

        Buffer headerBuffer = Buffers.allocate(RecordUtils.computeSerializedSize(header));
        writeRecord(headerBuffer, header);

        return Buffers.composite(headerBuffer, buffer);
    }
}
