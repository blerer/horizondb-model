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
import io.horizondb.model.core.DataBlock;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.RecordListBuilder;
import io.horizondb.model.core.RecordUtils;
import io.horizondb.model.core.ResourceIterator;
import io.horizondb.model.core.records.BlockHeaderBuilder;
import io.horizondb.model.core.records.BlockHeaderUtils;
import io.horizondb.model.core.records.TimeSeriesRecord;
import io.horizondb.model.core.util.TimeUtils;
import io.horizondb.model.schema.FieldType;
import io.horizondb.model.schema.RecordTypeDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BinaryBlockIteratorTest {

    /**
     * The time reference.
     */
    private static long TIME_IN_MILLIS = TimeUtils.parseDateTime("2013-11-26 12:00:00.000");

    /**
     * The time reference.
     */
    private static long TIME_IN_NANOS = TimeUnit.MILLISECONDS.toNanos(TIME_IN_MILLIS);

    @Test
    public void testNextWithOnlyOneBlock() throws IOException {

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

        List<TimeSeriesRecord> records = new RecordListBuilder(def).newRecord("exchangeState")
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

        int serializedSize = RecordUtils.computeSerializedSize(records);

        TimeSeriesRecord blockHeader = new BlockHeaderBuilder(def).firstTimestampInNanos(TIME_IN_NANOS + 12000700)
                                                                  .lastTimestampInNanos(TIME_IN_NANOS + 13004400)
                                                                  .compressedBlockSize(serializedSize)
                                                                  .uncompressedBlockSize(serializedSize)
                                                                  .recordCount(0, 5)
                                                                  .build();

        List<Record> list = new ArrayList<>();
        list.add(blockHeader);
        list.addAll(records);

        int headerSize = RecordUtils.computeSerializedSize(blockHeader);
        Buffer buffer = Buffers.allocate(headerSize + serializedSize);
        RecordUtils.writeRecords(buffer, list);
        ReadableBuffer expectedData = buffer.duplicate().slice(headerSize, buffer.readableBytes() - headerSize);

        try (ResourceIterator<DataBlock> iterator = new BinaryBlockIterator(def, buffer)) {
            assertTrue(iterator.hasNext());
            DataBlock block = iterator.next();

            Record header = block.getHeader();
            assertEquals(TIME_IN_NANOS + 12000700, BlockHeaderUtils.getFirstTimestampInNanos(header));
            assertEquals(TIME_IN_NANOS + 13004400, BlockHeaderUtils.getLastTimestampInNanos(header));
            assertEquals(serializedSize, BlockHeaderUtils.getCompressedBlockSize(header));
            assertEquals(serializedSize, BlockHeaderUtils.getUncompressedBlockSize(header));
            assertEquals(5, BlockHeaderUtils.getRecordCount(header, 0));

            assertEquals(expectedData, block.getData());

            assertFalse(iterator.hasNext());
        }
    }

}
