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
package io.horizondb.model.core;

import io.horizondb.model.core.blocks.DataBlockBuilder;
import io.horizondb.model.core.util.TimeUtils;
import io.horizondb.model.schema.PartitionType;
import io.horizondb.model.schema.RecordTypeDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Range;

import static io.horizondb.model.core.records.BlockHeaderUtils.getRecordCount;
import static io.horizondb.model.schema.FieldType.MILLISECONDS_TIMESTAMP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class AbstractDataBlockTest {

    private TimeSeriesDefinition def;

    @Before
    public void setUp() throws Exception {

        RecordTypeDefinition exchangeStateType = RecordTypeDefinition.newBuilder("exchangeState")
                                                                     .addMillisecondTimestampField("timestampInMillis")
                                                                     .addByteField("status")
                                                                     .build();
        

        RecordTypeDefinition tradeType = RecordTypeDefinition.newBuilder("trade")
                                                             .addMillisecondTimestampField("timestampInMillis")
                                                             .addDecimalField("price")
                                                             .build();

        this.def = TimeSeriesDefinition.newBuilder("test")
                                       .timeUnit(TimeUnit.MILLISECONDS)
                                       .addRecordType(exchangeStateType)
                                       .addRecordType(tradeType)
                                       .partitionType(PartitionType.BY_DAY)
                                       .build();
    }

    @After
    public void tearDown() throws Exception {

        this.def = null;
    }
    
    @Test
    public void testSplitWithBlockWithinOnePartition() throws IOException {

        Range<Field> range = MILLISECONDS_TIMESTAMP.range("'2013-11-26 00:00:00.000'", "'2013-11-27 00:00:00.000'");

        long timestamp = TimeUtils.parseDateTime("2013-11-26 12:32:12.000");

        DataBlock block = new DataBlockBuilder(this.def).newRecord("exchangeState")
                                                        .setTimestampInMillis(0, timestamp)
                                                        .setTimestampInMillis(1, timestamp)
                                                        .setByte(2, 10)
                                                        .newRecord("exchangeState")
                                                        .setTimestampInMillis(0, timestamp + 100)
                                                        .setTimestampInMillis(1, timestamp + 100)
                                                        .setByte(2, 5)
                                                        .newRecord("exchangeState")
                                                        .setTimestampInMillis(0, timestamp + 350)
                                                        .setTimestampInMillis(1, timestamp + 350)
                                                        .setByte(2, 10)
                                                        .build();

        Map<Range<Field>, DataBlock> map = block.split(this.def).asMapOfRanges();
        assertEquals(1, map.size());
        assertTrue(map.containsKey(range));
        assertEquals(3, getRecordCount(map.get(range).getHeader(), 0));
    }

    @Test
    public void testSplitWithBlockWithinTwoPartitions() throws IOException {

        Range<Field> first = MILLISECONDS_TIMESTAMP.range("'2013-11-26 00:00:00.000'", "'2013-11-27 00:00:00.000'");
        Range<Field> second = MILLISECONDS_TIMESTAMP.range("'2013-11-27 00:00:00.000'", "'2013-11-28 00:00:00.000'");

        long timestamp = TimeUtils.parseDateTime("2013-11-26 12:32:12.000");
        long timestamp2 = TimeUtils.parseDateTime("2013-11-27 12:00:00.000");

        DataBlock block = new DataBlockBuilder(this.def).newRecord("exchangeState")
                                                        .setTimestampInMillis(0, timestamp)
                                                        .setTimestampInMillis(1, timestamp)
                                                        .setByte(2, 10)
                                                        .newRecord("exchangeState")
                                                        .setTimestampInMillis(0, timestamp + 100)
                                                        .setTimestampInMillis(1, timestamp + 100)
                                                        .setByte(2, 5)
                                                        .newRecord("exchangeState")
                                                        .setTimestampInMillis(0, timestamp2)
                                                        .setTimestampInMillis(1, timestamp2)
                                                        .setByte(2, 10)
                                                        .build();

        Map<Range<Field>, DataBlock> map = block.split(this.def).asMapOfRanges();
        assertEquals(2, map.size());
        assertTrue(map.containsKey(first));
        assertEquals(2, getRecordCount(map.get(first).getHeader(), 0));
        assertTrue(map.containsKey(second));
        assertEquals(1, getRecordCount(map.get(second).getHeader(), 0));
    }
    
    @Test
    public void testSplitWithBlockWithinTreePartitions() throws IOException {

        Range<Field> first = MILLISECONDS_TIMESTAMP.range("'2013-11-26 00:00:00.000'", "'2013-11-27 00:00:00.000'");
        Range<Field> second = MILLISECONDS_TIMESTAMP.range("'2013-11-27 00:00:00.000'", "'2013-11-28 00:00:00.000'");
        Range<Field> third = MILLISECONDS_TIMESTAMP.range("'2013-11-28 00:00:00.000'", "'2013-11-29 00:00:00.000'");

        long timestamp = TimeUtils.parseDateTime("2013-11-26 12:32:12.000");
        long timestamp2 = TimeUtils.parseDateTime("2013-11-27 12:00:00.000");
        long timestamp3 = TimeUtils.parseDateTime("2013-11-28 14:00:00.000");

        DataBlock block = new DataBlockBuilder(this.def).newRecord("exchangeState")
                                                        .setTimestampInMillis(0, timestamp)
                                                        .setTimestampInMillis(1, timestamp)
                                                        .setByte(2, 10)
                                                        .newRecord("exchangeState")
                                                        .setTimestampInMillis(0, timestamp + 100)
                                                        .setTimestampInMillis(1, timestamp + 100)
                                                        .setByte(2, 5)
                                                        .newRecord("trade")
                                                        .setTimestampInMillis(0, timestamp2)
                                                        .setTimestampInMillis(1, timestamp2)
                                                        .setDecimal(2, 125, 1)
                                                        .newRecord("exchangeState")
                                                        .setTimestampInMillis(0, timestamp3)
                                                        .setTimestampInMillis(1, timestamp3)
                                                        .setByte(2, 10)
                                                        .build();

        Map<Range<Field>, DataBlock> map = block.split(this.def).asMapOfRanges();
        assertEquals(3, map.size());
        assertTrue(map.containsKey(first));
        assertEquals(2, getRecordCount(map.get(first).getHeader(), 0));
        assertEquals(0, getRecordCount(map.get(first).getHeader(), 1));
        assertTrue(map.containsKey(second));
        assertEquals(0, getRecordCount(map.get(second).getHeader(), 0));
        assertEquals(1, getRecordCount(map.get(second).getHeader(), 1));
        assertTrue(map.containsKey(third));
        assertEquals(1, getRecordCount(map.get(third).getHeader(), 0));
        assertEquals(0, getRecordCount(map.get(third).getHeader(), 1));
    }
}
