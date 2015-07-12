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
import io.horizondb.io.ReadableBuffer;
import io.horizondb.io.buffers.Buffers;
import io.horizondb.io.compression.CompressionType;
import io.horizondb.model.core.RecordListBuilder;
import io.horizondb.model.core.RecordUtils;
import io.horizondb.model.core.util.TimeUtils;
import io.horizondb.model.schema.FieldType;
import io.horizondb.model.schema.RecordTypeDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Benjamin
 *
 */
public class BlockHeaderUtilsTest {

    /**
     * The time reference.
     */
    private static long TIME_IN_MILLIS = TimeUtils.parseDateTime("2013-11-26 12:00:00.000");

    /**
     * The time reference.
     */
    private static long TIME_IN_NANOS = TimeUnit.MILLISECONDS.toNanos(TIME_IN_MILLIS);

    @Test
    public void testSerializationWithLZ4Compression() throws IOException {
        
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

        int blockSize = RecordUtils.computeSerializedSize(firstBlock);

        Buffer uncompressedFirstBlock = Buffers.allocate(blockSize);
        RecordUtils.writeRecords(uncompressedFirstBlock, firstBlock);

        ReadableBuffer compressedBlock = CompressionType.LZ4.newCompressor().compress(uncompressedFirstBlock);
        int compressedBlockSize = compressedBlock.readableBytes();

        TimeSeriesRecord blockHeader = def.newBlockHeader();
        BlockHeaderUtils.setFirstTimestamp(blockHeader, TIME_IN_NANOS + 12000700);
        BlockHeaderUtils.setLastTimestamp(blockHeader, TIME_IN_NANOS + 13004400);
        BlockHeaderUtils.setCompressionType(blockHeader, CompressionType.LZ4);
        BlockHeaderUtils.setCompressedBlockSize(blockHeader, compressedBlockSize);
        BlockHeaderUtils.setUncompressedBlockSize(blockHeader, blockSize);
        BlockHeaderUtils.setRecordCount(blockHeader, 0, 3);
        
        Buffer buffer = Buffers.allocate(blockHeader.computeSerializedSize());
        blockHeader.writeTo(buffer);
        
        BinaryTimeSeriesRecord newBlockHeader = def.newBinaryBlockHeader();
        newBlockHeader.fill(buffer);
        
        assertEquals(TIME_IN_NANOS + 12000700, BlockHeaderUtils.getFirstTimestamp(newBlockHeader));
        assertEquals(TIME_IN_NANOS + 13004400, BlockHeaderUtils.getLastTimestamp(newBlockHeader));
        assertEquals(CompressionType.LZ4, BlockHeaderUtils.getCompressionType(newBlockHeader));
        assertEquals(compressedBlockSize, BlockHeaderUtils.getCompressedBlockSize(newBlockHeader));
        assertEquals(blockSize, BlockHeaderUtils.getUncompressedBlockSize(newBlockHeader));
        assertEquals(3, BlockHeaderUtils.getRecordCount(blockHeader, 0));
    }

}
