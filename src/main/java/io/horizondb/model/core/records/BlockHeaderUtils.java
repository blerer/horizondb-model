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

import io.horizondb.io.compression.CompressionType;
import io.horizondb.model.core.Counter;
import io.horizondb.model.core.Record;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static io.horizondb.model.core.Record.TIMESTAMP_FIELD_INDEX;

/**
 * Utility methods to work with block header. 
 * 
 * @author Benjamin
 *
 */
public final class BlockHeaderUtils {
    
    /**
     * The index of the last timestamp of the block.
     */
    private static final int LAST_TIMESTAMP_INDEX = 1;
    
    /**
     * The index of block size. 
     */
    private static final int BLOCK_SIZE_INDEX = 2;
        
    /**
     * The index of the compression used to compress the block data.
     */
    private static final int COMPRESSION_TYPE_INDEX = 3;

    /**
     * The offset of the counter indices.
     */
    private static final int RECORD_COUNTERS_OFFSET = 4;

    /**
     * Sets the first timestamp of the block.
     * 
     * @param header the block header 
     * @param record the first record of the block
     * @throws IOException if an I/O problem occurs
     */
    public static void setFirstTimestamp(TimeSeriesRecord header, Record record) throws IOException {
        record.getField(TIMESTAMP_FIELD_INDEX).copyTo(header.getField(TIMESTAMP_FIELD_INDEX));
    }
    
    /**
     * Returns the first timestamp of the block.
     * 
     * @param header the block header 
     * @param timestampInNanos the time stamp in nanoseconds
     * @throws IOException if an I/O problem occurs
     */
    public static void setFirstTimestampInNanos(TimeSeriesRecord header, long timestampInNanos) {
        header.setTimestamp(TIMESTAMP_FIELD_INDEX, timestampInNanos, TimeUnit.NANOSECONDS);
    }
    
    /**
     * Sets the first timestamp of the block.
     * 
     * @param header the block header 
     * @param record the first record of the block
     * @throws IOException if an I/O problem occurs
     */
    public static long getFirstTimestampInNanos(TimeSeriesRecord header) {
        return header.getTimestampInNanos(TIMESTAMP_FIELD_INDEX);
    }
    
    /**
     * Sets the last timestamp of the block
     * 
     * @param header the block header
     * @param record the new last record of the block
     * @throws IOException if an I/O problem occurs
     */
    public static void setLastTimestamp(TimeSeriesRecord header, Record record) throws IOException {
        
        if (record.isDelta()) {    
            header.getField(LAST_TIMESTAMP_INDEX).add(record.getField(TIMESTAMP_FIELD_INDEX));
        } else {
            record.getField(TIMESTAMP_FIELD_INDEX).copyTo(header.getField(1));
            header.getField(LAST_TIMESTAMP_INDEX).subtract(header.getField(TIMESTAMP_FIELD_INDEX));
        }
    }

    /**
     * Sets the last timestamp of the block. This method must be called after the first timestamp has been set.
     * 
     * @param header the block header 
     * @param timestampInNanos the last in nanoseconds of the block
     * @throws IOException if an I/O problem occurs
     */
    public static void setLastTimestampInNanos(TimeSeriesRecord header, long timestampInNanos) {
        long delta = timestampInNanos - getFirstTimestampInNanos(header);
        header.setTimestamp(LAST_TIMESTAMP_INDEX, delta, TimeUnit.NANOSECONDS);
    }
    
    /**
     * Sets the block size.
     * 
     * @param header the block header
     * @param size the new block size
     */
    public static void setBlockSize(TimeSeriesRecord header, int size) {
        header.setInt(BLOCK_SIZE_INDEX, size);
    }

    /**
     * Increments the counter of the number of records of the specified type.
     * 
     * @param header the block header
     * @param type the record type
     */
    public static void incrementRecordCount(TimeSeriesRecord header, int type) {
        ((Counter) header.getField(RECORD_COUNTERS_OFFSET + type)).increment();
    }
    
    /**
     * Return the number of record from the specified type within the block.
     * 
     * @param header the block header
     * @param type the record type
     * @return the number of record from the specified type within the block
     */
    public static int getRecordCount(TimeSeriesRecord header, int type) {
        
        return header.getField(RECORD_COUNTERS_OFFSET + type).getInt();
    }
    
    /**
     * Returns the compression type used to compress the data of the block. 
     * 
     * @param header the block header
     * @return the compression type used to compress the data of the block. 
     */
    public static void setCompressionType(TimeSeriesRecord header, CompressionType type) {
        
        header.setByte(COMPRESSION_TYPE_INDEX, type.toByte());
    }
    
    /**
     * Returns the compression type used to compress the data of the block. 
     * 
     * @param header the block header
     * @return the compression type used to compress the data of the block. 
     */
    public static CompressionType getCompressionType(TimeSeriesRecord header) {
        
        return CompressionType.toCompressionType(header.getByte(COMPRESSION_TYPE_INDEX));
    }

    /**
     * Sets the number of records from the specified type.
     * 
     * @param header the block header
     * @param type the record type 
     * @param count the record count
     */
    public static void setRecordCount(TimeSeriesRecord header, int type, int count) {
        
        header.setInt(RECORD_COUNTERS_OFFSET + type, count);
    }
}
