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
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.fields.ImmutableField;
import io.horizondb.model.core.fields.TimestampField;

import java.io.IOException;

import com.google.common.collect.Range;

import static io.horizondb.model.core.Record.TIMESTAMP_FIELD_INDEX;

/**
 * Utility methods to work with block header.
 */
public final class BlockHeaderUtils {

    /**
     * The index of the last timestamp of the block.
     */
    public static final int LAST_TIMESTAMP_INDEX = 1;

    /**
     * The index of the compressed block size.
     */
    public static final int COMPRESSED_BLOCK_SIZE_INDEX = 2;

    /**
     * The index of the uncompressed block size.
     */
    public static final int UNCOMPRESSED_BLOCK_SIZE_INDEX = 3;

    /**
     * The index of the compression used to compress the block data.
     */
    public static final int COMPRESSION_TYPE_INDEX = 4;

    /**
     * The offset of the counter indices.
     */
    public static final int RECORD_COUNTERS_OFFSET = 5;

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
     * Sets the first timestamp of the block.
     * 
     * @param header the block header
     * @param timestamp the time stamp
     * @throws IOException if an I/O problem occurs
     */
    public static void setFirstTimestamp(TimeSeriesRecord header, long timestamp) {
        TimestampField field = (TimestampField) header.getField(TIMESTAMP_FIELD_INDEX);
        field.setTimestamp(timestamp, field.getTimeUnit());
    }

    /**
     * Gets the first timestamp of the block.
     * 
     * @param header the block header
     * @throws IOException if an I/O problem occurs
     */
    public static long getFirstTimestamp(Record header) throws IOException {
        TimestampField field = (TimestampField) header.getField(TIMESTAMP_FIELD_INDEX);
        return field.getTimestampIn(field.getTimeUnit());
    }

    /**
     * Returns the field containing the first timestamp of the block.
     * 
     * @param header the block header
     * @throws IOException if an I/O problem occurs
     */
    public static Field getFirstTimestampField(Record header) throws IOException {
        return ImmutableField.of(header.getField(TIMESTAMP_FIELD_INDEX));
    }

    /**
     * Returns the time range that contains the block.
     * 
     * @param header the block header
     * @return the time range that contains the block.
     */
    public static Range<Field> getRange(Record header) throws IOException {
        return Range.closed(getFirstTimestampField(header), getLastTimestampField(header));
    }

    /**
     * Returns the field containing the last timestamp of the block.
     * 
     * @param header the block header
     * @throws IOException if an I/O problem occurs
     */
    public static Field getLastTimestampField(Record header) throws IOException {

        return header.getField(TIMESTAMP_FIELD_INDEX).newInstance().add(header.getField(LAST_TIMESTAMP_INDEX));
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
     * @param timestamp the last timestamp of the block
     * @throws IOException if an I/O problem occurs
     */
    public static void setLastTimestamp(TimeSeriesRecord header, long timestamp) throws IOException {
        TimestampField field = (TimestampField) header.getField(TIMESTAMP_FIELD_INDEX);
        long delta = timestamp - getFirstTimestamp(header);
        header.setTimestamp(LAST_TIMESTAMP_INDEX, delta, field.getTimeUnit());
    }

    /**
     * Returns the last timestamp of the block.
     * 
     * @param header the block header
     * @return the last timestamp of the block
     * @throws IOException if an I/O problem occurs
     */
    public static long getLastTimestamp(Record header) throws IOException {
        TimestampField field = (TimestampField) header.getField(LAST_TIMESTAMP_INDEX);
        return getFirstTimestamp(header) + field.getTimestampIn(field.getTimeUnit());
    }

    /**
     * Sets the compressed block size.
     * 
     * @param header the block header
     * @param size the new compressed block size
     */
    public static void setCompressedBlockSize(TimeSeriesRecord header, int size) {
        header.setInt(COMPRESSED_BLOCK_SIZE_INDEX, size);
    }

    /**
     * Sets the uncompressed block size.
     * 
     * @param header the block header
     * @param size the new uncompressed block size
     * @throws IOException if an I/O problem occurs
     */
    public static void setUncompressedBlockSize(TimeSeriesRecord header, int size) throws IOException {
        int delta = size - getCompressedBlockSize(header);
        header.setInt(UNCOMPRESSED_BLOCK_SIZE_INDEX, delta);
    }

    /**
     * Returns the compressed block size.
     * 
     * @param header the header
     * @return the compressed block size
     * @throws IOException if an I/O problem occurs
     */
    public static int getCompressedBlockSize(Record header) throws IOException {
        return header.getInt(COMPRESSED_BLOCK_SIZE_INDEX);
    }

    /**
     * Returns the uncompressed block size.
     * 
     * @param header the header
     * @return the uncompressed block size
     * @throws IOException if an I/O problem occurs
     */
    public static int getUncompressedBlockSize(Record header) throws IOException {
        return getCompressedBlockSize(header) + header.getInt(UNCOMPRESSED_BLOCK_SIZE_INDEX);
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
     * @throws IOException if an I/O problem occurs
     */
    public static int getRecordCount(Record header, int type) throws IOException {

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
     * @throws IOException if an I/O problem occurs
     */
    public static CompressionType getCompressionType(Record header) throws IOException {
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
