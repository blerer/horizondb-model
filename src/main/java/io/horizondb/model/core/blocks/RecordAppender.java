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
package io.horizondb.model.core.blocks;

import io.horizondb.io.Buffer;
import io.horizondb.io.BufferAllocator;
import io.horizondb.io.buffers.CompositeBuffer;
import io.horizondb.model.core.DataBlock;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.records.TimeSeriesRecord;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;

import static io.horizondb.io.encoding.VarInts.computeUnsignedIntSize;
import static io.horizondb.io.encoding.VarInts.writeByte;
import static io.horizondb.io.encoding.VarInts.writeUnsignedInt;
import static io.horizondb.model.core.records.BlockHeaderUtils.getCompressedBlockSize;
import static io.horizondb.model.core.records.BlockHeaderUtils.getRecordCount;
import static io.horizondb.model.core.records.BlockHeaderUtils.incrementRecordCount;
import static io.horizondb.model.core.records.BlockHeaderUtils.setCompressedBlockSize;
import static io.horizondb.model.core.records.BlockHeaderUtils.setFirstTimestamp;
import static io.horizondb.model.core.records.BlockHeaderUtils.getFirstTimestamp;
import static io.horizondb.model.core.records.BlockHeaderUtils.setLastTimestamp;

/**
 * Utility class to append records to the end of a {@link DataBlock}.
 */
public final class RecordAppender {

    /**
     * The maximum block size in bytes.
     */
    private final int blockSizeInBytes;

    /**
     * The buffer allocator.
     */
    private final BufferAllocator allocator;

    /**
     * The block header.
     */
    private final TimeSeriesRecord header;

    /**
     * The last records for each type.
     */
    private final TimeSeriesRecord[] lastRecords;

    /**
     * The buffer containing the data.
     */
    private final CompositeBuffer buffer;

    /**
     * @param blockSizeInBytes
     * @param allocator
     * @param header
     * @param lastRecords
     * @param buffer
     * @throws IOException 
     */
    public RecordAppender(TimeSeriesDefinition definition,
                          BufferAllocator allocator,
                          TimeSeriesRecord[] lastRecords,
                          DataBlock block) throws IOException {

        this.blockSizeInBytes = definition.getBlockSizeInBytes();
        this.allocator = allocator;
        this.header = block.getHeader().toTimeSeriesRecord();
        this.lastRecords = lastRecords;
        this.buffer = (CompositeBuffer) block.getData();
    }
    
    public RecordAppender(TimeSeriesDefinition definition,
                          BufferAllocator allocator,
                          TimeSeriesRecord[] lastRecords) {

        this.blockSizeInBytes = definition.getBlockSizeInBytes();
        this.allocator = allocator;
        this.header = definition.newBlockHeader();
        this.lastRecords = lastRecords;
        this.buffer = new CompositeBuffer();
    }

    /**
     * Appends the specified record to the block.
     *
     * @param record the record to append
     * @return <code>true</code> if their was enough space left to append the record, <code>false</code> otherwise.
     * @throws IOException if an I/O error occurs
     */
    public boolean append(Record record) throws IOException {

        if (record.isDelta()) {
            return appendDelta(record);
        }

        return appendFullRecord(record);
    }

    /**
     * Appends the specified full record to the block.
     *
     * @param record the full record to append
     * @return <code>true</code> if their was enough space left to append the record, <code>false</code> otherwise.
     * @throws IOException if an I/O error occurs
     */
    private boolean appendFullRecord(Record record) throws IOException {

        int type = record.getType();

        if (getRecordCount(this.header, type) == 0) {
            this.lastRecords[type] = record.toTimeSeriesRecord();
            return doAppend(record);
        }

        Record delta = toDelta(record);
        this.lastRecords[type].add(delta);
        return doAppend(delta);
    }

    /**
     * Appends the specified delta to the block.
     *
     * @param record the delta to append
     * @return <code>true</code> if their was enough space left to append the record, <code>false</code> otherwise.
     * @throws IOException if an I/O error occurs
     */
    private boolean appendDelta(Record record) throws IOException {
        int type = record.getType();

        this.lastRecords[type].add(record);

        if (getRecordCount(this.header, type) == 0) {
            return doAppend(this.lastRecords[type]);
        }
        return doAppend(record);
    }

    /**
     * Returns the data block to which the delta have been appended.
     * @return the data block to which the delta have been appended.
     */
    public DataBlock getDataBlock() {
        return new DefaultDataBlock(this.header, this.buffer);
    }

    /**
     * Converts the specified record into a delta.
     * 
     * @param record the record to convert
     * @return the delta between specified record and the previous one.
     * @throws IOException if an I/O problem occurs while computing the delta
     */
    private Record toDelta(Record record) throws IOException {

        TimeSeriesRecord delta = record.toTimeSeriesRecord();
        delta.subtract(this.lastRecords[record.getType()]);
        return delta;
    }
        
    /**
     * Appends the specified record to the block.
     *
     * @param record the record to appends
     * @return <code>true</code> if their was enough space left to append the record, <code>false</code> otherwise.
     * @throws IOException if an I/O problem occurs
     */
    private boolean doAppend(Record record) throws IOException {

        int recordSize = record.computeSerializedSize();
        int totalSize = 1 + computeUnsignedIntSize(recordSize) + recordSize;

        if (getCompressedBlockSize(this.header) + totalSize > this.blockSizeInBytes) {
            return false;
        }

        Buffer bytes = serializeRecord(this.allocator.allocate(totalSize), record, recordSize);

        int type = record.getType();

        if (getFirstTimestamp(this.header) == 0) {
            setFirstTimestamp(this.header, this.lastRecords[type]);
        }

        setLastTimestamp(this.header, this.lastRecords[type]);
        incrementRecordCount(this.header, type);

        this.buffer.addBytes(bytes);

        setCompressedBlockSize(this.header, getCompressedBlockSize(this.header) + totalSize);
        return true; 
    }

    /**
     * Serializes the specified record.
     * 
     * @param allocator the buffer allocator used to allocate the returned <code>Buffer</code>
     * @param record the record to serialize
     * @return a buffer containing the serialized record
     * @throws IOException if an I/O problem occurs
     */
    private static Buffer serializeRecord(Buffer buffer,
                                          Record record,
                                          int recordSize) throws IOException {

        writeByte(buffer, record.getType());
        writeUnsignedInt(buffer, recordSize);
        record.writeTo(buffer);

        return buffer;
    }
}