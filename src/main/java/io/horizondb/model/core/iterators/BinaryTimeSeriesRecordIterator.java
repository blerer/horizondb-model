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
import io.horizondb.io.ByteReader;
import io.horizondb.io.ReadableBuffer;
import io.horizondb.io.buffers.Buffers;
import io.horizondb.io.buffers.CompositeBuffer;
import io.horizondb.io.compression.CompressionType;
import io.horizondb.io.compression.Decompressor;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Filter;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.RecordIterator;
import io.horizondb.model.core.fields.TimestampField;
import io.horizondb.model.core.filters.Filters;
import io.horizondb.model.core.records.BinaryTimeSeriesRecord;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.Closeable;
import java.io.IOException;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

import static io.horizondb.model.core.records.BlockHeaderUtils.getCompressedBlockSize;
import static io.horizondb.model.core.records.BlockHeaderUtils.getCompressionType;
import static io.horizondb.model.core.records.BlockHeaderUtils.getFirstTimestampField;
import static io.horizondb.model.core.records.BlockHeaderUtils.getLastTimestampField;
import static io.horizondb.model.core.records.BlockHeaderUtils.getUncompressedBlockSize;
import static org.apache.commons.lang.Validate.notNull;

/**
 * <code>RecordIterator</code> for <code>BinaryTimeSeriesRecord</code>s.
 * 
 * @author Benjamin
 * 
 */
public final class BinaryTimeSeriesRecordIterator extends AbstractRecordIterator<BinaryTimeSeriesRecord> {

    /**
     * The block header record.
     */
    private final BinaryTimeSeriesRecord blockHeader;
    
    /**
     * The records per type.
     */
    private final BinaryTimeSeriesRecord[] records;

    /**
     * The ByteReader reader containing the records data.
     */
    private final ByteReader reader;
    
    /**
     * The timestamp ranges for which data must be returned.
     */
    private final RangeSet<Field> rangeSet;
    
    /**
     * The decompressor used to uncompress the blocks.
     */
    private Decompressor decompressor;
    
    /**
     * The buffer used to store the compressed block.
     */
    private ReadableBuffer blockBuffer;

    public BinaryTimeSeriesRecordIterator(TimeSeriesDefinition definition, ByteReader reader) {
        
        this(definition, reader, TimestampField.ALL);
    }
    
    public BinaryTimeSeriesRecordIterator(TimeSeriesDefinition definition, ByteReader reader, RangeSet<Field> rangeSet) {
        
        this(definition, reader, rangeSet, Filters.<String>noop());
    }
    
    public BinaryTimeSeriesRecordIterator(TimeSeriesDefinition definition, 
                                          ByteReader reader, 
                                          RangeSet<Field> rangeSet, 
                                          Filter<String> filter) {
        
        this.blockHeader = definition.newBinaryBlockHeader();
        this.records = definition.newBinaryRecords(filter);
        this.reader = reader;
        this.rangeSet = rangeSet;
    }


    /**    
     * {@inheritDoc}
     */
    @Override
    protected void computeNext() throws IOException {
        
        while (this.reader.isReadable() || isBlockBufferReadable()) {

            ByteReader in = getCurrentInput();

            while (in.isReadable()) {

                int type = in.readByte();
                int length = VarInts.readUnsignedInt(in);

                ReadableBuffer slice = in.slice(length);

                if (isBlockHeader(type)) {

                    this.blockHeader.fill(slice.duplicate());

                    ReadableBuffer compressedBlock = this.reader.slice(getCompressedBlockSize(this.blockHeader));
                   
                    Range<Field> range = Range.closed(getFirstTimestampField(this.blockHeader),
                                                      getLastTimestampField(this.blockHeader));
                    
                    if (this.rangeSet.subRangeSet(range).isEmpty()) {
                        this.blockBuffer = Buffers.EMPTY_BUFFER;
                        continue;
                    }
                    
                    this.blockBuffer = decompress(compressedBlock, getUncompressedBlockSize(this.blockHeader));

                    in = getCurrentInput();
                    
                } else {

                    BinaryTimeSeriesRecord record = this.records[type];
                    
                    if (record != null) {
                        setNext(record.fill(slice));
                        return;
                    }
                }
            }
        }

        done();
    }

    /**
     * Returns <code>true</code> if the specified type is the one of a block header, <code>false</code> otherwise.
     * 
     * @param type the record type
     * @return <code>true</code> if the specified type is the one of a block header, <code>false</code> otherwise.
     */
    private static boolean isBlockHeader(int type) {
        return type == Record.BLOCK_HEADER_TYPE;
    }
    
    /**
     * Uncompress the specified block.
     * 
     * @param compressedBlock the compressed block
     * @param uncompressedBlockSize the size of the block once uncompressed
     * @return the uncompressed data
     * @throws IOException if an I/O problem occurs.
     */
    private ReadableBuffer decompress(ReadableBuffer compressedBlock, int uncompressedBlockSize) 
            throws IOException {
        
        createDecompressorIfNeeded();
        return this.decompressor.decompress(compressedBlock, 
                                            getUncompressedBlockSize(this.blockHeader));
    }
    
    /**
     * Creates the <code>Decompressor</code> needed to uncompress the blocks if needed.
     * 
     * @throws IOException if an I/O problem occurs.
     */
    private void createDecompressorIfNeeded() throws IOException {
        
        CompressionType compressionType = getCompressionType(this.blockHeader);
        
        if (this.decompressor == null || this.decompressor.getType() != compressionType) {

            this.decompressor = compressionType.newDecompressor();
        }
    }

    /**
     * Returns the <code>ByteReader</code> to read from.
     * 
     * @return the <code>ByteReader</code> to read from.
     * @throws IOException if an I/O problem occurs
     */
    private ByteReader getCurrentInput() throws IOException {

        if (isBlockBufferReadable()) {
            return this.blockBuffer;
        } 
        
        return this.reader;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {

        if (this.reader instanceof Closeable) {
            ((Closeable) this.reader).close();
        }
    }

    /**
     * Returns <code>true</code> if the block buffer is readable, <code>false</code> otherwise.
     *  
     * @return <code>true</code> if the block buffer is readable, <code>false</code> otherwise.
     * @throws IOException if an I/O problem occurs.
     */
    private boolean isBlockBufferReadable() throws IOException {
        return this.blockBuffer != null && this.blockBuffer.isReadable();
    }
    
    /**
     * Creates a new <code>Builder</code> to build instances of <code>BinaryTimeSeriesRecordIterator</code>.
     * 
     * @param definition the time series definition
     * @return a new <code>Builder</code> to build instances of <code>BinaryTimeSeriesRecordIterator</code>
     */
    public static Builder newBuilder(TimeSeriesDefinition definition) {
        return new Builder(definition);
    }
    
    /**
     * Builder for <code>BinaryTimeSeriesRecordIterator</code> instances.
     * 
     * @author Benjamin
     */
    public static final class Builder {
        
        /**
         * The builder used to build the record list.
         */
        private final DefaultRecordIterator.Builder builder;
        
        /**
         * The time series definition.
         */
        private final TimeSeriesDefinition definition;
        
        /**
         * Adds a new record of the specified type.
         * 
         * @param recordType the type of record
         * @return this <code>Builder</code>
         */
        public final Builder newRecord(String recordType) {

            this.builder.newRecord(recordType);
            return this;
        }

        /**
         * Adds a new record of the specified type.
         * 
         * @param recordTypeIndex the record type index
         * @return this <code>Builder</code>
         */
        public final Builder newRecord(int recordTypeIndex) {

            this.builder.newRecord(recordTypeIndex);
            return this;
        }

        /**
         * Sets the specified field to the specified <code>long</code> value. 
         * 
         * @param index the field index
         * @param l the <code>long</code> value
         * @return this <code>Builder</code>
         */
        public final Builder setLong(int index, long l) {

            this.builder.setLong(index, l);
            return this;
        }

        /**
         * Sets the specified field to the specified <code>int</code> value. 
         * 
         * @param index the field index
         * @param i the <code>int</code> value
         * @return this <code>Builder</code>
         */
        public final Builder setInt(int index, int i) {

            this.builder.setInt(index, i);
            return this;
        }

        /**
         * Sets the specified field to the specified timestamp value. 
         * 
         * @param index the field index
         * @param l the timestamp value in nanoseconds.
         * @return this <code>Builder</code>
         */
        public final Builder setTimestampInNanos(int index, long l) {

            this.builder.setTimestampInNanos(index, l);
            return this;
        }

        /**
         * Sets the specified field to the specified timestamp value. 
         * 
         * @param index the field index
         * @param l the timestamp value in microseconds.
         * @return this <code>Builder</code>
         */
        public final Builder setTimestampInMicros(int index, long l) {

            this.builder.setTimestampInMicros(index, l);
            return this;
        }

        /**
         * Sets the specified field to the specified timestamp value. 
         * 
         * @param index the field index
         * @param l the timestamp value in milliseconds.
         * @return this <code>Builder</code>
         */
        public final Builder setTimestampInMillis(int index, long l) {

            this.builder.setTimestampInMillis(index, l);
            return this;
        }

        /**
         * Sets the specified field to the specified timestamp value. 
         * 
         * @param index the field index
         * @param l the timestamp value in seconds.
         * @return this <code>Builder</code>
         */
        public final Builder setTimestampInSeconds(int index, long l) {

            this.builder.setTimestampInSeconds(index, l);
            return this;
        }

        /**
         * Sets the specified field to the specified <code>byte</code> value. 
         * 
         * @param index the field index
         * @param b the <code>byte</code> value
         * @return this <code>Builder</code>
         */
        public final Builder setByte(int index, int b) {

            this.builder.setByte(index, b);
            return this;
        }

        /**
         * Sets the specified field to the specified decimal value. 
         * 
         * @param index the field index
         * @param mantissa the decimal mantissa
         * @param exponent the decimal exponent
         * @return this <code>Builder</code>
         */
        public final Builder setDecimal(int index, long mantissa, int exponent) {

            this.builder.setDecimal(index, mantissa, exponent);
            return this;
        }

        /**
         * Builds a new <code>BinaryTimeSeriesRecordIterator</code> instance.
         * @return a new <code>BinaryTimeSeriesRecordIterator</code> instance.
         */
        public final BinaryTimeSeriesRecordIterator build() {

            try (RecordIterator iterator = this.builder.build()) {

                CompositeBuffer compositeBuffer = new CompositeBuffer();

                while (iterator.hasNext()) {

                    Record next = iterator.next();
                    
                    compositeBuffer.add(toBuffer(next));
                }
                return new BinaryTimeSeriesRecordIterator(this.definition, compositeBuffer);
                
            } catch (IOException e) {
                
                // Should never happen as TimeSeriesRecord do not really throw IOException
                throw new IllegalArgumentException(e);
            }
        }

        /**
         * Serializes the specified record.
         * 
         * @param record the record to serialize
         * @return the byte representation of specified record.
         * @throws IOException if an I/O problem occurs
         */
        private static Buffer toBuffer(Record record) throws IOException {
            
            int recordSize = record.computeSerializedSize();
            int totalSize = 1 + VarInts.computeUnsignedIntSize(recordSize) + recordSize;
            
            Buffer buffer = Buffers.allocate(totalSize);
            buffer.writeByte(record.getType());
            VarInts.writeUnsignedInt(buffer, recordSize);
            record.writeTo(buffer);
            
            return buffer;
        }

        /**
         * Creates a new <code>Builder</code> instance.
         * 
         * @param definition the time series definition 
         */
        private Builder(TimeSeriesDefinition definition) {
            notNull(definition, "the definition parameter must not be null."); 
            
            this.builder = DefaultRecordIterator.newBuilder(definition);
            this.definition = definition;
        }
    }
}
