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
import io.horizondb.io.encoding.VarInts;
import io.horizondb.model.core.DataBlock;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Filter;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.ResourceIterator;
import io.horizondb.model.core.fields.TimestampField;
import io.horizondb.model.core.filters.Filters;
import io.horizondb.model.core.records.BinaryTimeSeriesRecord;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;

import com.google.common.collect.RangeSet;

import static io.horizondb.model.core.iterators.BlockIterators.decompress;

import static io.horizondb.model.core.iterators.BlockIterators.filter;

import static io.horizondb.model.core.iterators.BlockIterators.iterator;

import static org.apache.commons.lang.Validate.notNull;

/**
 * <code>RecordIterator</code> for <code>BinaryTimeSeriesRecord</code>s.
 * 
 * @author Benjamin
 * 
 */
public final class BinaryTimeSeriesRecordIterator extends AbstractResourceIterator<BinaryTimeSeriesRecord> {
    
    /**
     * The records per type.
     */
    private final BinaryTimeSeriesRecord[] records;

    /**
     * The <code>DataBlock</code> iterator.
     */
    private final ResourceIterator<DataBlock> iterator;
    
    /**
     * The buffer used to store the compressed block.
     */
    private ReadableBuffer buffer = Buffers.EMPTY_BUFFER;

    public BinaryTimeSeriesRecordIterator(TimeSeriesDefinition definition, ByteReader reader) {
        
        this(definition, reader, TimestampField.ALL);
    }
    
    public BinaryTimeSeriesRecordIterator(TimeSeriesDefinition definition, ByteReader reader, RangeSet<Field> rangeSet) {
        
        this(definition, reader, rangeSet, Filters.<String>noop());
    }

    public BinaryTimeSeriesRecordIterator(TimeSeriesDefinition definition, ResourceIterator<DataBlock> iterator) {
        
        this(definition, iterator, Filters.<String>noop());
    }

    public BinaryTimeSeriesRecordIterator(TimeSeriesDefinition definition, 
                                          ByteReader reader, 
                                          RangeSet<Field> rangeSet, 
                                          Filter<String> filter) {

        this(definition, filter(rangeSet, iterator(definition, reader)), filter);
    }

    public BinaryTimeSeriesRecordIterator(TimeSeriesDefinition definition, 
                                          ResourceIterator<DataBlock> iterator, 
                                          Filter<String> filter) {

        this.records = definition.newBinaryRecords(filter);
        this.iterator = decompress(iterator);
    }
    
    /**    
     * {@inheritDoc}
     */
    @Override
    protected void computeNext() throws IOException {
        
        while (this.iterator.hasNext() || this.buffer.isReadable()) {

            while (this.buffer.isReadable()) {

                int type = this.buffer.readByte();
                int length = VarInts.readUnsignedInt(this.buffer);
                ReadableBuffer slice = this.buffer.slice(length);

                BinaryTimeSeriesRecord record = this.records[type];
                if (record != null) {
                    setNext(record.fill(slice));
                    return;
                }
            }

            if (this.iterator.hasNext()) {
                
                DataBlock block = this.iterator.next();
                this.buffer = block.getData();
            }
        }
        done();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        this.iterator.close();
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

            try (ResourceIterator<Record> iterator = this.builder.build()) {

                CompositeBuffer compositeBuffer = new CompositeBuffer();

                while (iterator.hasNext()) {

                    Record next = iterator.next();
                    
                    compositeBuffer.addBytes(toBuffer(next));
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
