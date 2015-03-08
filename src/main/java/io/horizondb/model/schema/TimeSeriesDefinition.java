/**
 * Copyright 2013 Benjamin Lerer
 * 
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
package io.horizondb.model.schema;

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.compression.CompressionType;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.io.serialization.Parser;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.fields.ByteField;
import io.horizondb.model.core.fields.IntegerField;
import io.horizondb.model.core.fields.TimestampField;
import io.horizondb.model.core.records.BinaryTimeSeriesRecord;
import io.horizondb.model.core.records.TimeSeriesRecord;

import java.io.IOException;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.google.common.collect.Range;

import static io.horizondb.io.files.FileUtils.ONE_KB;
import static io.horizondb.model.core.Record.TIMESTAMP_FIELD_INDEX;
import static io.horizondb.model.core.records.BlockHeaderUtils.COMPRESSED_BLOCK_SIZE_INDEX;
import static io.horizondb.model.core.records.BlockHeaderUtils.COMPRESSION_TYPE_INDEX;
import static io.horizondb.model.core.records.BlockHeaderUtils.LAST_TIMESTAMP_INDEX;
import static io.horizondb.model.core.records.BlockHeaderUtils.RECORD_COUNTERS_OFFSET;
import static io.horizondb.model.core.records.BlockHeaderUtils.UNCOMPRESSED_BLOCK_SIZE_INDEX;
import static org.apache.commons.lang.SystemUtils.LINE_SEPARATOR;
import static org.apache.commons.lang.Validate.notNull;

/**
 * The definition of a time series stored in the databaseName.
 */
@Immutable
public final class TimeSeriesDefinition extends ForwardingRecordSetDefinition {

    /**
     * The parser instance.
     */
    private static final Parser<TimeSeriesDefinition> PARSER = new Parser<TimeSeriesDefinition>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public TimeSeriesDefinition parseFrom(ByteReader reader) throws IOException {

            String name = VarInts.readString(reader);
            long timestamp = VarInts.readLong(reader);
            RecordSetDefinition recordSetDefinition = DefaultRecordSetDefinition.parseFrom(reader);
            PartitionType partitionType = PartitionType.parseFrom(reader);
            int blockSize = VarInts.readUnsignedInt(reader);
            CompressionType compressionType = CompressionType.parseFrom(reader);

            return new TimeSeriesDefinition(name,
                                            timestamp,
                                            recordSetDefinition,
                                            partitionType,
                                            blockSize,
                                            compressionType);
        }
    };

    /**
     * The name of the time series.
     */
    private final String name;

    /**
     * The time at which the series was created.
     */
    private final long timestamp;
    
    /**
     * The definition of the record set.
     */
    private final RecordSetDefinition recordSetDefinition;
    
    /**
     * The partitionType type.
     */
    private final PartitionType partitionType;
    
    /**
     * The block size used within this time series.
     */
    private final int blockSizeInBytes;
    
    /**
     * The type of compression used by this time series.
     */
    private final CompressionType compressionType;

    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() throws IOException {

        return VarInts.computeStringSize(this.name)
                + VarInts.computeLongSize(this.timestamp)
                + super.computeSerializedSize()
                + this.partitionType.computeSerializedSize()
                + VarInts.computeUnsignedIntSize(this.blockSizeInBytes)
                + this.compressionType.computeSerializedSize();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {

        VarInts.writeString(writer, this.name);
        VarInts.writeLong(writer, this.timestamp);
        super.writeTo(writer);
        this.partitionType.writeTo(writer);
        VarInts.writeUnsignedInt(writer, this.blockSizeInBytes);
        this.compressionType.writeTo(writer);
    }

    /**
     * Returns the time range of the partition to which belongs the specified time.
     * 
     * @param timestamp the timestamp field
     * @return the time range of the partition to which belongs the specified time.
     */
    public Range<Field> getPartitionTimeRange(Field timestamp) {

        Calendar calendar = Calendar.getInstance(getTimeZone());
        calendar.setTimeInMillis(timestamp.getTimestampInMillis());

        return this.partitionType.getPartitionTimeRange(calendar);
    }

    /**
     * Returns a new block header.
     * @return a new block header.
     */
    public TimeSeriesRecord newBlockHeader() {

        Field[] fields = getBlockHeaderFields();
                
        return new TimeSeriesRecord(Record.BLOCK_HEADER_TYPE, fields);
    }

    /**
     * Returns a new binary block header.
     * 
     * @return a new binary block header.
     */
    public BinaryTimeSeriesRecord newBinaryBlockHeader() {

        return new BinaryTimeSeriesRecord(Record.BLOCK_HEADER_TYPE, getBlockHeaderFields());
    }
    
    /**
     * Returns the size in bytes of the blocks used by the time series.
     *     
     * @return the size in bytes of the blocks used by the time series
     */
    public int getBlockSizeInBytes() {
        return this.blockSizeInBytes;
    }

    /**
     * Returns the type of compression used within the time series.
     * 
     * @return the type of compression used within the time series
     */
    public CompressionType getCompressionType() {
        return this.compressionType;
    }

    /**
     * Creates a new <code>TimeSeriesMetaData</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static TimeSeriesDefinition parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>TimeSeriesMetaData</code> instances.
     * 
     * @return the parser that can be used to deserialize <code>TimeSeriesMetaData</code> instances.
     */
    public static Parser<TimeSeriesDefinition> getParser() {

        return PARSER;
    }

    /**
     * Returns the time series name.
     * 
     * @return the time series name.
     */
    public String getName() {
        return this.name;
    }

    /**
     * Returns the partitionType type of the time series.
     * 
     * @return the partitionType type of the time series.
     */
    public PartitionType getPartitionType() {
        return this.partitionType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordSetDefinition delegate() {
        return this.recordSetDefinition;
    }

    /**
     * Returns the creation timestamp.
     * @return the creation timestamp
     */
    public long getTimestamp() {
        return this.timestamp;
    }

    /**
     * Creates a new <code>Builder</code> instance.
     * 
     * @param name the time series name
     * @return a new <code>Builder</code> instance.
     */
    public static Builder newBuilder(String name) {

        return new Builder(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof TimeSeriesDefinition)) {
            return false;
        }
        TimeSeriesDefinition rhs = (TimeSeriesDefinition) object;
        return new EqualsBuilder().append(this.name, rhs.name)
                                  .append(this.recordSetDefinition, rhs.recordSetDefinition)
                                  .append(this.partitionType, rhs.partitionType)
                                  .append(this.blockSizeInBytes, rhs.blockSizeInBytes)
                                  .append(this.compressionType, rhs.compressionType)
                                  .isEquals();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(1753288271, -183737521).append(this.name)
                                                          .append(this.recordSetDefinition)
                                                          .append(this.partitionType)
                                                          .append(this.blockSizeInBytes)
                                                          .append(this.compressionType)
                                                          .toHashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("name", this.name)
                                        .append("timestamp", this.timestamp)
                                        .append("recordSetDefinition", this.recordSetDefinition)
                                        .append("partitionType", this.partitionType)
                                        .append("compressionType", this.compressionType)
                                        .append("blockSizeInBytes", this.blockSizeInBytes)
                                        .toString();
    }

    /**
     * Duplicates this <code>TimeSeriesDefinition</code> changing the timestamp.
     * @return a copy of this <code>TimeSeriesDefinition</code> with a new timestamp.
     */
    public TimeSeriesDefinition newInstance() {

        return new TimeSeriesDefinition(this.name, 
                                        System.currentTimeMillis(),
                                        this.recordSetDefinition,
                                        this.partitionType,
                                        this.blockSizeInBytes,
                                        this.compressionType);
    }
    
    /**
     * Creates a new <code>TimeSeriesMetaData</code> using the specified builder.
     * 
     * @param builder the builder.
     */
    private TimeSeriesDefinition(Builder builder) {

        this(builder.name,
             System.currentTimeMillis(),
             builder.builder.build(),
             builder.partitionType,
             builder.blockSize,
             builder.compressionType);
    }

    private TimeSeriesDefinition(String name,
                                 long timestamp,
                                 RecordSetDefinition recordSetDefinition,
                                 PartitionType partitionType,
                                 int blockSize,
                                 CompressionType compressionType) {

        this.name = name;
        this.timestamp = timestamp;
        this.recordSetDefinition = recordSetDefinition;
        this.partitionType = partitionType;
        this.blockSizeInBytes = blockSize;
        this.compressionType = compressionType;
    }

    /**
     * Returns the HQL query that can be used to create this <code>TimeSeriesDefinition</code>.
     * 
     * @return  the HQL query that can be used to create this <code>TimeSeriesDefinition</code>.
     */
    public String toHql() {
        
        StringBuilder builder = new StringBuilder().append("CREATE TIMESERIES ")
                                                   .append(this.name)
                                                   .append(" (")
                                                   .append(LINE_SEPARATOR);
        
        for (int i = 0, m = this.recordSetDefinition.getNumberOfRecordTypes(); i < m; i++) {
            
            RecordTypeDefinition definition = this.recordSetDefinition.getRecordType(i);
            
            if (i != 0) {
                builder.append(",")
                       .append(LINE_SEPARATOR);
            }
            
            builder.append(definition.toHql());
        }
        
        builder.append(')');
        
        builder.append("TIME_UNIT = ")
               .append(getTimeUnit())
               .append(" TIMEZONE = '")
               .append(getTimeZone().getID())
               .append("';");
        
        return builder.toString(); 
    }

    
    /**
     * Returns the fields of the block header.
     * 
     * @return the fields of the block header.
     */
    private Field[] getBlockHeaderFields() {
        
        Field[] fields = new Field[RECORD_COUNTERS_OFFSET + getNumberOfRecordTypes()];
        fields[TIMESTAMP_FIELD_INDEX] = new TimestampField(getTimeUnit());
        fields[LAST_TIMESTAMP_INDEX] = new TimestampField(getTimeUnit());
        fields[COMPRESSED_BLOCK_SIZE_INDEX] = new IntegerField();
        fields[UNCOMPRESSED_BLOCK_SIZE_INDEX] = new IntegerField();
        fields[COMPRESSION_TYPE_INDEX] = new ByteField();
        
        for (int i = RECORD_COUNTERS_OFFSET, m = fields.length; i < m; i++) {
            fields[i] = new IntegerField();
        }
        return fields;
    }

    /**
     * Builds instance of <code>TimeSerieDefinition</code>.
     */
    public static class Builder {

        /**
         * The time series name.
         */
        private final String name;

        /**
         * The partitionType type.
         */
        private PartitionType partitionType = PartitionType.BY_DAY;
        
        /**
         * The block size used within this time series.
         */
        private int blockSize = 64 * ONE_KB;
        
        /**
         * The type of compression used by this time series.
         */
        private CompressionType compressionType = CompressionType.LZ4;

        /**
         * The record set definition builder.
         */
        private final DefaultRecordSetDefinition.Builder builder = DefaultRecordSetDefinition.newBuilder();

        /**
         * Must not be called from outside the enclosing class.
         */
        private Builder(String name) {

            Validate.notEmpty(name, "the time series name must not be empty.");

            this.name = name;
        }

        /**
         * Sets the time unit of the time series.
         * 
         * @param timeUnit the time unit of the time series.
         * @return this <code>Builder</code>.
         */
        public Builder timeUnit(TimeUnit timeUnit) {
            this.builder.timeUnit(timeUnit);
            return this;
        }

        /**
         * Sets the time zone of the time series.
         * 
         * @param timeZone the time zone of the time series.
         * @return this <code>Builder</code>.
         */
        public Builder timeZone(TimeZone timeZone) {
            this.builder.timeZone(timeZone);
            return this;
        }

        /**
         * Sets the way the time series must be partitioned.
         * 
         * @param partitionType the way the time series must be partitioned.
         * @return this <code>Builder</code>.
         */
        public Builder partitionType(PartitionType partitionType) {

            notNull(partitionType, "the partitionType parameter must not be null.");

            this.partitionType = partitionType;
            return this;
        }

        /**
         * Sets the size of the block used by the time series.
         * 
         * @param blockSizeInBytes the size of the block used by the time series.
         * @return this <code>Builder</code>.
         */
        public Builder blockSize(int blockSize) {

            this.blockSize = blockSize;
            return this;
        }
        
        /**
         * Sets the type of compression used by the time series.
         * 
         * @param compressionType the type of compression used by the time series.
         * @return this <code>Builder</code>.
         */
        public Builder compressionType(CompressionType compressionType) {

            notNull(compressionType, "the compressionType parameter must not be null.");

            this.compressionType = compressionType;
            return this;
        }
        
        /**
         * Adds the specified record type to the type of records that will be composing the time series.
         * 
         * @param builder the builder of the record type to add.
         * @return this <code>Builder</code>.
         */
        public Builder addRecordType(RecordTypeDefinition.Builder builder) {

            return addRecordType(builder.build());
        }

        /**
         * Adds the specified record type to the type of records that will be composing the time series.
         * 
         * @param recordType the record type to add.
         * @return this <code>Builder</code>.
         */
        public Builder addRecordType(RecordTypeDefinition recordType) {

            this.builder.addRecordType(recordType);
            return this;
        }

        /**
         * Creates a new <code>TimeSeriesDefinition</code> instance.
         * 
         * @return a new <code>TimeSeriesDefinition</code> instance.
         */
        public TimeSeriesDefinition build() {
            return new TimeSeriesDefinition(this);
        }
    }
}
