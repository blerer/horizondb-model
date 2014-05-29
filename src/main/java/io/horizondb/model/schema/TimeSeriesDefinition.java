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
import io.horizondb.io.encoding.VarInts;
import io.horizondb.io.serialization.Parser;
import io.horizondb.io.serialization.Serializable;
import io.horizondb.io.serialization.Serializables;
import io.horizondb.model.Globals;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.fields.TimestampField;
import io.horizondb.model.core.records.BinaryTimeSeriesRecord;
import io.horizondb.model.core.records.TimeSeriesRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Range;

import static org.apache.commons.lang.SystemUtils.LINE_SEPARATOR;
import static org.apache.commons.lang.Validate.notNull;

/**
 * The definition of a time series stored in the databaseName.
 * 
 * @author Benjamin
 */
@Immutable
public final class TimeSeriesDefinition implements Serializable {

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
            TimeUnit timestampUnit = TimeUnit.values()[reader.readByte()];
            TimeZone timeZone = TimeZone.getTimeZone(VarInts.readString(reader));
            PartitionType partitionType = PartitionType.parseFrom(reader);
            Serializables<RecordTypeDefinition> recordTypes = Serializables.parseFrom(RecordTypeDefinition.getParser(),
                                                                                      reader);

            return new TimeSeriesDefinition(name, timestampUnit, timeZone, partitionType, recordTypes);
        }
    };

    /**
     * The name of the time series.
     */
    private final String name;

    /**
     * The unit of time of the series.
     */
    private final TimeUnit timeUnit;

    /**
     * The time series timeZone.
     */
    private final TimeZone timeZone;

    /**
     * The partitionType type.
     */
    private final PartitionType partitionType;

    /**
     * The type of records composing this time series.
     */
    private final Serializables<RecordTypeDefinition> recordTypes;
    
    /**
     * The record type index per name.
     */
    private final BiMap<String, Integer> recordTypeIndices; 

    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {

        return VarInts.computeStringSize(this.name)
                + 1 // TimeUnit
                + VarInts.computeStringSize(this.timeZone.getID()) + this.partitionType.computeSerializedSize()
                + this.recordTypes.computeSerializedSize();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {

        VarInts.writeString(writer, this.name);
        VarInts.writeByte(writer, this.timeUnit.ordinal());
        VarInts.writeString(writer, this.timeZone.getID());
        this.partitionType.writeTo(writer);
        this.recordTypes.writeTo(writer);
    }

    /**
     * Returns binary records instances corresponding to this time series records.
     * 
     * @return binary records instances corresponding to this time series records.
     */
    public BinaryTimeSeriesRecord[] newBinaryRecords() {

        int numberOfTypes = this.recordTypes.size();

        BinaryTimeSeriesRecord[] records = new BinaryTimeSeriesRecord[numberOfTypes];

        for (int i = 0; i < numberOfTypes; i++) {

            records[i] = this.recordTypes.get(i).newBinaryRecord(i, this.timeUnit);
        }

        return records;
    }

    /**
     * Returns records instances corresponding to this time series records.
     * 
     * @return records instances corresponding to this time series records.
     */
    public TimeSeriesRecord[] newRecords() {

        int numberOfTypes = this.recordTypes.size();

        TimeSeriesRecord[] records = new TimeSeriesRecord[numberOfTypes];

        for (int i = 0; i < numberOfTypes; i++) {

            records[i] = this.recordTypes.get(i).newRecord(i, this.timeUnit);
        }

        return records;
    }

    /**
     * Returns the time range of the partition to which belongs the specified time.
     * 
     * @param timestamp the timestamp field
     * @return the time range of the partition to which belongs the specified time.
     */
    public Range<Field> getPartitionTimeRange(Field timestamp) {

        Calendar calendar = Calendar.getInstance(this.timeZone);
        calendar.setTimeInMillis(timestamp.getTimestampInMillis());

        return this.partitionType.getPartitionTimeRange(calendar);
    }

    /**
     * Returns a new record instances of the specified type.
     * 
     * @param type the name of the record type.
     * @return a new record instances of the specified type.
     */
    public TimeSeriesRecord newRecord(String name) {

        int index = getRecordTypeIndex(name);

        if (index < 0) {
            return null;
        }
        
        return newRecord(index);
    }
    
    /**
     * Returns a new record instances of the specified type.
     * 
     * @param type the name of the record type.
     * @return a new record instances of the specified type.
     */
    public TimeSeriesRecord newRecord(int index) {

        Validate.isTrue(index >= 0 && index <= this.recordTypes.size(), "No record has been defined for the index: "
                + index);

        return this.recordTypes.get(index).newRecord(index, this.timeUnit);
    }

    /**
     * Returns new field instance for the specified fieldName. 
     * 
     * @param fieldName the field name
     * @return a new field instance for the specified fieldName. 
     */
    public Field newField(String fieldName) {
        
        if (Globals.TIMESTAMP_FIELD.equals(fieldName)) {
            return new TimestampField(getTimeUnit());
        }
        
        for (int i = 0, m = this.recordTypes.size(); i < m; i++) {
            
            RecordTypeDefinition def = this.recordTypes.get(i);
            
            int index = def.getFieldIndex(fieldName);
            
            if (index >= 0) {
                
                return def.newField(index);
            }
        }
        
        return null;
    }
    
    /**
     * Returns a new field instance of the specified record type. 
     * 
     * @param recordTypeIndex the index of the record type
     * @param fieldIndex the field index
     * @return a new field instance of the specified record type
     */
    public Field newField(int recordTypeIndex, int fieldIndex) {
        
        Validate.isTrue(recordTypeIndex >= 0 && recordTypeIndex <= this.recordTypes.size(), 
                "No record has been defined for the index: " + recordTypeIndex);
        
        if (0 == fieldIndex) {
            return new TimestampField(getTimeUnit());
        }
        
        return this.recordTypes.get(recordTypeIndex).newField(fieldIndex);
    }
    
    /**
     * Returns the index of the specified record type.
     * 
     * @param type the record type.
     * @return the index of the specified record type.
     */
    public int getRecordTypeIndex(String type) {

        Integer index = this.recordTypeIndices.get(type);

        if (index == null) {
        
            throw new IllegalArgumentException("No " + type + " records have not been defined within the "
                    + this.name + " time series.");
        }
        
        return index.intValue();
    }

    /**
     * Returns the index of the field belonging to the specified record type with the specified name.  
     * 
     * @param type the record type index
     * @param name the field name
     */
    public int getFieldIndex(int type, String name) {
                
        if (type >= this.recordTypes.size()) {
            
            throw new NoSuchElementException("No records have not been defined with the index " + type 
                                               + " within the " + this.name + " time series.");
        }
        
        RecordTypeDefinition recordType = this.recordTypes.get(type);
 
        
        return recordType.getFieldIndex(name);
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
        return new EqualsBuilder().append(this.recordTypes, rhs.recordTypes)
                                  .append(this.timeUnit, rhs.timeUnit)
                                  .append(this.name, rhs.name)
                                  .append(this.timeZone, rhs.timeZone)
                                  .append(this.partitionType, rhs.partitionType)
                                  .isEquals();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(510479313, 1641137635).append(this.recordTypes)
                                                         .append(this.timeUnit)
                                                         .append(this.name)
                                                         .append(this.timeZone)
                                                         .append(this.partitionType)
                                                         .toHashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("name", this.name)
                                                                          .append("timeUnit", this.timeUnit)
                                                                          .append("timeZone", this.timeZone)
                                                                          .append("partitionType", this.partitionType)
                                                                          .append("recordTypes", this.recordTypes)
                                                                          .toString();
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
     * Returns the time unit of the time series timestamps.
     * 
     * @return the time unit of the time series timestamps.
     */
    public TimeUnit getTimeUnit() {
        return this.timeUnit;
    }

    /**
     * Returns the timezone of the time series.
     * 
     * @return the timezone of the time series.
     */
    public TimeZone getTimeZone() {
        return this.timeZone;
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
     * Returns the number of record types.
     * 
     * @return the number of record types.
     */
    public int getNumberOfRecordTypes() {

        return this.recordTypes.size();
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
     * Creates a new <code>TimeSeriesMetaData</code> using the specified builder.
     * 
     * @param builder the builder.
     */
    private TimeSeriesDefinition(Builder builder) {

        this(builder.name,
             builder.timeUnit,
             builder.timeZone,
             builder.partitionType,
             new Serializables<>(builder.recordTypes));
    }

    private TimeSeriesDefinition(String name,
            TimeUnit timeUnit,
            TimeZone timeZone,
            PartitionType partitionType,
            Serializables<RecordTypeDefinition> recordTypes) {

        this.name = name;
        this.timeUnit = timeUnit;
        this.timeZone = timeZone;
        this.partitionType = partitionType;
        this.recordTypes = recordTypes;
        this.recordTypeIndices = buildRecordTypeIndices(recordTypes);
    }

    /**
     * Builds the mapping between the record type names and indices.
     * 
     * @param recordTypes the record types
     * @return the mapping between the record type names and indices.
     */
    private static BiMap<String, Integer> buildRecordTypeIndices(Serializables<RecordTypeDefinition> recordTypes) {
        
        ImmutableBiMap.Builder<String, Integer> builder = ImmutableBiMap.builder();         
                
        for (int i = 0, m = recordTypes.size(); i < m; i++) {

            RecordTypeDefinition recordType = recordTypes.get(i);

            builder.put(recordType.getName(), Integer.valueOf(i));
        }
        
        return builder.build();
    }

    /**
     * Returns the name of the record type with the specified index.
     * 
     * @param index the record type index
     * @return the name of the record type with the specified index
     */
    public String getRecordName(int index) {
        return this.recordTypeIndices.inverse().get(Integer.valueOf(index));
    }

    /**
     * Returns the name of the specified field of the specified record type.
     * 
     * @param recordTypeIndex the index of the record type
     * @param fieldIndex the index of the field
     * @return the name of the specified field of the specified record type
     */
    public String getFieldName(int recordTypeIndex, int fieldIndex) {
        return this.recordTypes.get(recordTypeIndex).fieldName(fieldIndex);
    }

    /**
     * Splits the specified time range per partitions.
     * 
     * @param range the range to split
     * @return the ranges resulting from the split
     */
    public List<Range<Field>> splitRange(Range<Field> range) {
        
        Range<Field> remaining = range;
        List<Range<Field>> ranges = new ArrayList<>();
        
        Range<Field> partition = getPartitionTimeRange(remaining.lowerEndpoint());
        
        while (!partition.encloses(remaining)) {
            
            ranges.add(Range.closedOpen(remaining.lowerEndpoint(), partition.upperEndpoint()));
            remaining = Range.closedOpen(partition.upperEndpoint(), remaining.upperEndpoint());
            partition = getPartitionTimeRange(remaining.lowerEndpoint());
        }
        
        ranges.add(remaining);
        
        return ranges;
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
        
        for (int i = 0, m = this.recordTypes.size(); i < m; i++) {
            
            RecordTypeDefinition definition = this.recordTypes.get(i);
            
            if (i != 0) {
                builder.append(",")
                       .append(LINE_SEPARATOR);
            }
            
            builder.append(definition.toHql());
        }
        
        builder.append(')');
        
        builder.append("TIME_UNIT = ")
               .append(this.timeUnit)
               .append(" TIMEZONE = '")
               .append(this.timeZone.getID())
               .append("';");
        
        return builder.toString(); 
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
         * The unit of time of the series.
         */
        private TimeUnit timeUnit = TimeUnit.MILLISECONDS;

        /**
         * The time series timeZone.
         */
        private TimeZone timeZone = TimeZone.getDefault();

        /**
         * The partitionType type.
         */
        private PartitionType partitionType = PartitionType.BY_DAY;

        /**
         * The type of records that will be composing the time series.
         */
        private final List<RecordTypeDefinition> recordTypes = new ArrayList<>();

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

            notNull(timeUnit, "the timeUnit parameter must not be null.");

            this.timeUnit = timeUnit;
            return this;
        }

        /**
         * Sets the time zone of the time series.
         * 
         * @param timeZone the time zone of the time series.
         * @return this <code>Builder</code>.
         */
        public Builder timeZone(TimeZone timeZone) {

            notNull(timeZone, "the timeZone parameter must not be null.");

            this.timeZone = timeZone;
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

            this.recordTypes.add(recordType);
            return this;
        }

        /**
         * Creates a new <code>TimeSeriesMetaData</code> instance.
         * 
         * @return a new <code>TimeSeriesMetaData</code> instance.
         */
        public TimeSeriesDefinition build() {

            Validate.notEmpty(this.recordTypes, "no record type has been specified");

            return new TimeSeriesDefinition(this);
        }
    }
}
