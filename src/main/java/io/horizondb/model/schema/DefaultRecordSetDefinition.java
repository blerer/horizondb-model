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
package io.horizondb.model.schema;

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.io.serialization.Parser;
import io.horizondb.io.serialization.Serializables;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Filter;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.fields.TimestampField;
import io.horizondb.model.core.filters.Filters;
import io.horizondb.model.core.records.BinaryTimeSeriesRecord;
import io.horizondb.model.core.records.TimeSeriesRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
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

import static org.apache.commons.lang.Validate.notNull;

/**
 * The definition of set of records.
 * 
 * @author Benjamin
 */
@Immutable
public final class DefaultRecordSetDefinition implements RecordSetDefinition {

    /**
     * An empty <code>DefaultRecordSetDefinition</code>.
     */
    public static final RecordSetDefinition EMPTY_DEFINITION = new DefaultRecordSetDefinition(new Builder());
    
    /**
     * The parser instance.
     */
    private static final Parser<DefaultRecordSetDefinition> PARSER = new Parser<DefaultRecordSetDefinition>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public DefaultRecordSetDefinition parseFrom(ByteReader reader) throws IOException {

            TimeUnit timestampUnit = TimeUnit.values()[reader.readByte()];
            TimeZone timeZone = TimeZone.getTimeZone(VarInts.readString(reader));
            Serializables<RecordTypeDefinition> recordTypes = Serializables.parseFrom(RecordTypeDefinition.getParser(),
                                                                                      reader);

            return new DefaultRecordSetDefinition(timestampUnit,
                                           timeZone,
                                           recordTypes);
        }
    };

    /**
     * The unit of time of the series.
     */
    private final TimeUnit timeUnit;

    /**
     * The time series timeZone.
     */
    private final TimeZone timeZone;

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

        return  1 // TimeUnit
                + VarInts.computeStringSize(this.timeZone.getID()) 
                + this.recordTypes.computeSerializedSize();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {

        VarInts.writeByte(writer, this.timeUnit.ordinal());
        VarInts.writeString(writer, this.timeZone.getID());
        this.recordTypes.writeTo(writer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<RecordTypeDefinition> iterator() {
        return this.recordTypes.iterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BinaryTimeSeriesRecord[] newBinaryRecords() {

        return newBinaryRecords(Filters.<String>noop());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BinaryTimeSeriesRecord[] newBinaryRecords(Filter<String> filter) {

        int numberOfTypes = this.recordTypes.size();

        BinaryTimeSeriesRecord[] records = new BinaryTimeSeriesRecord[numberOfTypes];

        for (int i = 0; i < numberOfTypes; i++) {

            RecordTypeDefinition recordTypeDefinition = this.recordTypes.get(i);
            
            if (isAcceptedBy(filter, recordTypeDefinition.getName())) {
                records[i] = recordTypeDefinition.newBinaryRecord(i, this.timeUnit);
            }
        }

        return records;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TimeSeriesRecord[] newRecords() {

        int numberOfTypes = this.recordTypes.size();

        TimeSeriesRecord[] records = new TimeSeriesRecord[numberOfTypes];

        for (int i = 0; i < numberOfTypes; i++) {

            records[i] = this.recordTypes.get(i).newRecord(i, this.timeUnit);
        }

        return records;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TimeSeriesRecord newRecord(String name) {

        int index = getRecordTypeIndex(name);

        if (index < 0) {
            return null;
        }
        
        return newRecord(index);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public TimeSeriesRecord newRecord(int index) {

        Validate.isTrue(index >= 0 && index <= this.recordTypes.size(), "No record has been defined for the index: "
                + index);

        return this.recordTypes.get(index).newRecord(index, this.timeUnit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field newField(String fieldName) {
        
        if (Record.TIMESTAMP_FIELD_NAME.equals(fieldName)) {
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
     * {@inheritDoc}
     */
    @Override
    public Field newField(int recordTypeIndex, int fieldIndex) {
        
        Validate.isTrue(recordTypeIndex >= 0 && recordTypeIndex <= this.recordTypes.size(), 
                "No record has been defined for the index: " + recordTypeIndex);
        
        if (0 == fieldIndex) {
            return new TimestampField(getTimeUnit());
        }
        
        return this.recordTypes.get(recordTypeIndex).newField(fieldIndex);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int getRecordTypeIndex(String type) {

        Integer index = this.recordTypeIndices.get(type);

        if (index == null) {
        
            throw new IllegalArgumentException("No " + type + " records have not been defined within this record set");
        }
        
        return index.intValue();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getFieldIndex(int type, String name) {
                
        if (type >= this.recordTypes.size()) {
            
            throw new NoSuchElementException("No records have not been defined with the index " + type 
                                               + " within this record set.");
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
        if (!(object instanceof DefaultRecordSetDefinition)) {
            return false;
        }
        DefaultRecordSetDefinition rhs = (DefaultRecordSetDefinition) object;
        return new EqualsBuilder().append(this.recordTypes, rhs.recordTypes)
                                  .append(this.timeUnit, rhs.timeUnit)
                                  .append(this.timeZone, rhs.timeZone)
                                  .isEquals();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(510479313, 1641137635).append(this.recordTypes)
                                                         .append(this.timeUnit)
                                                         .append(this.timeZone)
                                                         .toHashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("timeUnit", this.timeUnit)
                                                                          .append("timeZone", this.timeZone)
                                                                          .append("recordTypes", this.recordTypes)
                                                                          .toString();
    }

    /**
     * Creates a new <code>TimeSeriesMetaData</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static RecordSetDefinition parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>TimeSeriesMetaData</code> instances.
     * 
     * @return the parser that can be used to deserialize <code>TimeSeriesMetaData</code> instances.
     */
    public static Parser<DefaultRecordSetDefinition> getParser() {

        return PARSER;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TimeUnit getTimeUnit() {
        return this.timeUnit;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TimeZone getTimeZone() {
        return this.timeZone;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumberOfRecordTypes() {

        return this.recordTypes.size();
    }

    /**
     * Creates a new <code>Builder</code> instance.
     * 
     * @return a new <code>Builder</code> instance.
     */
    public static Builder newBuilder() {

        return new Builder();
    }

    /**
     * Creates a new <code>TimeSeriesMetaData</code> using the specified builder.
     * 
     * @param builder the builder.
     */
    private DefaultRecordSetDefinition(Builder builder) {

        this(builder.timeUnit,
             builder.timeZone,
             new Serializables<>(builder.recordTypes));
    }

    private DefaultRecordSetDefinition(TimeUnit timeUnit, TimeZone timeZone, Serializables<RecordTypeDefinition> recordTypes) {

        this.timeUnit = timeUnit;
        this.timeZone = timeZone;
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
     * {@inheritDoc}
     */
    @Override
    public String getRecordName(int index) {
        return this.recordTypeIndices.inverse().get(Integer.valueOf(index));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordTypeDefinition getRecordType(int index) {
        return this.recordTypes.get(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getFieldName(int recordTypeIndex, int fieldIndex) {
        return this.recordTypes.get(recordTypeIndex).fieldName(fieldIndex);
    }
    
    /**
     * Checks that the specified name is accepted by the specified filter.
     * 
     * @param filter the filter used to test the name.
     * @param name the name to test
     * @return <code>true</code> if the name is accepted, <code>false</code> otherwise.
     */
    private static boolean isAcceptedBy(Filter<String> filter, String name) {
        try {
            return filter.accept(name);
        } catch (IOException e) {
            // Should never happend.
            throw new IllegalStateException(e);
        }
    }

    /**
     * Builds instance of <code>DefaultRecordSetDefinition</code>.
     */
    public static class Builder {

        /**
         * The unit of time of the series.
         */
        private TimeUnit timeUnit = TimeUnit.MILLISECONDS;

        /**
         * The time series timeZone.
         */
        private TimeZone timeZone = TimeZone.getDefault();

        /**
         * The type of records that will be composing the time series.
         */
        private final List<RecordTypeDefinition> recordTypes = new ArrayList<>();

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
         * Creates a new <code>DefaultRecordSetDefinition</code> instance.
         * 
         * @return a new <code>DefaultRecordSetDefinition</code> instance.
         */
        public RecordSetDefinition build() {

            Validate.notEmpty(this.recordTypes, "no record type has been specified");

            return new DefaultRecordSetDefinition(this);
        }
    }
}
