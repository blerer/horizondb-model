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
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.records.BinaryTimeSeriesRecord;
import io.horizondb.model.core.records.TimeSeriesRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

/**
 * Definition of a record type.
 * 
 * @author Benjamin
 * 
 */
@Immutable
public class RecordTypeDefinition implements Iterable<FieldDefinition>, Serializable {
    
    /**
     * The parser instance.
     */
    private static final Parser<RecordTypeDefinition> PARSER = new Parser<RecordTypeDefinition>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public RecordTypeDefinition parseFrom(ByteReader reader) throws IOException {

            String name = VarInts.readString(reader);
            Serializables<FieldDefinition> fieldDefinitions = Serializables.parseFrom(FieldDefinition.getParser(),
                                                                                      reader);

            return new RecordTypeDefinition(name, fieldDefinitions);
        }
    };

    /**
     * The record type name.
     */
    private final String name;

    /**
     * The fields.
     */
    private final Serializables<FieldDefinition> fields;
    
    /**
     * The field type index per name.
     */
    private final BiMap<String, Integer> fieldIndices; 

    /**
     * Creates a new <code>Builder</code> instance.
     * 
     * @param the record name.
     * @return a new <code>Builder</code> instance.
     */
    public static Builder newBuilder(String name) {

        return new Builder(name);
    }

    /**
     * Returns the record type name.
     * 
     * @return the record type name.
     */
    public String getName() {
        return this.name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<FieldDefinition> iterator() {
        return this.fields.iterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() throws IOException{

        return VarInts.computeStringSize(this.name) + this.fields.computeSerializedSize();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {

        VarInts.writeString(writer, this.name);
        this.fields.writeTo(writer);
    }

    /**
     * Returns a new <code>TimeSeriesRecord</code> instance.
     * 
     * @param recordType the type of the record.
     * @param timestampUnit the unit of the timestamp.
     * @return a new <code>TimeSeriesRecord</code> instance.
     */
    public TimeSeriesRecord newRecord(int recordType, TimeUnit timestampUnit) {

        return new TimeSeriesRecord(recordType, timestampUnit, getFieldTypes());
    }

    /**
     * Returns a new <code>BinaryTimeSeriesRecord</code> instance.
     * 
     * @param recordType the type of the record.
     * @param timestampUnit the unit of the timestamp.
     * @return a new <code>BinaryTimeSeriesRecord</code> instance.
     */
    public BinaryTimeSeriesRecord newBinaryRecord(int recordType, TimeUnit timestampUnit) {

        return new BinaryTimeSeriesRecord(recordType, timestampUnit, getFieldTypes());
    }

    /**
     * Return the index of the field with the specified name.
     *  
     * @param fieldName the field name
     * @return the index of the field with the specified name or -1 if no field has the specified name.
     */
    public int getFieldIndex(String fieldName) {
        
        if (Record.TIMESTAMP_FIELD_NAME.equals(fieldName)) {
            return 0;
        }
        
        Integer index = this.fieldIndices.get(fieldName);
        
        if (index == null) {
            
            return -1;
        }
        
        return index.intValue();
    }
   
    /**
     * Returns the name of the field with the specified index.
     * 
     * @param index the field index
     * @return the name of the field with the specified index
     */
    public String fieldName(int index) {

        return this.fieldIndices.inverse().get(Integer.valueOf(index));
    }
    

    /**
     * Returns the number of fields.
     * @return the number of fields.
     */
    public int getNumberOfFields() {
        return this.fields.size();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof RecordTypeDefinition)) {
            return false;
        }
        RecordTypeDefinition rhs = (RecordTypeDefinition) object;
        return new EqualsBuilder().append(this.name, rhs.name).append(this.fields, rhs.fields).isEquals();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(-685004919, 1598947217).append(this.name).append(this.fields).toHashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("name", this.name)
                                                                          .append("fields", this.fields)
                                                                          .toString();
    }

    /**
     * Returns the parser that can be used to deserialize <code>RecordTypeDefinition</code> instances.
     * 
     * @return the parser that can be used to deserialize <code>RecordTypeDefinition</code> instances.
     */
    public static Parser<RecordTypeDefinition> getParser() {

        return PARSER;
    }
    
    /**
     * Creates a new instance of the specified <code>Field</code>. 
     * 
     * @param fieldIndex the field index
     * @return a new instance of the specified <code>Field</code>
     */
    Field newField(int fieldIndex) {
        
        Validate.isTrue(fieldIndex >= 0 && fieldIndex <= this.fields.size(), 
                "No field has been defined with the index: " + fieldIndex 
                + " fot the record type: " + this.name);
        
        return this.fields.get(fieldIndex - 1).getType().newField();
    } 
    
    /**
     * Creates a new <code>RecordTypeDefinition</code> instance using the specified <code>Builder</code>.
     * 
     * @param builder the builder used to build this instance.
     */
    private RecordTypeDefinition(Builder builder) {

        this(builder.name, new Serializables<>(builder.fields));
    }

    /**
     * Creates a new <code>RecordTypeDefinition</code> instance with the specified name and fields.
     * 
     * @param name the record type name.
     * @param fields the fields definition.
     */
    private RecordTypeDefinition(String name, Serializables<FieldDefinition> fields) {

        this.name = name;
        this.fields = fields;
        this.fieldIndices = buildFieldIndices(fields);
    }

    /**
     * Builds the mapping between the field names and indices.
     * 
     * @param fields the fields
     * @return the mapping between the field names and indices.
     */
    private static BiMap<String, Integer> buildFieldIndices(Serializables<FieldDefinition> fields) {
        
        ImmutableBiMap.Builder<String, Integer> builder = ImmutableBiMap.builder();         
                
        builder.put("timestamp", Integer.valueOf(0));
        
        for (int i = 0, m = fields.size(); i < m; i++) {

            FieldDefinition field = fields.get(i);
            
            builder.put(field.getName(), Integer.valueOf(i + 1));
        }
        
        return builder.build();
    }
    
    /**
     * Returns the types of the fields in order.
     * 
     * @return the types of the fields in order.
     */
    private FieldType[] getFieldTypes() {

        int numberOfFields = this.fields.size();

        FieldType[] types = new FieldType[numberOfFields];

        for (int i = 0; i < numberOfFields; i++) {

            types[i] = this.fields.get(i).getType();
        }

        return types;
    }

    /**
     * Builds instance of <code>RecordTypeDefinition</code>.
     * 
     */
    public static class Builder {

        /**
         * The record type name.
         */
        private final String name;

        /**
         * The fields.
         */
        private final List<FieldDefinition> fields = new ArrayList<>();

        /**
         * Adds the specified decimal field to the list of fields of the record type.
         * 
         * @param name the field name.
         * @return this <code>Builder</code>.
         */
        public Builder addDecimalField(String name) {

            return addField(name, FieldType.DECIMAL);
        }

        /**
         * Adds the specified long field to the list of fields of the record type.
         * 
         * @param name the field name.
         * @return this <code>Builder</code>.
         */
        public Builder addLongField(String name) {

            return addField(name, FieldType.LONG);
        }

        /**
         * Adds the specified integer field to the list of fields of the record type.
         * 
         * @param name the field name.
         * @return this <code>Builder</code>.
         */
        public Builder addIntegerField(String name) {

            return addField(name, FieldType.INTEGER);
        }

        /**
         * Adds the specified byte field to the list of fields of the record type.
         * 
         * @param name the field name.
         * @return this <code>Builder</code>.
         */
        public Builder addByteField(String name) {

            return addField(name, FieldType.BYTE);
        }

        /**
         * Adds the specified nanosecond timestamp field to the list of fields of the record type.
         * 
         * @param name the field name.
         * @return this <code>Builder</code>.
         */
        public Builder addNanosecondTimestampField(String name) {

            return addField(name, FieldType.NANOSECONDS_TIMESTAMP);
        }

        /**
         * Adds the specified milliseconds timestamp field to the list of fields of the record type.
         * 
         * @param name the field name.
         * @return this <code>Builder</code>.
         */
        public Builder addMillisecondTimestampField(String name) {

            return addField(name, FieldType.MILLISECONDS_TIMESTAMP);
        }

        /**
         * Adds the specified microseconds timestamp field to the list of fields of the record type.
         * 
         * @param name the field name.
         * @return this <code>Builder</code>.
         */
        public Builder addMicrosecondTimestampField(String name) {

            return addField(name, FieldType.MICROSECONDS_TIMESTAMP);
        }

        /**
         * Adds the specified second timestamp field to the list of fields of the record type.
         * 
         * @param name the field name.
         * @return this <code>Builder</code>.
         */
        public Builder addSecondTimestampField(String name) {

            return addField(name, FieldType.SECONDS_TIMESTAMP);
        }

        /**
         * Adds the specified field to the list of fields of the record type.
         * 
         * @param name the field name.
         * @param type the field type.
         * @return this <code>Builder</code>.
         */
        public Builder addField(String name, FieldType type) {
            return addField(FieldDefinition.newInstance(name, type));
        }

        /**
         * Adds the specified field definition to the list of fields of the record type.
         * 
         * @param definition the field definition.
         * @return this <code>Builder</code>.
         */
        public Builder addField(FieldDefinition definition) {

            this.fields.add(definition);
            return this;
        }
        
        /**
         * Creates a new <code>RecordTypeDefinition</code> instance.
         * 
         * @return a new <code>RecordTypeDefinition</code> instance.
         */
        public RecordTypeDefinition build() {

            return new RecordTypeDefinition(this);
        }

        /**
         * Must not be called from outside the enclosing class.
         */
        private Builder(String name) {

            this.name = name;
        }
    }

    /**
     * Returns the HQL corresponding to this <code>RecordTypeDefinition</code>.
     * 
     * @return the HQL corresponding to this <code>RecordTypeDefinition</code>.
     */
    public String toHql() {

        StringBuilder builder = new StringBuilder().append(this.name).append('(');

        for (int i = 0, m = this.fields.size(); i < m; i++) {

            FieldDefinition definition = this.fields.get(i);

            if (i != 0) {
                builder.append(", ");
            }

            builder.append(definition.toHql());
        }

        builder.append(')');
        return builder.toString();
    }
}
