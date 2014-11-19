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
package io.horizondb.model.core;

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.serialization.Parser;
import io.horizondb.io.serialization.Serializable;
import io.horizondb.model.core.filters.Filters;
import io.horizondb.model.core.iterators.FieldFilteringIterator;
import io.horizondb.model.core.records.FieldFilter;
import io.horizondb.model.schema.DefaultRecordSetDefinition;
import io.horizondb.model.schema.FieldDefinition;
import io.horizondb.model.schema.RecordSetDefinition;
import io.horizondb.model.schema.RecordTypeDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;

import static io.horizondb.io.encoding.VarInts.computeStringSize;
import static io.horizondb.io.encoding.VarInts.computeUnsignedIntSize;
import static io.horizondb.io.encoding.VarInts.readString;
import static io.horizondb.io.encoding.VarInts.readUnsignedInt;
import static io.horizondb.io.encoding.VarInts.writeString;
import static io.horizondb.io.encoding.VarInts.writeUnsignedInt;

/**
 */
public final class Projection implements Serializable {
         
    /**
     * Represents all the records or all the fields.
     */
    private static final String ALL = "*";
    
    /**
     * The parser instance.
     */
    private static final Parser<Projection> PARSER = new Parser<Projection>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public Projection parseFrom(ByteReader reader) throws IOException {

            ListMultimap<String, String> multimap = ArrayListMultimap.create();
            
            int numberOfRecords = readUnsignedInt(reader);
            
            for (int i = 0; i < numberOfRecords; i++) {
                
                String record = readString(reader);
                int numberOfFields = readUnsignedInt(reader);
                
                for (int j = 0; j < numberOfFields; j++) {
                    
                    String field = readString(reader);
                    multimap.put(record, field);
                }
            }
            return new Projection(multimap);
        }
    };
    
    /**
     * The fields that must be returned by records.
     */
    private final ListMultimap<String, String> fieldsPerRecords;

    /**
     * @param expressions the expression specifying the records and field that must be returned.
     */
    public Projection(List<String> expressions) {
        
        this.fieldsPerRecords = convertToMultimap(expressions);
    }

    /**
     * Creates a new <code>Projection</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static Projection parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>Projection</code> instances.
     * @return the parser that can be used to deserialize <code>Projection</code> instances.
     */
    public static Parser<Projection> getParser() {

        return PARSER;
    }
    
    /**
     * Returns the filter used to filter the record types.
     * 
     * @return the filter used to filter the record types.
     */
    public Filter<String> getRecordTypeFilter() {
        
        if (this.fieldsPerRecords.containsKey(ALL)) {
            return Filters.noop();
        }
        
        SortedSet<String> types = new TreeSet<>(this.fieldsPerRecords.keySet());
        return Filters.in(types, false);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {
        
        int size = 0;
        
        Set<String> records = this.fieldsPerRecords.keySet();
        
        size += computeUnsignedIntSize(records.size());
        
        for (String record : records) {
            
            size += computeStringSize(record);
            
            List<String> fields = this.fieldsPerRecords.get(record);
            
            size += computeUnsignedIntSize(fields.size());
            
            for (int i = 0, m = fields.size(); i < m; i++) {
                size += computeStringSize(fields.get(i));
            }
        }
        
        return size;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {

        Set<String> records = this.fieldsPerRecords.keySet();
        
        writeUnsignedInt(writer, records.size());
        
        for (String record : records) {
            
            writeString(writer, record);
            
            List<String> fields = this.fieldsPerRecords.get(record);
            
            writeUnsignedInt(writer, fields.size());
            
            for (int i = 0, m = fields.size(); i < m; i++) {
                writeString(writer, fields.get(i));
            }
        }
    } 
    
    public RecordSetDefinition getDefinition(TimeSeriesDefinition timeSeriesDefinition) {
        
        DefaultRecordSetDefinition.Builder builder = DefaultRecordSetDefinition.newBuilder()
                                                                               .timeUnit(timeSeriesDefinition.getTimeUnit())
                                                                               .timeZone(timeSeriesDefinition.getTimeZone());
        for (RecordTypeDefinition recordType : timeSeriesDefinition) {
            
            if (this.fieldsPerRecords.containsKey(ALL)) {
                
                builder.addRecordType(recordType);
                continue;
            }
            
            final String name = recordType.getName();
            
            if (this.fieldsPerRecords.containsKey(name)) {
                
                List<String> fields = this.fieldsPerRecords.get(name);
                RecordTypeDefinition.Builder recordTypeBuilder = RecordTypeDefinition.newBuilder(name); 
                
                for (FieldDefinition fieldDefinition : recordType) {
                    
                    if (fields.contains(ALL) || fields.contains(fieldDefinition.getName())) {
                        recordTypeBuilder.addField(fieldDefinition);
                    }
                }
                builder.addRecordType(recordTypeBuilder);
            }
        }
        
        return builder.build();
    }
    
    public RecordIterator filterFields(TimeSeriesDefinition timeSeriesDefinition, RecordIterator iterator) {

        if (this.fieldsPerRecords.containsKey(ALL)) {
            return iterator;
        }

        FieldFilter[] filters = new FieldFilter[timeSeriesDefinition.getNumberOfRecordTypes()];
        int type = 0;
        for (String record : this.fieldsPerRecords.keySet()) {

            int recordIndex = timeSeriesDefinition.getRecordTypeIndex(record);
            List<String> fields = this.fieldsPerRecords.get(record);
            int[] mapping;
            if (fields.contains(ALL)) {
                mapping = new int [timeSeriesDefinition.getRecordType(recordIndex).getNumberOfFields() + 1];
                for (int i = 0, m = mapping.length; i < m; i++) {
                    mapping[i] = i;
                }
            } else {
                mapping = new int[fields.size()];
                for (int i = 0, m = fields.size(); i < m; i++) {
                    String fieldName = fields.get(i);
                    int index = timeSeriesDefinition.getFieldIndex(recordIndex, fieldName);
                    mapping[i] = index;
                }
                
            }
            filters[recordIndex] = new FieldFilter(type, mapping);
            type++;
        }
        return new FieldFilteringIterator(iterator, filters);
    }

    private static ListMultimap<String, String> convertToMultimap(List<String> expressions) {
        
        if (expressions.size() == 1 && ALL.equals(expressions.get(0))) {
            return ImmutableListMultimap.of(ALL, ALL);
        }
        
        ListMultimap<String, String> multimap = ArrayListMultimap.create();
        
        for (String expression : expressions) {
            String[] elements = expression.split("\\.");
            String record = elements[0];
            String field = elements.length > 1 ? elements[1] : ALL; 
            
            multimap.put(record, field);
        }
        return multimap;
    }
    
    private Projection(ListMultimap<String, String> fieldsPerRecords) {
    
        this.fieldsPerRecords = fieldsPerRecords;
    }
}
