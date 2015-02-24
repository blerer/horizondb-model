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
package io.horizondb.model.core.projections;

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.serialization.Parser;
import io.horizondb.model.core.Filter;
import io.horizondb.model.core.Projection;
import io.horizondb.model.core.RecordIterator;
import io.horizondb.model.core.filters.Filters;
import io.horizondb.model.core.iterators.FieldFilteringIterator;
import io.horizondb.model.core.records.FieldFilter;
import io.horizondb.model.schema.DefaultRecordSetDefinition;
import io.horizondb.model.schema.RecordSetDefinition;
import io.horizondb.model.schema.RecordTypeDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.annotation.concurrent.Immutable;

import static io.horizondb.io.encoding.VarInts.computeUnsignedIntSize;
import static io.horizondb.io.encoding.VarInts.readUnsignedInt;
import static io.horizondb.io.encoding.VarInts.writeUnsignedInt;

/**
 */
@Immutable
public final class DefaultProjection implements Projection {
         
    /**
     * The type of this projection.
     */
    public static final int TYPE = 1;

    /**
     * The parser instance.
     */
    private static final Parser<DefaultProjection> PARSER = new Parser<DefaultProjection>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public DefaultProjection parseFrom(ByteReader reader) throws IOException {

            int numberOfProjections = readUnsignedInt(reader);
            List<RecordTypeProjection> projections = new ArrayList<>();
            
            for (int i = 0; i < numberOfProjections; i++) {
                
                RecordTypeProjection projection = ProjectionParser.INSTANCE.parseFrom(reader);
                projections.add(projection);
            }
            return new DefaultProjection(projections);
        }
    };
    
    /**
     * The fields that must be returned by records.
     */
    private final List<RecordTypeProjection> projections;

    /**
     * @param projections the record type projections
     */
    public DefaultProjection(List<RecordTypeProjection> projections) {
        
        this.projections = projections;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getType() {
        return TYPE;
    }
    
    /**
     * Creates a new <code>Projection</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static DefaultProjection parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>Projection</code> instances.
     * @return the parser that can be used to deserialize <code>Projection</code> instances.
     */
    public static Parser<DefaultProjection> getParser() {

        return PARSER;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Filter<String> getRecordTypeFilter(TimeSeriesDefinition timeSeriesDefinition) {
       
        SortedSet<String> types = new TreeSet<>();
        
        for (int i = 0, m = this.projections.size(); i < m; i++) {
            RecordTypeProjection projection = this.projections.get(i);
            String name = timeSeriesDefinition.getRecordName(projection.getRecordType());
            types.add(name);
        }
        return Filters.in(types, false);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {
        
        int size = 0;

        size += computeUnsignedIntSize(this.projections.size());
        
        for (int i = 0, m = this.projections.size(); i < m; i++) {
            RecordTypeProjection projection = this.projections.get(i);
            size += 1 + projection.computeSerializedSize();
        }
        
        return size;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {

        writeUnsignedInt(writer, this.projections.size());
        
        for (int i = 0, m = this.projections.size(); i < m; i++) {
            
            RecordTypeProjection projection = this.projections.get(i);
            writer.writeByte(projection.getType());
            projection.writeTo(writer);
        }
    } 
    
    /**
     * {@inheritDoc}
     */
    @Override
    public RecordSetDefinition getDefinition(TimeSeriesDefinition timeSeriesDefinition) {
        
        DefaultRecordSetDefinition.Builder builder = DefaultRecordSetDefinition.newBuilder()
                                                                               .timeUnit(timeSeriesDefinition.getTimeUnit())
                                                                               .timeZone(timeSeriesDefinition.getTimeZone());
        for (int i = 0, m = this.projections.size(); i < m; i++) {
            
            RecordTypeProjection projection = this.projections.get(i);
            
            RecordTypeDefinition newDefinition = projection.getRecordTypeDefinition(timeSeriesDefinition);
            builder.addRecordType(newDefinition);
        }
        
        return builder.build();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public RecordIterator filterFields(TimeSeriesDefinition timeSeriesDefinition, RecordIterator iterator) {

        FieldFilter[] filters = new FieldFilter[timeSeriesDefinition.getNumberOfRecordTypes()];
        int type = 0;
        
        for (int i = 0, m = this.projections.size(); i < m; i++) {
            
            RecordTypeProjection projection = this.projections.get(i);
            int recordIndex = projection.getRecordType();
            int[] mapping = projection.getFieldMapping(timeSeriesDefinition);
            filters[recordIndex] = new FieldFilter(type, mapping);
            type++;
        }
        return new FieldFilteringIterator(iterator, filters);
    }
    
//    private static ListMultimap<String, String> convertToMultimap(List<String> expressions) {
//        
//        if (expressions.size() == 1 && ALL.equals(expressions.get(0))) {
//            return ImmutableListMultimap.of(ALL, ALL);
//        }
//        
//        ListMultimap<String, String> multimap = ArrayListMultimap.create();
//        
//        for (String expression : expressions) {
//            String[] elements = expression.split("\\.");
//            String record = elements[0];
//            String field = elements.length > 1 ? elements[1] : ALL; 
//            
//            multimap.put(record, field);
//        }
//        return multimap;
//    }
}
