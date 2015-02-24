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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.io.serialization.Parser;
import io.horizondb.model.schema.FieldDefinition;
import io.horizondb.model.schema.RecordTypeDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import javax.annotation.concurrent.Immutable;

import static io.horizondb.io.encoding.VarInts.readString;
import static io.horizondb.io.encoding.VarInts.readUnsignedInt;

import static io.horizondb.io.encoding.VarInts.writeString;
import static io.horizondb.io.encoding.VarInts.writeUnsignedInt;

import static io.horizondb.io.encoding.VarInts.computeStringSize;
import static io.horizondb.io.encoding.VarInts.computeUnsignedIntSize;

/**
 * A projection that filter out unwanted fields.  
 */
@Immutable
public final class DefaultRecordTypeProjection implements RecordTypeProjection {
    
    /**
     * The parser instance.
     */
    private static final Parser<DefaultRecordTypeProjection> PARSER = new Parser<DefaultRecordTypeProjection>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public DefaultRecordTypeProjection parseFrom(ByteReader reader) throws IOException {

            int name = VarInts.readUnsignedInt(reader);
            
            int numberOfFields = readUnsignedInt(reader);
            List<String> fields = new ArrayList<>(numberOfFields);
            
            for (int i = 0; i < numberOfFields; i++) {
                fields.add(readString(reader));
            }
            return new DefaultRecordTypeProjection(name, fields);
        }
    };
    
    /**
     * The type of this projection.
     */
    public static final int TYPE = 1;
    
    /**
     * The record type index
     */
    private final int recordType;
    
    /**
     * The fields that must be retained by the projection.
     */
    private final List<String> fields;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int getType() {
        return TYPE;
    }

    /**
     * Creates a new <code>DefaultRecordTypeProjection</code> for the specified record type.
     * @param recordType the record type index
     * @param fields the fields that must be returned in the order in which they must be returned.
     */
    public DefaultRecordTypeProjection(int recordType, List<String> fields) {
        this.recordType = recordType;
        this.fields = fields;
    }
    
    /**
     * Creates a new <code>DefaultRecordTypeProjection</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static DefaultRecordTypeProjection parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>DefaultRecordTypeProjection</code> instances.
     * @return the parser that can be used to deserialize <code>DefaultRecordTypeProjection</code> instances.
     */
    public static Parser<DefaultRecordTypeProjection> getParser() {

        return PARSER;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getRecordType() {
        return this.recordType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordTypeDefinition getRecordTypeDefinition(TimeSeriesDefinition timeSeriesDefinition) {
        
        RecordTypeDefinition original = timeSeriesDefinition.getRecordType(this.recordType);
                
        RecordTypeDefinition.Builder recordTypeBuilder = RecordTypeDefinition.newBuilder(original.getName()); 
        
        for (FieldDefinition fieldDefinition : original) {
            
            if (this.fields.contains(fieldDefinition.getName())) {
                recordTypeBuilder.addField(fieldDefinition);
            }
        }
        
        return recordTypeBuilder.build();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int[] getFieldMapping(TimeSeriesDefinition timeSeriesDefinition) {
        
        RecordTypeDefinition original = timeSeriesDefinition.getRecordType(this.recordType);
        
        int[] mapping = new int[this.fields.size()];
        for (int i = 0; i < mapping.length; i++) {
            String fieldName = this.fields.get(i);
            mapping[i] = original.getFieldIndex(fieldName);
        }
        return mapping;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {
        
        int size = computeUnsignedIntSize(this.recordType);

        size += computeUnsignedIntSize(this.fields.size());
        
        for (int i = 0, m = this.fields.size(); i < m; i++) {
            size += computeStringSize(this.fields.get(i));
        }
        
        return size;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
        
        writeUnsignedInt(writer, this.recordType);

        writeUnsignedInt(writer, this.fields.size());
        
        for (int i = 0, m = this.fields.size(); i < m; i++) {
            writeString(writer, this.fields.get(i));
        }
    }
}
