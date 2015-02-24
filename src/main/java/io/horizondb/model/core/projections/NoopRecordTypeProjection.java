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

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.io.serialization.Parser;
import io.horizondb.model.schema.RecordTypeDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import javax.annotation.concurrent.Immutable;

/**
 * A projection that keeps all the fields of the record to which it is associated.  
 */
@Immutable
public final class NoopRecordTypeProjection implements RecordTypeProjection {
    
    /**
     * The parser instance.
     */
    private static final Parser<NoopRecordTypeProjection> PARSER = new Parser<NoopRecordTypeProjection>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public NoopRecordTypeProjection parseFrom(ByteReader reader) throws IOException {

            int index = VarInts.readUnsignedInt(reader);
            return new NoopRecordTypeProjection(index);
        }
    };
    
    /**
     * The type of this projection.
     */
    public static final int TYPE = 0;
    
    /**
     * The record type index
     */
    private final int recordType;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int getType() {
        return TYPE;
    }

    /**
     * Creates a new <code>NoopRecordTypeProjection</code> for the specified record type.
     * @param recordType the record type index
     */
    public NoopRecordTypeProjection(int recordType) {
        this.recordType = recordType;
    }
    
    /**
     * Creates a new <code>NoopRecordTypeProjection</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static NoopRecordTypeProjection parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>NoopRecordTypeProjection</code> instances.
     * @return the parser that can be used to deserialize <code>NoopRecordTypeProjection</code> instances.
     */
    public static Parser<NoopRecordTypeProjection> getParser() {

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
        
        return timeSeriesDefinition.getRecordType(this.recordType);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int[] getFieldMapping(TimeSeriesDefinition timeSeriesDefinition) {
        
        RecordTypeDefinition original = timeSeriesDefinition.getRecordType(this.recordType);
        
        int[] mapping = new int [original.getNumberOfFields() + 1];
        for (int i = 0; i < mapping.length; i++) {
            mapping[i] = i;
        }
        return mapping;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {
        return VarInts.computeUnsignedIntSize(this.recordType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
        VarInts.writeUnsignedInt(writer, this.recordType);
    }
}
