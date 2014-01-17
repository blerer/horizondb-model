/**
 * Copyright 2014 Benjamin Lerer
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
package io.horizondb.model.protocol;

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.serialization.Parser;
import io.horizondb.model.DatabaseDefinition;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

/**
 * <code>Payload</code> used to respond to a database request.
 * 
 * @author Benjamin
 *
 */
@Immutable
public final class GetDatabaseResponsePayload implements Payload {
    
    /**
     * The parser instance.
     */
    private static final Parser<GetDatabaseResponsePayload> PARSER = new Parser<GetDatabaseResponsePayload>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public GetDatabaseResponsePayload parseFrom(ByteReader reader) throws IOException {

            DatabaseDefinition definition = DatabaseDefinition.parseFrom(reader);
            
            return new GetDatabaseResponsePayload(definition);
        }
    };
    
    /**
     * The database definition.
     */
    private final DatabaseDefinition definition;
        
    /**
     * Creates a new payload for the response to a message of type <code>GET_DATABASE</code>.
     * 
     * @param definition the definition of the database.
     */
    public GetDatabaseResponsePayload(DatabaseDefinition definition) {
        this.definition = definition;
    }

    /**
     * Returns the database definition.    
     * 
     * @return the database definition. 
     */
    public DatabaseDefinition getDefinition() {
        return this.definition;
    }

    /**
     * Creates a new <code>CreateDatabaseRequestPayload</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static GetDatabaseResponsePayload parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>CreateDatabaseRequestPayload</code> instances.
     * @return the parser that can be used to deserialize <code>CreateDatabaseRequestPayload</code> instances.
     */
    public static Parser<GetDatabaseResponsePayload> getParser() {

        return PARSER;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {
        return this.definition.computeSerializedSize();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
        this.definition.writeTo(writer);
    }
}
