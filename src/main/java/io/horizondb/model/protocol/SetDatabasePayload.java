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
import io.horizondb.model.schema.DatabaseDefinition;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

/**
 * <code>Payload</code> used to notify the client that the used database has changed.
 * 
 * @author Benjamin
 *
 */
@Immutable
public final class SetDatabasePayload implements Payload {
    
    /**
     * The parser instance.
     */
    private static final Parser<SetDatabasePayload> PARSER = new Parser<SetDatabasePayload>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public SetDatabasePayload parseFrom(ByteReader reader) throws IOException {

            DatabaseDefinition definition = DatabaseDefinition.parseFrom(reader);
            
            return new SetDatabasePayload(definition);
        }
    };
    
    /**
     * The database definition.
     */
    private final DatabaseDefinition definition;
        
    /**
     * Creates a new payload for the response to a <code>USE DATABASE</code> query.
     * 
     * @param definition the definition of the database.
     */
    public SetDatabasePayload(DatabaseDefinition definition) {
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
     * Creates a new <code>SetDatabasePayload</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static SetDatabasePayload parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>SetDatabasePayload</code> instances.
     * @return the parser that can be used to deserialize <code>SetDatabasePayload</code> instances.
     */
    public static Parser<SetDatabasePayload> getParser() {

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
