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
import io.horizondb.io.encoding.VarInts;
import io.horizondb.io.serialization.Parser;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

/**
 * <code>Payload</code> used to change the database.
 * 
 * @author Benjamin
 *
 */
@Immutable
public final class UseDatabasePayload implements Payload {
    
    /**
     * The parser instance.
     */
    private static final Parser<UseDatabasePayload> PARSER = new Parser<UseDatabasePayload>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public UseDatabasePayload parseFrom(ByteReader reader) throws IOException {

            String database = VarInts.readString(reader);
            
            return new UseDatabasePayload(database);
        }
    };
    
    /**
     * The database name.
     */
    private final String database;
        
    /**
     * Creates a new payload to request a database change.
     * 
     * @param database the database name.
     */
    public UseDatabasePayload(String database) {
        this.database = database;
    }

    /**
     * Returns the database name.    
     * 
     * @return the database name. 
     */
    public String getDatabase() {
        return this.database;
    }

    /**
     * Creates a new <code>UseDatabasePayload</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static UseDatabasePayload parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>UseDatabasePayload</code> instances.
     * @return the parser that can be used to deserialize <code>UseDatabasePayload</code> instances.
     */
    public static Parser<UseDatabasePayload> getParser() {

        return PARSER;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {
        return VarInts.computeStringSize(this.database);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
        VarInts.writeString(writer, this.database);
    }
}
