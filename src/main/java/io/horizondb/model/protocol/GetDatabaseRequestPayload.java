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
 * <code>Payload</code> used to request a database.
 * 
 * @author Benjamin
 *
 */
@Immutable
public final class GetDatabaseRequestPayload implements Payload {
    
    /**
     * The parser instance.
     */
    private static final Parser<GetDatabaseRequestPayload> PARSER = new Parser<GetDatabaseRequestPayload>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public GetDatabaseRequestPayload parseFrom(ByteReader reader) throws IOException {

            String databaseName = VarInts.readString(reader);
            
            return new GetDatabaseRequestPayload(databaseName);
        }
    };
    
    /**
     * The database name.
     */
    private final String databaseName;
        
    /**
     * Creates a new payload for a message of type <code>GET_DATABASE</code>.
     * 
     * @param databaseName the name of the requested database
     */
    public GetDatabaseRequestPayload(String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * Returns the database name.
     * 
     * @return the database name.
     */
    public String getDatabaseName() {
        return this.databaseName;
    }

    /**
     * Creates a new <code>CreateDatabaseRequestPayload</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static GetDatabaseRequestPayload parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>CreateDatabaseRequestPayload</code> instances.
     * @return the parser that can be used to deserialize <code>CreateDatabaseRequestPayload</code> instances.
     */
    public static Parser<GetDatabaseRequestPayload> getParser() {

        return PARSER;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {
        return VarInts.computeStringSize(this.databaseName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
        VarInts.writeString(writer, this.databaseName);
    }
}
