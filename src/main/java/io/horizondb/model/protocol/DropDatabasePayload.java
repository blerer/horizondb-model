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
 * <code>Payload</code> used to request a database deletion.
 *
 */
@Immutable
public final class DropDatabasePayload implements Payload {
    
    /**
     * The parser instance.
     */
    private static final Parser<DropDatabasePayload> PARSER = new Parser<DropDatabasePayload>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public DropDatabasePayload parseFrom(ByteReader reader) throws IOException {

            String database = VarInts.readString(reader);
            return new DropDatabasePayload(database);
        }
    };
    
    /**
     * The name of the database.
     */
    private final String database;
        
    /**
     * Creates a new payload for a message of type <code>DROP_DATABASE</code>.
     * 
     * @param database the name of the database to drop
     */
    public DropDatabasePayload(String database) {
        this.database = database;
    }


    /**
     * Creates a new <code>DropDatabasePayload</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static DropDatabasePayload parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }
    
    /**
     * Returns the database name.
     * @return the database name
     */
    public String getDatabase() {
        return this.database;
    }

    /**
     * Returns the parser that can be used to deserialize <code>DropDatabasePayload</code> instances.
     * @return the parser that can be used to deserialize <code>DropDatabasePayload</code> instances.
     */
    public static Parser<DropDatabasePayload> getParser() {

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
        VarInts.writeString(writer, this.database);;
    }
}
