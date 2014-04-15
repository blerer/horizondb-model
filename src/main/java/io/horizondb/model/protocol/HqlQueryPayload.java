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

import static org.apache.commons.lang.Validate.notEmpty;

/**
 * <code>Payload</code> used to execute an HQL query on a specified database.
 * 
 * @author Benjamin
 *
 */
@Immutable
public final class HqlQueryPayload implements Payload {
    
    /**
     * The parser instance.
     */
    private static final Parser<HqlQueryPayload> PARSER = new Parser<HqlQueryPayload>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public HqlQueryPayload parseFrom(ByteReader reader) throws IOException {

            String databaseName = VarInts.readString(reader);
            String query = VarInts.readString(reader);
            
            return new HqlQueryPayload(databaseName, query);
        }
    };
    
    /**
     * The database name.
     */
    private final String databaseName;
    
    /**
     * The HQL query.
     */
    private final String query;
        
    /**
     * Creates a new payload for a message of type <code>QUERY</code>.
     * 
     * @param databaseName the name of the database on which the query must be executed.
     * @param query the HQL query to execute.
     */
    public HqlQueryPayload(String databaseName, String query) {
        
        notEmpty(query, "the query parameter must not be empty.");
        
        this.databaseName = databaseName;
        this.query = query;
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
     * Returns the HQL query to execute.
     * @return the HQL query to execute.
     */
    public String getQuery() {
        return this.query;
    }
    
    /**
     * Creates a new <code>CreateDatabaseRequestPayload</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static HqlQueryPayload parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>CreateDatabaseRequestPayload</code> instances.
     * @return the parser that can be used to deserialize <code>CreateDatabaseRequestPayload</code> instances.
     */
    public static Parser<HqlQueryPayload> getParser() {

        return PARSER;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {
        return VarInts.computeStringSize(this.databaseName) 
                + VarInts.computeStringSize(this.query);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
        VarInts.writeString(writer, this.databaseName);
        VarInts.writeString(writer, this.query);
    }
}
