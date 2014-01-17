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
import io.horizondb.model.TimeSeriesDefinition;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

/**
 * <code>Payload</code> used to request a time series creation.
 * 
 * @author Benjamin
 *
 */
@Immutable
public final class CreateTimeSeriesRequestPayload implements Payload {
    
    /**
     * The parser instance.
     */
    private static final Parser<CreateTimeSeriesRequestPayload> PARSER = new Parser<CreateTimeSeriesRequestPayload>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public CreateTimeSeriesRequestPayload parseFrom(ByteReader reader) throws IOException {

            String databaseName = VarInts.readString(reader);    
            TimeSeriesDefinition definition = TimeSeriesDefinition.parseFrom(reader);
            
            return new CreateTimeSeriesRequestPayload(databaseName, definition);
        }
    };
    
    /**
     * The name of the database were the time series must be created.
     */
    private final String databaseName;
    
    /**
     * The time series definition
     */
    private final TimeSeriesDefinition definition;
        
    /**
     * Creates a new payload for a message of type <code>CREATE_TIMESERIES</code>.
     * 
     * @param databaseName the name of the database were the time series must be created
     * @param definition the definition of the time series to create.
     */
    public CreateTimeSeriesRequestPayload(String databaseName, TimeSeriesDefinition definition) {
        
        this.databaseName = databaseName;
        this.definition = definition;
    }

    /**
     * Returns the name of the database were the time series must be created.
     * 
     * @return the name of the database were the time series must be created.
     */
    public String getDatabaseName() {
        return this.databaseName;
    }

    /**
     * Returns the definition of the time series to create.
     * 
     * @return the definition of the time series to create.
     */
    public TimeSeriesDefinition getDefinition() {
        return this.definition;
    }

    /**
     * Creates a new <code>CreateDatabaseRequestPayload</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static CreateTimeSeriesRequestPayload parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>CreateDatabaseRequestPayload</code> instances.
     * @return the parser that can be used to deserialize <code>CreateDatabaseRequestPayload</code> instances.
     */
    public static Parser<CreateTimeSeriesRequestPayload> getParser() {

        return PARSER;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {
        return VarInts.computeStringSize(this.databaseName) + this.definition.computeSerializedSize();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
        
        VarInts.writeString(writer, this.databaseName);
        this.definition.writeTo(writer);
    }
}
