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
import io.horizondb.model.schema.RecordSetDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

/**
 * 
 * @author Benjamin
 *
 */
@Immutable
public final class DataHeaderPayload implements Payload {
    
    /**
     * The parser instance.
     */
    private static final Parser<DataHeaderPayload> PARSER = new Parser<DataHeaderPayload>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public DataHeaderPayload parseFrom(ByteReader reader) throws IOException {

            TimeSeriesDefinition definition = TimeSeriesDefinition.parseFrom(reader);
            
            return new DataHeaderPayload(definition);
        }
    };
    
    /**
     * The record set definition.
     */
    private final RecordSetDefinition definition;
        
    /**
     * Creates a new payload for the response to a message of type <code>GET_TIMESERIES</code>.
     * 
     * @param definition the definition of the record set.
     */
    public DataHeaderPayload(RecordSetDefinition definition) {
        this.definition = definition;
    }

    /**
     * Returns the record set definition.    
     * 
     * @return the record set definition. 
     */
    public RecordSetDefinition getDefinition() {
        return this.definition;
    }

    /**
     * Creates a new <code>GetTimeSeriesResponsePayload</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static DataHeaderPayload parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>GetTimeSeriesResponsePayload</code> instances.
     * @return the parser that can be used to deserialize <code>GetTimeSeriesResponsePayload</code> instances.
     */
    public static Parser<DataHeaderPayload> getParser() {

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
