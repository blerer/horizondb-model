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
import io.horizondb.model.TimeSeriesDefinition;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

/**
 * <code>Payload</code> used to respond to a time series request.
 * 
 * @author Benjamin
 *
 */
@Immutable
public final class GetTimeSeriesResponsePayload implements Payload {
    
    /**
     * The parser instance.
     */
    private static final Parser<GetTimeSeriesResponsePayload> PARSER = new Parser<GetTimeSeriesResponsePayload>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public GetTimeSeriesResponsePayload parseFrom(ByteReader reader) throws IOException {

            TimeSeriesDefinition definition = TimeSeriesDefinition.parseFrom(reader);
            
            return new GetTimeSeriesResponsePayload(definition);
        }
    };
    
    /**
     * The time series definition.
     */
    private final TimeSeriesDefinition definition;
        
    /**
     * Creates a new payload for the response to a message of type <code>GET_TIMESERIES</code>.
     * 
     * @param definition the definition of the time series.
     */
    public GetTimeSeriesResponsePayload(TimeSeriesDefinition definition) {
        this.definition = definition;
    }

    /**
     * Returns the time series definition.    
     * 
     * @return the time series definition. 
     */
    public TimeSeriesDefinition getDefinition() {
        return this.definition;
    }

    /**
     * Creates a new <code>GetTimeSeriesResponsePayload</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static GetTimeSeriesResponsePayload parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>GetTimeSeriesResponsePayload</code> instances.
     * @return the parser that can be used to deserialize <code>GetTimeSeriesResponsePayload</code> instances.
     */
    public static Parser<GetTimeSeriesResponsePayload> getParser() {

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
