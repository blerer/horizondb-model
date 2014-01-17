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
 * <code>Payload</code> used to request a time series.
 * 
 * @author Benjamin
 *
 */
@Immutable
public final class GetTimeSeriesRequestPayload implements Payload {
    
    /**
     * The parser instance.
     */
    private static final Parser<GetTimeSeriesRequestPayload> PARSER = new Parser<GetTimeSeriesRequestPayload>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public GetTimeSeriesRequestPayload parseFrom(ByteReader reader) throws IOException {

            String databaseName = VarInts.readString(reader);
            String seriesName = VarInts.readString(reader);
            
            return new GetTimeSeriesRequestPayload(databaseName, seriesName);
        }
    };
    
    /**
     * The database name.
     */
    private final String databaseName;
    
    /**
     * The time series name.
     */
    private final String seriesName;
        
    /**
     * Creates a new payload for a message of type <code>GET_TIMESERIES</code>.
     * 
     * @param databaseName the name of the requested database
     */
    public GetTimeSeriesRequestPayload(String databaseName, String seriesName) {
        
        this.databaseName = databaseName;
        this.seriesName = seriesName;
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
     * Returns the time series name.
     * 
     * @return the time series name.
     */    
    public String getSeriesName() {
        return this.seriesName;
    }

    /**
     * Creates a new <code>GetTimeSeriesRequestPayload</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static GetTimeSeriesRequestPayload parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>GetTimeSeriesRequestPayload</code> instances.
     * @return the parser that can be used to deserialize <code>GetTimeSeriesRequestPayload</code> instances.
     */
    public static Parser<GetTimeSeriesRequestPayload> getParser() {

        return PARSER;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {
        return VarInts.computeStringSize(this.databaseName) + VarInts.computeStringSize(this.seriesName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
        VarInts.writeString(writer, this.databaseName);
        VarInts.writeString(writer, this.seriesName);
    }
}
