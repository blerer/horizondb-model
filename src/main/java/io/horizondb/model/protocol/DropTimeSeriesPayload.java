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
 * <code>Payload</code> used to request a time series deletion.
 *
 */
@Immutable
public final class DropTimeSeriesPayload implements Payload {
    
    /**
     * The parser instance.
     */
    private static final Parser<DropTimeSeriesPayload> PARSER = new Parser<DropTimeSeriesPayload>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public DropTimeSeriesPayload parseFrom(ByteReader reader) throws IOException {

            String database = VarInts.readString(reader);
            String timeSeries = VarInts.readString(reader);
            
            return new DropTimeSeriesPayload(database, timeSeries);
        }
    };
    
    /**
     * The name of the database associated to the time series.
     */
    private final String database;
    
    /**
     * The time series name.
     */
    private final String timeSeries;
        
    /**
     * Creates a new payload for a message of type <code>DROP_TIMESERIES</code>.
     * 
     * @param database the name of the time series database 
     * @param timeSeries the name of the time series to delete.
     */
    public DropTimeSeriesPayload(String database, String timeSeries) {
        this.database = database;
        this.timeSeries = timeSeries;
    }


    /**
     * Creates a new <code>DropTimeSeriesPayload</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static DropTimeSeriesPayload parseFrom(ByteReader reader) throws IOException {

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
     * Returns the name of the time series.
     * @return the timeSeries the name of the time series
     */
    public String getTimeSeries() {
        return this.timeSeries;
    }


    /**
     * Returns the parser that can be used to deserialize <code>DropTimeSeriesPayload</code> instances.
     * @return the parser that can be used to deserialize <code>DropTimeSeriesPayload</code> instances.
     */
    public static Parser<DropTimeSeriesPayload> getParser() {

        return PARSER;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {
        return VarInts.computeStringSize(this.database) 
                + VarInts.computeStringSize(this.timeSeries);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
        VarInts.writeString(writer, this.database); 
        VarInts.writeString(writer, this.timeSeries);
    }
}
