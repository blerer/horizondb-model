/**
 * Copyright 2013 Benjamin Lerer
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
import io.horizondb.model.TimeRange;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

/**
 * A query used to request data from the server.
 * 
 * @author Benjamin
 * 
 */
@Immutable
public final class QueryPayload implements Payload {

    /**
     * The parser instance.
     */
    private static final Parser<QueryPayload> PARSER = new Parser<QueryPayload>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public QueryPayload parseFrom(ByteReader reader) throws IOException {

            String databaseName = VarInts.readString(reader);
            String seriesName = VarInts.readString(reader);
            TimeRange range = TimeRange.parseFrom(reader);

            return new QueryPayload(databaseName, seriesName, range);
        }
    };

    /**
     * The database from which the records must be read.
     */
    private final String databaseName;
    
    /**
     * The time series from which the records must be read.
     */
    private final String seriesName;

    /**
     * The time range for which data must be returned from the partition.
     */
    private final TimeRange timeRange;

    /**
     * @param timeRange
     */
    public QueryPayload(String databaseName, String seriesName, TimeRange timeRange) {

        this.databaseName = databaseName;
        this.seriesName = seriesName;
        this.timeRange = timeRange;
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
     * Returns the time range for which data must be returned.
     * 
     * @return the time range for which data must be returned.
     */
    public TimeRange getTimeRange() {
        return this.timeRange;
    }

    /**
     * Creates a new <code>QueryPayload</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static QueryPayload parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>QueryPayload</code> instances.
     * 
     * @return the parser that can be used to deserialize <code>QueryPayload</code> instances.
     */
    public static Parser<QueryPayload> getParser() {

        return PARSER;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {
        return VarInts.computeStringSize(this.databaseName) 
                + VarInts.computeStringSize(this.seriesName) 
                + this.timeRange.computeSerializedSize();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
        
        VarInts.writeString(writer, this.databaseName);
        VarInts.writeString(writer, this.seriesName);
        this.timeRange.writeTo(writer);
    }
}
