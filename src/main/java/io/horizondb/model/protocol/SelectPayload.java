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
import io.horizondb.model.core.Predicate;
import io.horizondb.model.core.Projection;
import io.horizondb.model.core.predicates.Predicates;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

/**
 * A query used to request data from the server.
 * 
 * @author Benjamin
 * 
 */
@Immutable
public final class SelectPayload implements Payload {

    /**
     * The parser instance.
     */
    private static final Parser<SelectPayload> PARSER = new Parser<SelectPayload>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public SelectPayload parseFrom(ByteReader reader) throws IOException {

            String databaseName = VarInts.readString(reader);
            String seriesName = VarInts.readString(reader);
            
            Projection projection = Projection.parseFrom(reader);
            Predicate predicate = Predicates.parseFrom(reader);

            return new SelectPayload(databaseName, seriesName, projection, predicate);
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
     * The projection.
     */
    private final Projection projection;

    /**
     * The predicate used to select the record that must be returned.
     */
    private final Predicate predicate;

    /**
     * @param timeRange
     */
    public SelectPayload(String databaseName, String seriesName, Projection projection, Predicate predicate) {

        this.databaseName = databaseName;
        this.seriesName = seriesName;
        this.projection = projection;
        this.predicate = predicate;
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
     * Returns the projection.
     * 
     * @return the projection
     */
    public Projection getProjection() {
        return this.projection;
    }

    /**
     * Returns the predicate used to select the record that must be returned.
     * 
     * @return the predicate used to select the record that must be returned.
     */
    public Predicate getPredicate() {
        return this.predicate;
    }

    /**
     * Creates a new <code>SelectPayload</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static SelectPayload parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>SelectPayload</code> instances.
     * 
     * @return the parser that can be used to deserialize <code>SelectPayload</code> instances.
     */
    public static Parser<SelectPayload> getParser() {

        return PARSER;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {
        return VarInts.computeStringSize(this.databaseName) 
                + VarInts.computeStringSize(this.seriesName) 
                + this.projection.computeSerializedSize()
                + Predicates.computeSerializedSize(this.predicate);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
        
        VarInts.writeString(writer, this.databaseName);
        VarInts.writeString(writer, this.seriesName);
        this.projection.writeTo(writer);
        Predicates.write(writer, this.predicate);
    }
}
