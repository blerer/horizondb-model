/**
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
package io.horizondb.model.core.projections;

import java.io.IOException;

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.serialization.Parser;
import io.horizondb.model.core.Filter;
import io.horizondb.model.core.Projection;
import io.horizondb.model.core.RecordIterator;
import io.horizondb.model.core.filters.Filters;
import io.horizondb.model.schema.RecordSetDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

/**
 * Projection that return all the fields from all the records. 
 *
 */
public final class NoopProjection implements Projection {
    
    /**
     * The type of this projection.
     */
    public static final int TYPE = 0;
    
    /**
     * The parser instance.
     */
    private static final Parser<NoopProjection> PARSER = new Parser<NoopProjection>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public NoopProjection parseFrom(ByteReader reader) throws IOException {

            return new NoopProjection();
        }
    };
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int getType() {
        return TYPE;
    }

    /**
     * Creates a new <code>NoopProjection</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static NoopProjection parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>NoopProjection</code> instances.
     * @return the parser that can be used to deserialize <code>NoopProjection</code> instances.
     */
    public static Parser<NoopProjection> getParser() {

        return PARSER;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Filter<String> getRecordTypeFilter(TimeSeriesDefinition timeSeriesDefinition) {
        return Filters.noop();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordSetDefinition getDefinition(TimeSeriesDefinition timeSeriesDefinition) {
        return timeSeriesDefinition.delegate();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordIterator filterFields(TimeSeriesDefinition timeSeriesDefinition, RecordIterator iterator) {
        return iterator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
    }
}
