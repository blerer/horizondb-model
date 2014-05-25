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
package io.horizondb.model.core.predicates;

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.serialization.Parser;
import io.horizondb.model.core.Predicate;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Filter;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.filters.Filters;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.util.TimeZone;

import com.google.common.collect.RangeSet;

/**
 * <code>Predicate</code> that does nothing.
 * 
 * @author Benjamin
 *
 */
final class NoopPredicate implements Predicate {

    /**
     * The type of this predicate.
     */
    public static final int TYPE = 0;
    
    /**
     * The parser instance.
     */
    private static final Parser<NoopPredicate> PARSER = new Parser<NoopPredicate>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public NoopPredicate parseFrom(ByteReader reader) throws IOException {

            return new NoopPredicate();
        }
    };
    
    /**
     * Creates a new <code>NoopPredicate</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static NoopPredicate parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>NoopPredicate</code> instances.
     * @return the parser that can be used to deserialize <code>NoopPredicate</code> instances.
     */
    public static Parser<NoopPredicate> getParser() {

        return PARSER;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int getType() {
        return TYPE;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RangeSet<Field> getTimestampRanges(Field prototype, TimeZone timeZone) {
        
        return prototype.allValues();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Filter<Record> toFilter(TimeSeriesDefinition definition) {
        return Filters.noop();
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
