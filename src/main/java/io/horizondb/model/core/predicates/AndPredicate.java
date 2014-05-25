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
import io.horizondb.io.serialization.Parser;
import io.horizondb.model.core.Predicate;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Filter;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.filters.Filters;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.util.TimeZone;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.ImmutableRangeSet.Builder;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

/**
 * An AND predicate
 * 
 * @author Benjamin
 */
final class AndPredicate extends LogicalPredicate {
    
    /**
     * The type of this predicate.
     */
    public static final int TYPE = 4;
    
    /**
     * The parser instance.
     */
    private static final Parser<AndPredicate> PARSER = new Parser<AndPredicate>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public AndPredicate parseFrom(ByteReader reader) throws IOException {

            Predicate left = Predicates.parseFrom(reader);
            Predicate right = Predicates.parseFrom(reader);
            
            return new AndPredicate(left, right);
        }
    };
    
    /**
     * Creates a new <code>AndPredicate</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static AndPredicate parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>AndPredicate</code> instances.
     * @return the parser that can be used to deserialize <code>AndPredicate</code> instances.
     */
    public static Parser<AndPredicate> getParser() {

        return PARSER;
    }
    
    /**
     * Creates an <code>AndPredicate</code> 
     * 
     * @param left the predicate on the left hand side of the AND.
     * @param right the predicate on the right hand side of the AND.
     */
    public AndPredicate(Predicate left, Predicate right) {
        
        super(left, right);
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
    public RangeSet<Field> getTimestampRanges(Field prototype, TimeZone timeZone) {
                
        RangeSet<Field> leftRanges = this.left.getTimestampRanges(prototype, timeZone);
        RangeSet<Field> rightRanges = this.right.getTimestampRanges(prototype, timeZone);
        
        Builder<Field> builder = ImmutableRangeSet.builder();    
        
        for (Range<Field> range : leftRanges.asRanges()) {
            
            builder.addAll(rightRanges.subRangeSet(range));
        }
        
        return builder.build();
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public Filter<Record> toFilter(TimeSeriesDefinition definition) {
        return Filters.and(this.left.toFilter(definition), this.right.toFilter(definition));
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected String getOperatorAsString() {
        return "AND";
    }
}
