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
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Filter;
import io.horizondb.model.core.Predicate;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.filters.Filters;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;

import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

/**
 * An OR predicate
 */
final class OrPredicate extends LogicalPredicate {
    
    /**
     * The type of this predicate.
     */
    public static final int TYPE = 5;
    
    /**
     * The parser instance.
     */
    private static final Parser<OrPredicate> PARSER = new Parser<OrPredicate>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public OrPredicate parseFrom(ByteReader reader) throws IOException {

            Predicate left = Predicates.parseFrom(reader);
            Predicate right = Predicates.parseFrom(reader);
            
            return new OrPredicate(left, right);
        }
    };
    /**
     * @param left the predicate on the left hand side of the OR.
     * @param right the predicate on the right hand side of the OR.
     */
    public OrPredicate(Predicate left, Predicate right) {
        
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
     * Creates a new <code>OrPredicate</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static OrPredicate parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>OrPredicate</code> instances.
     * @return the parser that can be used to deserialize <code>OrPredicate</code> instances.
     */
    public static Parser<OrPredicate> getParser() {

        return PARSER;
    }
    
    /**    
     * {@inheritDoc}
     */
    @Override
    public RangeSet<Field> getTimestampRanges() {
        
        RangeSet<Field> leftRanges = this.left.getTimestampRanges();
        RangeSet<Field> rightRanges = this.right.getTimestampRanges();
        
        RangeSet<Field> rangeSet = TreeRangeSet.create(leftRanges);
        rangeSet.addAll(rightRanges);
        
        return rangeSet;
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public Filter<Record> toFilter(TimeSeriesDefinition definition) {
        return Filters.or(this.left.toFilter(definition), this.right.toFilter(definition));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getOperatorAsString() {
        return "OR";
    }
}
