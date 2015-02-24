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
import io.horizondb.io.encoding.VarInts;
import io.horizondb.io.serialization.Parser;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Filter;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.fields.ImmutableField;
import io.horizondb.model.core.fields.TimestampField;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

import static io.horizondb.model.core.filters.Filters.not;
import static io.horizondb.model.core.filters.Filters.range;
import static io.horizondb.model.core.filters.Filters.toRecordFilter;


/**
 * A BETWEEN predicate.
 */
final class BetweenPredicate extends FieldPredicate {
        
    /**
     * The type of this predicate.
     */
    public static final int TYPE = 2;
    
    /**
     * The parser instance.
     */
    private static final Parser<BetweenPredicate> PARSER = new Parser<BetweenPredicate>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public BetweenPredicate parseFrom(ByteReader reader) throws IOException {

            String fieldName = VarInts.readString(reader);
            boolean notBetween = reader.readBoolean();
            Field min = readField(reader);
            Field max = readField(reader);

            return new BetweenPredicate(fieldName, min, max, notBetween);
        }
    };
    
    /**
     * The minimum value of the closed range.
     */
    private final Field min;
    
    /**
     * The maximum value of the closed range.
     */
    private final Field max;
    
    /**
     * <code>true</code> if the predicate is a NOT BETWEEN predicate.
     */
    private final boolean notBetween;
         
    /**
     * Creates a new BETWEEN predicate.
     * 
     * @param fieldName the name of the field
     * @param min the minimum value of the closed range
     * @param max the maximum value of the closed range
     */
    public BetweenPredicate(String fieldName, Field min, Field max) {
        
        this(fieldName, min, max, false);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int getType() {
        return TYPE;
    }

    /**
     * Creates a new BETWEEN predicate.
     * 
     * @param fieldName the name of the field
     * @param min the minimum value of the closed range
     * @param max the maximum value of the closed range
     * @param notBetween <code>true</code> if the predicate is a NOT BETWEEN predicate
     */
    public BetweenPredicate(String fieldName, Field min, Field max, boolean notBetween) {
        
        super(fieldName);
        this.min = ImmutableField.of(min);
        this.max = ImmutableField.of(max);
        this.notBetween = notBetween;
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public RangeSet<Field> getTimestampRanges() {
        
        if (!isTimestamp()) {
            return TimestampField.ALL;
        }

        if (this.max.compareTo(this.min) < 0) {
            return ImmutableRangeSet.of();
        }

        Range<Field> range = Range.<Field>closed(this.min, this.max);
        RangeSet<Field> rangeSet = ImmutableRangeSet.of(range);
        
        if (this.notBetween) {
            return rangeSet.complement();
        }
        
        return rangeSet;
    }
    
    /**    
     * {@inheritDoc}
     */
    @Override
    public Filter<Record> toFilter(TimeSeriesDefinition definition) {

        Range<Field> range = Range.closed(this.min, this.max);
        Filter<Field> fieldFilter = range(range, isTimestamp());
        
        if (this.notBetween) {
            fieldFilter = not(fieldFilter);
        }
        
        return toRecordFilter(definition, getFieldName(), fieldFilter);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        
        StringBuilder builder = new StringBuilder().append(getFieldName());
        
        if (this.notBetween) {
            builder.append(" NOT");
        }
        
        return builder.append(" BETWEEN ")
                      .append(this.min)
                      .append(" AND ")
                      .append(this.max)
                      .toString();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {
        return VarInts.computeStringSize(getFieldName()) 
                + 1
                + computeFieldSerializedSize(this.min)
                + computeFieldSerializedSize(this.max);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
        
        VarInts.writeString(writer, getFieldName());
        writer.writeBoolean(this.notBetween);
        writeField(writer, this.min);
        writeField(writer, this.max);
    }
    
    /**
     * Creates a new <code>BetweenPredicate</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static BetweenPredicate parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>BetweenPredicate</code> instances.
     * @return the parser that can be used to deserialize <code>BetweenPredicate</code> instances.
     */
    public static Parser<BetweenPredicate> getParser() {

        return PARSER;
    }
}
