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
import io.horizondb.io.serialization.Serializable;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Filter;

import java.io.IOException;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

import static io.horizondb.model.core.filters.Filters.eq;
import static io.horizondb.model.core.filters.Filters.not;
import static io.horizondb.model.core.filters.Filters.range;

/**
 * The comparison operators.
 * 
 * @author Benjamin
 *
 */
public enum Operator implements Serializable {

    /**
     * The equal operator: '=' 
     */
    EQ(0) {
        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return "=";
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public RangeSet<Field> getRangeSet(Field value) {
            return ImmutableRangeSet.of(Range.closed(value, value));
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Filter<Field> getFilter(Field value, boolean timestamp) {
            return eq(value, timestamp);
        }
    },
    
    /**
     * The not equal operator: '!=' 
     */
    NE(1) {
        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return "!=";
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public RangeSet<Field> getRangeSet(Field value) {
            return ImmutableRangeSet.<Field>builder()
                                    .add(Range.closedOpen(value.minValue(), value))
                                    .add(Range.openClosed(value, value.maxValue()))
                                    .build();
                                             
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Filter<Field> getFilter(Field value, boolean timestamp) {
            return not(eq(value, timestamp));
        }
    },
    
    /**
     * The less than operator: '<' 
     */
    LT(2) {
        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return "<";
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public RangeSet<Field> getRangeSet(Field value) {
            return ImmutableRangeSet.of(Range.closedOpen(value.minValue(), value));
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public Filter<Field> getFilter(Field value, boolean timestamp) {
            return range(Range.closedOpen(value.minValue(), value), timestamp);
        }
    },
    
    /**
     * The less than or equal operator: '<='
     */
    LE(3) {
        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return "<=";
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public RangeSet<Field> getRangeSet(Field value) {
            return ImmutableRangeSet.of(Range.closed(value.minValue(), value));
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Filter<Field> getFilter(Field value, boolean timestamp) {
            return range(Range.closed(value.minValue(), value), timestamp);
        }
    },
    
    /**
     * The greater than operator: '>' 
     */
    GT(4) {        
        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return ">";
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public RangeSet<Field> getRangeSet(Field value) {
            return ImmutableRangeSet.of(Range.openClosed(value, value.maxValue()));
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public Filter<Field> getFilter(Field value, boolean timestamp) {
            return range(Range.openClosed(value, value.maxValue()), timestamp);
        }
    },
    
    /**
     * The greater than or equal operator: '>=' 
     */
    GE(5) {       
        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return ">=";
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public RangeSet<Field> getRangeSet(Field value) {
            return ImmutableRangeSet.of(Range.closed(value, value.maxValue()));
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public Filter<Field> getFilter(Field value, boolean timestamp) {
            return range(Range.closed(value, value.maxValue()), timestamp);
        }
    };
    
    /**
     * The parser instance.
     */
    private static final Parser<Operator> PARSER = new Parser<Operator>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public Operator parseFrom(ByteReader reader) throws IOException {

            int code = reader.readByte();

            Operator[] values = Operator.values();

            for (int i = 0; i < values.length; i++) {

                Operator operator = values[i];

                if (operator.b == code) {

                    return operator;
                }
            }

            throw new IllegalStateException("The byte " + code + " does not match any operator");
        }
    };
    
    /**
     * Returns the operator associated to the specified symbol.
     * 
     * @param symbol the symbol
     * @return the operator associated to the specified symbol.
     */
    public static Operator fromSymbol(String symbol) {
        
        for (Operator operator : Operator.values()) {
            
            if (operator.toString().equals(symbol)) {
                return operator;
            }
        }
                
        throw new IllegalArgumentException("Unknown operator: " + symbol);
    }

    /**
     * The operator binary representation.
     */
    private final int b;

    /**
     * Creates a new <code>Operator</code> with the specified binary representation.
     * 
     * @param b the byte representing the <code>Operator</code>.
     */
    private Operator(int b) {

        this.b = b;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {

        return 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {

        writer.writeByte(this.b);
    }
    
    /**
     * Creates a new <code>Operator</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static Operator parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>Operator</code> instances.
     * @return the parser that can be used to deserialize <code>Operator</code> instances.
     */
    public static Parser<Operator> getParser() {

        return PARSER;
    }
    
    /**
     * Returns the range corresponding to specified field for this operator.
     * 
     * @param field the field used to create the range.
     */
    public abstract RangeSet<Field> getRangeSet(Field value);
    
    /**
     * Returns the filter corresponding to this operator.
     * 
     * @param value the value
     * @param timestamp <code>true</code> if the field is the record timestamp
     * @return the filter corresponding to this operator.
     */
    public abstract Filter<Field> getFilter(Field value, boolean timestamp);
}
