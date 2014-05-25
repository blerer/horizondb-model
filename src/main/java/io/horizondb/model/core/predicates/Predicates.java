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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.serialization.Parser;
import io.horizondb.model.core.Predicate;

/**
 * Factory methods for <code>Predicate</code>s.
 * 
 * @author Benjamin
 */
public final class Predicates {

    /**
     * Predicate that does nothing.
     */
    private static final NoopPredicate NOOP_PREDICATE = new NoopPredicate();
        
    /**
     * The Predicate parser instance.
     */
    @SuppressWarnings("boxing")
    private static final Parser<Predicate> PARSER = new Parser<Predicate>() {

        /**
         * The parsers for the different predicate types.
         */
        private final Map<Integer, Parser<? extends Predicate>> parsers;
        
        {
            Map<Integer, Parser<? extends Predicate>> predicateParsers = new HashMap<>();
            predicateParsers.put(NoopPredicate.TYPE, NoopPredicate.getParser());
            predicateParsers.put(SimplePredicate.TYPE, SimplePredicate.getParser());
            predicateParsers.put(BetweenPredicate.TYPE, BetweenPredicate.getParser());
            predicateParsers.put(InPredicate.TYPE, InPredicate.getParser());
            predicateParsers.put(AndPredicate.TYPE, AndPredicate.getParser());
            predicateParsers.put(OrPredicate.TYPE, OrPredicate.getParser());
            
            this.parsers = Collections.unmodifiableMap(predicateParsers);
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public Predicate parseFrom(ByteReader reader) throws IOException {

            int type = reader.readByte();
            Parser<? extends Predicate> parser = this.parsers.get(type);
            
            return parser.parseFrom(reader);
        }
    };
    
    /**
     * Creates simple a predicate. 
     * 
     * @param fieldName the name of the field 
     * @param operator the operator 
     * @param value the value to which the value of the field must be compared
     * @return a simple a predicate
     */
    public static Predicate simplePredicate(String fieldName, Operator operator, String value) {
        return new SimplePredicate(fieldName, operator, value);
    }
    
    /**
     * Creates an EQUAL predicate. 
     * 
     * @param fieldName the name of the field 
     * @param value the value to which the value of the field must be compared
     * @return an EQUAL predicate
     */
    public static Predicate eq(String fieldName, String value) {
        return simplePredicate(fieldName, Operator.EQ, value);
    }
    
    /**
     * Creates a NOT EQUAL predicate. 
     * 
     * @param fieldName the name of the field 
     * @param value the value to which the value of the field must be compared
     * @return a NOT EQUAL predicate
     */
    public static Predicate ne(String fieldName, String value) {
        return simplePredicate(fieldName, Operator.NE, value);
    }
    
    /**
     * Creates a GREATER THAN predicate. 
     * 
     * @param fieldName the name of the field 
     * @param value the value to which the value of the field must be compared
     * @return a GREATER THAN predicate
     */
    public static Predicate gt(String fieldName, String value) {
        return simplePredicate(fieldName, Operator.GT, value);
    }
    
    /**
     * Creates a GREATER OR EQUAL predicate. 
     * 
     * @param fieldName the name of the field 
     * @param value the value to which the value of the field must be compared
     * @return a GREATER OR EQUAL predicate
     */
    public static Predicate ge(String fieldName, String value) {
        return simplePredicate(fieldName, Operator.GE, value);
    }
    
    /**
     * Creates a LESS OR EQUAL predicate. 
     * 
     * @param fieldName the name of the field 
     * @param value the value to which the value of the field must be compared
     * @return a LESS OR EQUAL predicate
     */
    public static Predicate le(String fieldName, String value) {
        return simplePredicate(fieldName, Operator.LE, value);
    }
    
    /**
     * Creates a LESS THAN predicate. 
     * 
     * @param fieldName the name of the field 
     * @param value the value to which the value of the field must be compared
     * @return a LESS THAN predicate
     */
    public static Predicate lt(String fieldName, String value) {
        return simplePredicate(fieldName, Operator.LT, value);
    }
    
    /**
     * Creates an AND predicate. 
     * 
     * @param left the predicate on the left hand side of the AND
     * @param right the predicate on the right hand side of the AND
     * @return the AND predicate
     */
    public static Predicate and(Predicate left, Predicate right) {
        return new AndPredicate(left, right);
    }
    
    /**
     * Creates an OR predicate. 
     * 
     * @param left the predicate on the left hand side of the OR
     * @param right the predicate on the right hand side of the OR
     * @return the OR predicate
     */
    public static Predicate or(Predicate left, Predicate right) {
        return new OrPredicate(left, right);
    }
    
    /**
     * Creates an IN predicate. 
     * 
     * @param fieldName the name of the field
     * @param values the values
     * @return the IN predicate
     */
    public static Predicate in(String fieldName, List<String> values) {
        return new InPredicate(fieldName, values, false);
    }
    
    /**
     * Creates a NOT IN predicate. 
     * 
     * @param fieldName the name of the field
     * @param values the values
     * @return the NOT IN predicate
     */
    public static Predicate notIn(String fieldName, List<String> values) {
        return new InPredicate(fieldName, values, true);
    }
    
    /**
     * Creates a BETWEEN predicate. 
     * 
     * @param fieldName the name of the field
     * @param min the minimum value of the closed range
     * @param max the maximum value of the closed range
     * @return a BETWEEN predicate
     */
    public static Predicate between(String fieldName, String min, String max) {
        return new BetweenPredicate(fieldName, min, max, false);
    }
    
    /**
     * Creates a NOT BETWEEN predicate. 
     * 
     * @param fieldName the name of the field
     * @param min the minimum value of the closed range
     * @param max the maximum value of the closed range
     * @return a BETWEEN predicate
     */
    public static Predicate notBetween(String fieldName, String min, String max) {
        return new BetweenPredicate(fieldName, min, max, true);
    }
    
    /**
     * Returns an predicate that does nothing.
     * 
     * @return an predicate that does nothing.
     */
    public static Predicate noop() {
        return NOOP_PREDICATE;
    }
    
    /**
     * Deserializes the predicate from the specified reader.
     * 
     * @param reader the reader to read from
     * @return the predicate deserialized
     * @throws IOException if an I/O problem occurs
     */
    public static Predicate parseFrom(ByteReader reader) throws IOException {
        
        return PARSER.parseFrom(reader);
    }
    
    /**
     * Serialize the specified predicate.
     * @param writer the writer
     * @param predicate the predicate to serialize
     * 
     * @throws IOException if an I/O problem occurs
     */
    public static void write(ByteWriter writer, Predicate predicate) throws IOException {
        
        writer.writeByte(predicate.getType());
        predicate.writeTo(writer);
    }
    
    /**
     * Computes the size of the specified predicate.
     * 
     * @param predicate the predicate for which the serialized size must be computed
     */
    public static int computeSerializedSize(Predicate predicate) {
        
        return 1 + predicate.computeSerializedSize();
    }
    
    /**
     * Must not be instantiated.
     */
    private Predicates() {
        
    }
}
