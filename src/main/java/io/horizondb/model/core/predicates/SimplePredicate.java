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
import io.horizondb.model.core.filters.Filters;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.google.common.collect.RangeSet;

/**
 * A simple predicate used to compare a field to given value.
 */
final class SimplePredicate extends FieldPredicate {
    
    /**
     * The type of this predicate.
     */
    public static final int TYPE = 1;
    
    /**
     * The parser instance.
     */
    private static final Parser<SimplePredicate> PARSER = new Parser<SimplePredicate>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public SimplePredicate parseFrom(ByteReader reader) throws IOException {

            String fieldName = VarInts.readString(reader);
            Operator operator = Operator.parseFrom(reader);
            Field value = ImmutableField.of(readField(reader));
            
            return new SimplePredicate(fieldName, operator, value);
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
     * The operator.
     */
    private final Operator operator;
    
    /**
     * The value to which the field value must be compared. 
     */
    private final Field value;

    /**
     * Creates a new <code>SimplePredicate</code> instance for the specified field.
     * 
     * @param fieldName the field name
     * @param operator the operator
     * @param value the value to which the field value must be compared
     */
    public SimplePredicate(String fieldName, Operator operator, Field value) {
        
        super(fieldName);
        this.operator = operator;
        this.value = ImmutableField.of(value);
    }

    /**
     * Returns the operator.
     * 
     * @return the operator.
     */
    public Operator getOperator() {
        return this.operator;
    }

    /**
     * Returns the value to which the field value must be compared.
     * @return the value to which the field value must be compared
     */
    public Field getValue() {
        return this.value;
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public RangeSet<Field> getTimestampRanges() {
        
        if (!isTimestamp()) {
            return TimestampField.ALL;
        }
        
        return this.operator.getRangeSet(this.value);
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public Filter<Record> toFilter(TimeSeriesDefinition definition) {

        Filter<Field> fieldFilter = this.operator.getFilter(this.value, isTimestamp());
        
        return Filters.toRecordFilter(definition, getFieldName(), fieldFilter);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object object) {
        
        if (object == this) {
            return true;
        }
        if (!(object instanceof SimplePredicate)) {
            return false;
        }
        SimplePredicate rhs = (SimplePredicate) object;
        return new EqualsBuilder().appendSuper(super.equals(object))
                                  .append(this.operator, rhs.operator)
                                  .append(this.value, rhs.value)
                                  .isEquals();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        
        return new HashCodeBuilder(-86018061, -1103579581).appendSuper(super.hashCode())
                .append(this.value)
                                                          .append(this.operator)
                                                          .toHashCode();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return new StringBuilder().append(getFieldName())
                                  .append(' ')
                                  .append(this.operator)
                                  .append(' ')
                                  .append(this.value)
                                  .toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {
        return VarInts.computeStringSize(getFieldName()) 
                + this.operator.computeSerializedSize()
                + computeFieldSerializedSize(this.value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
        VarInts.writeString(writer, getFieldName());
        this.operator.writeTo(writer);
        writeField(writer, this.value);
    }
    
    /**
     * Creates a new <code>SimplePredicate</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static SimplePredicate parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>SimplePredicate</code> instances.
     * @return the parser that can be used to deserialize <code>SimplePredicate</code> instances.
     */
    public static Parser<SimplePredicate> getParser() {

        return PARSER;
    }
}
