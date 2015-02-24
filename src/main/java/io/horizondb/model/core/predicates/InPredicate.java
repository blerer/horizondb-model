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
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang.text.StrBuilder;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

import static io.horizondb.model.core.filters.Filters.in;
import static io.horizondb.model.core.filters.Filters.not;
import static io.horizondb.model.core.filters.Filters.toRecordFilter;

/**
 * An IN predicate.
 */
final class InPredicate extends FieldPredicate {
    
    /**
     * The type of this predicate.
     */
    public static final int TYPE = 3;
        
    /**
     * The parser instance.
     */
    private static final Parser<InPredicate> PARSER = new Parser<InPredicate>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public InPredicate parseFrom(ByteReader reader) throws IOException {

            String fieldName = VarInts.readString(reader);
            boolean notIn = reader.readBoolean();
            
            int size = (int) reader.readUnsignedInt();
            
            SortedSet<Field> values = new TreeSet<>();
            for (int i = 0; i < size; i++) {
                values.add(ImmutableField.of(readField(reader)));
            }
            
            return new InPredicate(fieldName, values, notIn);
        }
    };
    
    /**
     * The values to which must be field must be compared.
     */
    private final SortedSet<Field> values;
    
    /**
     * <code>true</code> if the operator is a NOT IN operator.
     */
    private final boolean notIn;
    
    /**
     * Creates an IN predicate.
     * 
     * @param fieldName the name of the field
     * @param values the values
     */
    public InPredicate(String fieldName, SortedSet<Field> values) {
        
        this(fieldName, values, false);
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public int getType() {
        return TYPE;
    }

    /**
     * Creates an IN predicate.
     * 
     * @param fieldName the name of the field
     * @param values the values
     * @param notIn <code>true</code> if the field value must not be within the specified values
     */
    public InPredicate(String fieldName, SortedSet<Field> values, boolean notIn) {
        
        super(fieldName);
        this.values = values;
        this.notIn = notIn;
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public RangeSet<Field> getTimestampRanges() {
        
        if (!isTimestamp()) {
            return TimestampField.ALL;
        }
        
        ImmutableRangeSet.Builder<Field> builder = ImmutableRangeSet.builder();
 
        for (Field field : this.values) {

            builder.add(Range.closed(field, field));
        }
        
        RangeSet<Field> rangeSet = builder.build();
        
        if (this.notIn) {
            return rangeSet.complement();
        }
        
        return rangeSet;
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public Filter<Record> toFilter(TimeSeriesDefinition definition) {

        Filter<Field> fieldFilter = in(this.values, isTimestamp());
        
        if (this.notIn) {
            
            fieldFilter = not(fieldFilter);
        }
        
        return toRecordFilter(definition, getFieldName(), fieldFilter);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        
        StrBuilder builder = new StrBuilder().append(getFieldName())
                                             .append(' ');
        
        if (this.notIn) {
            
            builder.append("NOT ");
        }
        
        return builder.append("IN (")
                      .appendWithSeparators(this.values, ", ")
                      .append(")")
                      .toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {
        
        int size = VarInts.computeStringSize(this.getFieldName())   
                + 1 + VarInts.computeUnsignedIntSize(this.values.size());
        
        for (Field value : this.values) {
            size += computeFieldSerializedSize(value);  
        }
        return size;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {

        VarInts.writeString(writer, getFieldName());
        writer.writeBoolean(this.notIn);
        VarInts.writeUnsignedInt(writer, this.values.size());

        for (Field value : this.values) {
            writeField(writer, value);
        }
    }
    
    /**
     * Creates a new <code>InPredicate</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static InPredicate parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>InPredicate</code> instances.
     * @return the parser that can be used to deserialize <code>InPredicate</code> instances.
     */
    public static Parser<InPredicate> getParser() {

        return PARSER;
    }
}
