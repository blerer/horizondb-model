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
package io.horizondb.model.core.fields;

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.model.core.Field;
import io.horizondb.model.schema.FieldType;

import java.io.IOException;
import java.io.PrintStream;
import java.util.TimeZone;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * <code>Field</code> containing a long value.
 * 
 * @author Benjamin
 * 
 */
public final class LongField extends AbstractCounterField {

    /**
     * The field maximum value.
     */
    private static final Field MAX_VALUE = ImmutableField.of(new LongField(Long.MAX_VALUE));
    
    /**
     * The field minimum value.
     */
    private static final Field MIN_VALUE = ImmutableField.of(new LongField(Long.MIN_VALUE));
    
    
    /**
     * The field value.
     */
    private long value;

    /**
     * Creates a new <code>LongField</code> instance.
     */
    public LongField() {
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Field newInstance() {

        LongField copy = new LongField();
        copy.setLong(this.value);

        return copy;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isZero() {
        return this.value == 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field setValueToZero() {
        this.value = 0;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field add(Field field) {

        this.value += field.getLong();
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field subtract(Field field) {

        this.value -= ((LongField) field).value;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyTo(Field field) {

        ((LongField) field).value = this.value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
        VarInts.writeLong(writer, this.value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readFrom(ByteReader reader) throws IOException {
        this.value = VarInts.readLong(reader);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {
        return VarInts.computeLongSize(this.value);
    }
    
    /**    
     * {@inheritDoc}
     */
    @Override
    public int compareTo(Field other) {
        return Long.compare(this.value, other.getLong());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FieldType getType() {

        return FieldType.LONG;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field setByte(int b) {
        setLong(b);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field setInt(int i) {
        setLong(i);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field setLong(long l) {
        this.value = l;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getByte() {
        return (byte) getLong();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getInt() {
        return (int) getLong();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getLong() {
        return this.value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getDecimalMantissa() {
        return getLong();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte getDecimalExponent() {
        return 0;
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public Field setValueFromString(TimeZone timeZone, String s) {
        return setLong(Long.parseLong(s));
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return Long.toString(this.value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof LongField)) {
            return false;
        }
        Field rhs = (Field) object;
        return new EqualsBuilder().append(this.getLong(), rhs.getLong()).isEquals();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writePrettyPrint(PrintStream stream) {
        stream.print(this.value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(248797033, 1948140529).append(this.value).toHashCode();
    }
    
    /**    
     * {@inheritDoc}
     */
    @Override
    public Field maxValue() {
        return MAX_VALUE;
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public Field minValue() {
        return MIN_VALUE;
    }

    /**
     * Creates a new <code>LongField</code> instance with the specified value.
     * 
     * @param l the field value
     */
    private LongField(long l) {
        this.value = l;
    }
}
