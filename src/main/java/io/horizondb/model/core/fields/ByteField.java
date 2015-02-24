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
import io.horizondb.model.core.Field;
import io.horizondb.model.schema.FieldType;

import java.io.IOException;
import java.io.PrintStream;
import java.util.TimeZone;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * <code>Field</code> containing a byte value.
 */
public class ByteField extends AbstractCounterField {

    /**
     * The field maximum value.
     */
    private static final Field MAX_VALUE = ImmutableField.of(new ByteField(Byte.MAX_VALUE));
    
    /**
     * The field minimum value.
     */
    private static final Field MIN_VALUE = ImmutableField.of(new ByteField(Byte.MIN_VALUE));
    
    /**
     * The field value.
     */
    private int value;

    /**
     * Creates a new <code>ByteField</code> instance.
     */
    public ByteField() {
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Field newInstance() {

        return new ByteField(this.value);
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

        this.value += field.getByte();
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field subtract(Field field) {

        this.value -= field.getByte();
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyTo(Field field) {

        field.setByte(this.value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
        writer.writeByte(this.value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readFrom(ByteReader reader) throws IOException {
        this.value = reader.readByte();
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
    public int compareTo(Field other) {
        return Integer.compare(this.value, other.getByte());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FieldType getType() {
        return FieldType.BYTE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field setByte(int b) {
        this.value = b;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getByte() {
        return this.value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getInt() {
        return getByte();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getLong() {
        return getByte();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getDecimalMantissa() {
        return this.value;
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
        return setByte(Byte.parseByte(s));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return Integer.toString(this.value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof ByteField)) {
            return false;
        }
        Field rhs = (Field) object;
        return new EqualsBuilder().append(this.getLong(), rhs.getLong()).isEquals();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(1633523303, -473857531).append(this.value).toHashCode();
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
     * Creates a new <code>ByteField</code> instance with the specified value.
     * 
     * @param the field value.
     */
    private ByteField(int b) {
        this.value = b;
    }
}
