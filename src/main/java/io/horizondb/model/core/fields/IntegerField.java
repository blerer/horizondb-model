/**
 * Copyright 2013 Benjamin Lerer
 * 
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
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * <code>Field</code> containing an integer value.
 * 
 * @author Benjamin
 * 
 */
public class IntegerField extends AbstractField {

    /**
     * The field maximum value.
     */
    private static final Field MAX_VALUE = ImmutableField.of(new IntegerField(Integer.MAX_VALUE));
    
    /**
     * The field minimum value.
     */
    private static final Field MIN_VALUE = ImmutableField.of(new IntegerField(Integer.MIN_VALUE));
    
    /**
     * The field value.
     */
    private int value;

    /**
     * Creates a new <code>IntegerField</code> instance.
     */
    public IntegerField() {
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Field newInstance() {

        return new IntegerField(this.value);
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
    public void setValueToZero() {
        this.value = 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void add(Field field) {

        this.value += ((IntegerField) field).value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void subtract(Field field) {

        this.value -= ((IntegerField) field).value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyTo(Field field) {

        ((IntegerField) field).value = this.value;
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
        this.value = VarInts.readInt(reader);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {
        return VarInts.computeIntSize(this.value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FieldType getType() {
        return FieldType.INTEGER;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setByte(int b) {
        setInt(b);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setInt(int i) {
        this.value = i;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getByte() {
        return getInt();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getInt() {
        return this.value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getLong() {
        return getInt();
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
    public void setValueFromString(TimeZone timeZone, String s) {
        setInt(Integer.parseInt(s));
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public int compareTo(Field other) {
        return Integer.compare(this.value, other.getInt());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("value", this.value).toString();
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
    public boolean equals(Object object) {

        if (object == this) {
            return true;
        }
        if (!(object instanceof IntegerField)) {
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

        return new HashCodeBuilder(657043163, 1790494667).append(this.value).toHashCode();
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
     * Creates a new <code>IntegerField</code> instance with the specified value.
     * 
     * @param i the field value
     */
    private IntegerField(int i) {
        this.value = i;
    }
}
