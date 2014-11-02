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

import java.io.IOException;
import java.io.PrintStream;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

import static org.apache.commons.lang.Validate.notNull;

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.model.core.Field;
import io.horizondb.model.schema.FieldType;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Immutable <code>Field</code>.
 * 
 * @author Benjamin
 */
public final class ImmutableField implements Field {

    /**
     * The decorated field.
     */
    private final Field field;
    
    /**
     * Returns an immutable copy of the specified field.
     * 
     * @param field the field to copy
     * @return an immutable copy of the specified field.
     */
    public static ImmutableField of(Field field) {
        return new ImmutableField(field);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(Field o) {
        return this.field.compareTo(o);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FieldType getType() {
        return this.field.getType();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isZero() {
        return this.field.isZero();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field setByte(int b) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field setInt(int i) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field setLong(long l) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field setDouble(double d) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field setDecimal(long mantissa, int exponent) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field setTimestamp(long timestamp, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field setTimestampInNanos(long timestamp) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field setTimestampInMicros(long timestamp) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field setTimestampInMillis(long timestamp) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field setTimestampInSeconds(long timestamp) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getByte() {
        return this.field.getByte();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getInt() {
        return this.field.getInt();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getLong() {
        return this.field.getLong();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getDouble() {
        return this.field.getDouble();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getDecimalMantissa() {
        return this.field.getDecimalMantissa();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte getDecimalExponent() {
        return this.field.getDecimalExponent();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getTimestampIn(TimeUnit unit) {
        return this.field.getTimestampIn(unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getTimestampInNanos() {
        return this.field.getTimestampInNanos();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getTimestampInMicros() {
        return this.field.getTimestampInMicros();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getTimestampInMillis() {
        return this.field.getTimestampInMillis();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getTimestampInSeconds() {
        return this.field.getTimestampInSeconds();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field add(Field field) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyTo(Field field) {
        this.field.copyTo(field);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readFrom(ByteReader reader) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
        this.field.writeTo(writer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {
        return this.field.computeSerializedSize();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field newInstance() {
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Range<Field> range(TimeZone timezone, String from, String to) {
        return Range.closedOpen(newInstance().setValueFromString(timezone, from), 
                                newInstance().setValueFromString(timezone, to));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field subtract(Field field) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field setValueToZero() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field setValueFromString(TimeZone timeZone, String s) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writePrettyPrint(PrintStream stream) {
        this.field.writePrettyPrint(stream);
    }
    
    /**
     * {@inheritDoc}
     */    
    @Override
    public Field maxValue() {
        return this.field.maxValue();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field minValue() {
        return this.field.minValue();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RangeSet<Field> allValues() {
        return this.field.allValues();
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        return this.field.equals(obj);
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return this.field.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
            .append("field", this.field)
            .toString();
    }
    
    /**
     * Creates a new <code>ImmutableField</code> which is a copy of the specified field.
     * @param field the field to copy.
     */
    private ImmutableField(Field field) {
        notNull(field, "the field parameter must not be null.");  
        this.field = field.newInstance();
    }
}
