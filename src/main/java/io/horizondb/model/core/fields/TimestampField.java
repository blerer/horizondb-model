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
import io.horizondb.model.core.util.TimeUtils;
import io.horizondb.model.schema.FieldType;

import java.io.IOException;
import java.io.PrintStream;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

import static org.apache.commons.lang.Validate.notNull;

/**
 * @author Benjamin
 * 
 */
public final class TimestampField extends AbstractField {
    
    /**
     * The field maximum value: 9999-12-31 23:59:59.999 GMT
     */
    public static final Field MAX_VALUE = ImmutableField.of(new TimestampField(253402300799999L, TimeUnit.MILLISECONDS));
    
    /**
     * The field minimum value: 0000-01-01 00:00:00.000 GMT
     */
    public static final Field MIN_VALUE = ImmutableField.of(new TimestampField(-62167392000000L, TimeUnit.MILLISECONDS));
        
    /**
     * The range of field going from minimum value to maximum value.
     */
    public static final RangeSet<Field> ALL = ImmutableRangeSet.of(Range.closed(MIN_VALUE, MAX_VALUE));
    
    /**
     * The time unit.
     */
    private final TimeUnit sourceUnit;

    /**
     * The timestamp.
     */
    private long sourceTimestamp;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public FieldType getType() {

        if (this.sourceUnit.equals(TimeUnit.NANOSECONDS)) {

            return FieldType.NANOSECONDS_TIMESTAMP;
        }

        if (this.sourceUnit.equals(TimeUnit.MICROSECONDS)) {

            return FieldType.MICROSECONDS_TIMESTAMP;
        }

        if (this.sourceUnit.equals(TimeUnit.MILLISECONDS)) {

            return FieldType.MILLISECONDS_TIMESTAMP;
        }

        return FieldType.SECONDS_TIMESTAMP;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field newInstance() {

        return new TimestampField(this.sourceTimestamp, this.sourceUnit);
    }

    /**
	 * 
	 */
    public TimestampField(TimeUnit unit) {

        notNull(unit, "the unit parameter must not be null.");
        this.sourceUnit = unit;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isZero() {
        return this.sourceTimestamp == 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field setValueToZero() {
        this.sourceTimestamp = 0;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field add(Field field) {

        TimestampField timestampField = (TimestampField) field;
        
        this.sourceTimestamp += this.sourceUnit.convert(timestampField.sourceTimestamp, timestampField.sourceUnit);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field subtract(Field field) {

        TimestampField timestampField = (TimestampField) field;
        
        this.sourceTimestamp -= this.sourceUnit.convert(timestampField.sourceTimestamp, timestampField.sourceUnit);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyTo(Field field) {
        field.setTimestamp(this.sourceTimestamp, this.sourceUnit);
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public Field setValueFromString(TimeZone timeZone, String s) {
        
        if (s.startsWith("'") && s.endsWith("'")) {
            
            String dateTime = s.substring(1, s.length() - 1);
            long timeInMillis = TimeUtils.parseDateTime(timeZone, dateTime);
            setTimestampInMillis(timeInMillis);
        
        } else {
            
            int length = s.length();
            int index = length - 1;   

            while (Character.isLetter(s.charAt(index))) {
                
                index--;
            }
            
            String symbol = s.substring(index + 1);
            String t = s.substring(0, index + 1);
            
            setTimestamp(Long.parseLong(t), getTimeUnit(symbol));
        }
        
        return this;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
        VarInts.writeLong(writer, this.sourceTimestamp);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readFrom(ByteReader reader) throws IOException {
        this.sourceTimestamp = VarInts.readLong(reader);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {
        return VarInts.computeLongSize(this.sourceTimestamp);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getTimestampIn(TimeUnit unit) {
        return unit.convert(this.sourceTimestamp, this.sourceUnit);
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void writePrettyPrint(PrintStream stream) {
               
        stream.print(this.sourceTimestamp);
        stream.print(' ');
        stream.print(getTimeUnitSymbol(this.sourceUnit));
    }

    /**
     * Returns the symbol for the specified unit of time.
     * 
     * @param timeUnit the time unit
     * @return the symbol for the specified unit of time
     */
    private static String getTimeUnitSymbol(TimeUnit timeUnit) {
        
        switch (timeUnit) {
            
            case NANOSECONDS: return "ns";
            case MICROSECONDS: return "µs";
            case MILLISECONDS: return "ms";
            case SECONDS: return "s";
            default: return timeUnit.toString().toLowerCase();
        }
    }

    /**
     * Returns the unit of time for the specified symbol.
     * 
     * @param timeUnit the time unit
     * @return the unit of time for the specified symbol
     */
    private TimeUnit getTimeUnit(String symbol) {
        
        switch (symbol) {
            
            case "": return this.sourceUnit;
            case "ns": return TimeUnit.NANOSECONDS;
            case "µs": return TimeUnit.MICROSECONDS;
            case "ms": return TimeUnit.MILLISECONDS;
            case "s": return TimeUnit.SECONDS;
            default: throw new IllegalStateException("Unknown time unit: " + symbol);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Field setTimestamp(long sourceTimestamp, TimeUnit unit) {

        if (unit.compareTo(this.sourceUnit) < 0) {

            throw new TypeConversionException("A timestamp in " + unit.toString().toLowerCase()
                    + " cannot be stored in a field of type: " + getType() + ".");
        }

        this.sourceTimestamp = this.sourceUnit.convert(sourceTimestamp, unit);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(Field other) {
            return Long.compare(this.sourceTimestamp, other.getTimestampIn(this.sourceUnit));
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {

        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("timestamp", this.sourceTimestamp)
                                                                          .append("unit", this.sourceUnit)
                                                                          .toString();
    }

    /**
     * 
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof TimestampField)) {
            return false;
        }
        Field rhs = (Field) object;
        return new EqualsBuilder().append(getTimestampInNanos(), rhs.getTimestampInNanos())
                                  .isEquals();
    }

    /**
     * 
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(773109111, 83366071).append(this.sourceTimestamp)
                                                       .append(this.sourceUnit)
                                                       .toHashCode();
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
     * Creates a <code>TimestampField</code> with the specified value.
     * 
     * @param timestamp the timestamp
     * @param unit the timestamp unit
     */
    private TimestampField(long timestamp, TimeUnit unit) {

        notNull(unit, "the unit parameter must not be null.");
        this.sourceTimestamp = timestamp;
        this.sourceUnit = unit;
    }
}
