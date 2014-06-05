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
package io.horizondb.model.core;

import io.horizondb.io.ByteReader;
import io.horizondb.io.serialization.Serializable;
import io.horizondb.model.core.fields.TypeConversionException;
import io.horizondb.model.schema.FieldType;

import java.io.IOException;
import java.io.PrintStream;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

/**
 * Represents a field from a <code>Record</code> of a time series.
 * 
 * @author Benjamin
 * 
 */
public interface Field extends Comparable<Field>, Serializable {

    /**
     * Returns the field type.
     * 
     * @return the field type.
     */
    FieldType getType();

    /**
     * Returns <code>true</code> if the field value is zero, <code>false</code> otherwise.
     * 
     * @return <code>true</code> if the field value is zero, <code>false</code> otherwise.
     */
    boolean isZero();

    /**
     * Sets the value of this field to the specified byte.
     * 
     * @param b the byte value.
     * @throws TypeConversionException if this field does not accept byte values.
     */
    void setByte(int b);

    /**
     * Sets the value of this field to the specified int.
     * 
     * @param i the int value.
     * @throws TypeConversionException if this field does not accept int values.
     */
    void setInt(int i);

    /**
     * Sets the value of this field to the specified long.
     * 
     * @param l the long value.
     * @throws TypeConversionException if this field does not accept long values.
     */
    void setLong(long l);

    /**
     * Sets the value of this field to the specified double.
     * 
     * @param d the double value.
     * @throws TypeConversionException if this field does not accept double values.
     */
    void setDouble(double d);

    /**
     * Sets the value of this field to the specified decimal.
     * 
     * @param mantissa the decimal mantissa.
     * @param exponent the decimal exponent.
     * @throws TypeConversionException if this field does not accept decimal values.
     */
    void setDecimal(long mantissa, int exponent);
    
    /**
     * Sets the value of this field to the specified timestamp in the specified unit.
     * 
     * @param timestamp the timestamp value.
     * @param unit the timestamp unit.
     */
    void setTimestamp(long timestamp, TimeUnit unit);

    /**
     * Sets the value of this field to the specified nanoseconds timestamp.
     * 
     * @param l the long value.
     * @throws TypeConversionException if this field does not accept nanoseconds timestamp values.
     */
    void setTimestampInNanos(long timestamp);

    /**
     * Sets the value of this field to the specified microseconds timestamp.
     * 
     * @param l the long value.
     * @throws TypeConversionException if this field does not accept microseconds timestamp values.
     */
    void setTimestampInMicros(long timestamp);

    /**
     * Sets the value of this field to the specified milliseconds timestamp.
     * 
     * @param l the long value.
     * @throws TypeConversionException if this field does not accept milliseconds timestamp values.
     */
    void setTimestampInMillis(long timestamp);

    /**
     * Sets the value of this field to the specified seconds timestamp.
     * 
     * @param l the long value.
     * @throws TypeConversionException if this field does not accept seconds timestamp values.
     */
    void setTimestampInSeconds(long timestamp);

    /**
     * Returns the value of this field as a byte.
     * 
     * @return the value of this field as a byte.
     */
    int getByte();

    /**
     * Returns the value of this field as an int.
     * 
     * @return the value of this field as an int.
     */
    int getInt();

    /**
     * Returns the value of this field as a long.
     * 
     * @return the value of this field as a long.
     */
    long getLong();

    /**
     * Returns the double value of the field.
     * 
     * @return the double of the value of the field.
     */
    double getDouble();

    /**
     * Returns the mantissa of the value of the field.
     * 
     * @return the mantissa of the value of the field.
     */
    long getDecimalMantissa();

    /**
     * Returns the exponent of the value of the field.
     * 
     * @return the exponent of the value of the field.
     */
    byte getDecimalExponent();

    /**
     * Returns the value of this field as a timestamp with the specified unit.
     * 
     * @return the value of this field as a timestamp with the specified unit.
     */
    long getTimestampIn(TimeUnit unit);

    /**
     * Returns the value of this field as a nanoseconds timestamp.
     * 
     * @return the value of this field as a nanoseconds timestamp.
     */
    long getTimestampInNanos();

    /**
     * Returns the value of this field as a microseconds timestamp.
     * 
     * @return the value of this field as a microseconds timestamp.
     */
    long getTimestampInMicros();

    /**
     * Returns the value of this field as a milliseconds timestamp.
     * 
     * @return the value of this field as a milliseconds timestamp.
     */
    long getTimestampInMillis();

    /**
     * Returns the value of this field as a seconds timestamp.
     * 
     * @return the value of this field as a seconds timestamp.
     */
    long getTimestampInSeconds();

    /**
     * Adds the value of the specified field to this field.
     * 
     * @param field the field for which the value must be added.
     */
    void add(Field field);

    /**
     * Copies the value of the specified field to this field.
     * 
     * @param field the field from which the value must be copied.
     */
    void copyTo(Field field);

    /**
     * Reads the field value from the specified <code>ByteReader</code>.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs.
     */
    void readFrom(ByteReader reader) throws IOException;

    /**
     * Creates a clone of this field.
     * 
     * @return a clone of this field.
     */
    Field newInstance();
    
    /**
     * Creates a new instance of this field with the specified value.
     * 
     * @param timezone the time series time zone
     * @param value the value as a <code>String</code>
     * @return a new instance of this field with the specified value.
     */
    Field newInstance(TimeZone timezone, String value);

    /**
     * Creates a range of fields from the field with the <code>from</code> value, inclusive, to the 
     * field with the <code>to</code> value, exclusive.
     * 
     * @param timezone the time series time zone
     * @param from the lower end point of the range (inclusive)
     * @param to the upper end point of the range (exclusive)
     * @return a range of fields
     */
    Range<Field> range(TimeZone timezone, String from, String to);
    
    /**
     * Subtract the value of the specified field from this one.
     * 
     * @param field the field from which the value must be subtracted.
     */
    void subtract(Field field);

    /**
     * Sets the value of the field to zero.
     */
    void setValueToZero();

    /**
     * Sets the value from the specified <code>String</code>.
     * 
     * @param timeZone the time zone used
     * @param s the <code>String</code>
     */
    void setValueFromString(TimeZone timeZone, String s);
    
    /**
     * Writes the content of this <code>Field</code> in a readable format into the specified stream.
     * 
     * @param stream the stream into which the field representation must be written
     * @throws IOException if an I/O problem occurs
     */
    void writePrettyPrint(PrintStream stream);
    
    /**
     * Returns the maximum value that this field can have.
     * 
     * @return the maximum value that this field can have.
     */
    Field maxValue();
    
    /**
     * Returns the minimum value that this field can have.
     * 
     * @return the minimum value that this field can have.
     */
    Field minValue();
    
    /**
     * Returns a <code>RangeSet</code> that contains all the values that this field can have.
     * 
     * @return a <code>RangeSet</code> that contains all the values that this field can have.
     */
    RangeSet<Field> allValues();
}
