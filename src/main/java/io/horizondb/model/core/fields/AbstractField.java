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

import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

import io.horizondb.model.core.Field;

/**
 * This abstract class provides default implementations for the set and get methods in the Field interface.
 * 
 * @author Benjamin
 * 
 */
public abstract class AbstractField implements Field {

    /**
     * {@inheritDoc}
     */
    @Override
    public void setByte(int b) {
        throw new TypeConversionException("A byte cannot be stored in a field of type " + getType() + ".");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setInt(int i) {
        throw new TypeConversionException("An int cannot be stored in a field of type " + getType() + ".");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setLong(long l) {
        throw new TypeConversionException("A long cannot be stored in a field of type " + getType() + ".");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setDecimal(long mantissa, int exponent) {
        throw new TypeConversionException("A decimal cannot be stored in a field of type " + getType() + ".");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getByte() {
        throw new TypeConversionException("A value of type " + getType() + " cannot be converted into a byte.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getInt() {
        throw new TypeConversionException("A value of type " + getType() + " cannot be converted into an int.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getLong() {
        throw new TypeConversionException("A value of type " + getType() + " cannot be converted into a long.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getDouble() {
        throw new TypeConversionException("A value of type " + getType() + " cannot be converted into a double.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getDecimalMantissa() {
        throw new TypeConversionException("A value of type " + getType() + " cannot be converted into a decimal.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte getDecimalExponent() {
        throw new TypeConversionException("A value of type " + getType() + " cannot be converted into a decimal.");
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public long getTimestampIn(TimeUnit unit) {
        throw new TypeConversionException("A value of type " + getType() + " cannot be converted into a timestamp.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final long getTimestampInNanos() {
        return getTimestampIn(TimeUnit.NANOSECONDS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final long getTimestampInMicros() {
        return getTimestampIn(TimeUnit.MICROSECONDS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final long getTimestampInMillis() {
        return getTimestampIn(TimeUnit.MILLISECONDS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final long getTimestampInSeconds() {
        return getTimestampIn(TimeUnit.SECONDS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setDouble(double d) {
        throw new TypeConversionException("A double cannot be stored in a field of type " + getType() + ".");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void setTimestampInNanos(long timestamp) {
        setTimestamp(timestamp, TimeUnit.NANOSECONDS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void setTimestampInMicros(long timestamp) {
        setTimestamp(timestamp, TimeUnit.MICROSECONDS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void setTimestampInMillis(long timestamp) {
        setTimestamp(timestamp, TimeUnit.MILLISECONDS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void setTimestampInSeconds(long timestamp) {
        setTimestamp(timestamp, TimeUnit.SECONDS);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setTimestamp(long timestamp, TimeUnit unit) {

        throw new TypeConversionException("A timestamp cannot be stored in a field of type " + getType() + ".");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RangeSet<Field> allValues() {
        return ImmutableRangeSet.of(Range.closed(minValue(), maxValue()));
    }
}
