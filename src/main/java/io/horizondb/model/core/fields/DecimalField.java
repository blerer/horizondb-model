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

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * <code>Field</code> used to store decimal as mantissa and exponent of base 10.
 * 
 * @author Benjamin
 * 
 */
public class DecimalField extends AbstractField {

    /**
     * The exponent for a NaN double.
     */
    public static final byte NaN_EXPONENT = Byte.MIN_VALUE;

    /**
     * The mantissa for a NaN double.
     */
    public static final long NaN_MANTISSA = 0;
    
    /**
     * The exponent for a positive or negative infinite double.
     */
    public static final byte INFINITY_EXPONENT = Byte.MIN_VALUE;

    /**
     * The mantissa for the positive infinity.
     */
    public static final long POSITIVE_INFINITY_MANTISSA = 1;
    
    /**
     * The mantissa for the negative infinity.
     */
    public static final long NEGATIVE_INFINITY_MANTISSA = -1;

    /**
     * Constant used to speed up computation.
     */
    public final static double[] POW10 = new double[] { 1d, 10d, 100d, 1000d, 10000d, 100000d, 1000000d, 10000000d,
            100000000d, 1000000000d, 10000000000d, 100000000000d, 1000000000000d, 10000000000000d, 100000000000000d,
            1000000000000000d };

    /**
     * The field maximum value.
     */
    private static final Field MAX_VALUE = ImmutableField.of(new DecimalField(POSITIVE_INFINITY_MANTISSA, 
                                                                              INFINITY_EXPONENT));
    
    /**
     * The field minimum value.
     */
    private static final Field MIN_VALUE = ImmutableField.of(new DecimalField(NEGATIVE_INFINITY_MANTISSA, 
                                                                              INFINITY_EXPONENT));
    
    
    /**
     * The mantissa.
     */
    private long mantissa;

    /**
     * The exponent.
     */
    private byte exponent;

    /**
     * Creates a new <code>DecimalField</code> instance.
     */
    public DecimalField() {
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Field newInstance() {

        return new DecimalField(this.mantissa, this.exponent);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isZero() {
        return this.mantissa == 0 && this.exponent != NaN_EXPONENT;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setValueToZero() {
        this.mantissa = 0;
        this.exponent = 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void add(Field field) {

        DecimalField other = (DecimalField) field;
        add(other.mantissa, other.exponent);
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void setDouble(double d) {
        
        int doubleExponent = exponent(d);
        long doubleMantissa = mantissa(d, doubleExponent);
        
        if (toDouble(doubleMantissa, doubleExponent) != d && !Double.isNaN(d)) {
            throw new TypeConversionException("the double: " + d + " cannot be stored in a field of type decimal.");
        }  
        
        this.exponent = (byte) doubleExponent;
        this.mantissa = doubleMantissa;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void subtract(Field field) {

        DecimalField other = (DecimalField) field;

        if (isNaN(this.mantissa, this.exponent)) {

            add(other.mantissa, other.exponent);

        } else {

            add(-other.mantissa, other.exponent);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyTo(Field field) {

        DecimalField decimal = (DecimalField) field;

        decimal.mantissa = this.mantissa;
        decimal.exponent = this.exponent;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {

        VarInts.writeLong(writer, this.mantissa);
        VarInts.writeByte(writer, this.exponent);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readFrom(ByteReader reader) throws IOException {

        this.mantissa = VarInts.readLong(reader);
        this.exponent = (byte) VarInts.readByte(reader);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {
        return VarInts.computeLongSize(this.mantissa) + 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FieldType getType() {

        return FieldType.DECIMAL;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setByte(int b) {
        setDecimal(b, (byte) 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setInt(int i) {
        setDecimal(i, (byte) 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setLong(long l) {
        setDecimal(l, (byte) 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setDecimal(long mantissa, int exponent) {

        Validate.isTrue(exponent <= Byte.MAX_VALUE && exponent >= Byte.MIN_VALUE,
                        "the specified exponent is not a byte value");

        this.mantissa = mantissa;
        this.exponent = (byte) exponent;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getByte() {
        return (byte) getDouble();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getInt() {
        return (int) getDouble();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getLong() {
        return (long) getDouble();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getDouble() {
        return toDouble(this.mantissa, this.exponent);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public long getDecimalMantissa() {
        return this.mantissa;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte getDecimalExponent() {
        return this.exponent;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("mantissa", this.mantissa)
                                                                          .append("exponent", this.exponent)
                                                                          .toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object object) {

        if (object == this) {
            return true;
        }
        if (!(object instanceof DecimalField)) {
            return false;
        }
        Field rhs = (Field) object;

        return new EqualsBuilder().append(this.getDouble(), rhs.getDouble()).isEquals();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(-857079051, 1637524317).append(getDouble()).toHashCode();
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public int compareTo(Field other) {
        return Double.compare(getDouble(), other.getDouble());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setValueFromString(TimeZone timeZone, String s) {
        
        int exponentIndex = s.indexOf('E');
        
        if (!s.contains(".") && exponentIndex >= 0) {
            
            this.mantissa = Long.parseLong(s.substring(0, exponentIndex));
            this.exponent = Byte.parseByte(s.substring(exponentIndex + 1));            
        }
        
        setDouble(Double.parseDouble(s));
    }
    
    /**    
     * {@inheritDoc}
     */
    @Override
    public void writePrettyPrint(PrintStream stream) {
        
        if (isNaN(this.mantissa, this.exponent)) {
            
            stream.print("NaN");
            return;
        }

        if (isPositiveInfinity(this.mantissa, this.exponent)) {
            stream.print("Infinity");
            return;
        }
        
        if (isNegativeInfinity(this.mantissa, this.exponent)) {
            stream.print("-Infinity");
            return;
        }
        
        stream.print(this.mantissa);
        stream.print(" * 10E");
        stream.print(this.exponent);
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
     * Adds the specified decimal to this field.
     * 
     * @param otherMantissa the decimal mantissa
     * @param otherExponent the decimal exponent
     */
    private void add(long otherMantissa, byte otherExponent) {

        if (isNaN(otherMantissa, otherExponent)) {

            this.mantissa = NaN_MANTISSA;
            this.exponent = NaN_EXPONENT;

            return;
        }

        if (isNaN(this.mantissa, this.exponent)) {

            this.mantissa = otherMantissa;
            this.exponent = otherExponent;

            return;
        }

        int exponentDelta = otherExponent - this.exponent;

        if (exponentDelta == 0) {

            this.mantissa += otherMantissa;

        } else if (exponentDelta > 0) {

            this.mantissa += otherMantissa * pow10(exponentDelta);

        } else {

            this.mantissa *= pow10(-exponentDelta);

            this.mantissa += otherMantissa;
            this.exponent = otherExponent;
        }

    }

    /**
     * Returns the exponent of the specified double.
     * 
     * @param d the double for which the exponent must be computed.
     * @return the exponent of the specified double.
     */
    static int exponent(double d) {

        if (Double.isNaN(d)) {
            return NaN_EXPONENT;
        }

        if (d == ((long) d)) {

            if (d == 0) {
                return 0;
            }

            int exponent = -1;
            double remaining = d;

            do {

                exponent++;
                remaining /= 10;

            } while (remaining == ((long) remaining));

            return exponent;
        }

        String value = Double.toString(d);

        return exponent(value);
    }

    /**
     * Extracts the exponent value from the specified double.
     * 
     * @param s the double as <code>String</code>
     * @return the exponent value from the specified double
     */
    private static int exponent(String s) {
        
        int numberOfFractionDigits = 0;

        boolean startCounting = false;

        for (int i = s.length() - 1; i >= 0; i--) {

            char c = s.charAt(i);

            if (c == '.') {

                break;
            }

            if (c != '0' || startCounting) {

                startCounting = true;

                if (c == 'E') {

                    numberOfFractionDigits = -Integer.parseInt(s.substring(i + 1));
                    startCounting = false;

                } else {

                    numberOfFractionDigits++;
                }
            }

        }

        return -numberOfFractionDigits;
    }

    /**
     * Returns the mantissa of the specified double.
     * 
     * @param d the double
     * @param exponent the exponent
     * @return the mantissa of the specified double.
     */
    static long mantissa(double d, int exponent) {

        if (Double.isNaN(d)) {
            return 0;
        }

        if (exponent > 0) {

            return (long) (d / pow10(exponent));
        }

        return (long) Math.floor((d + (0.5 / POW10[-exponent])) * POW10[-exponent]);
    }

    /**
     * Creates a new <code>DecimalField</code> instance with the specified value.
     * 
     * @param mantissa the mantissa
     * @param exponent the exponent
     */
    private DecimalField(long mantissa, byte exponent) {
        
        this.mantissa = mantissa;
        this.exponent = exponent;
    }
    
    /**
     * Returns <code>true</code> if the double corresponding to specified mantissa and exponent is
     * <code>Double.NaN</code>, <code>false</code> otherwise.
     * 
     * @param mantissa the mantissa
     * @param exponent the exponent
     * @return <code>true</code> if the double corresponding to specified mantissa and exponent is
     * <code>Double.NaN</code>, <code>false</code> otherwise.
     */
    private static boolean isNaN(long mantissa, int exponent) {

        return exponent == NaN_EXPONENT && mantissa == NaN_MANTISSA;
    }
    
    /**
     * Returns <code>true</code> if the double corresponding to specified mantissa and exponent is
     * <code>Double.POSITIVE_INFINITY</code>, <code>false</code> otherwise.
     * 
     * @param mantissa the mantissa
     * @param exponent the exponent
     * @return <code>true</code> if the double corresponding to specified mantissa and exponent is
     * <code>Double.POSITIVE_INFINITY</code>, <code>false</code> otherwise.
     */
    private static boolean isPositiveInfinity(long mantissa, int exponent) {

        return exponent == INFINITY_EXPONENT && mantissa == POSITIVE_INFINITY_MANTISSA;
    }

    /**
     * Returns <code>true</code> if the double corresponding to specified mantissa and exponent is
     * <code>Double.POSITIVE_INFINITY</code>, <code>false</code> otherwise.
     * 
     * @param mantissa the mantissa
     * @param exponent the exponent
     * @return <code>true</code> if the double corresponding to specified mantissa and exponent is
     * <code>Double.POSITIVE_INFINITY</code>, <code>false</code> otherwise.
     */
    private static boolean isNegativeInfinity(long mantissa, int exponent) {

        return exponent == INFINITY_EXPONENT && mantissa == NEGATIVE_INFINITY_MANTISSA;
    }
    
    /**
     * Returns 10 raised to the power of the specified exponent.
     * 
     * @param exponent the exponent
     * @return 10 raised to the power of the specified exponent.
     */
    private static double pow10(int exponent) {

        if (exponent < POW10.length) {

            return POW10[exponent];
        }

        return Math.pow(10, exponent);
    }
    
    /**
     * Converts the specified decimal into a double.
     * 
     * @param mantissa the decimal mantissa
     * @param exponent the decimal exponent
     * @return the double corresponding to the specified decimal
     */
    private static double toDouble(long mantissa, int exponent) {
        
        if (isNaN(mantissa, exponent)) {
            return Double.NaN;
        }

        if (isPositiveInfinity(mantissa, exponent)) {
            return Double.POSITIVE_INFINITY;
        }
        
        if (isNegativeInfinity(mantissa, exponent)) {
            return Double.NEGATIVE_INFINITY;
        }
        
        if (exponent > 0) {

            return mantissa * pow10(exponent);
        }

        return mantissa / pow10(-exponent);
    }
}
