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
package io.horizondb.model.schema;

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.serialization.Parser;
import io.horizondb.io.serialization.Serializable;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.fields.ByteField;
import io.horizondb.model.core.fields.DecimalField;
import io.horizondb.model.core.fields.ImmutableField;
import io.horizondb.model.core.fields.IntegerField;
import io.horizondb.model.core.fields.LongField;
import io.horizondb.model.core.fields.TimestampField;

import java.io.IOException;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Range;

/**
 * The possible types for a field.
 * 
 * @author Benjamin
 * 
 */
public enum FieldType implements Serializable {

    NANOSECONDS_TIMESTAMP(0) {

        /**
         * {@inheritDoc}
         */
        @Override
        public Field newField() {
            return new TimestampField(TimeUnit.NANOSECONDS);
        }
    },

    MICROSECONDS_TIMESTAMP(1) {

        /**
         * {@inheritDoc}
         */
        @Override
        public Field newField() {
            return new TimestampField(TimeUnit.MICROSECONDS);
        }
    },

    MILLISECONDS_TIMESTAMP(2) {

        /**
         * {@inheritDoc}
         */
        @Override
        public Field newField() {
            return new TimestampField(TimeUnit.MILLISECONDS);
        }
    },

    SECONDS_TIMESTAMP(3) {

        /**
         * {@inheritDoc}
         */
        @Override
        public Field newField() {
            return new TimestampField(TimeUnit.SECONDS);
        }
    },

    BYTE(4) {

        /**
         * {@inheritDoc}
         */
        @Override
        public Field newField() {
            return new ByteField();
        }
    },

    INTEGER(5) {

        /**
         * {@inheritDoc}
         */
        @Override
        public Field newField() {
            return new IntegerField();
        }
    },

    LONG(6) {

        /**
         * {@inheritDoc}
         */
        @Override
        public Field newField() {
            return new LongField();
        }
    },

    DECIMAL(7) {

        /**
         * {@inheritDoc}
         */
        @Override
        public Field newField() {
            return new DecimalField();
        }
    };

    /**
     * The parser instance.
     */
    private static final Parser<FieldType> PARSER = new Parser<FieldType>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public FieldType parseFrom(ByteReader reader) throws IOException {

            byte code = reader.readByte();

            FieldType[] values = FieldType.values();

            for (int i = 0; i < values.length; i++) {

                FieldType fieldType = values[i];

                if (fieldType.b == code) {

                    return fieldType;
                }
            }

            throw new IllegalStateException("The byte " + code + " does not match any field type");
        }
    };

    /**
     * The field type binary representation.
     */
    private final int b;

    /**
     * Creates a new <code>FieldType</code> with the specified binary representation.
     * 
     * @param b the byte representing the <code>FieldType</code>.
     */
    private FieldType(int b) {

        this.b = b;
    }

    public abstract Field newField();
    
    /**
     * Creates a new field instance with the specified value.
     * 
     * @param timezone the time series time zone
     * @param value the value as a <code>String</code>
     * @return a new instance of this field with the specified value.
     */
    public Field newField(TimeZone timezone, String value) {
        
        Field field = newField();
        field.setValueFromString(timezone, value);
        
        return field;
    }

    /**
     * Creates a range of fields from the field with the <code>from</code> value, inclusive, to the 
     * field with the <code>to</code> value, exclusive.
     * 
     * @param timezone the time series time zone
     * @param from the lower end point of the range (inclusive)
     * @param to the upper end point of the range (exclusive)
     * @return a range of fields
     */
    public Range<Field> range(TimeZone timezone, String from, String to) {
        
        return Range.<Field>closedOpen(ImmutableField.of(newField(timezone, from)), 
                                       ImmutableField.of(newField(timezone, to)));
    }

    /**
     * Creates a range of fields from the field with the <code>from</code> value, inclusive, to the 
     * field with the <code>to</code> value, exclusive. The time zone used to parse the <code>String</code> will
     * be the default timezone.
     * 
     * @param from the lower end point of the range (inclusive)
     * @param to the upper end point of the range (exclusive)
     * @return a range of fields
     */
    public Range<Field> range(String from, String to) {
        
        return range(TimeZone.getDefault(), from, to);
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
    public void writeTo(ByteWriter writer) throws IOException {

        writer.writeByte(this.b);
    }

    public static Parser<FieldType> getParser() {

        return PARSER;
    }

    /**
     * Returns the type of field represented by the next readable byte in the specified reader.
     * 
     * @param reader the buffer to read from.
     * @return the type of field represented by the next readable byte in the specified buffer.
     * @throws IOException
     */
    public static FieldType parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }
}
