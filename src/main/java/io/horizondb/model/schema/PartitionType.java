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
import io.horizondb.model.core.util.TimeUtils;

import java.io.IOException;
import java.util.Calendar;

import javax.annotation.concurrent.Immutable;

import com.google.common.collect.Range;

/**
 * The possible way to partition time series.
 * 
 * @author Benjamin
 * 
 */
@Immutable
public enum PartitionType implements Serializable {

    BY_DAY(0) {

        /**
         * {@inheritDoc}
         */
        @Override
        public Range<Field> getPartitionTimeRange(Calendar calendar) {
            return getPartitionTimeRange(calendar, Calendar.DAY_OF_MONTH);
        }
    },

    BY_WEEK(1) {

        /**
         * {@inheritDoc}
         */
        @Override
        public Range<Field> getPartitionTimeRange(Calendar calendar) {

            int firstDayOfWeek = calendar.getFirstDayOfWeek();
            int currentDayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);

            int numberOfDay = firstDayOfWeek - currentDayOfWeek;

            if (numberOfDay > 0) {

                numberOfDay -= 7;
            }

            calendar.add(Calendar.DAY_OF_MONTH, numberOfDay);

            TimeUtils.truncate(calendar, Calendar.DAY_OF_MONTH);
            Field from = FieldType.MILLISECONDS_TIMESTAMP.newField();
            from.setTimestampInMillis(calendar.getTimeInMillis());

            calendar.add(Calendar.DAY_OF_MONTH, 7);
            Field to = FieldType.MILLISECONDS_TIMESTAMP.newField();
            to.setTimestampInMillis(calendar.getTimeInMillis());

            return Range.closedOpen(from, to);
        }
    },

    BY_MONTH(2) {

        /**
         * {@inheritDoc}
         */
        @Override
        public Range<Field> getPartitionTimeRange(Calendar calendar) {
            return getPartitionTimeRange(calendar, Calendar.MONTH);
        }
    };

    /**
     * The parser instance.
     */
    private static final Parser<PartitionType> PARSER = new Parser<PartitionType>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public PartitionType parseFrom(ByteReader reader) throws IOException {

            byte code = reader.readByte();

            PartitionType[] values = PartitionType.values();

            for (int i = 0; i < values.length; i++) {

                PartitionType fieldType = values[i];

                if (fieldType.b == code) {

                    return fieldType;
                }
            }

            throw new IllegalStateException("The byte " + code + " does not match any field type");
        }
    };

    /**
     * The partition binary representation.
     */
    private final int b;

    /**
     * Creates a new <code>PartitionType</code> with the specified binary representation.
     * 
     * @param b the byte representing the <code>PartitionType</code>.
     */
    private PartitionType(int b) {

        this.b = b;
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

    public static Parser<PartitionType> getParser() {

        return PARSER;
    }

    /**
     * Return the time range of the partition which contains the specified date.
     * 
     * @param calendar the date.
     */
    public abstract Range<Field> getPartitionTimeRange(Calendar calendar);

    /**
     * Returns the type of field represented by the next readable byte in the specified reader.
     * 
     * @param reader the buffer to read from.
     * @return the type of field represented by the next readable byte in the specified buffer.
     * @throws IOException
     */
    public static PartitionType parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the time range of the partition which contains the specified date.
     * 
     * @param calendar the date that the partition must contains
     * @param field the field corresponding to the partition type
     * @return the partition corresponding to the specified field which contains the specified date.
     */
    private static Range<Field> getPartitionTimeRange(Calendar calendar, int field) {

        TimeUtils.truncate(calendar, field);
        Field from = FieldType.MILLISECONDS_TIMESTAMP.newField();
        from.setTimestampInMillis(calendar.getTimeInMillis());

        calendar.add(field, 1);
        Field to = FieldType.MILLISECONDS_TIMESTAMP.newField();
        to.setTimestampInMillis(calendar.getTimeInMillis());

        return Range.closedOpen(from, to);
    }

}
