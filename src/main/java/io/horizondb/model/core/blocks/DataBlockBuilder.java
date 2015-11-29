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
package io.horizondb.model.core.blocks;

import io.horizondb.io.buffers.Buffers;
import io.horizondb.model.core.DataBlock;
import io.horizondb.model.core.records.TimeSeriesRecord;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.lang.String.format;

import static org.apache.commons.lang.Validate.notNull;

/**
 * Utility class to build <code>DataBlock</code>s.
 * 
 */
public final class DataBlockBuilder {

    /**
     * The records.
     */
    private final List<TimeSeriesRecord> records = new ArrayList<>();
    
    /**
     * The definition of the time series.
     */
    private final TimeSeriesDefinition definition;

    /**
     * The current record being filled.
     */
    private TimeSeriesRecord current;

    /**
     * Creates a new <code>DataBlockBuilder</code> that will be used to build a <code>DataBlock</code> for the 
     * specified time series.
     * 
     * @param definition the time series definition.
     */
    public DataBlockBuilder(TimeSeriesDefinition definition) {

        notNull(definition, "the definition parameter must not be null.");

        this.definition = definition;
    }

    /**
     * Adds a new record of the specified type.
     * 
     * @param recordType the type of record
     * @return this <code>DataBlockBuilder</code>
     */
    public final DataBlockBuilder newRecord(String recordType) {

        newRecord(this.definition.getRecordTypeIndex(recordType));
        return this;
    }

    /**
     * Adds a new record of the specified type.
     * 
     * @param recordTypeIndex the record type index
     * @return this <code>DataBlockBuilder</code>
     */
    public final DataBlockBuilder newRecord(int recordTypeIndex) {

        addCurrentToRecords();

        this.current = this.definition.newRecord(recordTypeIndex);

        return this;
    }

    /**
     * Sets the specified field to the specified <code>long</code> value. 
     * 
     * @param name the field name
     * @param l the <code>long</code> value
     * @return this <code>DataBlockBuilder</code>
     */
    public final DataBlockBuilder setLong(String name, long l) {

        return setLong(fieldIndex(name), l);
    }
    
    /**
     * Sets the specified field to the specified <code>long</code> value. 
     * 
     * @param index the field index
     * @param l the <code>long</code> value
     * @return this <code>DataBlockBuilder</code>
     */
    public final DataBlockBuilder setLong(int index, long l) {

        this.current.setLong(index, l);
        return this;
    }

    /**
     * Sets the specified field to the specified <code>int</code> value. 
     * 
     * @param name the field name
     * @param i the <code>int</code> value
     * @return this <code>DataBlockBuilder</code>
     */
    public final DataBlockBuilder setInt(String name, int i) {

        return setInt(fieldIndex(name), i);
    }
    
    /**
     * Sets the specified field to the specified <code>int</code> value. 
     * 
     * @param index the field index
     * @param i the <code>int</code> value
     * @return this <code>DataBlockBuilder</code>
     */
    public final DataBlockBuilder setInt(int index, int i) {

        this.current.setInt(index, i);
        return this;
    }

    /**
     * Sets the specified field to the specified timestamp value. 
     * 
     * @param name the field name
     * @param l the timestamp value in nanoseconds.
     * @return this <code>DataBlockBuilder</code>
     */
    public final DataBlockBuilder setTimestampInNanos(String name, long l) {

        return setTimestampInNanos(fieldIndex(name), l);
    }
    
    /**
     * Sets the specified field to the specified timestamp value. 
     * 
     * @param index the field index
     * @param l the timestamp value in nanoseconds.
     * @return this <code>DataBlockBuilder</code>
     */
    public final DataBlockBuilder setTimestampInNanos(int index, long l) {

        this.current.setTimestampInNanos(index, l);
        return this;
    }

    /**
     * Sets the specified field to the specified timestamp value. 
     * 
     * @param name the field name
     * @param l the timestamp value in microseconds.
     * @return this <code>DataBlockBuilder</code>
     */
    public final DataBlockBuilder setTimestampInMicros(String name, long l) {

        return setTimestampInMicros(fieldIndex(name), l);
    }
    
    /**
     * Sets the specified field to the specified timestamp value. 
     * 
     * @param index the field index
     * @param l the timestamp value in microseconds.
     * @return this <code>DataBlockBuilder</code>
     */
    public final DataBlockBuilder setTimestampInMicros(int index, long l) {

        this.current.setTimestampInMicros(index, l);
        return this;
    }

    /**
     * Sets the specified field to the specified timestamp value. 
     * 
     * @param name the field name
     * @param l the timestamp value in milliseconds.
     * @return this <code>DataBlockBuilder</code>
     */
    public final DataBlockBuilder setTimestampInMillis(String name, long l) {

        return setTimestampInMillis(fieldIndex(name), l);
    }
    
    /**
     * Sets the specified field to the specified timestamp value. 
     * 
     * @param index the field index
     * @param l the timestamp value in milliseconds.
     * @return this <code>DataBlockBuilder</code>
     */
    public final DataBlockBuilder setTimestampInMillis(int index, long l) {

        this.current.setTimestampInMillis(index, l);
        return this;
    }
    
    /**
     * Sets the specified field to the specified timestamp value. 
     * 
     * @param name the field name
     * @param l the timestamp value in seconds.
     * @return this <code>DataBlockBuilder</code>
     */
    public final DataBlockBuilder setTimestampInSeconds(String name, long l) {

        return setTimestampInSeconds(fieldIndex(name), l);
    }

    /**
     * Sets the specified field to the specified timestamp value. 
     * 
     * @param index the field index
     * @param l the timestamp value in seconds.
     * @return this <code>DataBlockBuilder</code>
     */
    public final DataBlockBuilder setTimestampInSeconds(int index, long l) {

        this.current.setTimestampInSeconds(index, l);
        return this;
    }

    /**
     * Sets the specified field to the specified <code>byte</code> value. 
     * 
     * @param name the field name
     * @param b the <code>byte</code> value
     * @return this <code>DataBlockBuilder</code>
     */
    public final DataBlockBuilder setByte(String name, int b) {

        return setByte(fieldIndex(name), b);
    }
    
    /**
     * Sets the specified field to the specified <code>byte</code> value. 
     * 
     * @param index the field index
     * @param b the <code>byte</code> value
     * @return this <code>DataBlockBuilder</code>
     */
    public final DataBlockBuilder setByte(int index, int b) {

        this.current.setByte(index, b);
        return this;
    }

    /**
     * Sets the specified field to the specified decimal value. 
     * 
     * @param name the field name
     * @param mantissa the decimal mantissa
     * @param exponent the decimal exponent
     * @return this <code>DataBlockBuilder</code>
     */
    public final DataBlockBuilder setDecimal(String name, long mantissa, int exponent) {

        return setDecimal(fieldIndex(name), mantissa, exponent);
    }
    
    /**
     * Sets the specified field to the specified decimal value. 
     * 
     * @param index the field index
     * @param mantissa the decimal mantissa
     * @param exponent the decimal exponent
     * @return this <code>DataBlockBuilder</code>
     */
    public final DataBlockBuilder setDecimal(int index, long mantissa, int exponent) {

        this.current.setDecimal(index, mantissa, exponent);
        return this;
    }

    /**
     * Sets the specified field to the specified double value. 
     * 
     * @param String the field name
     * @param d the double value
     * @return this <code>DataBlockBuilder</code>
     */
    public final DataBlockBuilder setDouble(String name, double d) {

        return setDouble(fieldIndex(name), d);
    }
    
    /**
     * Sets the specified field to the specified double value. 
     * 
     * @param index the field index
     * @param d the double value
     * @return this <code>DataBlockBuilder</code>
     */
    public final DataBlockBuilder setDouble(int index, double d) {

        this.current.setDouble(index, d);
        return this;
    }
    
    /**
     * Creates a data block.
     * 
     * @return a data block
     * @throws IOException if an I/O problem occurs
     */
    public final DataBlock build() throws IOException {

        addCurrentToRecords();
        Collections.sort(this.records);
        RecordAppender appender = new RecordAppender(this.definition,
                                                     Buffers.DEFAULT_ALLOCATOR,
                                                     this.definition.newRecords());

        for (TimeSeriesRecord record : this.records){
            if (!appender.append(record)) {
                throw new BlockOverflowException(format("The record %s cannot be appended to the block as it is full.",
                                                        record));
            }
        }

        return appender.getDataBlock();
    }

    /**
     * Return the time series definition.
     * 
     * @return the time series definition.
     */
    public final TimeSeriesDefinition getDefinition() {
        return this.definition;
    }

    /**
     * Adds the current record to the record set.
     */
    private void addCurrentToRecords() {

        if (this.current != null) {
            this.records.add(this.current);
        }
    }
    
    /**
     * Returns the index of the field with the specified name.
     * 
     * @param name the field name
     * @return the index of the field with the specified name. 
     */
    private int fieldIndex(String name) {
        return this.definition.getFieldIndex(this.current.getType(), name);
    }
}
