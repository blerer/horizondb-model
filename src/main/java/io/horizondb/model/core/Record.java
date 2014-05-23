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

import io.horizondb.io.serialization.Serializable;
import io.horizondb.model.core.records.BinaryTimeSeriesRecord;
import io.horizondb.model.core.records.TimeSeriesRecord;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.io.PrintStream;

/**
 * Represents a record from a time series.
 * 
 * @author Benjamin
 * 
 */
public interface Record extends Serializable {

    /**
     * Checks if this record is a delta that must be resolved from the previous record or a full record.
     * 
     * @return <code>true</code> if this record is a delta, <code>false</code> otherwise.
     * @throws IOException an I/O problem occurs while trying to check if the record is a delta.
     */
    boolean isDelta() throws IOException;

    /**
     * Creates a clone of this instance.
     * 
     * @return a clone of this instance.
     */
    Record newInstance();

    /**
     * Returns this record type.
     * 
     * @return this record type.
     */
    int getType();

    /**
     * Returns the field at the specified index.
     * 
     * @param index the field index.
     * @return the field at the specified index.
     * @throws IOException if a problem occurs while trying to retrieve the specified field.
     */
    Field getField(int index) throws IOException;

    /**
     * Returns all the fields from this record.
     * 
     * @return all the fields from this record.
     * @throws IOException if a problem occurs while trying to retrieve the fields.
     */
    Field[] getFields() throws IOException;

    /**
     * Returns the value of the specified field as a time stamp in seconds.
     * 
     * @param index the field index.
     * @return the value of the specified field as a time stamp in seconds.
     * @throws IOException if a problem occurs while trying to retrieve the specified field.
     */
    long getTimestampInSeconds(int index) throws IOException;

    /**
     * Returns the value of the specified field as a time stamp in milliseconds.
     * 
     * @param index the field index.
     * @return the value of the specified field as a time stamp in milliseconds.
     * @throws IOException if a problem occurs while trying to retrieve the specified field.
     */
    long getTimestampInMillis(int index) throws IOException;

    /**
     * Returns the value of the specified field as a time stamp in microseconds.
     * 
     * @param index the field index.
     * @return the value of the specified field as a time stamp in microseconds.
     * @throws IOException if a problem occurs while trying to retrieve the specified field.
     */
    long getTimestampInMicros(int index) throws IOException;

    /**
     * Returns the value of the specified field as a time stamp in nanoseconds.
     * 
     * @param index the field index.
     * @return the value of the specified field as a time stamp in nanoseconds.
     * @throws IOException if a problem occurs while trying to retrieve the specified field.
     */
    long getTimestampInNanos(int index) throws IOException;

    /**
     * Returns the value of the specified field as a <code>double</code>.
     * 
     * @param index the field index.
     * @return the value of the specified field as a <code>double</code>.
     * @throws IOException if a problem occurs while trying to retrieve the specified field.
     */
    double getDouble(int index) throws IOException;

    /**
     * Returns the value of the specified field as a <code>long</code>.
     * 
     * @param index the field index.
     * @return the value of the specified field as a <code>long</code>.
     * @throws IOException if a problem occurs while trying to retrieve the specified field.
     */
    long getLong(int index) throws IOException;

    /**
     * Returns the value of the specified field as an <code>int</code>.
     * 
     * @param index the field index.
     * @return the value of the specified field as an <code>int</code>.
     * @throws IOException if a problem occurs while trying to retrieve the specified field.
     */
    int getInt(int index) throws IOException;

    /**
     * Returns the value of the specified field as a <code>byte</code>.
     * 
     * @param index the field index.
     * @return the value of the specified field as a <code>byte</code>.
     * @throws IOException if a problem occurs while trying to retrieve the specified field.
     */
    int getByte(int index) throws IOException;

    /**
     * Returns the value of the mantissa of the specified decimal field.
     * 
     * @param index the field index.
     * @return the value of the mantissa of the specified decimal field.
     * @throws IOException if a problem occurs while trying to retrieve the specified field.
     */
    long getDecimalMantissa(int index) throws IOException;

    /**
     * Returns the value of the exponent of the specified decimal field.
     * 
     * @param index the field index.
     * @return the value of the exponent of the specified decimal field.
     * @throws IOException if a problem occurs while trying to retrieve the specified field.
     */
    byte getDecimalExponent(int index) throws IOException;

    /**
     * Copies the fields from this record into the specified record.
     * 
     * @param record the record where the fields must be copied.
     * @throws IOException if an I/O problem occurs.
     */
    void copyTo(TimeSeriesRecord record) throws IOException;

    /**
     * Return a <code>TimeSeriesRecord</code> containing the same data as this record.
     * 
     * @return a <code>TimeSeriesRecord</code> containing the same data as this record.
     * @throws IOException if an I/O problem occurs.
     */
    TimeSeriesRecord toTimeSeriesRecord() throws IOException;
    
    /**
     * Return a <code>BinaryTimeSeriesRecord</code> containing the same data as this record.
     * 
     * @return a <code>BinaryTimeSeriesRecord</code> containing the same data as this record.
     * @throws IOException if an I/O problem occurs.
     */
    BinaryTimeSeriesRecord toBinaryTimeSeriesRecord() throws IOException;
    
    /**
     * Writes the content of this <code>Record</code> in a readable format into the specified stream.
     * 
     * @param definition the time series definition
     * @param stream the stream into which the record representation must be written
     * @throws IOException if an I/O problem occurs
     */
    void writePrettyPrint(TimeSeriesDefinition definition, PrintStream stream) throws IOException;
}
