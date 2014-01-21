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

import io.horizondb.model.TimeRange;
import io.horizondb.model.core.records.TimeSeriesRecord;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.LinkedListMultimap;

import static org.apache.commons.lang.Validate.notNull;

/**
 * Utility class that help to build <code>List</code> of records per <code>TimeRange</code>.
 * 
 * @author Benjamin
 * 
 */
public class RecordListMultimapBuilder {

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
     * Creates a new <code>RecordListMultimapBuilder</code> that will be used to build a <code>List</code> of records for the 
     * specified time series.
     * 
     * @param definition the time series definition.
     */
    public RecordListMultimapBuilder(TimeSeriesDefinition definition) {

        notNull(definition, "the definition parameter must not be null.");

        this.definition = definition;
    }

    /**
     * Adds a new record of the specified type.
     * 
     * @param recordType the type of record
     * @return this <code>RecordListMultimapBuilder</code>
     */
    public final RecordListMultimapBuilder newRecord(String recordType) {

        newRecord(this.definition.getRecordTypeIndex(recordType));
        return this;
    }

    /**
     * Adds a new record of the specified type.
     * 
     * @param recordTypeIndex the record type index
     * @return this <code>RecordListMultimapBuilder</code>
     */
    public final RecordListMultimapBuilder newRecord(int recordTypeIndex) {

        addCurrentToRecords();

        this.current = this.definition.newRecord(recordTypeIndex);

        return this;
    }

    /**
     * Sets the specified field to the specified <code>long</code> value. 
     * 
     * @param index the field index
     * @param l the <code>long</code> value
     * @return this <code>RecordListMultimapBuilder</code>
     */
    public final RecordListMultimapBuilder setLong(int index, long l) {

        this.current.setLong(index, l);
        return this;
    }

    /**
     * Sets the specified field to the specified <code>int</code> value. 
     * 
     * @param index the field index
     * @param i the <code>int</code> value
     * @return this <code>RecordListMultimapBuilder</code>
     */
    public final RecordListMultimapBuilder setInt(int index, int i) {

        this.current.setInt(index, i);
        return this;
    }

    /**
     * Sets the specified field to the specified timestamp value. 
     * 
     * @param index the field index
     * @param l the timestamp value in nanoseconds.
     * @return this <code>RecordListMultimapBuilder</code>
     */
    public final RecordListMultimapBuilder setTimestampInNanos(int index, long l) {

        this.current.setTimestampInNanos(index, l);
        return this;
    }

    /**
     * Sets the specified field to the specified timestamp value. 
     * 
     * @param index the field index
     * @param l the timestamp value in microseconds.
     * @return this <code>RecordListMultimapBuilder</code>
     */
    public final RecordListMultimapBuilder setTimestampInMicros(int index, long l) {

        this.current.setTimestampInMicros(index, l);
        return this;
    }

    /**
     * Sets the specified field to the specified timestamp value. 
     * 
     * @param index the field index
     * @param l the timestamp value in milliseconds.
     * @return this <code>RecordListMultimapBuilder</code>
     */
    public final RecordListMultimapBuilder setTimestampInMillis(int index, long l) {

        this.current.setTimestampInMillis(index, l);
        return this;
    }

    /**
     * Sets the specified field to the specified timestamp value. 
     * 
     * @param index the field index
     * @param l the timestamp value in seconds.
     * @return this <code>RecordListMultimapBuilder</code>
     */
    public final RecordListMultimapBuilder setTimestampInSeconds(int index, long l) {

        this.current.setTimestampInSeconds(index, l);
        return this;
    }

    /**
     * Sets the specified field to the specified <code>byte</code> value. 
     * 
     * @param index the field index
     * @param b the <code>byte</code> value
     * @return this <code>RecordListMultimapBuilder</code>
     */
    public final RecordListMultimapBuilder setByte(int index, int b) {

        this.current.setByte(index, b);
        return this;
    }

    /**
     * Sets the specified field to the specified decimal value. 
     * 
     * @param index the field index
     * @param mantissa the decimal mantissa
     * @param exponent the decimal exponent
     * @return this <code>RecordListMultimapBuilder</code>
     */
    public final RecordListMultimapBuilder setDecimal(int index, long mantissa, int exponent) {

        this.current.setDecimal(index, mantissa, exponent);
        return this;
    }


    public final LinkedListMultimap<TimeRange, TimeSeriesRecord> buildMultimap() {

        addCurrentToRecords();
        
        Collections.sort(this.records);

        LinkedListMultimap<TimeRange, TimeSeriesRecord> map = LinkedListMultimap.create();
        
        TimeRange range = null;
        
        for (int i = 0, m = this.records.size(); i < m; i++) {
            
            TimeSeriesRecord record = this.records.get(i);
            
            long timestamp = record.getTimestampInMillis(0);
            
            if (range == null || !range.includes(timestamp)) {

                range = this.definition.getPartitionTimeRange(timestamp);
            }
            
            map.put(range, record);
        }
        
        int numberOfRecordTypes = this.definition.getNumberOfRecordTypes();
        

        for (TimeRange timeRange : map.keySet()) {

            TimeSeriesRecord[] nextRecords = new TimeSeriesRecord[numberOfRecordTypes];
            
            List<TimeSeriesRecord> list = map.get(timeRange);

            for (int i = list.size() - 1; i >= 0; i--) {

                TimeSeriesRecord current = list.get(i);
                int type = current.getType();

                if (nextRecords[type] != null) {

                    try {

                        nextRecords[type].subtract(current);

                    } catch (IOException e) {

                        // must never occurs
                        throw new IllegalStateException(e);
                    }
                }

                nextRecords[type] = current;
            }
        }
        
//        this.records.clear();
        
        return map;
    }
    
    public final Collection<TimeSeriesRecord> build() {

        return buildMultimap().values();
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
}
