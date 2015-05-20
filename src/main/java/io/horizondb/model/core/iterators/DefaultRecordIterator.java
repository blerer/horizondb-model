/**
 * Copyright 2013-2014 Benjamin Lerer
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
package io.horizondb.model.core.iterators;

import io.horizondb.model.core.Record;
import io.horizondb.model.core.RecordListBuilder;
import io.horizondb.model.core.ResourceIterator;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.util.Iterator;

import static org.apache.commons.lang.Validate.notNull;

/**
 * <code>RecordIterator</code> that iterate over a list of records.
 * 
 * @author Benjamin
 * 
 */
public class DefaultRecordIterator implements ResourceIterator<Record> {

    /**
     * The records.
     */
    private final Iterator<? extends Record> iterator;

    /**
     * Creates a new <code>DefaultRecordIterator</code> that will iterate over the specified 
     * records.
     * 
     * @param records the records
     */
    public DefaultRecordIterator(Iterable<? extends Record> iterable) {

        this.iterator = iterable.iterator();
    }

    /**
     * Creates a new builder.  
     * 
     * @param definition the time series definition
     * @return a new builder
     */
    public static Builder newBuilder(TimeSeriesDefinition definition) {
        
        return new Builder(definition);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Record next() {
        return this.iterator.next();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {

    }
    
    /**
     * Builder for <code>DefaultRecordIterator</code> instances.
     * 
     * @author Benjamin
     */
    public static final class Builder {
        
        /**
         * The builder used to build the record list.
         */
        private final RecordListBuilder builder;
        
        /**
         * Adds a new record of the specified type.
         * 
         * @param recordType the type of record
         * @return this <code>Builder</code>
         */
        public final Builder newRecord(String recordType) {

            this.builder.newRecord(recordType);
            return this;
        }

        /**
         * Adds a new record of the specified type.
         * 
         * @param recordTypeIndex the record type index
         * @return this <code>Builder</code>
         */
        public final Builder newRecord(int recordTypeIndex) {

            this.builder.newRecord(recordTypeIndex);
            return this;
        }

        /**
         * Sets the specified field to the specified <code>long</code> value. 
         * 
         * @param index the field index
         * @param l the <code>long</code> value
         * @return this <code>Builder</code>
         */
        public final Builder setLong(int index, long l) {

            this.builder.setLong(index, l);
            return this;
        }

        /**
         * Sets the specified field to the specified <code>int</code> value. 
         * 
         * @param index the field index
         * @param i the <code>int</code> value
         * @return this <code>Builder</code>
         */
        public final Builder setInt(int index, int i) {

            this.builder.setInt(index, i);
            return this;
        }

        /**
         * Sets the specified field to the specified timestamp value. 
         * 
         * @param index the field index
         * @param l the timestamp value in nanoseconds.
         * @return this <code>Builder</code>
         */
        public final Builder setTimestampInNanos(int index, long l) {

            this.builder.setTimestampInNanos(index, l);
            return this;
        }

        /**
         * Sets the specified field to the specified timestamp value. 
         * 
         * @param index the field index
         * @param l the timestamp value in microseconds.
         * @return this <code>Builder</code>
         */
        public final Builder setTimestampInMicros(int index, long l) {

            this.builder.setTimestampInMicros(index, l);
            return this;
        }

        /**
         * Sets the specified field to the specified timestamp value. 
         * 
         * @param index the field index
         * @param l the timestamp value in milliseconds.
         * @return this <code>Builder</code>
         */
        public final Builder setTimestampInMillis(int index, long l) {

            this.builder.setTimestampInMillis(index, l);
            return this;
        }

        /**
         * Sets the specified field to the specified timestamp value. 
         * 
         * @param index the field index
         * @param l the timestamp value in seconds.
         * @return this <code>Builder</code>
         */
        public final Builder setTimestampInSeconds(int index, long l) {

            this.builder.setTimestampInSeconds(index, l);
            return this;
        }

        /**
         * Sets the specified field to the specified <code>byte</code> value. 
         * 
         * @param index the field index
         * @param b the <code>byte</code> value
         * @return this <code>Builder</code>
         */
        public final Builder setByte(int index, int b) {

            this.builder.setByte(index, b);
            return this;
        }

        /**
         * Sets the specified field to the specified decimal value. 
         * 
         * @param index the field index
         * @param mantissa the decimal mantissa
         * @param exponent the decimal exponent
         * @return this <code>Builder</code>
         */
        public final Builder setDecimal(int index, long mantissa, int exponent) {

            this.builder.setDecimal(index, mantissa, exponent);
            return this;
        }

        /**
         * Builds a new <code>DefaultRecordIterator</code> instance.
         * @return a new <code>DefaultRecordIterator</code> instance.
         */
        public final DefaultRecordIterator build() {

            return new DefaultRecordIterator(this.builder.build());
        }

        /**
         * Creates a new <code>Builder</code> instance.
         * 
         * @param definition the time series definition 
         */
        private Builder(TimeSeriesDefinition definition) {
            notNull(definition, "the definition parameter must not be null.");            
            this.builder = new RecordListBuilder(definition);
        }
    }
}
