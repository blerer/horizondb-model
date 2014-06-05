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
package io.horizondb.model.core.iterators;

import io.horizondb.model.core.Filter;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.RecordIterator;
import io.horizondb.model.core.records.TimeSeriesRecord;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;

import static org.apache.commons.lang.Validate.notNull;

/**
 * <code>RecordIterator</code> that perform filter
 * 
 * @author Benjamin
 *
 */
public final class FilteringRecordIterator extends AbstractRecordIterator<Record> {

    /**
     * The decorated iterator.
     */
    private final RecordIterator iterator;

    /**
     * The filter.
     */
    private final Filter<Record> filter;

    /**
     * The records used to store not returned data.
     */
    private TimeSeriesRecord[] records;

    /**
     * Specify if some records have been buffered.
     */
    private boolean[] addToRecord;

    public FilteringRecordIterator(TimeSeriesDefinition definition, RecordIterator iterator, Filter<Record> filter) {

        notNull(iterator, "the iterator parameter must not be null.");
        notNull(filter, "the filter parameter must not be null.");

        this.records = definition.newRecords();
        this.addToRecord = new boolean[this.records.length];
        this.iterator = iterator;
        this.filter = filter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        this.iterator.close();
    }

    @Override
    protected void computeNext() throws IOException {
        
        while (this.iterator.hasNext()) {

            Record record = this.iterator.next();

            int type = record.getType();

            if (this.filter.accept(record)) {

                if (record.isDelta() && this.addToRecord[type]) {

                    this.records[type].add(record);
                    setNext(this.records[type]);
                    this.addToRecord[type] = false;

                } else {

                    setNext(record);
                }

                break;
            }

            if (this.filter.isDone()) {

                done();
                break;
            }

            if (record.isDelta() && this.addToRecord[type]) {
                this.records[type].add(record);
            } else {
                record.copyTo(this.records[type]);
                this.addToRecord[type] = true;
            }
        }
    }
}
