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

import java.io.IOException;

import io.horizondb.model.core.Record;
import io.horizondb.model.core.RecordIterator;
import io.horizondb.model.core.records.FieldFilter;

/**
 * <code>RecordFilter</code> decorator that filter the fields of the records returned by the decorated iterator.
 */
public final class FieldFilteringIterator extends AbstractRecordIterator<Record> {

    /**
     * The decorated iterator.
     */
    private final RecordIterator iterator;

    /**
     * The record filters.
     */
    private final FieldFilter[] filters;

    /**
     * Creates a new <code>FieldFilteringIterator</code> that filter the records returned by the specified iterator.
     *
     * @param iterator the iterator for which the fields must be filtered
     * @param filters the field filters
     */
    public FieldFilteringIterator(RecordIterator iterator, FieldFilter[] filters) {
        this.iterator = iterator;
        this.filters = filters;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        this.iterator.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void computeNext() throws IOException {

        if (this.iterator.hasNext()) {

            Record record = this.iterator.next();
            setNext(filterFieldsIfNeeded(record));

        } else {
            done();
        }
    }

    /**
     * Filters the fields if needed.
     *
     * @param record the record for which some fields might need to be filtered
     * @return a record
     */
    private Record filterFieldsIfNeeded(Record record) {
        FieldFilter filter = this.filters[record.getType()];
        if (filter == null) {
            return record;
        }
        return filter.wrap(record);
    }
}
