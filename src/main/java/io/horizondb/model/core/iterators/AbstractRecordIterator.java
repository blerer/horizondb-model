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

import io.horizondb.model.core.Record;
import io.horizondb.model.core.RecordIterator;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Base class for the <code>RecordIterator</code> implementation.
 * 
 * @author Benjamin
 *
 */
abstract class AbstractRecordIterator<T extends Record> implements RecordIterator {

    /**
     * The record to return on the call to next.
     */
    private T next;

    /**
     * <code>true</code> if no more records need to be returned, <code>false</code> otherwise.
     */
    private boolean endOfRecords;

    /**
     * {@inheritDoc}
     */
    @Override
    public final boolean hasNext() throws IOException {

        if (this.endOfRecords) {
            return false;
        }

        if (this.next != null) {

            return true;
        }

        computeNext();

        return this.next != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final T next() throws IOException {

        if (!hasNext()) {

            throw new NoSuchElementException("No more elements are available.");
        }

        T record = this.next;

        this.next = null;

        return record;
    }

    /**
     * Sets the next record.
     * @param next the next record
     */
    protected final void setNext(T next) {
        this.next = next;
    }
    
    /**
     * Marks the iterator has done.
     */
    protected final void done() {
        this.endOfRecords = true;
    }
    
    /**
     * Computes and set the next record.
     * @throws IOException if an I/O problem occurs
     */
    protected abstract void computeNext() throws IOException; 
}
