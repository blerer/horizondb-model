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
import io.horizondb.model.core.ResourceIterator;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Base class for the <code>ResourceIterator</code> implementation.
 *
 */
public abstract class AbstractResourceIterator<E extends Record> implements ResourceIterator<E> {

    /**
     * The element to return on the call to next.
     */
    private E next;

    /**
     * <code>true</code> if no more data need to be returned, <code>false</code> otherwise.
     */
    private boolean endOfData;

    /**
     * {@inheritDoc}
     */
    @Override
    public final boolean hasNext() throws IOException {

        if (this.endOfData) {
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
    public final E next() throws IOException {

        if (!hasNext()) {

            throw new NoSuchElementException("No more elements are available.");
        }

        E record = this.next;

        this.next = null;

        return record;
    }

    /**
     * Sets the next element.
     * @param next the next element
     */
    protected final void setNext(E next) {
        this.next = next;
    }
    
    /**
     * Marks the iterator has done.
     */
    protected final void done() {
        this.endOfData = true;
    }
    
    /**
     * Computes and set the next element.
     * @throws IOException if an I/O problem occurs
     */
    protected abstract void computeNext() throws IOException; 
}
