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
import java.util.Arrays;
import java.util.Iterator;

/**
 * Utility class to convert an {@link Iterator} into a <code>ResourceIterator</code>.
 *
 */
class IteratorAdapter<E> extends AbstractResourceIterator<E> {

    /**
     * The block iterator.
     */
    private final Iterator<? extends E> iterator;

    /**
     * Creates a new <code>IteratorAdapter</code> that convert the specified elements into a
     * <code>ResourceIterator</code>.
     *
     * @param elements the elements to iterate above.
     */
    @SafeVarargs
    public IteratorAdapter(E... elements) {
        this(Arrays.asList(elements));
    }

    /**
     * Creates a new <code>IteratorAdapter</code> that convert the iterator returned by the specified
     * <code>Iterable</code> into a <code>ResourceIterator</code>.
     *
     * @param iterable the elements to iterate above.
     */
    public IteratorAdapter(Iterable<? extends E> iterable) {
        this(iterable.iterator());
    }

    /**
     * Creates a new <code>IteratorAdapter</code> that convert the specified iterator into a
     * <code>ResourceIterator</code>.
     *
     * @param iterator the iterator to convert.
     */
    public IteratorAdapter(Iterator<? extends E> iterator) {
        this.iterator = iterator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected final void computeNext() throws IOException {
        if (this.iterator.hasNext()) {
            setNext(this.iterator.next());
        } else {
            done();
        }
    }
}
