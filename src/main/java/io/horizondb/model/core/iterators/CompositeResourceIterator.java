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

import io.horizondb.model.core.ResourceIterator;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * A composite <code>ResourceIterator</code>.
 *
 */
final class CompositeResourceIterator<E> extends AbstractResourceIterator<E> {

    /**
     * The composing iterators.
     */
    private final Iterator<ResourceIterator<E>> iterators;

    /**
     * The current iterator being iterated upon.
     */
    private ResourceIterator<E> current;
    
    /**
     * Creates a new <code>CompositeResourceIterator</code> composed of the specified iterators.
     * @param iterators the composing iterators
     */
    @SafeVarargs
    public CompositeResourceIterator(ResourceIterator<E>... iterators) {
        this(Arrays.asList(iterators));
    }
    
    /**
     * Creates a new <code>CompositeResourceIterator</code> composed of the specified iterators.
     * @param iterators the composing iterators
     */
    public CompositeResourceIterator(Iterable<ResourceIterator<E>> iterators) {
        this(iterators.iterator());
    }

    /**
     * Creates a new <code>CompositeResourceIterator</code> composed of the specified iterators.
     * @param iterators the composing iterators
     */
    private CompositeResourceIterator(Iterator<ResourceIterator<E>> iterators) {

        this.iterators = iterators;
        init();
    }

    /**
     * Initializes this <code>CompositeResourceIterator</code>.
     */
    private void init() {
        if (this.iterators.hasNext()) {
            this.current = this.iterators.next();
        } else {
            done();
        }
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
    protected void computeNext() throws IOException {
        while (true) {
            if (this.current.hasNext()) {
                setNext(this.current.next());
                break;
            }

            if (!this.iterators.hasNext()) {
                done();
                break;
            }
            this.current.close();
            this.current = this.iterators.next();
        } 
    }
}
