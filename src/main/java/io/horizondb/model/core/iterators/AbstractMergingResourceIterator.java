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

/**
 * Base class for <code>ResourceIterators</code> classes that merge the elements of two <code>ResourceIterators</code>.
 */
abstract class AbstractMergingResourceIterator<E> extends AbstractResourceIterator<E> {

    /**
     * The left side iterator. 
     */
    protected final ResourceIterator<? extends E> left;

    /**
     * The right side iterator.
     */
    protected final ResourceIterator<? extends E> right;

    /**
     * The next element from the left iterator.
     */
    protected E nextFromLeft;

    /**
     * The next element from the right iterator.
     */
    protected E nextFromRight;

    /**
     * Creates a <code>AbstractMergingRecordIterator</code> that will merge the elements returned by the two specified
     * iterators.
     * @param left the left iterator
     * @param right the right iterator
     */
    public AbstractMergingResourceIterator(ResourceIterator<? extends E> left,
                                 ResourceIterator<? extends E> right) {

        this.left = left;
        this.right = right;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {

        IOException ioe = null;

        try {
            this.left.close();
        } catch (IOException e) {
            ioe = e;
        }

        try {
            this.right.close();
        } catch (IOException e) {
            if (ioe != null) {
                ioe = e;
            }
        }

        if (ioe != null) {
            throw ioe;
        }
    }
}
