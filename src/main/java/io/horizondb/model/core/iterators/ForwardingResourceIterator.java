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

import io.horizondb.model.core.ResourceIterator;

/**
 * A {@link ResourceIterator} which forwards all its method calls to another iterator.
 *
 */
abstract class ForwardingResourceIterator<E> implements ResourceIterator<E> {

    /**
     * Returns the backing delegate instance that methods are forwarded to. 
     * @return the backing delegate instance that methods are forwarded to. 
     */
    protected abstract ResourceIterator<E> getDelegate();

    protected ForwardingResourceIterator() {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        getDelegate().close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() throws IOException {
        return getDelegate().hasNext();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public E next() throws IOException {
        return getDelegate().next();
    }
}
