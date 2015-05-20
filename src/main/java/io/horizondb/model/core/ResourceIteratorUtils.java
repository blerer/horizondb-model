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
package io.horizondb.model.core;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Utility methods for working with {@link ResourceIterator}
 *
 */
public final class ResourceIteratorUtils {

    /**
     * An empty resource iterator
     */
    private static final ResourceIterator<Object> EMPTY_RESOURCE_ITERATOR = new ResourceIterator<Object>() {

        @Override
        public void close() throws IOException {

        }

        @Override
        public Object next() throws IOException {
            throw new NoSuchElementException();
        }

        @Override
        public boolean hasNext() throws IOException {
            return false;
        }
    }; 

    @SuppressWarnings("unchecked")
    public static <E> ResourceIterator<E> emptyIterator() {
        return (ResourceIterator<E>) EMPTY_RESOURCE_ITERATOR;
    }

    /**
     * The class should not be instantiated
     */
    private ResourceIteratorUtils() {
    }
}
