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


import java.io.Closeable;
import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * An iterator over a set of records.
 */
public interface ResourceIterator<E> extends Closeable {

    /**
     * Returns <code>true</code> if the iteration has more records.
     * 
     * @return <code>true</code> if the iteration has more records.
     * @throws IOException if an I/O problem occurs while checking
     */
    boolean hasNext() throws IOException;

    /**
     * Returns the next record in the iteration.
     * 
     * @return the next record in the iteration
     * @throws NoSuchElementException if the iteration has no more elements
     */
    E next() throws IOException;
}
