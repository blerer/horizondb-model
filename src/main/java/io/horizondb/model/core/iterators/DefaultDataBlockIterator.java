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

import io.horizondb.model.core.DataBlock;

import java.io.IOException;
import java.util.Iterator;

/**
 * A <code>ResourceIterator</code> over a set of blocks.
 *
 */
public final class DefaultDataBlockIterator extends AbstractResourceIterator<DataBlock> {

    /**
     * The block iterator.
     */
    private final Iterator<DataBlock> iterator;
    
    /**
     * Creates a new <code>DefaultDataBlockIterator</code> to iterate over the specified set 
     * of blocks.
     * @param blocks the blocks to iterate above.
     */
    public DefaultDataBlockIterator(Iterable<DataBlock> blocks) {
        this.iterator = blocks.iterator();
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
        if (this.iterator.hasNext()) {
            setNext(this.iterator.next());
        } else {
            done();
        }
    }
}
