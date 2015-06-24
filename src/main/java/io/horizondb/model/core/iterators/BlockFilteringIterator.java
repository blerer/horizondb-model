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
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.ResourceIterator;

import java.io.IOException;

import com.google.common.collect.RangeSet;

import static io.horizondb.model.core.records.BlockHeaderUtils.getRange;

/**
 * <code>DataBlock</code>s iterator that filter out the blocks of the iterator that it decorates if they
 * do not contains data within a specific set of time ranges.
 */
final class BlockFilteringIterator extends AbstractResourceIterator<DataBlock> {

    /**
     * The ranges of times for which the blocks must be returned.
     */
    private final RangeSet<Field> rangeSet;

    /**
     * The blocks to filter.
     */
    private final ResourceIterator<DataBlock> iterator;

    /**
     * Creates a <code>BlockFilteringIterator</code> 
     * @param rangeSet the ranges of time for which the values must be returned
     * @param iterator the iterator for which the blocks must be filtered out.
     */
    public BlockFilteringIterator(RangeSet<Field> rangeSet, ResourceIterator<DataBlock> iterator) {
        this.rangeSet = rangeSet;
        this.iterator = iterator;
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

        while (true) {

            if (!this.iterator.hasNext()) {
                done();
                break;
            }

            DataBlock block = this.iterator.next();
            Record header = block.getHeader();
            if (!this.rangeSet.subRangeSet(getRange(header)).isEmpty()) {
                setNext(block);
                break;
            }
        }
    }
}
