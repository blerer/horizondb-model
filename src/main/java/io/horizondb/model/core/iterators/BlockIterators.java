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

import io.horizondb.io.ByteReader;
import io.horizondb.io.compression.CompressionType;
import io.horizondb.model.core.DataBlock;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.ResourceIterator;
import io.horizondb.model.schema.TimeSeriesDefinition;

import com.google.common.collect.RangeSet;

/**
 * Utility methods to work with <code>DataBlock</code> iterators.
 *
 */
public final class BlockIterators {

    /**
     * Creates a <code>ResourceIterator</code> to iterate over the blocks read from the specified input.
     *
     * @param input the input to read the blocks from.
     * @return a <code>ResourceIterator</code> to iterate over the blocks read from the specified input.
     */
    public static ResourceIterator<DataBlock> iterator(TimeSeriesDefinition definition, ByteReader input) {
        return new BinaryBlockIterator(definition, input);
    }

    /**
     * Creates a <code>ResourceIterator</code> to iterate over the specified blocks.
     *
     * @param blocks the blocks to iterate above.
     * @return a <code>ResourceIterator</code> to iterate over the blocks
     */
    public static ResourceIterator<DataBlock> iterator(Iterable<DataBlock> blocks) {
        return new IteratorAdapter<DataBlock>(blocks);
    }

    /**
     * Creates a <code>ResourceIterator</code> that compress the block returned by the specified iterator.
     *
     * @param blocks the blocks to compress
     * @return a <code>ResourceIterator</code> that compress the block returned by the specified iterator.
     */
    public static ResourceIterator<DataBlock> compress(CompressionType compressionType,
                                                       ResourceIterator<DataBlock> blocks) {
        return new CompressingIterator(compressionType, blocks);
    }

    /**
     * Creates a <code>ResourceIterator</code> that uncompress the block returned by the specified iterator.
     *
     * @param blocks the blocks to uncompress
     * @return a <code>ResourceIterator</code> that uncompress the block returned by the specified iterator.
     */
    public static ResourceIterator<DataBlock> decompress(ResourceIterator<DataBlock> blocks) {
        return new DecompressingIterator(blocks);
    }

    /**
     * Creates a <code>ResourceIterator</code> to filter out the blocks that are not within the specified time ranges.
     *
     * @param rangeSet the ranges of time for which the blocks must be returned
     * @param iterator the iterator for which the blocks must be filtered out.
     * @return an iterator that return only the block containing records within the specified time ranges.
     */
    public static ResourceIterator<DataBlock> filter(RangeSet<Field> rangeSet, ResourceIterator<DataBlock> iterator) {
        return new BlockFilteringIterator(rangeSet, iterator);
    }

    /**
     * Combines multiple iterators into a single iterator.
     *
     * @param iterators the iterators to be combined
     * @return an iterator combining the multiple iterators into a single one
     */
    public static ResourceIterator<DataBlock> concat(Iterable<ResourceIterator<DataBlock>> iterators) {
        return new CompositeResourceIterator<>(iterators);
    }

    /**
     * The class must not be instantiated.
     */
    private BlockIterators() {
    }
}
