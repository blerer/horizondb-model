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

import io.horizondb.io.files.SeekableFileDataInput;
import io.horizondb.model.core.DataBlock;
import io.horizondb.model.core.blocks.BinaryDataBlock;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;

/**
 * Utility to convert a <code>SeekableFileDataInput</code> into a <code>DataBlock</code> iterator.
 *
 */
class BinaryBlockIterator extends AbstractResourceIterator<DataBlock> {

    /**
     * The block
     */
    private final BinaryDataBlock block;

    /**
     * The underlying input.
     */
    private final SeekableFileDataInput input;

    public BinaryBlockIterator(TimeSeriesDefinition definition, SeekableFileDataInput input) {

        this.block = new BinaryDataBlock(definition.newBinaryBlockHeader());
        this.input = input;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        this.input.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void computeNext() throws IOException {
        if (this.input.isReadable()) {
            this.block.fill(this.input);
            setNext(this.block);
        } else {
            done();
        }
    }

}
