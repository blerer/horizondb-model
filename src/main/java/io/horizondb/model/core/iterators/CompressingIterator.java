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

import io.horizondb.io.ReadableBuffer;
import io.horizondb.io.compression.CompressionType;
import io.horizondb.io.compression.Compressor;
import io.horizondb.model.core.DataBlock;
import io.horizondb.model.core.ResourceIterator;
import io.horizondb.model.core.blocks.DefaultDataBlock;
import io.horizondb.model.core.records.BlockHeaderUtils;
import io.horizondb.model.core.records.TimeSeriesRecord;

import java.io.IOException;

import static io.horizondb.model.core.records.BlockHeaderUtils.setCompressedBlockSize;
import static io.horizondb.model.core.records.BlockHeaderUtils.setCompressionType;
import static io.horizondb.model.core.records.BlockHeaderUtils.setUncompressedBlockSize;

/**
 * A {@link DataBlock} iterator that compress the data of the blocks. 
 *
 */
final class CompressingIterator extends ForwardingResourceIterator<DataBlock> {

    /**
     * The compressor used to compress the data.
     */
    private final Compressor compressor;

    /**
     * The iterator to which are delegated the calls.
     */
    private final ResourceIterator<DataBlock> delegate;

    /**
     * Creates a <code>CompressingIterator</code> that compress the data of the blocks
     * returned by the specified iterator.
     * 
     * @param compressionType the type of compression to use
     * @param delegate the decorated iterator 
     */
    public CompressingIterator(CompressionType compressionType, ResourceIterator<DataBlock> delegate) {
         this.compressor = compressionType.newCompressor();
         this.delegate = delegate;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ResourceIterator<DataBlock> getDelegate() {
        return this.delegate;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataBlock next() throws IOException {

        DataBlock block = this.delegate.next();

        TimeSeriesRecord header = block.getHeader().toTimeSeriesRecord();
        int blockSize = BlockHeaderUtils.getCompressedBlockSize(header);

        ReadableBuffer compressedData = this.compressor.compress(block.getData());

        setCompressionType(header, this.compressor.getType());
        setCompressedBlockSize(header, compressedData.readableBytes());
        setUncompressedBlockSize(header, blockSize);

        return new DefaultDataBlock(header, compressedData);
    }
}
