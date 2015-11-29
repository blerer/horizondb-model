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
import io.horizondb.io.compression.Decompressor;
import io.horizondb.model.core.DataBlock;
import io.horizondb.model.core.ResourceIterator;
import io.horizondb.model.core.blocks.DefaultDataBlock;
import io.horizondb.model.core.records.TimeSeriesRecord;

import java.io.IOException;

import static io.horizondb.model.core.records.BlockHeaderUtils.getCompressionType;
import static io.horizondb.model.core.records.BlockHeaderUtils.getUncompressedBlockSize;

import static io.horizondb.model.core.records.BlockHeaderUtils.setCompressedBlockSize;
import static io.horizondb.model.core.records.BlockHeaderUtils.setCompressionType;

/**
 * A {@link DataBlock} iterator that uncompress the data of the blocks. 
 *
 */
final class DecompressingIterator extends ForwardingResourceIterator<DataBlock> {

    /**
     * The decompressor used to uncompress the blocks.
     */
    private Decompressor decompressor;

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
    public DecompressingIterator(ResourceIterator<DataBlock> delegate) {
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

        ReadableBuffer uncompressedData = decompress(header, block.getData());

        setCompressionType(header, CompressionType.NONE);
        setCompressedBlockSize(header, uncompressedData.readableBytes());

        return new DefaultDataBlock(header, uncompressedData);
    }
    
    /**
     * Uncompress the specified block.
     * 
     * @param header the block header
     * @param compressedBlock the compressed block
     * @return the uncompressed data
     * @throws IOException if an I/O problem occurs.
     */
    private ReadableBuffer decompress(TimeSeriesRecord header, ReadableBuffer compressedBlock) 
            throws IOException {

        createDecompressorIfNeeded(getCompressionType(header));
        return this.decompressor.decompress(compressedBlock, 
                                            getUncompressedBlockSize(header));
    }

    /**
     * Creates the <code>Decompressor</code> needed to uncompress the blocks.
     * 
     * @throws IOException if an I/O problem occurs.
     */
    private void createDecompressorIfNeeded(CompressionType compressionType) throws IOException {

        if (this.decompressor == null || this.decompressor.getType() != compressionType) {

            this.decompressor = compressionType.newDecompressor();
        }
    }
}
