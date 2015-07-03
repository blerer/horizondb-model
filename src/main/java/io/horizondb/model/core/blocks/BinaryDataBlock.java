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
package io.horizondb.model.core.blocks;

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.ReadableBuffer;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.model.core.DataBlock;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.RecordUtils;
import io.horizondb.model.core.records.BinaryTimeSeriesRecord;

import java.io.IOException;

import static io.horizondb.model.core.records.BlockHeaderUtils.getCompressedBlockSize;
import static org.apache.commons.lang.Validate.isTrue;

/**
 *
 */
public final class BinaryDataBlock implements DataBlock {

    /**
     * The block header.
     */
    private final BinaryTimeSeriesRecord header;

    /**
     * The buffer containing the block data.
     */
    private ReadableBuffer buffer;

    /**
     * Creates a new <code>BinaryDataBlock</code> that will use the specified header.
     * 
     * @param header the record used to store the block headers
     */
    public BinaryDataBlock(BinaryTimeSeriesRecord header) {
        this.header = header;
    }

    /**
     * Fills this block with the data read from the specified <code>ByteReader</code>.
     *
     * @param reader the <code>ByteReader</code> to read the block from.
     * @return this <code>DataBlock</code>
     * @throws IOException if an I/O problem occurs
     */
    public BinaryDataBlock fill(ByteReader reader) throws IOException {

        isTrue(reader.readByte() == Record.BLOCK_HEADER_TYPE,
                "The first record should be a block header but was not.");

        int length = VarInts.readUnsignedInt(reader);
        ReadableBuffer headerBytes = reader.slice(length).duplicate();
        this.header.fill(headerBytes);

        int blockSize = getCompressedBlockSize(this.header);
        this.buffer = reader.slice(blockSize);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() throws IOException {
        return RecordUtils.computeSerializedSize(this.header)
                + getCompressedBlockSize(this.header);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
        RecordUtils.writeRecord(writer, this.header);
        writer.transfer(this.buffer.readerIndex(0));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Record getHeader() {
        return this.header;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ReadableBuffer getData() {
        return this.buffer;
    }
}
