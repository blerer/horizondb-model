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

import io.horizondb.io.ByteWriter;
import io.horizondb.io.ReadableBuffer;
import io.horizondb.model.core.DataBlock;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.RecordUtils;

import java.io.IOException;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * A {@link DataBlock} that always return a copy of the header and of the buffer.
 *
 */
public final class DefaultDataBlock extends AbstractDataBlock {

    /**
     * The block header.
     */
    private final Record header;
    
    /**
     * The block data.
     */
    private final ReadableBuffer data;

    /**
     * Creates a new <code>DataBlock</code> with the specified header and data.
     *
     * @param header the block header
     * @param data the block data
     */
    public DefaultDataBlock(Record header, ReadableBuffer data) {
        this.header = header;
        this.data = data.readerIndex(0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Record getHeader() {
        return this.header.newInstance();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ReadableBuffer getData() {
        return this.data.duplicate();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("header", this.header)
                                                                          .append("data", this.data)
                                                                          .toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() throws IOException {
        return RecordUtils.computeSerializedSize(this.header)
                + this.data.readableBytes();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
        RecordUtils.writeRecord(writer, getHeader());
        writer.transfer(getData());
    }
}
