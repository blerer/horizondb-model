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
package io.horizondb.model.schema;

import java.io.IOException;

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.io.serialization.Parser;
import io.horizondb.io.serialization.Serializable;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * The position of a block of data within a stream.
 * 
 * @author Benjamin
 *
 */
public final class BlockPosition implements Serializable {
    
    /**
     * The parser instance.
     */
    private static final Parser<BlockPosition> PARSER = new Parser<BlockPosition>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public BlockPosition parseFrom(ByteReader reader) throws IOException {

            long offset = VarInts.readUnsignedLong(reader);
            long length = VarInts.readUnsignedLong(reader);
            return new BlockPosition(offset, length);
        }
    };
    
    /**
     * The position of the start of the block.
     */
    private final long offset;
    
    /**
     * The block length.
     */
    private final long length;

    /**
     * Creates a new <code>BlockPosition</code> instance.
     * 
     * @param offset the position of the start of the block
     * @param length the block length
     */
    public BlockPosition(long offset, long length) {
        this.offset = offset;
        this.length = length;
    }

    /**
     * Returns the position of the start of the block.
     * @return the position of the start of the block
     */
    public long getOffset() {
        return this.offset;
    }

    /**
     * Returns the block length
     * @return the block length
     */
    public long getLength() {
        return this.length;
    }

    /**
     * Creates a new <code>BlockPosition</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static BlockPosition parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>BlockPosition</code> instances.
     * @return the parser that can be used to deserialize <code>BlockPosition</code> instances.
     */
    public static Parser<BlockPosition> getParser() {

        return PARSER;
    }
    
    /**    
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {
        return VarInts.computeUnsignedLongSize(this.offset) 
                + VarInts.computeUnsignedLongSize(this.length);
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
        VarInts.writeUnsignedLong(writer, this.offset);
        VarInts.writeUnsignedLong(writer, this.length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof BlockPosition)) {
            return false;
        }
        BlockPosition rhs = (BlockPosition) object;
        return new EqualsBuilder().append(this.length, rhs.length)
                                  .append(this.offset, rhs.offset)
                                  .isEquals();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(2002115733, 1375707659).append(this.length)
                                                          .append(this.offset)
                                                          .toHashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("offset", this.offset)
                                                                          .append("length", this.length)
                                                                          .toString();
    }
    
    
}
