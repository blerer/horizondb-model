/**
 * Copyright 2013 Benjamin Lerer
 * 
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
package io.horizondb.model.protocol;

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.ReadableBuffer;
import io.horizondb.io.serialization.Parser;

import java.io.IOException;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * @author Benjamin
 * 
 */
public final class DataChunkPayload implements Payload {

    /**
     * The parser instance.
     */
    private static final Parser<DataChunkPayload> PARSER = new Parser<DataChunkPayload>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public DataChunkPayload parseFrom(ByteReader reader) throws IOException {

            ReadableBuffer buffer = (ReadableBuffer) reader;

            return new DataChunkPayload(buffer);
        }
    };

    /**
     * The buffer containing the data.
     */
    private final ReadableBuffer buffer;

    /**
     * Creates a new data chunk containing the specified bytes.
     * 
     * @param buffer the buffer
     */
    public DataChunkPayload(ReadableBuffer buffer) {
        this.buffer = buffer;
    }

    /**
     * Creates a new <code>DataChunkPayload</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static DataChunkPayload parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>DataChunkPayload</code> instances.
     * 
     * @return the parser that can be used to deserialize <code>DataChunkPayload</code> instances.
     */
    public static Parser<DataChunkPayload> getParser() {

        return PARSER;
    }

    public ReadableBuffer getBuffer() {
        return this.buffer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {
        return this.buffer.readableBytes();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
        writer.transfer(this.buffer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object object) {

        if (object == this) {
            return true;
        }
        if (!(object instanceof DataChunkPayload)) {
            return false;
        }
        DataChunkPayload rhs = (DataChunkPayload) object;
        return new EqualsBuilder().append(this.buffer, rhs.buffer).isEquals();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(952880905, 769187925).append(this.buffer).toHashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("buffer", this.buffer).toString();
    }

}
