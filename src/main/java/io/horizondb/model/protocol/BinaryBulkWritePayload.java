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

import com.google.common.collect.Range;

/**
 * @author Benjamin
 * 
 */
public class BinaryBulkWritePayload extends AbstractBulkWritePayload {

    /**
     * The parser instance.
     */
    private static final Parser<BinaryBulkWritePayload> PARSER = new AbstractBulkWritePayload.AbstractParser<BinaryBulkWritePayload, ReadableBuffer>() {

        /**
         * {@inheritDoc}
         */
        @Override
        protected ReadableBuffer parseRecordSetFrom(ByteReader reader) throws IOException {

            ReadableBuffer readableBuffer = (ReadableBuffer) reader;

            return readableBuffer.slice(readableBuffer.readableBytes());
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected BinaryBulkWritePayload newBulkWritePayload(String databaseName,
                                                   String seriesName,
                                                   Range<Long> partitionTimeRange,
                                                   ReadableBuffer buffer) {

            return new BinaryBulkWritePayload(databaseName, seriesName, partitionTimeRange, buffer);
        }
    };

    /**
     * The buffer containing the serialized record data.
     */
    private final ReadableBuffer buffer;

    /**
     * Creates a new <code>BinaryBulkWritePayload</code> for the specified database and the specified series.
     * 
     * @param databaseName the database name
     * @param seriesName the time series name
     * @param partitionTimeRange the partition time range
     * @param buffer the readable buffer
     */
    private BinaryBulkWritePayload(String databaseName, String seriesName, Range<Long> partitionTimeRange, ReadableBuffer buffer) {
        super(databaseName, seriesName, partitionTimeRange);
        this.buffer = buffer;
    }

    /**
     * Creates a new <code>BinaryBulkWritePayload</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static BinaryBulkWritePayload parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>BinaryBulkWritePayload</code> instances.
     * @return the parser that can be used to deserialize <code>BinaryBulkWritePayload</code> instances.
     */
    public static Parser<BinaryBulkWritePayload> getParser() {

        return PARSER;
    }

    /**
     * Returns the binary representation of the records.
     * 
     * @return the binary representation of the records.
     */
    public ReadableBuffer getBuffer() {
        return this.buffer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected int computeRecordSetSerializedSize() {
        return this.buffer.readableBytes();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void writeRecordSetTo(ByteWriter writer) throws IOException {
        writer.transfer(this.buffer);
    }
}
