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
package io.horizondb.model.protocol;

import io.horizondb.io.Buffer;
import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.ReadableBuffer;
import io.horizondb.io.buffers.Buffers;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.io.serialization.Parser;

import java.io.IOException;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 */
public final class InsertPayload implements Payload {
   
    /**
     * The parser instance.
     */
    private static final Parser<InsertPayload> PARSER = new Parser<InsertPayload>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public InsertPayload parseFrom(ByteReader reader) throws IOException {

            String database = VarInts.readString(reader);
            String timeSeries = VarInts.readString(reader);
            int recordType = reader.readByte();
            int length = VarInts.readUnsignedInt(reader);
            Buffer buffer = Buffers.allocate(length);
            buffer.transfer(reader.slice(length)).readerIndex(0);
            
            return new InsertPayload(database, timeSeries, recordType, buffer);
        }
    };
    
    /**
     * The name of the database in which the data must be inserted.
     */
    private final String database; 
    
    /**
     * The time series in which the data must be inserted.
     */
    private final String series;

    /**
     * The record type
     */
    private final int recordType;

    /**
     * The record data.
     */
    private final Buffer buffer;

    /**
     * Creates a new <code>InsertPayload</code>. 
     * 
     * @param database the database in which the data must be inserted
     * @param series the time series in which the data must be inserted
     * @param recordType the record type
     * @param buffer the record data
     */
    public InsertPayload(String database, String series, int recordType, Buffer buffer) {
        
        this.database = database;
        this.series = series;
        this.recordType = recordType;
        this.buffer = buffer;
    }

    /**
     * Returns the name of the database in which the data must be inserted.   
     * 
     * @return the name of the database in which the data must be inserted.  
     */
    public String getDatabase() {
        return this.database;
    }

    /**
     * Returns the name of the time series in which the data must be inserted.   
     *
     * @return the name of the time series in which the data must be inserted.  
     */
    public String getSeries() {
        return this.series;
    }

    /**
     * Returns the type of the record that must be inserted.
     * 
     * @return the type of the record that must be inserted.
     */
    public int getRecordType() {
        return this.recordType;
    }

    /**
     * Returns the record data
     *
     * @return the buffer the record data
     */
    public ReadableBuffer getBuffer() {
        return this.buffer.readerIndex(0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {

        int length = this.buffer.readerIndex(0).readableBytes();
        return VarInts.computeStringSize(this.database) 
                + VarInts.computeStringSize(this.series) 
                + 1
                + VarInts.computeUnsignedIntSize(length)
                + length;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
        VarInts.writeString(writer, this.database);
        VarInts.writeString(writer, this.series);
        VarInts.writeByte(writer, this.recordType);
        VarInts.writeUnsignedInt(writer, this.buffer.readableBytes());
        writer.transfer(this.buffer.readerIndex(0));
    }

    /**
     * Creates a new <code>InsertPayload</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static InsertPayload parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>InsertPayload</code> instances.
     * @return the parser that can be used to deserialize <code>InsertPayload</code> instances.
     */
    public static Parser<InsertPayload> getParser() {

        return PARSER;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("database", this.database)
                                                                          .append("series", this.series)
                                                                          .append("recordType", this.recordType)
                                                                          .append("buffer", this.buffer)
                                                                          .toString();
    }
}
