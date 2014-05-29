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
import io.horizondb.io.serialization.Parser;
import io.horizondb.io.serialization.Serializable;

import java.io.IOException;

/**
 * The messages operation codes. 
 * 
 * @author Benjamin
 * 
 */
public enum OpCode implements Serializable {

    /**
     * The operation code returned by the server when the message could not be deserialized.
     */
    ERROR(0) {

        /**
         * {@inheritDoc}
         */
        @Override
        public Parser<?> getPayloadParser(boolean request) {

            // Does not exists within a request
            return ErrorPayload.getParser();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isMutation() {
            return false;
        }
    },

    /**
     * The operation code to request the creation of a database.
     */
    CREATE_DATABASE(1) {

        @Override
        public Parser<?> getPayloadParser(boolean request) {

            return CreateDatabasePayload.getParser();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isMutation() {
            return false;
        }
    },
    
    /**
     * The operation code to request the use of an other database.
     */
    USE_DATABASE(2) {

        @Override
        public Parser<?> getPayloadParser(boolean request) {

            return UseDatabasePayload.getParser();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isMutation() {
            return false;
        }
    },
    
    /**
     * The operation code to request a time series.
     */
    CREATE_TIMESERIES(3) {

        @Override
        public Parser<?> getPayloadParser(boolean request) {

            return CreateTimeSeriesPayload.getParser();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isMutation() {
            return false;
        }
    },
    
    /**
     * The operation code to request a time series.
     */
    GET_TIMESERIES(4) {

        @Override
        public Parser<?> getPayloadParser(boolean request) {

            if (request) {

                return GetTimeSeriesRequestPayload.getParser();
            }

            return GetTimeSeriesResponsePayload.getParser();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isMutation() {
            return false;
        }
    },

    /**
     * The operation code to perform a bulk write.
     */
    BULK_WRITE(5) {

        @Override
        public Parser<?> getPayloadParser(boolean request) {

            if (request) {

                return BinaryBulkWritePayload.getParser();
            }

            return Parser.NOOP_PARSER;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isMutation() {
            return true;
        }
    },

    /**
     * The operation code to execute a selection on the server.
     */
    SELECT(6) {

        @Override
        public Parser<?> getPayloadParser(boolean request) {

            return SelectPayload.getParser();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isMutation() {
            return false;
        }
    },
    
    /**
     * The operation code to execute an HQL query on the server.
     */
    HQL_QUERY(7) {

        @Override
        public Parser<?> getPayloadParser(boolean request) {

            return HqlQueryPayload.getParser();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isMutation() {
            return false;
        }
    },
    
    /**
     * The operation code used when no operation need to be performed.
     */
    NOOP(8) {

        @Override
        public Parser<?> getPayloadParser(boolean request) {

            return Parser.NOOP_PARSER;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isMutation() {
            return false;
        }
    },
    
    /**
     * The operation code use to notify the client that the used database has changed.
     */
    SET_DATABASE(9) {

        @Override
        public Parser<?> getPayloadParser(boolean request) {

            return SetDatabasePayload.getParser();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isMutation() {
            return false;
        }
    },
    
    /**
     * The operation code use to transfert data.
     */
    DATA_CHUNK(10) {

        @Override
        public Parser<?> getPayloadParser(boolean request) {

            return DataChunkPayload.getParser();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isMutation() {
            return false;
        }
    },
    
    /**
     * The operation code use to insert data.
     */
    INSERT(11) {

        @Override
        public Parser<?> getPayloadParser(boolean request) {

            return InsertPayload.getParser();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isMutation() {
            return true;
        }
    },

    /**
     * The operation code use to describe returned data.
     */
    DATA_HEADER(12) {

        @Override
        public Parser<?> getPayloadParser(boolean request) {

            return DataHeaderPayload.getParser();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isMutation() {
            return false;
        }
    };   

    /**
     * The parser instance.
     */
    private static final Parser<OpCode> PARSER = new Parser<OpCode>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public OpCode parseFrom(ByteReader reader) throws IOException {

            int code = reader.readByte();

            OpCode[] values = OpCode.values();

            for (int i = 0; i < values.length; i++) {

                OpCode opCode = values[i];

                if (opCode.b == code) {

                    return opCode;
                }
            }

            throw new IllegalStateException("The byte " + code + " does not match any OpCode");
        }
    };

    /**
     * The operation code binary representation.
     */
    private final int b;

    /**
     * Creates a new <code>OpCode</code> with the specified binary representation.
     * 
     * @param b the byte representing the <code>OpCode</code>.
     */
    private OpCode(int b) {

        this.b = b;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {

        return 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {

        writer.writeByte(this.b);
    }

    public static Parser<OpCode> getParser() {

        return PARSER;
    }

    /**
     * Returns the operation code represented by the next readable byte in the specified reader.
     * 
     * @param reader the buffer to read from.
     * @return the operation code represented by the next readable byte in the specified buffer.
     * @throws IOException if an I/O problem occurs
     */
    public static OpCode parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns <code>true</code> if the operation is an operation that will modify the database, <code>false</code>
     * otherwise.
     * 
     * @return <code>true</code> if the operation is an operation that will modify the database, <code>false</code>
     * otherwise.
     */
    public abstract boolean isMutation();
    
    /**
     * Returns <code>true</code> if the message contains an HQL query, <code>false</code> otherwise.
     * 
     * @return <code>true</code> if the message contains an HQL query, <code>false</code> otherwise.
     */
    public boolean isHql() {
        
        return this == HQL_QUERY;
    }

    /**
     * Returns the parser that can be used to parse the payload of this operation.
     * 
     * @param request <code>true</code> if the message is a request, <code>false</code> otherwise.
     * @return the parser that can be used to parse the payload of this operation.
     */
    public abstract Parser<?> getPayloadParser(boolean request);
}
