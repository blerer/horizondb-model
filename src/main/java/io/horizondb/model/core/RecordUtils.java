/**
 * Copyright 2014 Benjamin Lerer
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
package io.horizondb.model.core;

import io.horizondb.io.ByteWriter;
import io.horizondb.io.encoding.VarInts;

import java.io.IOException;

/**
 * Utility methods to work with records.
 * 
 * @author Benjamin
 */
public final class RecordUtils {

    /**
     * Computes the serialized size of the specified list of records.
     * 
     * @param records the record list
     * @return the serialized size of the specified list of records.
     */
    public static int computeSerializedSize(Iterable<? extends Record> records) {

        int size = 0;

        for (Record record : records) {

            int serializedSize = record.computeSerializedSize();
            size += 1 + VarInts.computeUnsignedIntSize(serializedSize) + serializedSize;
        }

        return size;
    }

    /**
     * writes the specified list of records to the specified writer.
     * 
     * @param writer the writer to write to
     * @param records the records to write
     */
    public static void writeRecords(ByteWriter writer, Iterable<? extends Record> records) throws IOException {

        for (Record record : records) {

            int serializedSize = record.computeSerializedSize();
            writer.writeByte(record.getType());
            VarInts.writeUnsignedInt(writer, serializedSize);
            record.writeTo(writer);
        }
    }

    /**
     * Must not be instantiated.
     */
    private RecordUtils() {
    }
}
