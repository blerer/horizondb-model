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
package io.horizondb.model.core.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.model.core.Field;
import io.horizondb.model.schema.FieldType;

/**
 * Utility methods to serialize and deserialized model elements.
 * 
 * @author Benjamin
 *
 */
public final class SerializationUtils {

    /**
     * Deserializes a <code>Field</code> from the specified reader.
     * 
     * @param reader the reader to read from
     * @return the deserialized <code>Field</code>
     * @throws IOException if an I/O problem occurs
     */
    public static Field parseFieldFrom(ByteReader reader) throws IOException {
        
        FieldType type = FieldType.parseFrom(reader);
        Field field = type.newField();
        field.readFrom(reader);
        
        return field;
    }
    
    /**
     * Deserializes a <code>BoundType</code> from the specified reader.
     * 
     * @param reader the reader to read from
     * @return the deserialized <code>BoundType</code>
     * @throws IOException if an I/O problem occurs
     */
    public static BoundType parseBoundTypeFrom(ByteReader reader) throws IOException {
        
        byte b = reader.readByte();

        if (b == 0) {
            return BoundType.CLOSED;
        }
            
        return BoundType.OPEN;
    }
    
    /**
     * Serializes the specified <code>Field</code> into the specified writer.
     * @param writer the writer to write to
     * @param field the field to serialize
     * 
     * @throws IOException if an I/O problem occurs
     */
    public static void writeField(ByteWriter writer, Field field) throws IOException {
        
        field.getType().writeTo(writer);
        field.writeTo(writer);
    }
    
    /**
     * Serializes the specified <code>BoundType</code> into the specified writer.
     * @param writer the writer to write to
     * @param boundType the BoundType to serialize
     * 
     * @throws IOException if an I/O problem occurs
     */
    public static void writeBoundType(ByteWriter writer, BoundType boundType) throws IOException {
        
        if (boundType == BoundType.CLOSED) {
            writer.writeByte(0);
        } else {
            writer.writeByte(1);
        }
    }
    
    /**
     * Computes the serialized size of the specified field. 
     * 
     * @param field the field
     * @return the serialized size of the specified field. 
     */
    public static int computeFieldSerializedSize(Field field) {
        return field.getType().computeSerializedSize() + field.computeSerializedSize();
    }
    
    /**
     * Computes the serialized size of the specified bound type. 
     * 
     * @param boundType the bound type
     * @return the serialized size of the specified bound type. 
     */
    public static int computeBoundTypeSerializedSize(BoundType boundType) {
        return 1;
    }
    
    /**
     * Computes the serialized size of the specified range of fields. 
     * 
     * @param range the range
     * @return the serialized size of the specified range of fields 
     */
    public static int computeRangeSerializedSize(Range<Field> range) {
        
        return computeFieldSerializedSize(range.lowerEndpoint()) 
                + computeBoundTypeSerializedSize(range.lowerBoundType())
                + computeFieldSerializedSize(range.upperEndpoint()) 
                + computeBoundTypeSerializedSize(range.upperBoundType());
    }
    
    /**
     * Deserializes a range of <code>Field</code>s from the specified reader.
     * 
     * @param reader the reader to read from
     * @return the deserialized range of <code>Field</code>s
     * @throws IOException if an I/O problem occurs
     */
    public static Range<Field> parseRangeFrom(ByteReader reader) throws IOException {
        
        Field lowerEndPoint = parseFieldFrom(reader);
        BoundType lowerBoundType = parseBoundTypeFrom(reader);
        Field upperEndPoint = parseFieldFrom(reader);
        BoundType upperBoundType = parseBoundTypeFrom(reader);
        
        return Range.range(lowerEndPoint, lowerBoundType, upperEndPoint, upperBoundType);
    }
    
    /**
     * Serializes the specified range of <code>Field</code>s into the specified writer.
     * @param writer the writer to write to
     * @param range the range of fields to serialize
     * 
     * @throws IOException if an I/O problem occurs
     */
    public static void writeRange(ByteWriter writer, Range<Field> range) throws IOException {
        
        writeField(writer, range.lowerEndpoint());
        writeBoundType(writer, range.lowerBoundType());
        writeField(writer, range.upperEndpoint());
        writeBoundType(writer, range.upperBoundType());
    }
    
    /**
     * Computes the serialized size of the specified list of <code>String</code>. 
     * 
     * @param list the list of <code>String</code> to serialize
     * @return the serialized size of the specified list of <code>String</code>
     */
    public static int computeStringListSerializedSize(List<String> list) {
        
        int size = VarInts.computeUnsignedIntSize(list.size());
        
        for (int i = 0, m = list.size(); i < m; i++) {
            size += VarInts.computeStringSize(list.get(i));
        }
        
        return size;
    }
    
    /**
     * Serializes the specified list of <code>String</code>s into the specified writer.
     * @param writer the writer to write to
     * @param list the list of <code>String</code>s to serialize
     * 
     * @throws IOException if an I/O problem occurs
     */
    public static void writeStringList(ByteWriter writer, List<String> list) throws IOException {
        
        VarInts.writeUnsignedInt(writer, list.size());
        
        for (int i = 0, m = list.size(); i < m; i++) {
            VarInts.writeString(writer, list.get(i));
        }
    }
    
    /**
     * Deserializes a list of <code>String</code>s from the specified reader.
     * 
     * @param reader the reader to read from
     * @return the deserialized list of <code>String</code>s
     * @throws IOException if an I/O problem occurs
     */
    public static List<String> parseStringListFrom(ByteReader reader) throws IOException {
        
        int size = VarInts.readUnsignedInt(reader);
        
        if (size == 0) {
            
            return Collections.emptyList();
        }
        
        List<String> list = new ArrayList<>(size);
        
        for (int i = 0; i < size; i++) {
            list.add(VarInts.readString(reader));
        }
        
        return list;
    }
    
    /**
     * Must not be instantiated.
     */
    private SerializationUtils() {
        
    }
}
