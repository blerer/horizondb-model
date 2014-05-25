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

import io.horizondb.io.Buffer;
import io.horizondb.io.buffers.Buffers;
import io.horizondb.io.checksum.ChecksumByteReader;
import io.horizondb.io.checksum.ChecksumByteWriter;
import io.horizondb.model.core.Field;

import org.junit.Test;

import com.google.common.collect.Range;

import static io.horizondb.model.schema.FieldType.MILLISECONDS_TIMESTAMP;

import static org.junit.Assert.*;

/**
 * @author Benjamin
 *
 */
public class SerializationUtilsTest {

    @Test
    public void testWriteRange() throws IOException {
        
        Range<Field> range = MILLISECONDS_TIMESTAMP.range("'2013-11-26'", "'2013-11-27'");

        Buffer buffer = Buffers.allocate(SerializationUtils.computeRangeSerializedSize(range)); 
        
        SerializationUtils.writeRange(buffer, range);
        
        Range<Field> result = SerializationUtils.parseRangeFrom(buffer);
        
        assertEquals(range, result);
    }

    @Test
    public void testWriteRangeWithCRC() throws IOException {
        
        Range<Field> range = MILLISECONDS_TIMESTAMP.range("'2013-11-26'", "'2013-11-27'");

        Buffer buffer = Buffers.allocate(SerializationUtils.computeRangeSerializedSize(range) + 8); 
        
        ChecksumByteWriter crcWriter = ChecksumByteWriter.wrap(buffer);
        
        SerializationUtils.writeRange(crcWriter, range);
        crcWriter.writeChecksum();
        
        ChecksumByteReader crcReader = ChecksumByteReader.wrap(buffer);
        
        Range<Field> result = SerializationUtils.parseRangeFrom(crcReader);
        
        assertEquals(range, result);
    }
    
}
