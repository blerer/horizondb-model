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
package io.horizondb.model.core.projections;

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.serialization.Parser;
import io.horizondb.model.core.Projection;

import java.io.IOException;

/**
 * Utility methods to work with <code>Projection</code>s.
 *
 */
public class Projections {
    
    private static final Parser<Projection> PARSER = new Parser<Projection>() {

        @Override
        public Projection parseFrom(ByteReader reader) throws IOException {
            
            int type = reader.readByte();
            
            if (type == NoopRecordTypeProjection.TYPE) {
                return NoopProjection.parseFrom(reader);
            }
            
            return DefaultProjection.parseFrom(reader);
        }
    };
    
    public static Projection parseFrom(ByteReader reader) throws IOException {
        
        return PARSER.parseFrom(reader);
    }
    
    /**
     * Serialize the specified projection.
     * @param writer the writer
     * @param projection the projection to serialize
     * 
     * @throws IOException if an I/O problem occurs
     */
    public static void writeTo(ByteWriter writer, Projection projection) throws IOException {
        writer.writeByte(projection.getType());
        projection.writeTo(writer);
    }
    
    /**
     * Computes the size of the specified projection.
     * 
     * @param projection the projection for which the serialized size must be computed
     */
    public static int computeSerializedSize(Projection projection) {
        
        return 1 + projection.computeSerializedSize();
    }
}
