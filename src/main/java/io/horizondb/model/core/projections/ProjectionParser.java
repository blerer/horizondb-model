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

import java.io.IOException;

import io.horizondb.io.ByteReader;
import io.horizondb.io.serialization.Parser;

/**
 * <code>Parser</code> for <code>Projection</code>
 */
final class ProjectionParser implements Parser<RecordTypeProjection> {

    /**
     * The singleton instance.
     */
    public static final ProjectionParser INSTANCE = new ProjectionParser();
    
    /**
     * {@inheritDoc}
     */
    @Override
    public RecordTypeProjection parseFrom(ByteReader reader) throws IOException {
        
        int type = reader.readByte();
        
        if (type == NoopRecordTypeProjection.TYPE) {
            return NoopRecordTypeProjection.parseFrom(reader);
        }
        
        return DefaultRecordTypeProjection.parseFrom(reader);
    }
    
    /**
     * Singleton
     */
    private ProjectionParser() {
    }
}
