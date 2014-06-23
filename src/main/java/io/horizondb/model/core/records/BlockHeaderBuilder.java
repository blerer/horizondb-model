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
package io.horizondb.model.core.records;

import static org.apache.commons.lang.Validate.notNull;

import java.io.IOException;

import io.horizondb.io.compression.CompressionType;
import io.horizondb.model.schema.TimeSeriesDefinition;

/**
 * @author Benjamin
 *
 */
public final class BlockHeaderBuilder {

    /**
     * The record containing the header information.
     */
    private final TimeSeriesRecord header;

    /**
     * 
     */
    public BlockHeaderBuilder(TimeSeriesDefinition definition) {
        
        notNull(definition, "the definition parameter must not be null.");
        
        this.header = definition.newBlockHeader(); 
    }
    
    /**
     * Sets the first timestamp of the block in nanoseconds. 
     *     
     * @param timestampInNanos the first timestamp of the block in nanoseconds
     * @return this <code>BlockHeaderBuilder</code>.
     */
    public BlockHeaderBuilder firstTimestampInNanos(long timestampInNanos) {
        
        BlockHeaderUtils.setFirstTimestampInNanos(this.header, timestampInNanos);
        return this;
    }
    
    /**
     * Sets the last timestamp of the block in nanoseconds. 
     *     
     * @param timestampInNanos the last timestamp of the block in nanoseconds
     * @return this <code>BlockHeaderBuilder</code>.
     * @throws IOException if an I/O problem occurs
     */
    public BlockHeaderBuilder lastTimestampInNanos(long timestampInNanos) throws IOException {
        
        BlockHeaderUtils.setLastTimestampInNanos(this.header, timestampInNanos);
        return this;
    }
    
    /**
     * Sets the type of compression used to compress the block. 
     *     
     * @param compressionType the type of compression used to compress the block
     * @return this <code>BlockHeaderBuilder</code>.
     */
    public BlockHeaderBuilder compressionType(CompressionType compressionType) {
        
        BlockHeaderUtils.setCompressionType(this.header, compressionType);
        return this;
    }
    
    /**
     * Sets the size of the compressed block. 
     *     
     * @param size the size of the compressed block
     * @return this <code>BlockHeaderBuilder</code>.
     */
    public BlockHeaderBuilder compressedBlockSize(int size) {
        
        BlockHeaderUtils.setCompressedBlockSize(this.header, size);
        return this;
    }
    
    /**
     * Sets the size of the uncompressed block. 
     *     
     * @param size the size of the uncompressed block
     * @return this <code>BlockHeaderBuilder</code>.
     * @throws IOException if an I/O problem occurs.
     */
    public BlockHeaderBuilder uncompressedBlockSize(int size) throws IOException {
        
        BlockHeaderUtils.setUncompressedBlockSize(this.header, size);
        return this;
    }
    
    /**
     * Sets the number of records of the specified type. 
     *     
     * @param recordType the type of record
     * @param count the number of records of the specified type
     * @return this <code>BlockHeaderBuilder</code>.
     */
    public BlockHeaderBuilder recordCount(int recordType, int count) {
        
        BlockHeaderUtils.setRecordCount(this.header, recordType, count);
        return this;
    }
    
    /**
     * Returns a new block header <code>Record</code> instance.
     * @return a new block header <code>Record</code> instance.
     */
    public TimeSeriesRecord build() {
        
        return this.header.newInstance();
    }
}
