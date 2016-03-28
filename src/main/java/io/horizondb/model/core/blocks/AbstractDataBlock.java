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
package io.horizondb.model.core.blocks;

import io.horizondb.io.buffers.Buffers;
import io.horizondb.model.core.DataBlock;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.iterators.BinaryTimeSeriesRecordIterator;
import io.horizondb.model.core.records.BlockHeaderUtils;
import io.horizondb.model.core.records.TimeSeriesRecord;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;

import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;

import static io.horizondb.model.core.iterators.BlockIterators.singleton;

/**
 * Base class for the <code>DataBlock</code> implementations.
 *
 */
public abstract class AbstractDataBlock implements DataBlock {

    /**
     * {@inheritDoc}
     */
    @Override
    public final RangeMap<Field, DataBlock> split(TimeSeriesDefinition definition) throws IOException {

        Range<Field> range = BlockHeaderUtils.getRange(getHeader());

        Range<Field> partitionRange = definition.getPartitionTimeRange(range.lowerEndpoint());

        if (partitionRange.contains(range.upperEndpoint())) {
            return ImmutableRangeMap.<Field, DataBlock> of(partitionRange, this);
        }

        TimeSeriesRecord[] records = definition.newRecords();

        ImmutableRangeMap.Builder<Field, DataBlock> builder = ImmutableRangeMap.builder();

        RecordAppender appender = new RecordAppender(definition, Buffers.getDefaultAllocator(), records);

        Field[] timestamps = new Field[records.length];
        for (int i = 0; i < timestamps.length; i++) {
            timestamps[i] = definition.newField(Record.TIMESTAMP_FIELD_NAME);
        }

        try (BinaryTimeSeriesRecordIterator iterator = new BinaryTimeSeriesRecordIterator(definition, singleton(this))) {

            while (iterator.hasNext()) {

                Record record = iterator.next();
                Field timestamp = timestamps[record.getType()];
                if (record.isDelta()) {
                    timestamp.add(record.getField(Record.TIMESTAMP_FIELD_INDEX));
                } else {
                    record.getField(Record.TIMESTAMP_FIELD_INDEX).copyTo(timestamp);
                }

                if (!partitionRange.contains(timestamp)) {

                    builder.put(partitionRange, appender.getDataBlock());
                    partitionRange = definition.getPartitionTimeRange(timestamp);
                    appender = new RecordAppender(definition, Buffers.getDefaultAllocator(), records);
                }
                appender.append(record);
            }
            builder.put(partitionRange, appender.getDataBlock());
        }

        return builder.build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getFirstTimestamp() throws IOException {
        return BlockHeaderUtils.getFirstTimestamp(getHeader());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getLastTimestamp() throws IOException {
        return BlockHeaderUtils.getLastTimestamp(getHeader());
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public final boolean isAfter(DataBlock block) throws IOException {
        return getFirstTimestamp() > block.getLastTimestamp();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final boolean overlap(DataBlock block) throws IOException {

        long timestamp = getFirstTimestamp();
        long otherTimestamp = block.getFirstTimestamp();

        if (timestamp < otherTimestamp) {
            return otherTimestamp <= getLastTimestamp();
        }
        return timestamp <= block.getLastTimestamp();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final boolean hasSpaceAvailable(TimeSeriesDefinition definition) throws IOException {

        long blockSize = BlockHeaderUtils.getUncompressedBlockSize(getHeader());
        long averageRecordSize = blockSize / getNumberOfRecords(definition);
        // If we have at least twice the space of an average record we should be able 
        // to add an extra one.
        return (blockSize + (2 * averageRecordSize)) <= definition.getBlockSizeInBytes();
    }

    /**
     * Returns the total number of records within this block.
     *
     * @return the total number of records within this block.
     * @throws IOException if an I/O problem occurs
     */
    private long getNumberOfRecords(TimeSeriesDefinition definition) throws IOException {
        int numberOfRecords = 0; 
        for (int i = 0; i < definition.getNumberOfRecordTypes(); i++)
        {
            numberOfRecords += BlockHeaderUtils.getRecordCount(getHeader(), i);
        }
        return numberOfRecords;
    }
}
