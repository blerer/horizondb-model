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

import io.horizondb.io.ByteWriter;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.RecordUtils;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.google.common.collect.Range;

/**
 * <code>Payload</code> used to request the write of a bulk of records.
 * 
 * @author Benjamin
 *
 */
public final class BulkWritePayload extends AbstractBulkWritePayload {

    /**
     * The records that must be inserted.
     */
    private final List<? extends Record> records;

    /**
     * Creates a new <code>BulkWritePayload</code> for writing the specified data.
     * 
     * @param databaseName the database name
     * @param seriesName the time series name
     * @param partitionTimeRange the partition time range
     * @param records the records to write
     */
    public BulkWritePayload(String databaseName, 
                            String seriesName, 
                            Range<Field> partitionTimeRange, 
                            List<? extends Record> records) {

        super(databaseName, seriesName, partitionTimeRange);
        this.records = records;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected int computeRecordSetSerializedSize() {
        return RecordUtils.computeSerializedSize(this.records);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void writeRecordSetTo(ByteWriter writer) throws IOException {
        RecordUtils.writeRecords(writer, this.records);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).appendSuper(super.toString())
                                                                          .append("records", this.records)
                                                                          .toString();
    }

}
