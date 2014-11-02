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
package io.horizondb.model.schema;

import io.horizondb.io.ByteWriter;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Filter;
import io.horizondb.model.core.records.BinaryTimeSeriesRecord;
import io.horizondb.model.core.records.TimeSeriesRecord;

import java.io.IOException;
import java.util.Iterator;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * A <code>RecordSetDefinition</code> that forward all its call to another <code>RecordSetDefinition</code>.
 * 
 * @author Benjamin
 *
 */
abstract class ForwardingRecordSetDefinition implements RecordSetDefinition {

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<RecordTypeDefinition> iterator() {
        return delegate().iterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() throws IOException {
        return delegate().computeSerializedSize();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {
        delegate().writeTo(writer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BinaryTimeSeriesRecord[] newBinaryRecords() {
        return delegate().newBinaryRecords();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BinaryTimeSeriesRecord[] newBinaryRecords(Filter<String> filter) {
        return delegate().newBinaryRecords(filter);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TimeSeriesRecord[] newRecords() {
        return delegate().newRecords();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TimeSeriesRecord newRecord(String name) {
        return delegate().newRecord(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TimeSeriesRecord newRecord(int index) {
        return delegate().newRecord(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field newField(String fieldName) {
        return delegate().newField(fieldName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field newField(int recordTypeIndex, int fieldIndex) {
        return delegate().newField(recordTypeIndex, fieldIndex);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getRecordTypeIndex(String type) {
        return delegate().getRecordTypeIndex(type);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getFieldIndex(int type, String name) {
        return delegate().getFieldIndex(type, name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TimeUnit getTimeUnit() {
        return delegate().getTimeUnit();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TimeZone getTimeZone() {
        return delegate().getTimeZone();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumberOfRecordTypes() {
        return delegate().getNumberOfRecordTypes();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getRecordName(int index) {
        return delegate().getRecordName(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getFieldName(int recordTypeIndex, int fieldIndex) {
        return delegate().getFieldName(recordTypeIndex, fieldIndex);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordTypeDefinition getRecordType(int index) {
        return delegate().getRecordType(index);
    }

    /**
     * Returns the backing delegate instance that methods are forwarded to.
     * 
     * @return the delegate 
     */
    protected abstract RecordSetDefinition delegate();
}
