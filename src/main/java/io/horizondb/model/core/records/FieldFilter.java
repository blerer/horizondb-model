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

import io.horizondb.io.BitSet;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Record;

import java.io.IOException;

/**
 * Decorator that filter the fields of the <code>Record</code> that it decorate.
 */
public class FieldFilter extends AbstractRecord {

    /**
     * The type of this record.
     */
    private final int type;

    /**
     * The decorated record.
     */
    private Record record;

    /**
     * The field mapping between the input and output.
     */
    private final int[] mapping;

    /**
     * The bit set.
     */
    private final BitSet bitSet;

    /**
     * Creates a new <code>FieldFilter</code> instance with the specified mappings.
     * 
     * @param type the new record type
     * @param mapping the field mapping between the input and output.
     */
    public FieldFilter(int type, int... mapping) {
        this.type = type;
        this.mapping = mapping;
        this.bitSet = new BitSet(this.mapping.length + 1);
    }

    /**
     * Decorates the specified record.
     * 
     * @param record the record for which the field must be filtered
     * @return this <code>FieldFilter</code>
     */
    public FieldFilter wrap(Record record) {
        this.record = record;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isDelta() throws IOException {
        return this.record.isDelta();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Record newInstance() {
        return new FieldFilter(this.type, this.mapping).wrap(this.record.newInstance());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getType() {
        return this.type;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumberOfFields() {
        return this.mapping.length;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field getField(int index) throws IOException {
        return this.record.getField(this.mapping[index]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isBinary() {
        return this.record.isBinary();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getFieldLengthInBytes(int index) throws IOException {
        return this.record.getFieldLengthInBytes(this.mapping[index]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field[] getFields() throws IOException {

        Field[] recordFields = this.record.getFields();
        Field[] newFields = new Field[this.mapping.length];

        for (int i = 0; i < newFields.length; i++) {
            newFields[i] = recordFields[this.mapping[i]];
        }

        return newFields;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TimeSeriesRecord toTimeSeriesRecord() throws IOException {
        return new TimeSeriesRecord(getType(), deepCopy(getFields()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BinaryTimeSeriesRecord toBinaryTimeSeriesRecord() throws IOException {
        return toTimeSeriesRecord().toBinaryTimeSeriesRecord();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() throws IOException {
        BitSet bitSet = getBitSet();

        int size = 0;

        size += VarInts.computeUnsignedLongSize(bitSet.toLong());

        bitSet.readBit(); // skip isDelta

        for (int i = 0; i < this.mapping.length; i++) {
            if (bitSet.readBit()) {
                size += this.record.getFieldLengthInBytes(this.mapping[i]);
            }
        }
        return size;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeFieldTo(int index, ByteWriter writer) throws IOException {
        this.record.writeFieldTo(this.mapping[index], writer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {

        BitSet bitSet = getBitSet().readerIndex(1);
        VarInts.writeUnsignedLong(writer, bitSet.toLong());

        for (int i = 0; i < this.mapping.length; i++) {
            if (bitSet.readBit()) {
                writeFieldTo(i, writer);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BitSet getBitSet() throws IOException {

        this.bitSet.reset().writeBit(isDelta());
        BitSet original = this.record.getBitSet();
        for (int i = 0; i < this.mapping.length; i++) {
            this.bitSet.writeBit(original.getBit(this.mapping[i] + 1));
        }
        return this.bitSet.readerIndex(0);
    }
}
