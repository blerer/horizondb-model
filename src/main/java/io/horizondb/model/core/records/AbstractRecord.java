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
package io.horizondb.model.core.records;

import io.horizondb.io.BitSet;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Record;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.io.PrintStream;

/**
 * @author Benjamin
 * 
 */
abstract class AbstractRecord implements Record {

    /**
     * {@inheritDoc}
     */
    @Override
    public int getByte(int index) throws IOException {

        return getField(index).getByte();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getInt(int index) throws IOException {

        return getField(index).getInt();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getLong(int index) throws IOException {

        return getField(index).getLong();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getDouble(int index) throws IOException {

        return getField(index).getDouble();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getTimestampInNanos(int index) throws IOException {

        return getField(index).getTimestampInNanos();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getTimestampInMicros(int index) throws IOException {

        return getField(index).getTimestampInMicros();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getTimestampInMillis(int index) throws IOException {

        return getField(index).getTimestampInMillis();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getTimestampInSeconds(int index) throws IOException {

        return getField(index).getTimestampInSeconds();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getDecimalMantissa(int index) throws IOException {

        return getField(index).getDecimalMantissa();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte getDecimalExponent(int index) throws IOException {

        return getField(index).getDecimalExponent();
    }

    /**
     * 
     * {@inheritDoc}
     */
    @Override
    public void copyTo(TimeSeriesRecord record) throws IOException {

        record.setDelta(isDelta());
        
        for (int i = 0, m = getNumberOfFields(); i < m; i++) {
            getField(i).copyTo(record.getField(i));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void writePrettyPrint(TimeSeriesDefinition definition, PrintStream stream) throws IOException {

        stream.append('[').append(definition.getRecordName(getType())).append(']');
        stream.append('[').append(Integer.toString(computeSerializedSize())).append(']');
        
        BitSet bitSet = getBitSet().duplicate();
        bitSet.readerIndex(1);    
        
        stream.append('[').append(bitSet.toString()).append(']');
        
        for (int i = 0, m = getNumberOfFields(); i < m; i++) {
            
            Field field = getField(i);
            
            if (bitSet.readBit()) {
                                
                stream.append('[')
                      .append(definition.getFieldName(getType(), i))
                      .append(" = ");
                
                field.writePrettyPrint(stream);
                
                stream.append(']');
            }
        }
    }

    /**
     * Creates a deep copy of the specified fields.
     * 
     * @param fields the fields
     * @return a deep copy of the specified fields.
     */
    protected static Field[] deepCopy(Field[] fields) {

        Field[] copy = new Field[fields.length];

        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            copy[i] = field.newInstance();
        }

        return copy;
    }
}
