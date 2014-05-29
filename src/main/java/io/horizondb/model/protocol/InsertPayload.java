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
package io.horizondb.model.protocol;

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.io.serialization.Parser;
import io.horizondb.model.core.util.SerializationUtils;

import java.io.IOException;
import java.util.List;

/**
 * 
 * @author Benjamin
 *
 */
public final class InsertPayload implements Payload {
   
    /**
     * The parser instance.
     */
    private static final Parser<InsertPayload> PARSER = new Parser<InsertPayload>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public InsertPayload parseFrom(ByteReader reader) throws IOException {

            String database = VarInts.readString(reader);
            String timeSeries = VarInts.readString(reader);
            String recordType = VarInts.readString(reader);
            
            List<String> names = SerializationUtils.parseStringListFrom(reader);
            List<String> values = SerializationUtils.parseStringListFrom(reader);
            
            return new InsertPayload(database, timeSeries, recordType, names, values);
        }
    };
    
    /**
     * The name of the database in which the data must be inserted.
     */
    private final String database; 
    
    /**
     * The time series in which the data must be inserted.
     */
    private final String series;
    
    /**
     * The type of the record that must be inserted.
     */
    private final String recordType;
    
    /**
     * The name of the fields in which some data must be inserted.
     */
    private final List<String> fieldNames;
    
    /**
     * The field values.
     */
    private final List<String> fieldValues;
           
    /**
     * Creates a new <code>InsertPayload</code>. 
     * 
     * @param database the database in which the data must be inserted
     * @param series the time series in which the data must be inserted
     * @param recordType the type of the record that must be inserted
     * @param fieldNames the field name
     * @param fieldValues the field values
     */
    public InsertPayload(String database,
                         String series,
                         String recordType,
                         List<String> fieldNames,
                         List<String> fieldValues) {
        
        this.database = database;
        this.series = series;
        this.recordType = recordType;
        this.fieldNames = fieldNames;
        this.fieldValues = fieldValues;
    }

    /**
     * Returns the name of the database in which the data must be inserted.   
     * 
     * @return the name of the database in which the data must be inserted.  
     */
    public String getDatabase() {
        return this.database;
    }
    
    /**
     * Returns the name of the time series in which the data must be inserted.   
     * 
     * @return the name of the time series in which the data must be inserted.  
     */
    public String getSeries() {
        return this.series;
    }

    /**
     * Returns the type of the record that must be inserted.
     * 
     * @return the type of the record that must be inserted.
     */
    public String getRecordType() {
        return this.recordType;
    }

    /**
     * Returns the name of the fields in which some data must be inserted.
     * 
     * @return the name of the fields in which some data must be inserted.
     */
    public List<String> getFieldNames() {
        return this.fieldNames;
    }

    /**
     * Returns the field values
     * 
     * @return the field values
     */
    public List<String> getFieldValues() {
        return this.fieldValues;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {
        
        return VarInts.computeStringSize(this.database) 
                + VarInts.computeStringSize(this.series) 
                + VarInts.computeStringSize(this.recordType)
                + SerializationUtils.computeStringListSerializedSize(this.fieldNames)
                + SerializationUtils.computeStringListSerializedSize(this.fieldValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {

        VarInts.writeString(writer, this.database);
        VarInts.writeString(writer, this.series);
        VarInts.writeString(writer, this.recordType);
        SerializationUtils.writeStringList(writer, this.fieldNames);
        SerializationUtils.writeStringList(writer, this.fieldValues);
    }
    
    /**
     * Creates a new <code>InsertPayload</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static InsertPayload parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>InsertPayload</code> instances.
     * @return the parser that can be used to deserialize <code>InsertPayload</code> instances.
     */
    public static Parser<InsertPayload> getParser() {

        return PARSER;
    }
}
