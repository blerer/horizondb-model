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

import io.horizondb.io.serialization.Serializable;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Filter;
import io.horizondb.model.core.records.BinaryTimeSeriesRecord;
import io.horizondb.model.core.records.TimeSeriesRecord;

import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * @author Benjamin
 *
 */
public interface RecordSetDefinition extends Iterable<RecordTypeDefinition>, Serializable {

    /**
     * Returns binary records instances corresponding to this time series records.
     * 
     * @return binary records instances corresponding to this time series records.
     */
    BinaryTimeSeriesRecord[] newBinaryRecords();

    /**
     * Returns binary records instances corresponding to this time series records.
     * 
     * @param the filter used filter records base on names.
     * @return binary records instances corresponding to this time series records.
     */
    BinaryTimeSeriesRecord[] newBinaryRecords(Filter<String> filter);

    /**
     * Returns records instances corresponding to this time series records.
     * 
     * @return records instances corresponding to this time series records.
     */
    TimeSeriesRecord[] newRecords();

    /**
     * Returns a new record instances of the specified type.
     * 
     * @param type the name of the record type.
     * @return a new record instances of the specified type.
     */
    TimeSeriesRecord newRecord(String name);

    /**
     * Returns a new record instances of the specified type.
     * 
     * @param type the name of the record type.
     * @return a new record instances of the specified type.
     */
    TimeSeriesRecord newRecord(int index);

    /**
     * Returns new field instance for the specified fieldName. 
     * 
     * @param fieldName the field name
     * @return a new field instance for the specified fieldName. 
     */
    Field newField(String fieldName);

    /**
     * Returns a new field instance of the specified record type. 
     * 
     * @param recordTypeIndex the index of the record type
     * @param fieldIndex the field index
     * @return a new field instance of the specified record type
     */
    Field newField(int recordTypeIndex, int fieldIndex);

    /**
     * Returns the index of the specified record type.
     * 
     * @param type the record type.
     * @return the index of the specified record type.
     */
    int getRecordTypeIndex(String type);

    /**
     * Returns the index of the field belonging to the specified record type with the specified name.  
     * 
     * @param type the record type index
     * @param name the field name
     */
    int getFieldIndex(int type, String name);

    /**
     * Returns the time unit of the time series timestamps.
     * 
     * @return the time unit of the time series timestamps.
     */
    TimeUnit getTimeUnit();

    /**
     * Returns the timezone of the time series.
     * 
     * @return the timezone of the time series.
     */
    TimeZone getTimeZone();

    /**
     * Returns the number of record types.
     * 
     * @return the number of record types.
     */
    int getNumberOfRecordTypes();

    /**
     * Returns the name of the record type with the specified index.
     * 
     * @param index the record type index
     * @return the name of the record type with the specified index
     */
    String getRecordName(int index);

    /**
     * Returns the name of the specified field of the specified record type.
     * 
     * @param recordTypeIndex the index of the record type
     * @param fieldIndex the index of the field
     * @return the name of the specified field of the specified record type
     */
    String getFieldName(int recordTypeIndex, int fieldIndex);

    /**
     * Returns the definition of the record type with the specified index.
     * 
     * @param index the record type index
     * @return the definition of the record type with the specified index.
     */
    RecordTypeDefinition getRecordType(int index);
}