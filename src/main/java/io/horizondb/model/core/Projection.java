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
package io.horizondb.model.core;

import io.horizondb.io.serialization.Serializable;
import io.horizondb.model.schema.RecordSetDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

/**
 *
 */
public interface Projection extends Serializable {

    /**
     * Returns the type of this projection
     * @return  the type of this projection
     */
    int getType();
    
    /**
     * Returns the filter used to filter the record types.
     * 
     * @param timeSeriesDefinition the time series definition
     * @return the filter used to filter the record types.
     */
    Filter<String> getRecordTypeFilter(TimeSeriesDefinition timeSeriesDefinition);

    /**
     * Returns the <code>RecordSetDefinition</code> associated to the results of this projection.
     *
     * @param timeSeriesDefinition the time series definition
     * @return the <code>RecordSetDefinition</code> associated to the results of this projection
     */
    RecordSetDefinition getDefinition(TimeSeriesDefinition timeSeriesDefinition);

    /**
     * Filers the field of the records returned by the specified iterator.
     *
     * @param timeSeriesDefinition the time series definition
     * @param iterator the record iterator
     * @return an iterator which will filter out the unwanted fields 
     */
    ResourceIterator<? extends Record> filterFields(TimeSeriesDefinition timeSeriesDefinition,
                                                    ResourceIterator<? extends Record> iterator);
    
    /**
     * {@inheritDoc}
     */
    @Override
    int computeSerializedSize();
}