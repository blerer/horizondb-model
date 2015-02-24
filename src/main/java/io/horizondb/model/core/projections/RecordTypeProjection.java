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

import io.horizondb.io.serialization.Serializable;
import io.horizondb.model.schema.RecordTypeDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

/**
 * Defines a projection for a given record type. 
 *
 */
public interface RecordTypeProjection extends Serializable {

    /**
     * Returns the type of <code>RecordTypeProjection</code>.
     * @return the type of <code>RecordTypeProjection</code>.
     */
    int getType();

    /**
     * Returns the record type index
     * @return the record type index
     */
    int getRecordType();

    /**
     * Returns the <code>RecordTypeDefinition</code> corresponding to the result of this projection.
     *
     * @param timeSeriesDefinition the time series definition
     * @return the <code>RecordTypeDefinition</code> corresponding to the result of this projection.
     */
    RecordTypeDefinition getRecordTypeDefinition(TimeSeriesDefinition timeSeriesDefinition);

    /**
     * Returns the mapping between the original fields and the new ones.
     *
     * @param timeSeriesDefinition the time series definition
     * @return the mapping between the original fields and the new ones.
     */
    int[] getFieldMapping(TimeSeriesDefinition timeSeriesDefinition);
    
    /**
     * {@inheritDoc}
     */
    @Override
    int computeSerializedSize();

}