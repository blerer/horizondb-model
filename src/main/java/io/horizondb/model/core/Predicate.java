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
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.util.TimeZone;

import com.google.common.collect.RangeSet;

/***
 * Represents a predicate within an HQL statement. A predicate is a condition that can be 
 * evaluated to true/false.
 * 
 * @author Benjamin
 *
 */
public interface Predicate extends Serializable {

    /**
     * Returns the byte representing the type of this predicate.  
     * 
     * @return the byte representing the type of this predicate
     */
    int getType(); 
    
    /**
     * Returns the timestamp range corresponding to this predicate.
     * 
     * @param prototype the timestamp field used as prototype
     * @param timeZone the time series time zone
     * @return the timestamp ranges accepted by this predicate.
     */
    RangeSet<Field> getTimestampRanges(Field prototype, TimeZone timeZone);    
    
    /**
     * Converts this <code>Predicate</code> into a record filter.
     * 
     * @param definition the definition of the time series on which the filter will be applied.
     * @return a record filter corresponding to this predicate.
     */
    Filter<Record> toFilter(TimeSeriesDefinition definition);
}
