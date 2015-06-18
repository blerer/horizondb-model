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

import io.horizondb.io.ReadableBuffer;
import io.horizondb.io.serialization.Serializable;

/**
 * A block of data (e.g. a set of records in binary format).
 *
 */
public interface DataBlock extends Serializable {

    /**
     * Returns the block header.
     * @return the block header
     */
    Record getHeader();

    /**
     * Returns the data in a binary format.
     * @return the data in a binary format
     */
    ReadableBuffer getData();
}
