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
package io.horizondb.model.core.blocks;

/**
 * A <code>RuntimeException</code> thrown when trying to add data
 * to a block which is already full.
 */
public final class BlockOverflowException extends RuntimeException {

    /**
     * Serial Version UID
     */
    private static final long serialVersionUID = 8170124861535540348L;

    /**
     * Create a new <code>BlockOverflowException</code> with the specified error message.
     * @param message the error message
     */
    public BlockOverflowException(String message) {
        super(message);
    }
}
