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
package io.horizondb.model.core.predicates;

import io.horizondb.model.core.Field;
import io.horizondb.model.schema.FieldType;

import static io.horizondb.model.core.util.TimeUtils.EUROPE_BERLIN_TIMEZONE;

/**
 * Factory methods for fields.
 */
public final class FieldUtils {

    public static final Field toIntField(String s) {

        return FieldType.INTEGER.newField().setValueFromString(EUROPE_BERLIN_TIMEZONE, s);
    }

    public static final Field toMillisecondField(String s) {

        return FieldType.MILLISECONDS_TIMESTAMP.newField().setValueFromString(EUROPE_BERLIN_TIMEZONE, s);
    }

    private FieldUtils() {
    }
}
