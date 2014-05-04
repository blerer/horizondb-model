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
package io.horizondb.model.core.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Benjamin
 *
 */
public class TimeUtilsTest {

    @Test
    public void testParseDateTime() {
        
        long time = TimeUtils.parseDateTime("2014-05-03");
        assertEquals(1399068000000L, time);
        
        time = TimeUtils.parseDateTime("2014-05-03 22:11:34");
        assertEquals(1399147894000L, time);
        
        time = TimeUtils.parseDateTime("2014-05-03 22:11:34.150");
        assertEquals(1399147894150L, time);
    }

}
