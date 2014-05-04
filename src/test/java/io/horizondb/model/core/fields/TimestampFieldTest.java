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
package io.horizondb.model.core.fields;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Benjamin
 *
 */
public class TimestampFieldTest {

    @Test
    public void testConversion() {
        
        TimestampField field = new TimestampField(TimeUnit.NANOSECONDS);
        field.setTimestampInNanos(2000000000);
        
        assertEquals(2000000000, field.getTimestampInNanos());
        assertEquals(2000, field.getTimestampInMillis());
        
        field.setTimestampInMillis(1000);
        
        assertEquals(1000, field.getTimestampInMillis());
    }

    @Test
    public void testCompareTo() {
        
        TimestampField firstField = new TimestampField(TimeUnit.NANOSECONDS);
        firstField.setTimestampInMillis(1000);
        
        TimestampField secondField = new TimestampField(TimeUnit.NANOSECONDS);
        secondField.setTimestampInNanos(2000000000);

        TimestampField thirdField = new TimestampField(TimeUnit.NANOSECONDS);
        thirdField.setTimestampInSeconds(3);
        
        List<TimestampField> fields = Arrays.asList(secondField, thirdField, firstField);
        Collections.sort(fields);
        
        assertEquals(1000000000L, fields.get(0).getTimestampInNanos());  
        assertEquals(2000000000L, fields.get(1).getTimestampInNanos());
        assertEquals(3000000000L, fields.get(2).getTimestampInNanos());
    }
    
    @Test
    public void testSetValueFromString() {
        
        TimestampField field = new TimestampField(TimeUnit.NANOSECONDS);
        
        field.setValueFromString("2000000000ns");
        assertEquals(2000000000L, field.getTimestampInNanos());
        
        field.setValueFromString("3000000µs");
        assertEquals(3000000000L, field.getTimestampInNanos());
        
        field.setValueFromString("4000ms");
        assertEquals(4000000000L, field.getTimestampInNanos());
        
        field.setValueFromString("5s");
        assertEquals(5000000000L, field.getTimestampInNanos());
        
        field.setValueFromString("6000000000");
        assertEquals(6000000000L, field.getTimestampInNanos());
    }
}
