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

import static io.horizondb.model.core.util.TimeUtils.EUROPE_BERLIN_TIMEZONE;
import static org.junit.Assert.assertEquals;

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
        
        field.setValueFromString(EUROPE_BERLIN_TIMEZONE, "2000000000ns");
        assertEquals(2000000000L, field.getTimestampInNanos());
        
        field.setValueFromString(EUROPE_BERLIN_TIMEZONE, "3000000µs");
        assertEquals(3000000000L, field.getTimestampInNanos());
        
        field.setValueFromString(EUROPE_BERLIN_TIMEZONE, "4000ms");
        assertEquals(4000000000L, field.getTimestampInNanos());
        
        field.setValueFromString(EUROPE_BERLIN_TIMEZONE, "5s");
        assertEquals(5000000000L, field.getTimestampInNanos());
        
        field.setValueFromString(EUROPE_BERLIN_TIMEZONE, "6000000000");
        assertEquals(6000000000L, field.getTimestampInNanos());
    }
    
    @Test
    public void testSetValueFromStringWithDateTime() {

        TimestampField field = new TimestampField(TimeUnit.NANOSECONDS);
        
        field.setValueFromString(EUROPE_BERLIN_TIMEZONE, "'2014-05-03'");
        assertEquals(1399068000000L, field.getTimestampInMillis());
        
        field.setValueFromString(EUROPE_BERLIN_TIMEZONE, "'2014-05-03 22:11:34'");
        assertEquals(1399147894000L, field.getTimestampInMillis());
        
        field.setValueFromString(EUROPE_BERLIN_TIMEZONE, "'2014-05-03 22:11:34.150'");
        assertEquals(1399147894150L, field.getTimestampInMillis());
    }
    
    @Test
    public void testSetValueFromStringWithTime() {

        long timeInMillis = 1399147894150L;

        TimestampField field = new TimestampField(TimeUnit.MILLISECONDS);
        
        field.setValueFromString(EUROPE_BERLIN_TIMEZONE, timeInMillis + "ms");
        assertEquals(timeInMillis, field.getTimestampInMillis());
    }
        
    @Test
    public void testCopyTo() {
        
        long timeInMillis = 1399147894150L;

        TimestampField field = new TimestampField(TimeUnit.MILLISECONDS);
        field.setTimestampInMillis(timeInMillis);
        
        assertEquals(1399147894150L, field.getTimestampInMillis());
        
        long timeInMillis2 = 1399147894180L;
        
        TimestampField other = new TimestampField(TimeUnit.MILLISECONDS);
        other.setTimestampInMillis(timeInMillis2);
        
        field.copyTo(other);
        
        assertEquals(1399147894150L, other.getTimestampInMillis());
    }
    
    @Test
    public void testCopyToWithDifferentUnits() {
        
        long timeInMillis = 1399147894150L;

        TimestampField field = new TimestampField(TimeUnit.MILLISECONDS);
        field.setTimestampInMillis(timeInMillis);
        
        assertEquals(1399147894150L, field.getTimestampInMillis());
        
        long timeInNanos = 1399147894180000000L;
        
        TimestampField other = new TimestampField(TimeUnit.NANOSECONDS);
        other.setTimestampInNanos(timeInNanos);
        
        field.copyTo(other);
        
        assertEquals(1399147894150000000L, other.getTimestampInNanos());
    }
    
    @Test
    public void testAdd() {
        
        long timeInMillis = 1399147894150L;

        TimestampField field = new TimestampField(TimeUnit.MILLISECONDS);
        field.setTimestampInMillis(timeInMillis);
        
        assertEquals(1399147894150L, field.getTimestampInMillis());
        
        long timeInMillis2 = 15;
        
        TimestampField other = new TimestampField(TimeUnit.MILLISECONDS);
        other.setTimestampInMillis(timeInMillis2);
        
        field.add(other);
        
        assertEquals(1399147894165L, field.getTimestampInMillis());
    }
    
    @Test
    public void testAddWithDifferentUnits() {

        long timeInNanos = 1399147894150000000L;

        TimestampField field = new TimestampField(TimeUnit.NANOSECONDS);
        field.setTimestampInNanos(timeInNanos);

        assertEquals(1399147894150L, field.getTimestampInMillis());

        long timeInMillis = 15;

        TimestampField other = new TimestampField(TimeUnit.MILLISECONDS);
        other.setTimestampInMillis(timeInMillis);

        field.add(other);

        assertEquals(1399147894165000000L, field.getTimestampInNanos());
    }
    
    @Test
    public void testSubstract() {
        
        long timeInMillis = 1399147894150L;

        TimestampField field = new TimestampField(TimeUnit.MILLISECONDS);
        field.setTimestampInMillis(timeInMillis);
        
        assertEquals(1399147894150L, field.getTimestampInMillis());
        
        long timeInMillis2 = 50;
        
        TimestampField other = new TimestampField(TimeUnit.MILLISECONDS);
        other.setTimestampInMillis(timeInMillis2);
        
        field.subtract(other);
        
        assertEquals(1399147894100L, field.getTimestampInMillis());
    }
    
    @Test
    public void testSubstractWithDifferentUnits() {

        long timeInNanos = 1399147894150000000L;

        TimestampField field = new TimestampField(TimeUnit.NANOSECONDS);
        field.setTimestampInNanos(timeInNanos);

        assertEquals(1399147894150L, field.getTimestampInMillis());

        long timeInMillis = 50;

        TimestampField other = new TimestampField(TimeUnit.MILLISECONDS);
        other.setTimestampInMillis(timeInMillis);

        field.subtract(other);

        assertEquals(1399147894100000000L, field.getTimestampInNanos());
    }
}
