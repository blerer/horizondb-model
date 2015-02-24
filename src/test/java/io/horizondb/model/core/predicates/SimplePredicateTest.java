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
import io.horizondb.model.core.Predicate;
import io.horizondb.model.core.fields.TimestampField;
import io.horizondb.model.schema.FieldType;

import org.junit.Test;

import com.google.common.collect.RangeSet;

import static io.horizondb.model.core.predicates.FieldUtils.toIntField;
import static io.horizondb.model.core.predicates.FieldUtils.toMillisecondField;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SimplePredicateTest {

    @Test
    public void testGetTimestampRangesWithNonTimestampField() {
        
        Predicate predicate = Predicates.gt("price", toIntField("10")); 
        
        RangeSet<Field> rangeSet = predicate.getTimestampRanges();
        assertEquals(TimestampField.ALL, rangeSet);
    }
    
    @Test
    public void testGetTimestampRangesWithGT() {
        
        long timeInMillis = 1399147894150L;
        
        Predicate predicate = Predicates.gt("timestamp", toMillisecondField(timeInMillis + "ms")); 
        
        RangeSet<Field> rangeSet = predicate.getTimestampRanges();
        
        Field expected = FieldType.MILLISECONDS_TIMESTAMP.newField();
        expected.setTimestampInMillis(timeInMillis + 10);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis - 10);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(TimestampField.MAX_VALUE.getTimestampInMillis());
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(TimestampField.MIN_VALUE.getTimestampInMillis());
        
        assertFalse(rangeSet.contains(expected));
    }

    @Test
    public void testGetTimestampRangesWithGE() {
        
        long timeInMillis = 1399147894150L;
        
        Predicate predicate = Predicates.ge("timestamp", toMillisecondField(timeInMillis + "ms")); 
        
        RangeSet<Field> rangeSet = predicate.getTimestampRanges();
        
        Field expected = FieldType.MILLISECONDS_TIMESTAMP.newField();
        expected.setTimestampInMillis(timeInMillis + 10);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis - 10);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(TimestampField.MAX_VALUE.getTimestampInMillis());
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(TimestampField.MIN_VALUE.getTimestampInMillis());
        
        assertFalse(rangeSet.contains(expected));
    }
    
    @Test
    public void testGetTimestampRangesWithLE() {
        
        long timeInMillis = 1399147894150L;
        
        Predicate predicate = Predicates.le("timestamp", toMillisecondField(timeInMillis + "ms")); 
        
        RangeSet<Field> rangeSet = predicate.getTimestampRanges();
        
        Field expected = FieldType.MILLISECONDS_TIMESTAMP.newField();
        expected.setTimestampInMillis(timeInMillis + 10);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis - 10);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(Long.MAX_VALUE);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(0);
        
        assertTrue(rangeSet.contains(expected));
    }
    
    @Test
    public void testGetTimestampRangesWithLT() {
        
        long timeInMillis = 1399147894150L;
        
        Predicate predicate = Predicates.lt("timestamp", toMillisecondField(timeInMillis + "ms")); 
        
        RangeSet<Field> rangeSet = predicate.getTimestampRanges();
        
        Field expected = FieldType.MILLISECONDS_TIMESTAMP.newField();
        expected.setTimestampInMillis(timeInMillis + 10);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis - 10);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(Long.MAX_VALUE);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(0);
        
        assertTrue(rangeSet.contains(expected));
    }
    
    @Test
    public void testGetTimestampRangesWithEQ() {
        
        long timeInMillis = 1399147894150L;
        
        Predicate predicate = Predicates.eq("timestamp", toMillisecondField(timeInMillis + "ms")); 
        
        RangeSet<Field> rangeSet = predicate.getTimestampRanges();
        
        Field expected = FieldType.MILLISECONDS_TIMESTAMP.newField();
        expected.setTimestampInMillis(timeInMillis + 10);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis - 10);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(Long.MAX_VALUE);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(0);
        
        assertFalse(rangeSet.contains(expected));
    }
    
    @Test
    public void testGetTimestampRangesWithNE() {
        
        long timeInMillis = 1399147894150L;
        
        Predicate predicate = Predicates.ne("timestamp", toMillisecondField(timeInMillis + "ms")); 
        
        RangeSet<Field> rangeSet = predicate.getTimestampRanges();
        
        Field expected = FieldType.MILLISECONDS_TIMESTAMP.newField();
        expected.setTimestampInMillis(timeInMillis + 10);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis - 10);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(TimestampField.MAX_VALUE.getTimestampInMillis());
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(0);
        
        assertTrue(rangeSet.contains(expected));
    }
}
