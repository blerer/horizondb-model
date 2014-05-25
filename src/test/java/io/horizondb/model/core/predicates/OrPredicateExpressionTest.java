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

import java.util.Arrays;

import io.horizondb.model.core.Predicate;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.predicates.Predicates;
import io.horizondb.model.schema.FieldType;

import org.junit.Test;

import com.google.common.collect.RangeSet;

import static io.horizondb.model.core.util.TimeUtils.EUROPE_BERLIN_TIMEZONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Benjamin
 *
 */
public class OrPredicateExpressionTest {

    @Test
    public void testGetTimestampRangesWithNonTimestampField() {
        
        Predicate left = Predicates.gt("price", "10"); 
        Predicate right = Predicates.lt("price", "12"); 
        Field prototype = FieldType.MILLISECONDS_TIMESTAMP.newField();
        
        Predicate predicate = Predicates.or(left, right);
        
        RangeSet<Field> rangeSet = predicate.getTimestampRanges(prototype, EUROPE_BERLIN_TIMEZONE);
        assertEquals(prototype.allValues(), rangeSet);
    }
    
    @Test
    public void testGetTimestampRangesWithGTAndNonTimestampField() {
        
        long timeInMillis = 1399147894150L;
        
        Predicate left = Predicates.gt("timestamp", timeInMillis + "ms"); 
        Predicate right = Predicates.lt("price", "12"); 
        
        Predicate predicate = Predicates.or(left, right); 
        Field prototype = FieldType.MILLISECONDS_TIMESTAMP.newField();
        
        RangeSet<Field> rangeSet = predicate.getTimestampRanges(prototype, EUROPE_BERLIN_TIMEZONE);
        
        assertEquals(prototype.allValues(), rangeSet);
    }
    
    @Test
    public void testGetTimestampRangesWithGTAndLT() {
        
        long timeInMillis = 1399147894150L;
        
        Predicate left = Predicates.gt("timestamp", (timeInMillis + 100) + "ms"); 
        Predicate rigth = Predicates.lt("timestamp", timeInMillis + "ms"); 
        
        Predicate predicate = Predicates.or(left, rigth); 
        Field prototype = FieldType.MILLISECONDS_TIMESTAMP.newField();
        
        RangeSet<Field> rangeSet = predicate.getTimestampRanges(prototype, EUROPE_BERLIN_TIMEZONE);
        
        Field expected = FieldType.MILLISECONDS_TIMESTAMP.newField();
        expected.setTimestampInMillis(timeInMillis + 10);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis - 10);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis + 200);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(Long.MAX_VALUE);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(Long.MIN_VALUE);
        
        assertTrue(rangeSet.contains(expected));
    }
    
    @Test
    public void testGetTimestampRangesWithOpposedGTAndLT() {
        
        long timeInMillis = 1399147894150L;
        
        Predicate left = Predicates.lt("timestamp", (timeInMillis + 100) + "ms"); 
        Predicate rigth = Predicates.gt("timestamp", timeInMillis + "ms"); 
        
        Predicate predicate = Predicates.or(left, rigth); 
        Field prototype = FieldType.MILLISECONDS_TIMESTAMP.newField();
        
        RangeSet<Field> rangeSet = predicate.getTimestampRanges(prototype, EUROPE_BERLIN_TIMEZONE);
        
        assertEquals(prototype.allValues(), rangeSet);
    }
        
    @Test
    public void testGetTimestampRangesWithInAndBetween() {
        
        long timeInMillis = 1399147894150L;
        
        Predicate left = Predicates.in("timestamp", Arrays.asList((timeInMillis - 10) + "ms",
                                                                     timeInMillis + "ms",
                                                                     (timeInMillis + 10) + "ms")); 
        
        Predicate rigth = Predicates.between("timestamp", (timeInMillis - 20) + "ms", (timeInMillis + 20) + "ms");
        
        Predicate predicate = Predicates.or(left, rigth);
        
        Field prototype = FieldType.MILLISECONDS_TIMESTAMP.newField();
        
        RangeSet<Field> rangeSet = predicate.getTimestampRanges(prototype, EUROPE_BERLIN_TIMEZONE);
        
        Field expected = FieldType.MILLISECONDS_TIMESTAMP.newField();
        
        expected.setTimestampInMillis(timeInMillis + 10);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis + 20);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis + 25);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis + 15);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis - 10);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis - 15);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis - 20);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis - 25);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(Long.MAX_VALUE);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(Long.MIN_VALUE);
        
        assertFalse(rangeSet.contains(expected));
    }
}
