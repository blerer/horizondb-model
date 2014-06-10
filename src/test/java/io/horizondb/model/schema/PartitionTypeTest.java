/**
 * Copyright 2013 Benjamin Lerer
 * 
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
package io.horizondb.model.schema;

import io.horizondb.model.core.Field;

import java.util.Calendar;

import org.junit.Test;

import com.google.common.collect.Range;

import static io.horizondb.model.core.util.TimeUtils.EUROPE_BERLIN_TIMEZONE;
import static io.horizondb.model.core.util.TimeUtils.parseDateTime;
import static io.horizondb.model.schema.FieldType.MILLISECONDS_TIMESTAMP;
import static org.junit.Assert.assertEquals;

/**
 * @author Benjamin
 * 
 */
public class PartitionTypeTest {

    @Test
    public void testGetPartitionTimeRangeByDay() {

        long time = parseDateTime(EUROPE_BERLIN_TIMEZONE, "2013-11-16 09:12:35.670");

        Calendar calendar = toCalendar(time);

        Range<Field> range = PartitionType.BY_DAY.getPartitionTimeRange(calendar);

        Range<Field> expected = MILLISECONDS_TIMESTAMP.range(EUROPE_BERLIN_TIMEZONE, "'2013-11-16'", "'2013-11-17'");

        assertEquals(expected, range);
    }
    
    @Test
    public void testGetPartitionTimeRangeByDayWithStartTime() {

        long time = parseDateTime(EUROPE_BERLIN_TIMEZONE, "2013-11-16");

        Calendar calendar = toCalendar(time);

        Range<Field> range = PartitionType.BY_DAY.getPartitionTimeRange(calendar);

        Range<Field> expected = MILLISECONDS_TIMESTAMP.range(EUROPE_BERLIN_TIMEZONE, "'2013-11-16'", "'2013-11-17'");

        assertEquals(expected, range);
    }

    @Test
    public void testGetPartitionTimeRangeByMonthWithStartTime() {

        long time = parseDateTime(EUROPE_BERLIN_TIMEZONE, "2013-11-01");

        Calendar calendar = toCalendar(time);

        Range<Field> range = PartitionType.BY_MONTH.getPartitionTimeRange(calendar);

        Range<Field> expected = MILLISECONDS_TIMESTAMP.range(EUROPE_BERLIN_TIMEZONE, "'2013-11-01'", "'2013-12-01'");

        assertEquals(expected, range);
    }

    @Test
    public void testGetPartitionTimeRangeByMonth() {

        long time = parseDateTime(EUROPE_BERLIN_TIMEZONE, "2013-11-16 09:12:35.670");

        Calendar calendar = toCalendar(time);

        Range<Field> range = PartitionType.BY_MONTH.getPartitionTimeRange(calendar);

        Range<Field> expected = MILLISECONDS_TIMESTAMP.range(EUROPE_BERLIN_TIMEZONE, "'2013-11-01'", "'2013-12-01'");

        assertEquals(expected, range);
    }

    @Test
    public void testGetPartitionTimeRangeByWeekWithStartTime() {

        long time = parseDateTime(EUROPE_BERLIN_TIMEZONE, "2013-11-11");

        Calendar calendar = toCalendar(time);

        Range<Field> range = PartitionType.BY_WEEK.getPartitionTimeRange(calendar);

        Range<Field> expected = MILLISECONDS_TIMESTAMP.range(EUROPE_BERLIN_TIMEZONE, "'2013-11-11'", "'2013-11-18'");

        assertEquals(expected, range);
    }

    @Test
    public void testGetPartitionTimeRangeByWeek() {

        long time = parseDateTime(EUROPE_BERLIN_TIMEZONE, "2013-11-16 09:12:35.670");

        Calendar calendar = toCalendar(time);

        Range<Field> range = PartitionType.BY_WEEK.getPartitionTimeRange(calendar);

        Range<Field> expected = MILLISECONDS_TIMESTAMP.range(EUROPE_BERLIN_TIMEZONE, "'2013-11-11'", "'2013-11-18'");

        assertEquals(expected, range);
    }

    @Test
    public void testGetPartitionTimeRangeWithSunday() {

        long time = parseDateTime(EUROPE_BERLIN_TIMEZONE, "2013-11-17 09:12:35.670");

        Calendar calendar = toCalendar(time);

        Range<Field> range = PartitionType.BY_WEEK.getPartitionTimeRange(calendar);

        Range<Field> expected = MILLISECONDS_TIMESTAMP.range(EUROPE_BERLIN_TIMEZONE, "'2013-11-11'", "'2013-11-18'");

        assertEquals(expected, range);
    }

    @Test
    public void testGetPartitionTimeRangeWithMonday() {

        long time = parseDateTime(EUROPE_BERLIN_TIMEZONE, "2013-11-11");

        Calendar calendar = toCalendar(time);

        Range<Field> range = PartitionType.BY_WEEK.getPartitionTimeRange(calendar);

        Range<Field> expected = MILLISECONDS_TIMESTAMP.range(EUROPE_BERLIN_TIMEZONE, "'2013-11-11'", "'2013-11-18'");

        assertEquals(expected, range);
    }

    private static Calendar toCalendar(long timeInMilliseconds) {

        Calendar calendar = Calendar.getInstance(EUROPE_BERLIN_TIMEZONE);
        calendar.setTimeInMillis(timeInMilliseconds);
        return calendar;
    }
}
