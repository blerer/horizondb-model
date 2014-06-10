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

import java.util.Calendar;

import org.junit.Test;

import static io.horizondb.model.core.util.TimeUtils.EUROPE_BERLIN_TIMEZONE;
import static io.horizondb.model.core.util.TimeUtils.parseDateTime;
import static org.junit.Assert.assertEquals;

/**
 * @author Benjamin
 *
 */
public class TimeUtilsTest {

    @Test
    public void testParseDateTime() {
        
        long time = TimeUtils.parseDateTime(EUROPE_BERLIN_TIMEZONE, "2014-05-03");
        assertEquals(1399068000000L, time);
        
        time = TimeUtils.parseDateTime(EUROPE_BERLIN_TIMEZONE, "2014-05-03 22:11:34");
        assertEquals(1399147894000L, time);
        
        time = TimeUtils.parseDateTime(EUROPE_BERLIN_TIMEZONE, "2014-05-03 22:11:34.150");
        assertEquals(1399147894150L, time);
    }

    @Test
    public void testTruncateMilliseconds() {

        long time = parseDateTime(EUROPE_BERLIN_TIMEZONE, "2013-11-16 09:12:35.670");

        Calendar calendar = toCalendar(time);
        TimeUtils.truncate(calendar, Calendar.MILLISECOND);
        
        assertEquals(parseDateTime(EUROPE_BERLIN_TIMEZONE, "2013-11-16 09:12:35.670"), calendar.getTimeInMillis());
    }
    
    @Test
    public void testTruncateSeconds() {

        long time = parseDateTime(EUROPE_BERLIN_TIMEZONE, "2013-11-16 09:12:35.670");

        Calendar calendar = toCalendar(time);
        TimeUtils.truncate(calendar, Calendar.SECOND);
        
        assertEquals(parseDateTime(EUROPE_BERLIN_TIMEZONE, "2013-11-16 09:12:35.000"), calendar.getTimeInMillis());
    }
    
    @Test
    public void testTruncateMinutes() {

        long time = parseDateTime(EUROPE_BERLIN_TIMEZONE, "2013-11-16 09:12:35.670");

        Calendar calendar = toCalendar(time);
        TimeUtils.truncate(calendar, Calendar.MINUTE);
        
        assertEquals(parseDateTime(EUROPE_BERLIN_TIMEZONE, "2013-11-16 09:12:00.000"), calendar.getTimeInMillis());
    }
    
    @Test
    public void testTruncateHours() {

        long time = parseDateTime(EUROPE_BERLIN_TIMEZONE, "2013-11-16 09:12:35.670");

        Calendar calendar = toCalendar(time);
        TimeUtils.truncate(calendar, Calendar.HOUR_OF_DAY);
        
        assertEquals(parseDateTime(EUROPE_BERLIN_TIMEZONE, "2013-11-16 09:00:00.000"), calendar.getTimeInMillis());
    }
    
    @Test
    public void testTruncateDay() {

        long time = parseDateTime(EUROPE_BERLIN_TIMEZONE, "2013-11-16 09:12:35.670");

        Calendar calendar = toCalendar(time);
        TimeUtils.truncate(calendar, Calendar.DAY_OF_MONTH);
        
        assertEquals(parseDateTime(EUROPE_BERLIN_TIMEZONE, "2013-11-16 00:00:00.000"), calendar.getTimeInMillis());
    }
    
    @Test
    public void testTruncateMonth() {

        long time = parseDateTime(EUROPE_BERLIN_TIMEZONE, "2013-11-16 09:12:35.670");

        Calendar calendar = toCalendar(time);
        TimeUtils.truncate(calendar, Calendar.MONTH);
        
        assertEquals(parseDateTime(EUROPE_BERLIN_TIMEZONE, "2013-11-01 00:00:00.000"), calendar.getTimeInMillis());
    }
    
    @Test
    public void testTruncateYear() {

        long time = parseDateTime(EUROPE_BERLIN_TIMEZONE, "2013-11-16 09:12:35.670");

        Calendar calendar = toCalendar(time);
        TimeUtils.truncate(calendar, Calendar.YEAR);
        
        assertEquals(parseDateTime(EUROPE_BERLIN_TIMEZONE, "2013-01-01 00:00:00.000"), calendar.getTimeInMillis());
    }
    
    private static Calendar toCalendar(long timeInMilliseconds) {

        Calendar calendar = Calendar.getInstance(EUROPE_BERLIN_TIMEZONE);
        calendar.setTimeInMillis(timeInMilliseconds);
        return calendar;
    }
}
