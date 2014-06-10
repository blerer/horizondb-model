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
package io.horizondb.model.core.util;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.Validate;

/**
 * Utility methods to work with date/time.
 * 
 * @author Benjamin
 */
public final class TimeUtils {

    /**
     * Europe/Berlin time zone.
     */
    public static final TimeZone EUROPE_BERLIN_TIMEZONE = TimeZone.getTimeZone("Europe/Berlin");
    
    private static final int[] TRUNCATING_FIELDS = new int[] {Calendar.MILLISECOND, 
                                                              Calendar.SECOND, 
                                                              Calendar.MINUTE,
                                                              Calendar.HOUR_OF_DAY, 
                                                              Calendar.DAY_OF_MONTH, 
                                                              Calendar.MONTH,
                                                              Calendar.YEAR};

    /**
     * Parses the specified <code>String</code> representing a date/time.
     * <p>The supported patterns are: 'yyyy-MM-dd', 'yyyy-MM-dd HH:mm:ss' and 
     * 'yyyy-MM-dd HH:mm:ss.SSS'.</p>
     * 
     * @param dateTime the <code>String</code> representing a date/time.
     * @return the time in millisecond since epoch
     */
    public static long parseDateTime(String dateTime) {
        
        return parseDateTime(TimeZone.getDefault(), dateTime);
    }
    
    /**
     * Parses the specified <code>String</code> representing a date/time.
     * <p>The supported patterns are: 'yyyy-MM-dd', 'yyyy-MM-dd HH:mm:ss' and 
     * 'yyyy-MM-dd HH:mm:ss.SSS'.</p>
     * 
     * @param timeZone the date TimeZone
     * @param dateTime the <code>String</code> representing a date/time.
     * @return the time in millisecond since epoch
     */
    public static long parseDateTime(TimeZone timeZone, String dateTime) {
        
        String pattern = getPattern(dateTime);
        
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        
        Calendar calendar = Calendar.getInstance(timeZone);
        calendar.clear();
        
        format.setCalendar(calendar);
        
        return format.parse(dateTime, new ParsePosition(0)).getTime();
    }
    
    /**
     * Returns the pattern to use to parse the specified dateTime.
     * 
     * @param dateTime the dateTime
     * @return the pattern to use to parse the specified dateTime
     */
    private static String getPattern(String dateTime) {
        
        int length = dateTime.length();
        
        if (length == 10) {
            return "yyyy-MM-dd";
        }
        
        if (length == 19) {
            return "yyyy-MM-dd HH:mm:ss";
        }
        
        if (length == 23) {
            return "yyyy-MM-dd HH:mm:ss.SSS";
        }
        
        throw new IllegalArgumentException("The format of the date/time: " + dateTime + 
                                           " does not match the expected one: yyyy-MM-dd HH:mm:ss.SSS");
    }

    /**
     * Truncate this date, leaving the field specified as the most significant field.
     * 
     * @param calendar the calendar to truncate
     * @param field the most significant field
     */
    public static void truncate(Calendar calendar, int field) {
            
        Validate.isTrue(ArrayUtils.contains(TRUNCATING_FIELDS, field), 
                        "the specified field cannot be used to truncate the specified calendar");
        
        for (int calendarField : TRUNCATING_FIELDS) {
            
            if (field == calendarField) {
                
                return;
            }

            calendar.set(calendarField, calendar.getActualMinimum(calendarField));
        }
    }
    
    private TimeUtils() {

    }
}
