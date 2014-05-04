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

/**
 * Utility methods to work with date/time.
 * 
 * @author Benjamin
 */
public final class TimeUtils {

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

    private TimeUtils() {

    }
}
