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
package io.horizondb.model;

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.io.serialization.Parser;
import io.horizondb.io.serialization.Serializable;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import static org.apache.commons.lang.Validate.isTrue;

/**
 * A time range.
 * 
 * @author Benjamin
 * 
 */
@Immutable
public final class TimeRange implements Comparable<TimeRange>, Serializable {

    /**
     * The parser instance.
     */
    private static final Parser<TimeRange> PARSER = new Parser<TimeRange>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public TimeRange parseFrom(ByteReader reader) throws IOException {

            long start = VarInts.readUnsignedLong(reader);
            long end = VarInts.readUnsignedLong(reader);

            return new TimeRange(start, end);
        }
    };

    /**
     * The start of the time range.
     */
    private final long start;

    /**
     * The end of the time range.
     */
    private final long end;

    /**
     * Creates a new time range from the specified start time to the specified end time.
     * 
     * @param start the start time in milliseconds
     * @param end the end time in milliseconds
     */
    public TimeRange(long start, long end) {

        isTrue(start >= 0, "the start time must be greater or equals to zero");
        isTrue(start <= end, "The end of the time range must be greater or equals to the start. [" + start + ", " + end
                + "]");

        this.start = start;
        this.end = end;
    }

    /**
     * Returns the start time in milliseconds of the range.
     * 
     * @return the start time in milliseconds of the range.
     */
    public long getStart() {
        return this.start;
    }

    /**
     * Returns the end time in milliseconds of the range.
     * 
     * @return the end time in milliseconds of the range.
     */
    public long getEnd() {
        return this.end;
    }

    /**
     * Return <code>true</code> if this range is before the specified time, <code>false</code> otherwise.
     * 
     * @param timeInMillis the time in milliseconds
     * @return <code>true</code> if this range is before the specified time, <code>false</code> otherwise.
     */
    public boolean isBefore(long timeInMillis) {

        return timeInMillis > this.end;
    }

    /**
     * Return <code>true</code> if this range is after the specified time, <code>false</code> otherwise.
     * 
     * @param timeInMillis the time in milliseconds
     * @return <code>true</code> if this range is after the specified time, <code>false</code> otherwise.
     */
    public boolean isAfter(long timeInMillis) {

        return timeInMillis < this.start;
    }

    /**
     * Return <code>true</code> if the specified time is included within this range, <code>false</code> otherwise.
     * 
     * @param timeInMillis the time in milliseconds
     * @return <code>true</code> if the specified time is included within this range, <code>false</code> otherwise.
     */
    public boolean includes(long timeInMillis) {

        return timeInMillis >= this.start && timeInMillis <= this.end;
    }

    /**
     * Return <code>true</code> if the specified range is included within this range, <code>false</code> otherwise.
     * 
     * @param range the time range
     * @return <code>true</code> if the specified range is included within this range, <code>false</code> otherwise.
     */
    public boolean includes(TimeRange range) {

        return includes(range.getStart()) && includes(range.getEnd());
    }

    public TimeRange[] split(long timeInMilliseconds) {

        Validate.isTrue(includes(timeInMilliseconds), "the time (" + timeInMilliseconds
                + ") is not included within the range: [" + this.start + ", " + this.end + "]");

        if (timeInMilliseconds == this.start) {

            return new TimeRange[] { this };
        }

        TimeRange[] ranges = new TimeRange[2];
        ranges[0] = new TimeRange(this.start, timeInMilliseconds - 1);
        ranges[1] = new TimeRange(timeInMilliseconds, this.end);

        return ranges;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof TimeRange)) {
            return false;
        }
        TimeRange rhs = (TimeRange) object;
        return new EqualsBuilder().append(this.start, rhs.start).append(this.end, rhs.end).isEquals();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(528394433, -1170716017).append(this.start).append(this.end).toHashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("start", this.start)
                                                                          .append("end", this.end)
                                                                          .toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(TimeRange other) {
        return new CompareToBuilder().append(this.start, other.start).append(this.end, other.end).toComparison();
    }

    /**
     * Creates a new <code>TimeRange</code> by reading the data from the specified reader.
     * 
     * @param reader the reader to read from.
     * @throws IOException if an I/O problem occurs
     */
    public static TimeRange parseFrom(ByteReader reader) throws IOException {

        return getParser().parseFrom(reader);
    }

    /**
     * Returns the parser that can be used to deserialize <code>TimeRange</code> instances.
     * 
     * @return the parser that can be used to deserialize <code>TimeRange</code> instances.
     */
    public static Parser<TimeRange> getParser() {

        return PARSER;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int computeSerializedSize() {
        return VarInts.computeUnsignedLongSize(this.start) + VarInts.computeUnsignedLongSize(this.end);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(ByteWriter writer) throws IOException {

        VarInts.writeUnsignedLong(writer, this.start);
        VarInts.writeUnsignedLong(writer, this.end);
    }
}
