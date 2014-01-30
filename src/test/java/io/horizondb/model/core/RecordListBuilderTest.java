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
package io.horizondb.model.core;

import io.horizondb.model.TimeRange;
import io.horizondb.model.core.records.TimeSeriesRecord;
import io.horizondb.model.schema.FieldType;
import io.horizondb.model.schema.PartitionType;
import io.horizondb.model.schema.RecordTypeDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;
import io.horizondb.test.AssertCollections;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Multimap;

import static java.util.Arrays.asList;

/**
 * @author Benjamin
 * 
 */
public class RecordListBuilderTest {

    private static final TimeZone TIMEZONE = TimeZone.getTimeZone("Europe/Berlin");

    @Test
    public void testWithEmptyIterator() {

        RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
                                                         .addDecimalField("price")
                                                         .addLongField("volume")
                                                         .build();

        TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("DAX")
                                                              .timeUnit(TimeUnit.MILLISECONDS)
                                                              .timeZone(TIMEZONE)
                                                              .partitionType(PartitionType.BY_DAY)
                                                              .addRecordType(trade)
                                                              .build();

        Multimap<TimeRange, TimeSeriesRecord> multimap = new RecordListMultimapBuilder(definition).buildMultimap();

        Assert.assertTrue(multimap.isEmpty());
    }

    @Test
    public void testWithOnlyOneRecord() throws ParseException {

        RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
                                                         .addDecimalField("price")
                                                         .addLongField("volume")
                                                         .build();

        TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("DAX")
                                                              .timeUnit(TimeUnit.MILLISECONDS)
                                                              .timeZone(TIMEZONE)
                                                              .partitionType(PartitionType.BY_DAY)
                                                              .addRecordType(trade)
                                                              .build();

        long timestamp = getTime("2013.11.14 11:46:00.000");

        RecordListMultimapBuilder builder = new RecordListMultimapBuilder(definition).newRecord("Trade")
                                                                     .setTimestampInMillis(0, timestamp)
                                                                     .setDecimal(1, 125, 1)
                                                                     .setLong(2, 10);

        TimeSeriesRecord expected = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
        expected.setTimestampInMillis(0, timestamp);
        expected.setDecimal(1, 125, 1);
        expected.setLong(2, 10);

        Multimap<TimeRange, TimeSeriesRecord> multimap = builder.buildMultimap();

        TimeRange range = new TimeRange(getTime("2013.11.14 00:00:00.000"), getTime("2013.11.14 23:59:59.999"));

        AssertCollections.assertMapContains(multimap.asMap(), range, asList(expected));
    }

    @Test
    public void testWithTwoRecordsOfTheSameType() throws ParseException {

        long time = getTime("2013.11.14 11:46:00.000");

        RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
                                                         .addDecimalField("price")
                                                         .addLongField("volume")
                                                         .build();

        TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("DAX")
                                                              .timeUnit(TimeUnit.MILLISECONDS)
                                                              .timeZone(TIMEZONE)
                                                              .partitionType(PartitionType.BY_DAY)
                                                              .addRecordType(trade)
                                                              .build();

        Multimap<TimeRange, TimeSeriesRecord> multimap = new RecordListMultimapBuilder(definition).newRecord("Trade")
                                                                                          .setTimestampInMillis(0, time)
                                                                                          .setDecimal(1, 125, 1)
                                                                                          .setLong(2, 10)
                                                                                          .newRecord("Trade")
                                                                                          .setTimestampInMillis(0,
                                                                                                                time + 50)
                                                                                          .setDecimal(1, 124, 1)
                                                                                          .setLong(2, 5)
                                                                                          .buildMultimap();

        TimeSeriesRecord first = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
        first.setTimestampInMillis(0, time);
        first.setDecimal(1, 125, 1);
        first.setLong(2, 10);

        TimeSeriesRecord second = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
        second.setDelta(true);
        second.setTimestampInMillis(0, 50);
        second.setDecimal(1, -1, 1);
        second.setLong(2, -5);

        TimeRange range = new TimeRange(getTime("2013.11.14 00:00:00.000"), getTime("2013.11.14 23:59:59.999"));

        AssertCollections.assertMapContains(multimap.asMap(), range, asList(first, second));
    }

    @Test
    public void testWithTwoRecordsOfTheSameTypeInDifferentPartitions() throws ParseException {

        long time = getTime("2013.11.14 11:46:00.000");
        long time2 = getTime("2013.11.15 17:35:00.000");

        RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
                                                         .addDecimalField("price")
                                                         .addLongField("volume")
                                                         .build();

        TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("DAX")
                                                              .timeUnit(TimeUnit.MILLISECONDS)
                                                              .timeZone(TIMEZONE)
                                                              .partitionType(PartitionType.BY_DAY)
                                                              .addRecordType(trade)
                                                              .build();

        Multimap<TimeRange, TimeSeriesRecord> multimap = new RecordListMultimapBuilder(definition).newRecord("Trade")
                                                                                          .setTimestampInMillis(0, time)
                                                                                          .setDecimal(1, 125, 1)
                                                                                          .setLong(2, 10)
                                                                                          .newRecord("Trade")
                                                                                          .setTimestampInMillis(0,
                                                                                                                time2)
                                                                                          .setDecimal(1, 124, 1)
                                                                                          .setLong(2, 5)
                                                                                          .buildMultimap();

        TimeSeriesRecord first = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
        first.setTimestampInMillis(0, time);
        first.setDecimal(1, 125, 1);
        first.setLong(2, 10);

        TimeSeriesRecord second = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
        second.setTimestampInMillis(0, time2);
        second.setDecimal(1, 124, 1);
        second.setLong(2, 5);

        TimeRange firstRange = new TimeRange(getTime("2013.11.14 00:00:00.000"), getTime("2013.11.14 23:59:59.999"));
        TimeRange secondRange = new TimeRange(getTime("2013.11.15 00:00:00.000"), getTime("2013.11.15 23:59:59.999"));

        AssertCollections.assertMapContains(multimap.asMap(), firstRange, asList(first), secondRange, asList(second));
    }

    @Test
    public void testWithThreeRecordsOfSameType() throws ParseException {

        long time = getTime("2013.11.14 11:46:00.000");

        RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
                                                         .addDecimalField("price")
                                                         .addLongField("volume")
                                                         .build();

        TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("DAX")
                                                              .timeUnit(TimeUnit.MILLISECONDS)
                                                              .timeZone(TIMEZONE)
                                                              .partitionType(PartitionType.BY_DAY)
                                                              .addRecordType(trade)
                                                              .build();

        Multimap<TimeRange, TimeSeriesRecord> multimap = new RecordListMultimapBuilder(definition).newRecord("Trade")
                                                                                          .setTimestampInMillis(0, time)
                                                                                          .setDecimal(1, 125, -1)
                                                                                          .setLong(2, 10)
                                                                                          .newRecord("Trade")
                                                                                          .setTimestampInMillis(0,
                                                                                                                time + 50)
                                                                                          .setDecimal(1, 124, -1)
                                                                                          .setLong(2, 5)
                                                                                          .newRecord("Trade")
                                                                                          .setTimestampInMillis(0,
                                                                                                                time + 120)
                                                                                          .setDecimal(1, 13, 0)
                                                                                          .setLong(2, 6)
                                                                                          .buildMultimap();

        TimeSeriesRecord first = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
        first.setTimestampInMillis(0, time);
        first.setDecimal(1, 125, -1);
        first.setLong(2, 10);

        TimeSeriesRecord second = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
        second.setDelta(true);
        second.setTimestampInMillis(0, 50);
        second.setDecimal(1, -1, -1);
        second.setLong(2, -5);

        TimeSeriesRecord third = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
        third.setDelta(true);
        third.setTimestampInMillis(0, 70);
        third.setDecimal(1, 6, -1);
        third.setLong(2, 1);

        TimeRange range = new TimeRange(getTime("2013.11.14 00:00:00.000"), getTime("2013.11.14 23:59:59.999"));

        AssertCollections.assertMapContains(multimap.asMap(), range, asList(first, second, third));
    }

    @Test
    public void testWithThreeRecordsOfSameTypeWithinTwoDifferentPartitions() throws ParseException {

        long time = getTime("2013.11.14 11:46:00.000");
        long time2 = getTime("2013.11.15 17:35:00.000");

        RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
                                                         .addDecimalField("price")
                                                         .addLongField("volume")
                                                         .build();

        TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("DAX")
                                                              .timeUnit(TimeUnit.MILLISECONDS)
                                                              .timeZone(TIMEZONE)
                                                              .partitionType(PartitionType.BY_DAY)
                                                              .addRecordType(trade)
                                                              .build();

        Multimap<TimeRange, TimeSeriesRecord> multimap = new RecordListMultimapBuilder(definition).newRecord("Trade")
                                                                                          .setTimestampInMillis(0, time)
                                                                                          .setDecimal(1, 125, 1)
                                                                                          .setLong(2, 10)
                                                                                          .newRecord("Trade")
                                                                                          .setTimestampInMillis(0,
                                                                                                                time + 50)
                                                                                          .setDecimal(1, 124, 1)
                                                                                          .setLong(2, 5)
                                                                                          .newRecord("Trade")
                                                                                          .setTimestampInMillis(0,
                                                                                                                time2)
                                                                                          .setDecimal(1, 13, 0)
                                                                                          .setLong(2, 6)
                                                                                          .buildMultimap();

        TimeSeriesRecord first = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
        first.setTimestampInMillis(0, time);
        first.setDecimal(1, 125, 1);
        first.setLong(2, 10);

        TimeSeriesRecord second = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
        second.setDelta(true);
        second.setTimestampInMillis(0, 50);
        second.setDecimal(1, -1, 1);
        second.setLong(2, -5);

        TimeSeriesRecord third = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
        third.setTimestampInMillis(0, time2);
        third.setDecimal(1, 13, 0);
        third.setLong(2, 6);

        TimeRange firstRange = new TimeRange(getTime("2013.11.14 00:00:00.000"), getTime("2013.11.14 23:59:59.999"));
        TimeRange secondRange = new TimeRange(getTime("2013.11.15 00:00:00.000"), getTime("2013.11.15 23:59:59.999"));

        AssertCollections.assertMapContains(multimap.asMap(),
                                            firstRange,
                                            asList(first, second),
                                            secondRange,
                                            asList(third));
    }

    @Test
    public void testWithThreeRecordsOfSameTypeInDisorder() throws ParseException {

        long time = getTime("2013.11.14 11:46:00.000");

        RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
                                                         .addDecimalField("price")
                                                         .addLongField("volume")
                                                         .build();

        TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("DAX")
                                                              .timeUnit(TimeUnit.MILLISECONDS)
                                                              .timeZone(TIMEZONE)
                                                              .partitionType(PartitionType.BY_DAY)
                                                              .addRecordType(trade)
                                                              .build();

        Multimap<TimeRange, TimeSeriesRecord> multimap = new RecordListMultimapBuilder(definition).newRecord("Trade")
                                                                                          .setTimestampInMillis(0,
                                                                                                                time + 50)
                                                                                          .setDecimal(1, 124, -1)
                                                                                          .setLong(2, 5)
                                                                                          .newRecord("Trade")
                                                                                          .setTimestampInMillis(0,
                                                                                                                time + 120)
                                                                                          .setDecimal(1, 13, 0)
                                                                                          .setLong(2, 6)
                                                                                          .newRecord("Trade")
                                                                                          .setTimestampInMillis(0, time)
                                                                                          .setDecimal(1, 125, -1)
                                                                                          .setLong(2, 10)
                                                                                          .buildMultimap();

        TimeSeriesRecord first = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
        first.setTimestampInMillis(0, time);
        first.setDecimal(1, 125, -1);
        first.setLong(2, 10);

        TimeSeriesRecord second = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
        second.setDelta(true);
        second.setTimestampInMillis(0, 50);
        second.setDecimal(1, -1, -1);
        second.setLong(2, -5);

        TimeSeriesRecord third = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
        third.setDelta(true);
        third.setTimestampInMillis(0, 70);
        third.setDecimal(1, 6, -1);
        third.setLong(2, 1);

        TimeRange range = new TimeRange(getTime("2013.11.14 00:00:00.000"), getTime("2013.11.14 23:59:59.999"));

        AssertCollections.assertMapContains(multimap.asMap(), range, asList(first, second, third));
    }

    @Test
    public void testWithThreeRecordsOfSameTypeInDisorderWithTwoPartition() throws ParseException {

        long time = getTime("2013.11.14 11:46:00.000");
        long time2 = getTime("2013.11.15 17:35:00.000");

        RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
                                                         .addDecimalField("price")
                                                         .addLongField("volume")
                                                         .build();

        TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("DAX")
                                                              .timeUnit(TimeUnit.MILLISECONDS)
                                                              .timeZone(TIMEZONE)
                                                              .partitionType(PartitionType.BY_DAY)
                                                              .addRecordType(trade)
                                                              .build();

        Multimap<TimeRange, TimeSeriesRecord> multimap = new RecordListMultimapBuilder(definition).newRecord("Trade")
                                                                                          .setTimestampInMillis(0,
                                                                                                                time2 + 50)
                                                                                          .setDecimal(1, 124, -1)
                                                                                          .setLong(2, 5)
                                                                                          .newRecord("Trade")
                                                                                          .setTimestampInMillis(0,
                                                                                                                time2 + 120)
                                                                                          .setDecimal(1, 13, 0)
                                                                                          .setLong(2, 6)
                                                                                          .newRecord("Trade")
                                                                                          .setTimestampInMillis(0, time)
                                                                                          .setDecimal(1, 125, -1)
                                                                                          .setLong(2, 10)
                                                                                          .buildMultimap();

        TimeSeriesRecord first = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
        first.setTimestampInMillis(0, time);
        first.setDecimal(1, 125, -1);
        first.setLong(2, 10);

        TimeSeriesRecord second = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
        second.setTimestampInMillis(0, time2 + 50);
        second.setDecimal(1, 124, -1);
        second.setLong(2, 5);

        TimeSeriesRecord third = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
        third.setDelta(true);
        third.setTimestampInMillis(0, 70);
        third.setDecimal(1, 6, -1);
        third.setLong(2, 1);

        TimeRange firstRange = new TimeRange(getTime("2013.11.14 00:00:00.000"), getTime("2013.11.14 23:59:59.999"));
        TimeRange secondRange = new TimeRange(getTime("2013.11.15 00:00:00.000"), getTime("2013.11.15 23:59:59.999"));

        AssertCollections.assertMapContains(multimap.asMap(),
                                            firstRange,
                                            asList(first),
                                            secondRange,
                                            asList(second, third));
    }

    @Test
    public void testWithOnlyTwoRecordOfDifferentType() throws ParseException {

        long time = getTime("2013.11.14 11:46:00.000");

        RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
                                                         .addDecimalField("price")
                                                         .addLongField("volume")
                                                         .build();

        RecordTypeDefinition quote = RecordTypeDefinition.newBuilder("Quote")
                                                         .addDecimalField("bidPrice")
                                                         .addDecimalField("askPrice")
                                                         .addLongField("bidVolume")
                                                         .addLongField("askVolume")
                                                         .build();

        TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("DAX")
                                                              .timeUnit(TimeUnit.MILLISECONDS)
                                                              .timeZone(TIMEZONE)
                                                              .partitionType(PartitionType.BY_DAY)
                                                              .addRecordType(quote)
                                                              .addRecordType(trade)
                                                              .build();

        Multimap<TimeRange, TimeSeriesRecord> multimap = new RecordListMultimapBuilder(definition).newRecord("Quote")
                                                                                          .setTimestampInMillis(0, time)
                                                                                          .setDecimal(1, 123, 1)
                                                                                          .setDecimal(2, 125, 1)
                                                                                          .setLong(3, 6)
                                                                                          .setLong(4, 14)
                                                                                          .newRecord("Trade")
                                                                                          .setTimestampInMillis(0,
                                                                                                                time + 100)
                                                                                          .setDecimal(1, 125, 1)
                                                                                          .setLong(2, 10)
                                                                                          .buildMultimap();

        TimeSeriesRecord expectedQuote = new TimeSeriesRecord(0,
                                                              TimeUnit.MILLISECONDS,
                                                              FieldType.DECIMAL,
                                                              FieldType.DECIMAL,
                                                              FieldType.LONG,
                                                              FieldType.LONG);
        expectedQuote.setTimestampInMillis(0, time);
        expectedQuote.setDecimal(1, 123, 1);
        expectedQuote.setDecimal(2, 125, 1);
        expectedQuote.setLong(3, 6);
        expectedQuote.setLong(4, 14);

        TimeSeriesRecord expectedTrade = new TimeSeriesRecord(1,
                                                              TimeUnit.MILLISECONDS,
                                                              FieldType.DECIMAL,
                                                              FieldType.LONG);
        expectedTrade.setTimestampInMillis(0, time + 100);
        expectedTrade.setDecimal(1, 125, 1);
        expectedTrade.setLong(2, 10);

        TimeRange range = new TimeRange(getTime("2013.11.14 00:00:00.000"), getTime("2013.11.14 23:59:59.999"));

        AssertCollections.assertMapContains(multimap.asMap(), range, asList(expectedQuote, expectedTrade));
    }

    @Test
    public void testWithMultipleRecordsOfDifferentType() throws ParseException {

        long time = getTime("2013.11.14 11:46:00.000");

        RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
                                                         .addDecimalField("price")
                                                         .addLongField("volume")
                                                         .build();

        RecordTypeDefinition quote = RecordTypeDefinition.newBuilder("Quote")
                                                         .addDecimalField("bidPrice")
                                                         .addDecimalField("askPrice")
                                                         .addLongField("bidVolume")
                                                         .addLongField("askVolume")
                                                         .build();

        RecordTypeDefinition exchangeState = RecordTypeDefinition.newBuilder("ExchangeState")
                                                                 .addByteField("status")
                                                                 .build();

        TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("DAX")
                                                              .timeUnit(TimeUnit.MILLISECONDS)
                                                              .timeZone(TIMEZONE)
                                                              .partitionType(PartitionType.BY_DAY)
                                                              .addRecordType(trade)
                                                              .addRecordType(quote)
                                                              .addRecordType(exchangeState)
                                                              .build();

        Multimap<TimeRange, TimeSeriesRecord> multimap = new RecordListMultimapBuilder(definition).newRecord("ExchangeState")
                                                                                          .setTimestampInMillis(0, time)
                                                                                          .setByte(1, 1)
                                                                                          .newRecord("Quote")
                                                                                          .setTimestampInMillis(0, time)
                                                                                          .setDecimal(1, 123, -1)
                                                                                          .setDecimal(2, 125, -1)
                                                                                          .setLong(3, 6)
                                                                                          .setLong(4, 14)
                                                                                          .newRecord("Trade")
                                                                                          .setTimestampInMillis(0,
                                                                                                                time + 5)
                                                                                          .setDecimal(1, 125, -1)
                                                                                          .setLong(2, 10)
                                                                                          .newRecord("Quote")
                                                                                          .setTimestampInMillis(0,
                                                                                                                time + 15)
                                                                                          .setDecimal(1, 123, -1)
                                                                                          .setDecimal(2, 125, -1)
                                                                                          .setLong(3, 6)
                                                                                          .setLong(4, 4)
                                                                                          .newRecord("Trade")
                                                                                          .setTimestampInMillis(0,
                                                                                                                time + 50)
                                                                                          .setDecimal(1, 124, -1)
                                                                                          .setLong(2, 5)
                                                                                          .newRecord("Trade")
                                                                                          .setTimestampInMillis(0,
                                                                                                                time + 120)
                                                                                          .setDecimal(1, 13, 0)
                                                                                          .setLong(2, 6)
                                                                                          .newRecord("Quote")
                                                                                          .setTimestampInMillis(0,
                                                                                                                time + 150)
                                                                                          .setDecimal(1, 123, -1)
                                                                                          .setDecimal(2, 125, -1)
                                                                                          .setLong(3, 6)
                                                                                          .setLong(4, 4)
                                                                                          .buildMultimap();

        TimeSeriesRecord firstES = new TimeSeriesRecord(2, TimeUnit.MILLISECONDS, FieldType.BYTE);
        firstES.setTimestampInMillis(0, time);
        firstES.setByte(1, 1);

        TimeSeriesRecord firstQuote = new TimeSeriesRecord(1,
                                                           TimeUnit.MILLISECONDS,
                                                           FieldType.DECIMAL,
                                                           FieldType.DECIMAL,
                                                           FieldType.LONG,
                                                           FieldType.LONG);
        firstQuote.setTimestampInMillis(0, time);
        firstQuote.setDecimal(1, 123, -1);
        firstQuote.setDecimal(2, 125, -1);
        firstQuote.setLong(3, 6);
        firstQuote.setLong(4, 14);

        TimeSeriesRecord firstTrade = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
        firstTrade.setTimestampInMillis(0, time + 5);
        firstTrade.setDecimal(1, 125, -1);
        firstTrade.setLong(2, 10);

        TimeSeriesRecord secondQuote = new TimeSeriesRecord(1,
                                                            TimeUnit.MILLISECONDS,
                                                            FieldType.DECIMAL,
                                                            FieldType.DECIMAL,
                                                            FieldType.LONG,
                                                            FieldType.LONG);
        secondQuote.setDelta(true);
        secondQuote.setTimestampInMillis(0, 15);
        secondQuote.setDecimal(1, 0, -1);
        secondQuote.setDecimal(2, 0, -1);
        secondQuote.setLong(3, 0);
        secondQuote.setLong(4, -10);

        TimeSeriesRecord secondTrade = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
        secondTrade.setDelta(true);
        secondTrade.setTimestampInMillis(0, 45);
        secondTrade.setDecimal(1, -1, -1);
        secondTrade.setLong(2, -5);

        TimeSeriesRecord thirdTrade = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
        thirdTrade.setDelta(true);
        thirdTrade.setTimestampInMillis(0, 70);
        thirdTrade.setDecimal(1, 6, -1);
        thirdTrade.setLong(2, 1);

        TimeSeriesRecord thirdQuote = new TimeSeriesRecord(1,
                                                           TimeUnit.MILLISECONDS,
                                                           FieldType.DECIMAL,
                                                           FieldType.DECIMAL,
                                                           FieldType.LONG,
                                                           FieldType.LONG);
        thirdQuote.setDelta(true);
        thirdQuote.setTimestampInMillis(0, 135);
        thirdQuote.setDecimal(1, 0, -1);
        thirdQuote.setDecimal(2, 0, -1);
        thirdQuote.setLong(3, 0);
        thirdQuote.setLong(4, 0);

        TimeRange range = new TimeRange(getTime("2013.11.14 00:00:00.000"), getTime("2013.11.14 23:59:59.999"));

        AssertCollections.assertMapContains(multimap.asMap(),
                                            range,
                                            asList(firstES,
                                                   firstQuote,
                                                   firstTrade,
                                                   secondQuote,
                                                   secondTrade,
                                                   thirdTrade,
                                                   thirdQuote));
    }

    @Test
    public void testWithMultipleRecordsOfDifferentTypeAndTwoPartition() throws ParseException {

        long time = getTime("2013.11.14 11:46:00.000");
        long time2 = getTime("2013.11.15 17:35:00.000");

        RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
                                                         .addDecimalField("price")
                                                         .addLongField("volume")
                                                         .build();

        RecordTypeDefinition quote = RecordTypeDefinition.newBuilder("Quote")
                                                         .addDecimalField("bidPrice")
                                                         .addDecimalField("askPrice")
                                                         .addLongField("bidVolume")
                                                         .addLongField("askVolume")
                                                         .build();

        RecordTypeDefinition exchangeState = RecordTypeDefinition.newBuilder("ExchangeState")
                                                                 .addByteField("status")
                                                                 .build();

        TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("DAX")
                                                              .timeUnit(TimeUnit.MILLISECONDS)
                                                              .timeZone(TIMEZONE)
                                                              .partitionType(PartitionType.BY_DAY)
                                                              .addRecordType(trade)
                                                              .addRecordType(quote)
                                                              .addRecordType(exchangeState)
                                                              .build();

        Multimap<TimeRange, TimeSeriesRecord> multimap = new RecordListMultimapBuilder(definition)
                                                                        .newRecord("ExchangeState")
                                                                        .setTimestampInMillis(0, time)
                                                                        .setByte(1, 1)
                                                                        .newRecord("Quote")
                                                                        .setTimestampInMillis(0, time)
                                                                        .setDecimal(1, 123, -1)
                                                                        .setDecimal(2, 125, -1)
                                                                        .setLong(3, 6)
                                                                        .setLong(4, 14)
                                                                        .newRecord("Trade")
                                                                        .setTimestampInMillis(0, time + 5)
                                                                        .setDecimal(1, 125, -1)
                                                                        .setLong(2, 10)
                                                                        .newRecord("Quote")
                                                                        .setTimestampInMillis(0, time + 15)
                                                                        .setDecimal(1, 123, -1)
                                                                        .setDecimal(2, 125, -1)
                                                                        .setLong(3, 6)
                                                                        .setLong(4, 4)
                                                                        .newRecord("Trade")
                                                                        .setTimestampInMillis(0, time2 + 50)
                                                                        .setDecimal(1, 124, -1)
                                                                        .setLong(2, 5)
                                                                        .newRecord("Trade")
                                                                        .setTimestampInMillis(0, time2 + 120)
                                                                        .setDecimal(1, 13, 0)
                                                                        .setLong(2, 6)
                                                                        .newRecord("Quote")
                                                                        .setTimestampInMillis(0, time2 + 150)
                                                                        .setDecimal(1, 123, -1)
                                                                        .setDecimal(2, 125, -1)
                                                                        .setLong(3, 6)
                                                                        .setLong(4, 4)
                                                                        .buildMultimap();

        TimeSeriesRecord firstES = new TimeSeriesRecord(2, TimeUnit.MILLISECONDS, FieldType.BYTE);
        firstES.setTimestampInMillis(0, time);
        firstES.setByte(1, 1);

        TimeSeriesRecord firstQuote = new TimeSeriesRecord(1,
                                                              TimeUnit.MILLISECONDS,
                                                              FieldType.DECIMAL,
                                                              FieldType.DECIMAL,
                                                              FieldType.LONG,
                                                              FieldType.LONG);
        firstQuote.setTimestampInMillis(0, time);
        firstQuote.setDecimal(1, 123, -1);
        firstQuote.setDecimal(2, 125, -1);
        firstQuote.setLong(3, 6);
        firstQuote.setLong(4, 14);

        TimeSeriesRecord firstTrade = new TimeSeriesRecord(0,
                                                              TimeUnit.MILLISECONDS,
                                                              FieldType.DECIMAL,
                                                              FieldType.LONG);
        firstTrade.setTimestampInMillis(0, time + 5);
        firstTrade.setDecimal(1, 125, -1);
        firstTrade.setLong(2, 10);

        TimeSeriesRecord secondQuote = new TimeSeriesRecord(1,
                                             TimeUnit.MILLISECONDS,
                                             FieldType.DECIMAL,
                                             FieldType.DECIMAL,
                                             FieldType.LONG,
                                             FieldType.LONG);
        secondQuote.setDelta(true);
        secondQuote.setTimestampInMillis(0, 15);
        secondQuote.setDecimal(1, 0, -1);
        secondQuote.setDecimal(2, 0, -1);
        secondQuote.setLong(3, 0);
        secondQuote.setLong(4, -10);

        TimeSeriesRecord secondTrade = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
        secondTrade.setTimestampInMillis(0, time2 + 50);
        secondTrade.setDecimal(1, 124, -1);
        secondTrade.setLong(2, 5);

        TimeSeriesRecord thirdTrade = new TimeSeriesRecord(0, TimeUnit.MILLISECONDS, FieldType.DECIMAL, FieldType.LONG);
        thirdTrade.setDelta(true);
        thirdTrade.setTimestampInMillis(0, 70);
        thirdTrade.setDecimal(1, 6, -1);
        thirdTrade.setLong(2, 1);

        TimeSeriesRecord thirdQuote = new TimeSeriesRecord(1,
                                             TimeUnit.MILLISECONDS,
                                             FieldType.DECIMAL,
                                             FieldType.DECIMAL,
                                             FieldType.LONG,
                                             FieldType.LONG);
        thirdQuote.setTimestampInMillis(0, time2 + 150);
        thirdQuote.setDecimal(1, 123, -1);
        thirdQuote.setDecimal(2, 125, -1);
        thirdQuote.setLong(3, 6);
        thirdQuote.setLong(4, 4);
        
        TimeRange firstRange = new TimeRange(getTime("2013.11.14 00:00:00.000"), getTime("2013.11.14 23:59:59.999"));
        TimeRange secondRange = new TimeRange(getTime("2013.11.15 00:00:00.000"), getTime("2013.11.15 23:59:59.999"));

        AssertCollections.assertMapContains(multimap.asMap(), firstRange, asList(firstES, firstQuote, firstTrade, secondQuote), secondRange, asList(secondTrade, thirdTrade, thirdQuote));
    }

    /**
     * Returns the time in milliseconds corresponding to the specified {@link String}.
     * 
     * @param dateAsText the date/time to convert in milliseconds
     * @return the time in milliseconds corresponding to the specified {@link String}.
     * @throws ParseException if a problem occurs while generating the time.
     */
    private static long getTime(String dateAsText) throws ParseException {

        SimpleDateFormat format = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss.SSS");
        return format.parse(dateAsText).getTime();
    }

}
