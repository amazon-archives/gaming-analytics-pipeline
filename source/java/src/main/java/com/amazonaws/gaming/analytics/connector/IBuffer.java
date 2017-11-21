/*********************************************************************************************************************
* Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved. *
* *
* Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance *
* with the License. A copy of the License is located at *
* *
* http://aws.amazon.com/asl/ *
* *
* or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES *
* OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions *
* and limitations under the License. *
*********************************************************************************************************************/ 
package com.amazonaws.gaming.analytics.connector;

import java.util.List;

import org.joda.time.DateTime;

/**
 * Abstraction of a buffer, which is essentially a thing that stores up records
 * until they're ready to be emitted.
 * 
 * @author AWS Solutions Team
 */
public interface IBuffer<T>
{
    /**
     * @return The current list of records stored in the buffer.
     */
    public List<T> getRecords();

    /**
     * @return true if this buffer is ready to be flushed or false if not.
     */
    public boolean shouldFlush();

    /**
     * Clear out all the records in this buffer and reset any underlying statistics.
     */
    public void clear();

    /**
     * @return The timestamp of the oldest record in the buffer
     * or null if the buffer is empty.
     */
    public DateTime getFirstTimestamp();

    /**
     * @return The Kinesis sequence number of the oldest record in the buffer
     * or null if the buffer is empty.
     */
    public String getFirstSequenceNumber();

    /**
     * @return The Kinesis sequence number of the newest record in the buffer
     * or null if the buffer is empty.
     */
    public String getLastSequenceNumber();

    /**
     * @return The number of record bytes until the buffer should be flushed.
     */
    public long getNumBytesUntilFlush();

    /**
     * @return The number of records until the buffer should be flushed.
     */
    public long getNumRecordsUntilFlush();

    /**
     * @return The number of milliseconds until the buffer should be flushed.
     */
    public long getMillisecondsUntilFlush();

    /**
     * Add a new record to the buffer.
     * 
     * @param record The record to add.
     * @param recordByteLength The size of the record in bytes.
     * @param sequenceNumber The Kinesis sequence number of the record.
     * @param eventTimestamp The timestamp of the original event record.
     * @param arrivalTimestamp The timestmap of the server-side arrival of the event record.
     */
    public void consumeRecord(T record, int recordByteLength, String sequenceNumber, DateTime eventTimestamp, DateTime arrivalTimestamp);
}
