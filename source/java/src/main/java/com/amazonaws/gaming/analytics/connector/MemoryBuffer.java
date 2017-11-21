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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import com.amazonaws.gaming.analytics.common.AppConfiguration;

/**
 * An in-memory buffer for storing incoming events and deciding when to flush
 * them to their destination.
 * 
 * @author AWS Solutions Team
 */
public class MemoryBuffer<T> implements IBuffer<T>
{
    private static final Logger log = LogManager.getLogger(MemoryBuffer.class);
    
    /** The number of bytes to accumulate before a flush is needed. */
    private final long bytesPerFlush;
    
    /** The number of events to accumulate before a flush is needed. */
    private final long numRecordsToBuffer;
    
    /** The number of milliseconds to wait until flushing. */
    private final long millisecondsToBuffer;

    /** The list of all records currently in the buffer. */
    private final List<T> buffer;
    
    /** The number of bytes currently accumulated in the buffer. */
    private final AtomicLong byteCount;

    /** The first Kinesis sequence number in the buffer. */
    private String firstSequenceNumber;
    
    /** The last Kinesis sequence number in the buffer. */
    private String lastSequenceNumber;
    
    /** The timestamp of the oldest record in the buffer. */
    private DateTime firstTimestamp;

    /** The timestamp of the last flush that occurred. */
    private long previousFlushTimeMillisecond;

    public MemoryBuffer()
    {
        this(new LinkedList<T>());
    }

    public MemoryBuffer(final List<T> buffer)
    {
        this(buffer, AppConfiguration.INSTANCE.getLong("buffer_byte_size_limit"),
                AppConfiguration.INSTANCE.getLong("buffer_milliseconds_limit"),
                AppConfiguration.INSTANCE.getLong("buffer_record_count_limit"));
    }

    public MemoryBuffer(final List<T> buffer, long bytesPerFlush, long millisecondsToBuffer, long numRecordsToBuffer)
    {
        this.bytesPerFlush = bytesPerFlush;
        this.millisecondsToBuffer = millisecondsToBuffer;
        this.numRecordsToBuffer = numRecordsToBuffer;
        
        log.info("Creating " + getClass().getSimpleName() + 
                 " with BytesPerFlush=" + bytesPerFlush +
                 ", MillisToBuffer=" + millisecondsToBuffer +
                 ", RecordsToBuffer=" + numRecordsToBuffer);

        this.buffer = buffer;
        this.byteCount = new AtomicLong();
        this.previousFlushTimeMillisecond = System.currentTimeMillis();
    }

    /**
     * Consume a new record, add it to the buffer and update all buffer statistics.
     */
    public void consumeRecord(T record, int recordByteLength, String sequenceNumber, DateTime eventTimestamp,
            DateTime arrivalTimestamp)
    {
        if (this.buffer.isEmpty())
        {
            // if buffer is empty and we receive a record, reset all the flush conditions
            // (otherwise if this is the first event in a while, it would be ready for flush immediately)
            clear();

            this.firstSequenceNumber = sequenceNumber;
            this.firstTimestamp = arrivalTimestamp;
        }

        this.lastSequenceNumber = sequenceNumber;
        this.buffer.add(record);
        this.byteCount.addAndGet(recordByteLength);
    }

    /**
     * Empty all records out of the buffer and clear all buffer statistics.
     */
    public void clear()
    {
        this.buffer.clear();
        this.byteCount.set(0);
        this.previousFlushTimeMillisecond = System.currentTimeMillis();
        this.firstSequenceNumber = null;
        this.lastSequenceNumber = null;
        this.firstTimestamp = null;
    }

    /**
     * Determine whether or not the buffer is ready to be flushed.  A buffer is ready for flush if *any*
     * of the conditions are met (number of records, number of bytes or time since last flush).
     */
    public boolean shouldFlush()
    {
        long timeSinceLastFlush = System.currentTimeMillis() - this.previousFlushTimeMillisecond;

        boolean bufferReachedMaxRecords = this.buffer.size() >= this.numRecordsToBuffer;
        boolean bufferReachedMaxBytes = this.byteCount.get() >= this.bytesPerFlush;
        boolean bufferReachedMaxTimeout = timeSinceLastFlush >= this.millisecondsToBuffer;

        return !this.buffer.isEmpty() && (bufferReachedMaxRecords || bufferReachedMaxBytes || bufferReachedMaxTimeout);
    }

    public String getFirstSequenceNumber()
    {
        return this.firstSequenceNumber;
    }

    public String getLastSequenceNumber()
    {
        return this.lastSequenceNumber;
    }

    public DateTime getFirstTimestamp()
    {
        return this.firstTimestamp;
    }

    public List<T> getRecords()
    {
        return buffer;
    }

    /** See how many more bytes need to be accumulated until a flush will be necessary. */
    public long getNumBytesUntilFlush()
    {
        return this.bytesPerFlush - this.byteCount.get();
    }

    /** See how many more records need to be accumulated until a flush will be necessary. */
    public long getNumRecordsUntilFlush()
    {
        return this.numRecordsToBuffer - this.buffer.size();
    }
    
    /** See how much time remains until a flush will be necessary. */
    public long getMillisecondsUntilFlush()
    {
        long timeSinceLastFlush = System.currentTimeMillis() - this.previousFlushTimeMillisecond;
        return this.millisecondsToBuffer - timeSinceLastFlush;
    }
}
