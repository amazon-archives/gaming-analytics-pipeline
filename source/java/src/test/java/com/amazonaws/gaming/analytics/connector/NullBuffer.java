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

import java.util.Collections;
import java.util.List;

import org.joda.time.DateTime;

/**
 * Dummy buffer for testing.  Drops all records and always returns an empty list.
 * 
 * @author AWS Solutions Team
 */
public class NullBuffer<T> implements IBuffer<T>
{
    @Override
    public List<T> getRecords()
    {
        return Collections.emptyList();
    }

    @Override
    public boolean shouldFlush()
    {
        return false;
    }

    @Override
    public void clear()
    {
        // do nothing
    }

    @Override
    public DateTime getFirstTimestamp()
    {
        return DateTime.now();
    }

    @Override
    public String getFirstSequenceNumber()
    {
        return "0";
    }

    @Override
    public String getLastSequenceNumber()
    {
        return "0";
    }

    @Override
    public long getNumBytesUntilFlush()
    {
        return 0;
    }

    @Override
    public long getNumRecordsUntilFlush()
    {
        return 0;
    }

    @Override
    public long getMillisecondsUntilFlush()
    {
        return 0;
    }

    @Override
    public void consumeRecord(T record, int recordByteLength, String sequenceNumber, DateTime eventTimestamp,
            DateTime arrivalTimestamp)
    {
        // do nothing
    }
}
