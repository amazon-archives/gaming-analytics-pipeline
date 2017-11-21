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

import com.amazonaws.services.kinesis.model.Record;

/**
 * A helper factory class for generating KinesisEvent objects from incoming
 * Kinesis stream records.
 * 
 * @author AWS Solutions Team
 */
public class KinesisEventFactory
{
    /**
     * Create a new KinesisEvent from the incomning stream record.
     * 
     * @param shardId The Kinesis stream shard that the record came from
     * @param record The raw Kinesis stream record
     * 
     * @return The newly created KinesisEvent
     */
    public static KinesisEvent createEvent(final String shardId, final Record record)
    {
        KinesisEvent event = new KinesisEvent();
        event.setShardId(shardId);
        event.setPartitionKey(record.getPartitionKey());
        event.setSequenceNumber(record.getSequenceNumber());
        event.setServerTimestamp(record.getApproximateArrivalTimestamp().getTime());
        
        return event;
    }
}
