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
package com.amazonaws.gaming.analytics.connector.s3;

import java.nio.charset.StandardCharsets;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.amazonaws.gaming.analytics.common.AppConfiguration;
import com.amazonaws.gaming.analytics.common.exception.TelemetryEventParseException;
import com.amazonaws.gaming.analytics.common.exception.TelemetryEventSerializationException;
import com.amazonaws.gaming.analytics.common.exception.TelemetryEventValidationException;
import com.amazonaws.gaming.analytics.connector.AbstractRecordProcessor;
import com.amazonaws.gaming.analytics.connector.KinesisEvent;
import com.amazonaws.gaming.analytics.connector.MemoryBuffer;
import com.amazonaws.gaming.analytics.proprioception.MetricRecorder;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.model.Record;

/**
 * A record processor responsible for reading Kinesis stream records in the form of
 * analytics events and emitting them downstream.
 * 
 * @author AWS Solutions Team
 */
public class S3RecordProcessor extends AbstractRecordProcessor<byte[]>
{
    private static final Logger log = LogManager.getLogger(S3RecordProcessor.class);

    public S3RecordProcessor(final String componentName, MetricRecorder metricRecorder)
    {
        super(componentName, metricRecorder);
    }

    /**
     * ${@inheritDoc}
     */
    @Override
    protected Logger getLogger()
    {
        return log;
    }

    /**
     * The S3 record processor actually has two jobs: it emits event batches to S3 buckets
     * as well as emitting pointers to those files downstream to a secondary Kinesis stream.
     * 
     * ${@inheritDoc}
     */
    @Override
    public void initialize(InitializationInput input)
    {
        super.initialize(input);

        this.buffer = new MemoryBuffer<>();

        String bucketName = AppConfiguration.INSTANCE.getString("s3_telemetry_bucket");
        this.emitter = new S3FilePointerEmitter(bucketName, input.getShardId(), getComponentName(), this.metricRecorder);
    }

    /**
     * Parse an incoming telemetry event and buffer it in memory.
     * 
     * ${@inheritDoc}
     */
    @Override
    public void processRecord(final Record record)
            throws TelemetryEventParseException, TelemetryEventValidationException, TelemetryEventSerializationException
    {
        KinesisEvent event = getKinesisEventFromRecord(record);
        byte[] processedJsonBytes = event.getProcessedJson().getBytes(StandardCharsets.UTF_8);
        this.buffer.consumeRecord(processedJsonBytes, 
        						  processedJsonBytes.length, 
        						  record.getSequenceNumber(),
        						  new DateTime(event.getEventTimestamp().longValue()).withZone(DateTimeZone.UTC),
        						  new DateTime(record.getApproximateArrivalTimestamp().getTime()).withZone(DateTimeZone.UTC));
    }
}
