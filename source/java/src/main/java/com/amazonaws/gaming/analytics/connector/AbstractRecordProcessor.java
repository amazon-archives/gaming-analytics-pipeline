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

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.log4j.Logger;

import com.amazonaws.gaming.analytics.common.AppConfiguration;
import com.amazonaws.gaming.analytics.common.DecoderUtil;
import com.amazonaws.gaming.analytics.common.HexUtil;
import com.amazonaws.gaming.analytics.common.SafeUtil;
import com.amazonaws.gaming.analytics.common.exception.TelemetryEventParseException;
import com.amazonaws.gaming.analytics.common.exception.TelemetryEventSerializationException;
import com.amazonaws.gaming.analytics.common.exception.TelemetryEventValidationException;
import com.amazonaws.gaming.analytics.proprioception.Metric;
import com.amazonaws.gaming.analytics.proprioception.MetricRecorder;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * An abstract implementation of the IRecordProcessor interface from the KCL
 * that details with processing incoming records from a Kinesis stream.
 * 
 * @author AWS Solutions Team
 */
public abstract class AbstractRecordProcessor<T> implements IRecordProcessor
{
    private final String componentName;
    protected final MetricRecorder metricRecorder;

    protected final ObjectMapper jsonMapper;

    protected final int emitRetryLimit;
    protected final int checkpointRetryLimit;
    protected final boolean emitShardLevelMetrics;

    protected String shardId;
    
    protected IBuffer<T> buffer;
    protected IEmitter<T> emitter;

    public AbstractRecordProcessor(final String componentName, MetricRecorder metricRecorder)
    {
        this.componentName = componentName;
        this.metricRecorder = metricRecorder;

        this.jsonMapper = new ObjectMapper();
        this.jsonMapper.configure(SerializationFeature.EAGER_SERIALIZER_FETCH, true);
        this.jsonMapper.configure(SerializationFeature.CLOSE_CLOSEABLE, true);

        this.emitRetryLimit = AppConfiguration.INSTANCE.getInt("emit_retry_limit");
        this.checkpointRetryLimit = AppConfiguration.INSTANCE.getInt("checkpoint_retry_limit");
        this.emitShardLevelMetrics = AppConfiguration.INSTANCE.getBoolean("emit_shard_level_metrics");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(final InitializationInput input)
    {
        info("Initializing processor for Shard " + input.getShardId() + 
             " from sequence number " + input.getExtendedSequenceNumber());
        this.shardId = input.getShardId();
    }

    /**
     * Submit a metric for publishing to CloudWatch Metrics. Submitted metrics are published twice;
     * once with dimensions [Operation,Component] and once with dimensions [Operation,Component,ShardId]
     * 
     * @param name The name of the metric to publish
     * @param units The units of the metric
     * @param value The numeric value of the metric
     */
    protected void submitMetric(String name, StandardUnit units, double value)
    {
        Metric metric = this.metricRecorder.createMetric(name, units)
                        .withValue(value)
                        .withDimension("Operation", "ProcessRecords")
                        .withDimension("Component", getComponentName());
        this.metricRecorder.putMetric(metric);

        if(this.emitShardLevelMetrics)
        {
	        Metric metricWithShardId = this.metricRecorder.createMetric(name, units)
	                                   .withValue(value)
	                                   .withDimension("ShardId", this.shardId)
	                                   .withDimension("Operation", "ProcessRecords")
	                                   .withDimension("Component", getComponentName());
	        this.metricRecorder.putMetric(metricWithShardId);
        }
    }

    /**
     * Set a KCL checkpoint and retry a few times on failure to try to make sure it goes through.
     * 
     * @param checkpointer The checkpointer object from the KCL
     * @param atSequenceNumber The sequence number to checkpoint at
     */
    protected void checkpointWithExponentialBackoff(final IRecordProcessorCheckpointer checkpointer,
            final String atSequenceNumber)
    {
        for (int i = 1; i <= this.checkpointRetryLimit; i++)
        {
            try
            {
                if (atSequenceNumber != null)
                {
                    info("Checkpointing at sequence #" + atSequenceNumber + "...");
                    checkpointer.checkpoint(atSequenceNumber);
                }
                else
                {
                    info("Checkpointing at latest...");
                    checkpointer.checkpoint();
                }
                info("Checkpoint complete.");

                AppConfiguration.INSTANCE.getHealthCheckController().setHealthy(true);
                return;
            }
            catch (final Exception e)
            {
                warn("Checkpoint exception: " + e.getMessage() + ". Trying exponential backoff...");
                SafeUtil.safeSleep(Math.round(Math.pow(2, this.checkpointRetryLimit) * 100));
            }
        }

        error("Unable to checkpoint!");
        AppConfiguration.INSTANCE.getHealthCheckController().setHealthy(false);
    }

    /**
     * Use the specified emitter to emit the current buffer contents to the destination
     * and then checkpoint.
     * 
     * @param checkpointer The checkpointer from the KCL
     */
    protected void emit(final IRecordProcessorCheckpointer checkpointer)
    {
        List<T> emitItems = this.buffer.getRecords();

        info("Flushing " + emitItems.size() + " items to destination.");
        try
        {
            //retry emits with an exponential backoff
            for (int numTries = 1; numTries <= this.emitRetryLimit; numTries++)
            {
                emitItems = this.emitter.emit(buffer);
                if (emitItems.isEmpty())
                {
                    if (numTries > 1)
                    {
                        info("Emit successful after retry (" + numTries + ")");
                    }
                    break;
                }

                warn("Error emitting " + emitItems.size() + " items. Re-trying with exponential backoff...");
                SafeUtil.safeSleep(Math.round(Math.pow(2, this.emitRetryLimit) * 100));
            }

            //notify on any items that failed
            if (!emitItems.isEmpty())
            {
                warn("Error emitting " + emitItems.size() + " items after " + this.emitRetryLimit + " attempts.");
                this.emitter.fail(emitItems);
            }
        }
        catch (final Exception e)
        {
            error("Failed to emit records: " + e.getMessage(), e);
            this.emitter.fail(emitItems);
        }

        submitMetric("NumFailedRecords", StandardUnit.Count, emitItems.size());

        // checkpoint once all the records have been consumed
        try
        {
            String lastSequenceNumber = this.buffer.getLastSequenceNumber();
            this.buffer.clear();

            if (checkpointer != null)
            {
                checkpointWithExponentialBackoff(checkpointer, lastSequenceNumber);
            }
        }
        catch (final Exception e)
        {
            error("Failed to checkpoint after emit: " + e.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown(final ShutdownInput input)
    {
        info("Shutting down record processor with shardId: " + this.shardId + " with reason " + input.getShutdownReason());
        switch (input.getShutdownReason())
        {
            case TERMINATE:
                emit(input.getCheckpointer());
                break;
            case ZOMBIE:
                break;
            default:
                throw new IllegalStateException("invalid shutdown reason");
        }

        this.emitter.shutdown();
        this.metricRecorder.shutdown();
    }

    /**
     * Given a Kinesis record, translate it to a KinesisEvent object.
     */
    protected KinesisEvent getKinesisEventFromRecord(final Record record)
            throws TelemetryEventParseException, TelemetryEventValidationException, TelemetryEventSerializationException
    {
        String recordJson = getJsonFromRecord(record);

        KinesisEvent event = KinesisEventFactory.createEvent(this.shardId, record);
        event.parseFromJson(this.jsonMapper, recordJson);

        return event;
    }

    /**
     * Given a Kinesis record, translate it to a UTF-8 JSON string.
     */
    protected static String getJsonFromRecord(final Record record) throws TelemetryEventParseException
    {
        String rawJson = "";

        ByteBuffer recordBytes = record.getData();
        try
        {
            rawJson = DecoderUtil.getUtf8FromByteBuffer(recordBytes);
        }
        catch (final Exception e)
        {
            throw new TelemetryEventParseException(
                    "Could not parse incoming event (seq num= " + record.getSequenceNumber() + ").", e);
        }

        return rawJson;
    }

    /**
     * Given a Kinesis record, translate it to a hex string from record bytes.
     */
    protected String getRawHexFromRecord(final Record record)
    {
        ByteBuffer recordBytes = record.getData();
        recordBytes.rewind();
        return HexUtil.toHexFromBytes(recordBytes);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processRecords(final ProcessRecordsInput input)
    {
        // Need a try/catch-all to avoid missing records by letting an exception
        // propagate back up into the KCL framework from this method
        try
        {
            List<Record> records = input.getRecords();

            int numRecords = records.size();
            if (numRecords > 0)
            {
                info("Processing " + numRecords + " records from Kinesis..." + "(# Records Until Flush="
                        + this.buffer.getNumRecordsUntilFlush() + ", Seconds To Next Flush="
                        + this.buffer.getMillisecondsUntilFlush() / 1000 + ", Bytes To Next Flush="
                        + this.buffer.getNumBytesUntilFlush() + ")");

                submitMetric("NumRecordsReceived", StandardUnit.Count, numRecords);
                submitMetric("MillisBehindLatest", StandardUnit.Milliseconds, input.getMillisBehindLatest());
            }

            int totalParseFailures = 0;
            int totalProcessFailures = 0;
            int totalProcessSuccess = 0;
            for (Record record : records)
            {
                try
                {
                    processRecord(record);
                    totalProcessSuccess++;
                }
                catch (final TelemetryEventParseException e)
                {
                    totalParseFailures++;
                    error("Failed to parse bad JSON record (" + record.getSequenceNumber() + "): " + e.getMessage());
                }
                catch (final TelemetryEventValidationException e)
                {
                    totalParseFailures++;
                    error("Validation failed on record (" + record.getSequenceNumber() + "): " + e.getMessage());
                }
                catch (TelemetryEventSerializationException e)
                {
                    totalParseFailures++;
                    error("Serialization failed on record (" + record.getSequenceNumber() + "): " + e.getMessage());
                }
                catch (Exception e)
                {
                    totalProcessFailures++;
                    error("Encountered unexpected exception processing record: " + e.getMessage(), e);
                }
            }

            submitMetric("ProcessRecord.Success", StandardUnit.Count, totalProcessSuccess);
            submitMetric("ParseRecord.Failure", StandardUnit.Count, totalParseFailures);
            submitMetric("ProcessRecord.Failure", StandardUnit.Count, totalProcessFailures);

            //Check if it's time to flush and if so, emit records to the destination
            if (this.buffer.shouldFlush())
            {
                emit(input.getCheckpointer());
            }

            //Flush metrics to CloudWatch also
            this.metricRecorder.attemptFlush();
        }
        catch (final Exception e)
        {
            error("Encountered unexpected exception: " + e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    protected abstract void processRecord(Record record) throws TelemetryEventParseException,
            TelemetryEventValidationException, TelemetryEventSerializationException;

    protected String getComponentName()
    {
        return this.componentName;
    }

    //////////////////////////////////////////////
    // These are all convenience methods for logging
    // that will prepend the Shard ID onto the log
    // message.
    //////////////////////////////////////////////

    protected abstract Logger getLogger();

    protected void info(final String msg)
    {
        info(msg, null);
    }

    protected void info(final String msg, final Throwable t)
    {
        getLogger().info("[Shard " + this.shardId + "] " + msg, t);
    }

    protected void warn(final String msg)
    {
        warn(msg, null);
    }

    protected void warn(final String msg, final Throwable t)
    {
        getLogger().warn("[Shard " + this.shardId + "] " + msg, t);
    }

    protected void error(final String msg)
    {
        error(msg, null);
    }

    protected void error(final String msg, final Throwable t)
    {
        getLogger().error("[Shard " + this.shardId + "] " + msg, t);
    }
}
