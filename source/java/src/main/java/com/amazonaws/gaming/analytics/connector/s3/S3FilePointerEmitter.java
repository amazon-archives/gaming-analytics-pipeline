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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.amazonaws.gaming.analytics.common.AppConfiguration;
import com.amazonaws.gaming.analytics.common.AwsUtil;
import com.amazonaws.gaming.analytics.common.exception.TelemetryEventSerializationException;
import com.amazonaws.gaming.analytics.connector.IBuffer;
import com.amazonaws.gaming.analytics.connector.redshift.RedshiftLoadEvent;
import com.amazonaws.gaming.analytics.proprioception.MetricRecorder;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.google.common.base.Stopwatch;

/**
 * An add-on to the standard S3 Emitter; in addition to using the S3Emitter base class to emit
 * batches of telemetry events to S3, this class also emits the corresponding S3 file pointer downstream
 * to a seconddary Kinesis stream.
 * 
 * @author AWS Solutions Team
 * 
 */
public class S3FilePointerEmitter extends S3Emitter
{
    private static final Logger log = LogManager.getLogger(S3FilePointerEmitter.class);

    private final AmazonKinesis kinesisClient;
    private final String filePointerStream;

    public S3FilePointerEmitter(final String bucketName, final String shardId, final String componentName,
            final MetricRecorder metricRecorder)
    {
        super(bucketName, shardId, componentName, metricRecorder);

        this.filePointerStream = AppConfiguration.INSTANCE.getString("kinesis_file_stream");

        this.kinesisClient = AmazonKinesisClientBuilder.standard()
                .withCredentials(AppConfiguration.INSTANCE.getCredentialsProvider())
                .withRegion(AppConfiguration.INSTANCE.getString("aws_region_name"))
                .withClientConfiguration(AwsUtil.getDefaultClientConfig()).build();

        info("Emitting to manifest file stream " + this.filePointerStream);
    }

    /**
     * ${@inheritDoc}
     */
    @Override
    public List<byte[]> emit(final IBuffer<byte[]> buffer) throws IOException
    {
    	//Emit telemetry events to S3 using the base class
        List<byte[]> failed = super.emit(buffer);
        if (!failed.isEmpty())
        {
            return buffer.getRecords();
        }

        String s3File = getS3FileName(buffer);
      
        //construct the downstream file pointer event
        String json;
        try
        {
            RedshiftLoadEvent event = new RedshiftLoadEvent(s3File);
            json = event.toJson(this.jsonMapper);
        }
        catch (TelemetryEventSerializationException e)
        {
            error("Could not generate RedshiftLoadEvent for input string: " + s3File);
            return buffer.getRecords();
        }

        // wrap the name of the Amazon S3 file pointer event as the record data
        ByteBuffer data = ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8));

        // Put the list of file names to the manifest Amazon Kinesis stream
        PutRecordRequest putRecordRequest = new PutRecordRequest();
        putRecordRequest.setData(data);
        putRecordRequest.setStreamName(this.filePointerStream);

        // Use constant partition key to ensure file order within shards
        // putRecordRequest.setPartitionKey(this.manifestStream);
        // Or use random partition key to load balance across shards
        putRecordRequest.setPartitionKey(UUID.randomUUID().toString());

        //emit file pointer record downstream
        boolean kinesisUploadSuccess = true;
        try
        {
            Stopwatch timer = Stopwatch.createStarted();
            this.kinesisClient.putRecord(putRecordRequest);
            long kinesisUploadTime = timer.stop().elapsed(TimeUnit.MILLISECONDS);

            submitMetric("KinesisUploadTime", StandardUnit.Milliseconds, kinesisUploadTime);
            info("S3FilePointerEmitter emitted record downstream: " + json);
            return Collections.emptyList();
        }
        catch (Exception e)
        {
            kinesisUploadSuccess = false;
            error("Could not emit record: " + e.getMessage(), e);
            return buffer.getRecords();
        }
        finally
        {
            submitMetric("KinesisUploadAvailability", StandardUnit.Count, kinesisUploadSuccess ? 1 : 0);
        }
    }

    /**
     * ${@inheritDoc}
     */
    @Override
    public void fail(List<byte[]> records)
    {
        super.fail(records);
    }

    /**
     * ${@inheritDoc}
     */
    @Override
    public void shutdown()
    {
        super.shutdown();

        this.kinesisClient.shutdown();
    }

    /**
     * ${@inheritDoc}
     */
    @Override
    protected Logger getLogger()
    {
        return log;
    }
}
