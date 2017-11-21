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
package com.amazonaws.gaming.analytics.connector.redshift;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.amazonaws.gaming.analytics.common.AppConfiguration;
import com.amazonaws.gaming.analytics.common.AwsUtil;
import com.amazonaws.gaming.analytics.common.exception.TelemetryEventParseException;
import com.amazonaws.gaming.analytics.common.exception.TelemetryEventValidationException;
import com.amazonaws.gaming.analytics.connector.AbstractRecordProcessor;
import com.amazonaws.gaming.analytics.connector.MemoryBuffer;
import com.amazonaws.gaming.analytics.proprioception.MetricRecorder;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.util.IOUtils;
import com.google.common.io.Resources;

/**
 * A record processor responsible for reading Kinesis stream records in the form of
 * S3 file pointers and emitting them downstream.
 * 
 * @author AWS Solutions Team
 */
public class RedshiftRecordProcessor extends AbstractRecordProcessor<String>
{
    private static final Logger log = LogManager.getLogger(RedshiftRecordProcessor.class);

    public RedshiftRecordProcessor(final String componentName, MetricRecorder metricRecorder)
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
     * In addition to standard initialization, this method also uploads the current
     * JSONPath file to S3.
     * 
     * ${@inheritDoc}
     */
    @Override
    public void initialize(InitializationInput input)
    {
        super.initialize(input);

        try
        {
            UploadCurrentJsonPathFile();
        }
        catch (Exception e)
        {
            error("FAILED to upload new JSONPath file to S3: " + e.getMessage(), e);
        }

        this.emitter = new RedshiftManifestEmitter(input.getShardId(), getComponentName(), this.metricRecorder);
        this.buffer = new MemoryBuffer<>();
    }

    /**
     * Writes a JSONPath file to S3 for use in emitting to Redshift later.
     * 
     * Note that by doing this here, we'll actually upload this file one
     * time per shard in the stream at boot, but since it's the same file
     * being uploaded N times, we really don't need to worry. S3 will handle it.
     */
    protected void UploadCurrentJsonPathFile() throws IOException
    {
        String s3ConfigBucketName = AppConfiguration.INSTANCE.getString("s3_config_bucket");
        String s3JsonPathFileName = AppConfiguration.INSTANCE.getString("jsonpath_filename");

        info("Uploading new JSONPath file to " + s3ConfigBucketName + "/" + s3JsonPathFileName);

        // We only do this once at boot, so there's no reason to cache this client anywhere
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                            .withClientConfiguration(AwsUtil.getDefaultClientConfig())
                            .withCredentials(AppConfiguration.INSTANCE.getCredentialsProvider())
                            .withRegion(AppConfiguration.INSTANCE.getString("aws_region_name"))
                            .build();

        //find the JSONPath file on the Java classpath and read it into memory as a byte array 
        URL jsonPathFile = Resources.getResource(s3JsonPathFileName);
        byte[] jsonPathFileBytes = IOUtils.toByteArray(jsonPathFile.openStream());

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(jsonPathFileBytes.length);

        final PutObjectRequest putObjectRequest = new PutObjectRequest(s3ConfigBucketName, 
                                                                       s3JsonPathFileName,
                                                                       new ByteArrayInputStream(jsonPathFileBytes), 
                                                                       metadata);
        s3Client.putObject(putObjectRequest);
        
        info("JSONPath S3 upload successful!");
    }

    /**
     * Generate a RedshiftLoadEvent from a raw Kinesis record.
     */
    protected RedshiftLoadEvent getRedshiftLoadEventFromRecord(final Record record)
            throws TelemetryEventParseException, TelemetryEventValidationException
    {
        String recordJson = getJsonFromRecord(record);

        RedshiftLoadEvent event = new RedshiftLoadEvent();
        event.setServerTimestamp(record.getApproximateArrivalTimestamp().getTime());
        event.setSequenceNumber(record.getSequenceNumber());
        event.parseFromJson(this.jsonMapper, recordJson);

        return event;
    }

    /**
     * ${@inheritDoc}
     */
    @Override
    public void processRecord(final Record record)
            throws TelemetryEventParseException, TelemetryEventValidationException
    {
        //Buffer up events for emission later
        RedshiftLoadEvent event = getRedshiftLoadEventFromRecord(record);
        this.buffer.consumeRecord(event.getFilename(), 
                                  event.getFilename().length(), 
                                  event.getSequenceNumber(),
                                  DateTime.now().withZone(DateTimeZone.UTC), 
                                  DateTime.now().withZone(DateTimeZone.UTC));
    }
}
