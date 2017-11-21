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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import com.amazonaws.gaming.analytics.common.AppConfiguration;
import com.amazonaws.gaming.analytics.common.AwsUtil;
import com.amazonaws.gaming.analytics.connector.AbstractEmitter;
import com.amazonaws.gaming.analytics.connector.IBuffer;
import com.amazonaws.gaming.analytics.proprioception.MetricRecorder;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.base.Stopwatch;

/**
 * An emitter implementation that works on buffers full of telemetry events and
 * emits them downstream in batches to an S3 bucket.
 * 
 * @author AWS Solutions Team
 */
public class S3Emitter extends AbstractEmitter<byte[]>
{
    private static final Logger log = LogManager.getLogger(S3Emitter.class);

    private static final int S3_CLIENT_CONNECTION_TIMEOUT = 10000;
    private static final int S3_CLIENT_SOCKET_TIMEOUT = 60000;

    protected final String s3Bucket;
    protected final String eventPathPrefix;
    protected final boolean s3UseGzip;
    protected final AmazonS3 s3Client;

    public S3Emitter(final String bucketName, final String shardId, final String componentName,
            final MetricRecorder metricRecorder)
    {
        super(shardId, componentName, metricRecorder);

        this.s3Bucket = bucketName;
        this.s3UseGzip = true;
        this.eventPathPrefix = AppConfiguration.INSTANCE.getString("s3_event_path_prefix");

        this.s3Client = AmazonS3ClientBuilder.standard()
                		.withCredentials(AppConfiguration.INSTANCE.getCredentialsProvider())
                		.withRegion(AppConfiguration.INSTANCE.getString("aws_region_name"))
                		.withClientConfiguration(AwsUtil.getClientConfig(S3_CLIENT_CONNECTION_TIMEOUT, S3_CLIENT_SOCKET_TIMEOUT))
                		.build();

        info("Emitting to " + this.s3Bucket + " in region " + this.s3Client.getRegionName());
    }

    /**
     * Generate a unique S3 filename based on the Kinesis sequence numbers
     * contained in the buffer.
     */
    protected String getS3FileName(String firstSeq, String lastSeq)
    {
        StringBuilder sb = new StringBuilder(64);
        sb.append(firstSeq);
        sb.append("-");
        sb.append(lastSeq);
        return sb.toString();
    }

    /**
     * Generate a full S3 key based on the contents of the buffer.
     */
    protected String getS3FileName(IBuffer<byte[]> buffer)
    {
        DateTime timestamp = buffer.getFirstTimestamp();
        if (timestamp == null)
        {
            timestamp = DateTime.now();
        }

        StringBuilder sb = new StringBuilder(64);

        sb.append(this.eventPathPrefix);
        sb.append('/');
        sb.append(StringUtils.leftPad(Integer.toString(timestamp.getYear()), 4, '0'));
        sb.append('/');
        sb.append(StringUtils.leftPad(Integer.toString(timestamp.getMonthOfYear()), 2, '0'));
        sb.append('/');
        sb.append(StringUtils.leftPad(Integer.toString(timestamp.getDayOfMonth()), 2, '0'));
        sb.append('/');
        sb.append(StringUtils.leftPad(Integer.toString(timestamp.getHourOfDay()), 2, '0'));
        sb.append('/');
        sb.append(buffer.getFirstSequenceNumber());
        sb.append("-");
        sb.append(buffer.getLastSequenceNumber());
        sb.append(this.s3UseGzip ? ".gzip" : ".json");

        return sb.toString();
    }

    /**
     * Generate a full S3 URI file pointer based on the input filename.
     */
    protected String getS3URI(String s3FileName)
    {
        StringBuilder sb = new StringBuilder(64);
        sb.append("s3://");
        sb.append(this.s3Bucket);
        sb.append("/");
        sb.append(s3FileName);
        return sb.toString();
    }

    /**
     * ${@inheritDoc}
     */
    @Override
    public List<byte[]> emit(final IBuffer<byte[]> buffer) throws IOException
    {
        boolean emitSuccess = true;
        try
        {
        	//calculate buffer size
            int byteSize = 0;
            List<byte[]> records = buffer.getRecords();
            for (byte[] record : records)
            {
                byteSize += record.length;
            }

            submitMetric("NumRecordsReceived", StandardUnit.Count, records.size());
            submitMetric("NumRecordBytesReceived", StandardUnit.Bytes, byteSize);

            Stopwatch timer = Stopwatch.createUnstarted();

            ByteArrayOutputStream byteStream = new ByteArrayOutputStream(byteSize);
            
            //write all of the records to a compressed output stream
            if (this.s3UseGzip)
            {
                timer.reset().start();
                try (GZIPOutputStream zipStream = new GZIPOutputStream(byteStream))
                {
                    for (byte[] record : records)
                    {
                        try
                        {
                            zipStream.write(record);
                        }
                        catch (Exception e)
                        {
                            emitSuccess = false;
                            error("Error writing record to output stream. Failing this emit attempt. Record: "
                                    + Arrays.toString(record), e);
                            return buffer.getRecords();
                        }
                    }
                    zipStream.flush();
                }
                long zipTime = timer.stop().elapsed(TimeUnit.MILLISECONDS);
                submitMetric("FileCompressTime", StandardUnit.Milliseconds, zipTime);
            }
            //write all the records to an uncompressed output stream
            else
            {
                for (byte[] record : records)
                {
                    try
                    {
                        byteStream.write(record);
                    }
                    catch (Exception e)
                    {
                        emitSuccess = false;
                        error("Error writing record to output stream. Failing this emit attempt. Record: "
                                + Arrays.toString(record), e);
                        return buffer.getRecords();
                    }
                }
            }

            // Get the Amazon S3 filename
            String s3FileName = getS3FileName(buffer);
            String s3URI = getS3URI(s3FileName);
            
            boolean s3UploadSuccess = true;
            try
            {
                timer.reset().start();
                
                //upload the event batch to S3
                byte[] bytesToSend = byteStream.toByteArray();
                ByteArrayInputStream object = new ByteArrayInputStream(bytesToSend);
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentLength(bytesToSend.length);
                this.s3Client.putObject(s3Bucket, s3FileName, object, metadata);
                
                long s3UploadTime = timer.stop().elapsed(TimeUnit.MILLISECONDS);

                submitMetric("S3FileUploadTime", StandardUnit.Milliseconds, s3UploadTime);
                info("Successfully emitted " + records.size() + " records (" + byteSize + " bytes) to Amazon S3 at "
                        + s3URI);
                return Collections.emptyList();
            }
            catch (Exception e)
            {
                s3UploadSuccess = false;
                emitSuccess = false;
                error("Caught exception when uploading file " + s3URI + "to Amazon S3. Failing this emit attempt.", e);
                return buffer.getRecords();
            }
            finally
            {
                submitMetric("S3UploadAvailability", StandardUnit.Count, s3UploadSuccess ? 1 : 0);
            }
        }
        catch (Exception e)
        {
            emitSuccess = false;
            error("Uncaught exception when uploading file: " + e.getMessage(), e);
            return buffer.getRecords();
        }
        finally
        {
            submitMetric("EmitAvailability", StandardUnit.Count, emitSuccess ? 1 : 0);
        }
    }

    /**
     * ${@inheritDoc}
     */
    @Override
    public void fail(List<byte[]> records)
    {
        for (byte[] record : records)
        {
            error("Record failed: " + Arrays.toString(record));
        }
    }

    /**
     * ${@inheritDoc}
     */
    @Override
    public void shutdown()
    {
    	this.s3Client.shutdown();
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
