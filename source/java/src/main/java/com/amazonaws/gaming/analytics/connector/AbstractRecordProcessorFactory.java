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

import java.net.InetAddress;
import java.security.Security;
import java.util.UUID;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.gaming.analytics.common.AppConfiguration;
import com.amazonaws.gaming.analytics.common.exception.ApplicationInitializationException;
import com.amazonaws.gaming.analytics.proprioception.MetricRecorder;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;

/**
 * An implementation of IRecordProcessorFactory from the KCL that contains all the logic
 * about how we configure our KCL usage and connection to the stream.
 * 
 * @author AWS Solutions Team
 */
public abstract class AbstractRecordProcessorFactory implements IRecordProcessorFactory
{
    private static final Logger log = LogManager.getLogger(AbstractRecordProcessorFactory.class);

    protected final KinesisClientLibConfiguration kinesisConfig;
    protected final AWSCredentialsProvider credentialsProvider;
    protected final MetricRecorder metricRecorder;

    public AbstractRecordProcessorFactory()
    {
        this.credentialsProvider = AppConfiguration.INSTANCE.getCredentialsProvider();

        String appName = AppConfiguration.INSTANCE.getString("kinesis_app_name");
        if (appName == null)
        {
            appName = String.format("%s_%s", AppConfiguration.INSTANCE.getProjectName(), AppConfiguration.INSTANCE.getConnectorType());
        }
        log.info("KCL App Name = " + appName);

        final String streamName = AppConfiguration.INSTANCE.getString("kinesis_input_stream");
        if (streamName == null)
        {
            throw new ApplicationInitializationException("Missed required setting for \"kinesis_input_stream\"");
        }
        log.info("KCL Input Kinesis Stream Name = " + streamName);

        String workerId = UUID.randomUUID().toString();
        try
        {
            workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + workerId;
        }
        catch (final Exception e)
        {
            log.warn("Couldn't generate workerId from hostname; falling back to random ID: " + workerId);
        }
        log.info("KCL Worker ID = " + workerId);

        this.kinesisConfig = new KinesisClientLibConfiguration(appName, streamName, this.credentialsProvider, workerId);
                
        //necessary for time-based buffering of records
        this.kinesisConfig.withCallProcessRecordsEvenForEmptyRecordList(true);

        //NOTE: that this only affects first boot of a new application...from that point forward the checkpoint
        //stored by the KCL dictates where the app will start reading from
        final String streamPosition = AppConfiguration.INSTANCE.getString("kinesis_initial_stream_position");
        if (InitialPositionInStream.TRIM_HORIZON.toString().equalsIgnoreCase(streamPosition))
        {
            this.kinesisConfig.withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);
        }
        else
        {
            this.kinesisConfig.withInitialPositionInStream(InitialPositionInStream.LATEST);
        }
        log.info("Kinesis Stream Position = " + this.kinesisConfig.getInitialPositionInStream());

        String regionName = AppConfiguration.INSTANCE.getString("aws_region_name");
        if (regionName == null)
        {
            throw new ApplicationInitializationException("Missed required setting for \"aws_region_name\"");
        }
        this.kinesisConfig.withRegionName(regionName);
        log.info("Kinesis Region = " + this.kinesisConfig.getRegionName());

        final int networkCacheTtl = AppConfiguration.INSTANCE.getInt("network_cache_ttl", 60);
        Security.setProperty("networkaddress.cache.ttl", Integer.toString(networkCacheTtl));
        log.info("Network Cache TTL = " + networkCacheTtl);

        if (!AppConfiguration.INSTANCE.hasProperty("kinesis_max_records_per_get"))
        {
            throw new ApplicationInitializationException("Missed required setting for \"kinesis_max_records_per_get\"");
        }
        int maxRecordsPerGet = AppConfiguration.INSTANCE.getInt("kinesis_max_records_per_get");
        this.kinesisConfig.withMaxRecords(maxRecordsPerGet);
        log.info("Kinesis Max Records = " + this.kinesisConfig.getMaxRecords());

        long idleTimeBetweenReadMillis = AppConfiguration.INSTANCE.getLong("kinesis_idle_time_between_reads_millis", 1000L);
        this.kinesisConfig.withIdleTimeBetweenReadsInMillis(idleTimeBetweenReadMillis);

        this.metricRecorder = new MetricRecorder(appName);
        log.info("Created new MetricRecorder");

        log.info("Finished initializing " + getClass().getName());
    }

    public KinesisClientLibConfiguration getKinesisClientLibConfiguration()
    {
        return this.kinesisConfig;
    }
}
