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

import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;

import org.apache.log4j.Logger;

import com.amazonaws.gaming.analytics.common.AppConfiguration;
import com.amazonaws.gaming.analytics.proprioception.Metric;
import com.amazonaws.gaming.analytics.proprioception.MetricRecorder;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * An abstract implementation of an Emitter that will flush records
 * to a specific destination.
 * 
 * @author AWS Solutions Team
 */
public abstract class AbstractEmitter<T> implements IEmitter<T>
{
    protected String shardId;
    private final String componentName;
    protected MetricRecorder metricRecorder;

    protected final ObjectMapper jsonMapper;
    protected final CharsetDecoder utfDecoder;
    
    protected final boolean emitShardLevelMetrics;

    public AbstractEmitter(final String shardId, final String componentName, final MetricRecorder metricRecorder)
    {
        this.shardId = shardId;
        this.componentName = componentName;
        this.metricRecorder = metricRecorder;

        this.jsonMapper = new ObjectMapper();
        this.jsonMapper.configure(SerializationFeature.EAGER_SERIALIZER_FETCH, true);
        this.jsonMapper.configure(SerializationFeature.CLOSE_CLOSEABLE, true);

        this.utfDecoder = StandardCharsets.UTF_8.newDecoder();
        
        this.emitShardLevelMetrics = AppConfiguration.INSTANCE.getBoolean("emit_shard_level_metrics");
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
        Metric numRecordsMetric = this.metricRecorder.createMetric(name, units).withValue(value)
                .withDimension("Operation", "EmitRecords").withDimension("Component", getComponentName());
        this.metricRecorder.putMetric(numRecordsMetric);

        if(this.emitShardLevelMetrics)
        {
	        Metric numRecordsMetricWithShardId = this.metricRecorder.createMetric(name, units).withValue(value)
	                .withDimension("ShardId", this.shardId).withDimension("Operation", "EmitRecords")
	                .withDimension("Component", getComponentName());
	        this.metricRecorder.putMetric(numRecordsMetricWithShardId);
        }
    }

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
