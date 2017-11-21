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
package com.amazonaws.gaming.analytics.proprioception;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.Future;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.gaming.analytics.common.AppConfiguration;
import com.amazonaws.gaming.analytics.common.AwsUtil;
import com.amazonaws.gaming.analytics.common.SafeUtil;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsync;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsyncClientBuilder;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.PutMetricDataResult;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.Queues;

/**
 * An object that allows batching and pushing of metric data to CloudWatch. Has the ability
 * to send events synchronously on the current thread or asynchronously on a background
 * thread.
 * 
 * @author AWS Solutions Team
 */
public class MetricRecorder
{
    private static final Logger log = LogManager.getLogger(MetricRecorder.class);

    private static final String METRIC_QUEUE_SIZE_PROPERTY = "cloudwatch.metric_queue_size";
    private static final String METRIC_QUEUE_TIMEOUT_MILLIS_PROPERTY = "cloudwatch.metric_queue_timeout_millis";
    private static final String METRIC_BATCH_SIZE_PROPERTY = "cloudwatch.metric_batch_size";
    private static final String METRIC_THREAD_POOL_SIZE_PROPERTY = "cloudwatch.metric_thread_pool_size";
    private static final String MAX_DIMENSIONS_PER_METRIC_PROPERTY = "cloudwatch.max_dimensions_per_metric";
    private static final String MAX_DATUM_COUNT_PER_REQUEST_PROPERTY = "cloudwatch.max_datum_count_per_request";
    private static final String SHUTDOWN_WAIT_TIME_MILLIS = "cloudwatch.shutdown_wait_time_millis";

    private final int maxDimensionsPerMetric;
    private final int maxDatumCountPerRequest;
    private final long metricQueueTimeoutMillis;
    private final int metricBatchSize;
    private final boolean asynchronousMode;

    private String namespace;

    private final Map<String, String> globalDimensions;
    private final Queue<Metric> pendingMetrics;
    private List<Future<PutMetricDataResult>> pendingRequests;
    private long lastTransmitTimestamp;
    private long shutdownWaitTimeMillis;
    private int threadPoolSize;

    private final AmazonCloudWatchAsync cwClient;
    private final Object flushLock;
    private boolean flushInProgress;

    public MetricRecorder(String namespace)
    {
        this(namespace, true);
    }

    public MetricRecorder(String namespace, boolean asynchronousMode)
    {
        this.namespace = namespace;

        this.maxDimensionsPerMetric = AppConfiguration.INSTANCE.getInt(MAX_DIMENSIONS_PER_METRIC_PROPERTY);
        this.maxDatumCountPerRequest = AppConfiguration.INSTANCE.getInt(MAX_DATUM_COUNT_PER_REQUEST_PROPERTY);
        this.metricBatchSize = Math.min(this.maxDatumCountPerRequest,
                AppConfiguration.INSTANCE.getInt(METRIC_BATCH_SIZE_PROPERTY));
        this.metricQueueTimeoutMillis = AppConfiguration.INSTANCE.getLong(METRIC_QUEUE_TIMEOUT_MILLIS_PROPERTY);
        this.threadPoolSize = AppConfiguration.INSTANCE.getInt(METRIC_THREAD_POOL_SIZE_PROPERTY);
        this.shutdownWaitTimeMillis = AppConfiguration.INSTANCE.getLong(SHUTDOWN_WAIT_TIME_MILLIS);

        this.asynchronousMode = asynchronousMode;
        this.globalDimensions = new TreeMap<>();
        this.pendingMetrics = Queues
                .synchronizedQueue(EvictingQueue.create(AppConfiguration.INSTANCE.getInt(METRIC_QUEUE_SIZE_PROPERTY)));
        this.lastTransmitTimestamp = System.currentTimeMillis();

        this.pendingRequests = new LinkedList<>();
        this.flushLock = new Object();
        this.flushInProgress = false;

        ClientConfiguration cwConfig = AwsUtil.getDefaultClientConfig().withMaxConnections(this.threadPoolSize);

        this.cwClient = AmazonCloudWatchAsyncClientBuilder.standard()
        			    .withClientConfiguration(cwConfig)
        			    .withCredentials(AppConfiguration.INSTANCE.getCredentialsProvider())
        			    .withRegion(AppConfiguration.INSTANCE.getString("aws_region_name"))
        			    .build();

        log.info("Created new MetricRecorder with namespace \"" + namespace + "\"");
    }

    /**
     * Add a dimension that will be applied to ALL metrics send from this class.
     */
    public void addGlobalDimension(String key, String value)
    {
        this.globalDimensions.put(key, value);
    }
    
    /**
     * Create a new baseline metric object based on the settings in this class.
     * 
     * @see #createMetric(String, StandardUnit, DateTime)
     */
    public Metric createMetric(String name, StandardUnit units)
    {
        return createMetric(name, units, DateTime.now());
    }

    /**
     * Create a new baseline metric object based on the settings in this class.
     */
    public Metric createMetric(String name, StandardUnit units, DateTime timestamp)
    {
        Metric m = new Metric().withName(name).withUnits(units).withTimestamp(timestamp);

        for (Map.Entry<String, String> dimension : this.globalDimensions.entrySet())
        {
            m.addDimension(dimension.getKey(), dimension.getValue());
        }

        return m;
    }

    /**
     * Add a new metric to the buffer to be transmitted.
     */
    public void putMetric(Metric m)
    {
        log.debug("putMetric(" + m.getName() + ", " + m.getUnits() + ", " + m.getValue() + ")");
        this.pendingMetrics.add(m);
        attemptFlush();

    }

    /**
     * Shut down the recorder and wait for any background threads (async mode only) to terminate.
     */
    public void shutdown()
    {
        // Try one last flush...we might not get everything out, but it's not a big deal if we don't
        attemptFlush(true);

        if (this.asynchronousMode)
        {
            // Wait for the flush to finish
            long waitStartTime = System.currentTimeMillis();
            synchronized (this.pendingRequests)
            {
                while (!this.pendingRequests.isEmpty()
                        && (System.currentTimeMillis() - waitStartTime) < this.shutdownWaitTimeMillis)
                {
                    SafeUtil.safeWait(this.pendingRequests, this.shutdownWaitTimeMillis);
                }
            }
        }
    }

    /**
     * Check if the buffer needs to be flushed and if it does, flush it.
     * 
     * @see #attemptFlush(boolean)
     */
    public void attemptFlush()
    {
        attemptFlush(false);
    }

    
    /**
     * Check if the buffer needs to be flushed and if it does, flush it. Optionally
     * force it to flush.
     * 
     * @see #attemptFlush(boolean)
     */
    public void attemptFlush(boolean force)
    {
    	boolean batchSizeExceeded = this.pendingMetrics.size() >= this.metricBatchSize;
    	boolean timeoutExceeded = System.currentTimeMillis() - this.lastTransmitTimestamp > this.metricQueueTimeoutMillis;
        boolean shouldFlush = force || batchSizeExceeded || timeoutExceeded;

        if (!shouldFlush || this.pendingMetrics.isEmpty())
        {
            return;
        }

        //make sure we don't attempt multiple flushes at once
        synchronized (this.flushLock)
        {
            if (this.flushInProgress)
            {
                return;
            }
            this.flushInProgress = true;
        }

        //It's flush time; gather up the pending metrics and send them out
        try
        {
            int flushSize = Math.min(this.metricBatchSize, this.pendingMetrics.size());
            log.info("Flushing " + flushSize + " metrics...");

            List<Metric> metricsToFlush = new ArrayList<>(flushSize);
            while (metricsToFlush.size() < flushSize)
            {
                Metric m = this.pendingMetrics.poll();
                if (m == null)
                {
                    break;
                }
                metricsToFlush.add(m);
            }

            sendMetricsToCloudWatch(metricsToFlush);
            this.lastTransmitTimestamp = System.currentTimeMillis();
        }
        finally
        {
            synchronized (this.flushLock)
            {
                this.flushInProgress = false;
            }
        }
    }

    /**
     * Send a batch of metric objects to CloudWatch in a single request
     */
    private void sendMetricsToCloudWatch(List<Metric> metrics)
    {
        PutMetricDataRequest request = new PutMetricDataRequest();
        request.setNamespace(this.namespace);

        //format the metrics per the CloudWatch API
        List<MetricDatum> data = new ArrayList<>(metrics.size());
        for (Metric metric : metrics)
        {
            MetricDatum datum = new MetricDatum()
            					.withMetricName(metric.getName())
            					.withTimestamp(metric.getTimestamp().toDate())
            					.withValue(metric.getValue())
            					.withUnit(metric.getUnits());

            Map<String, String> metricDimensions = metric.getDimensions();
            final List<Dimension> dimensions = new ArrayList<>(metricDimensions.size());
            for (Entry<String, String> entry : metricDimensions.entrySet())
            {
                if (dimensions.size() == this.maxDimensionsPerMetric)
                {
                    break;
                }

                Dimension d = new Dimension()
                				  .withName(entry.getKey())
                				  .withValue(entry.getValue());
                dimensions.add(d);
            }

            datum.setDimensions(dimensions);
            data.add(datum);
        }

        request.setMetricData(data);

        //send the metrics out
        if (this.asynchronousMode)
        {
            log.info("Sending metrics to CloudWatch asynchronously...");
            Future<PutMetricDataResult> future = this.cwClient.putMetricDataAsync(request, new CloudWatchRequestAsyncHandler());
            synchronized (this.pendingRequests)
            {
                this.pendingRequests.add(future);
            }
        }
        else
        {
            log.info("Sending metrics to CloudWatch synchronously...");
            
            //NOTE: we create an async handler, but just call it synchronously ourselves after blocking
            CloudWatchRequestAsyncHandler resultHandler = new CloudWatchRequestAsyncHandler();
            try
            {
                this.cwClient.putMetricData(request);
                resultHandler.onSuccess(request, null);
            }
            catch (Exception ex)
            {
                resultHandler.onError(ex);
            }
        }
    }

    /**
     * Helper class to track all pending metric requests.
     */
    private class CloudWatchRequestAsyncHandler implements AsyncHandler<PutMetricDataRequest, PutMetricDataResult>
    {
    	/**
    	 * ${@inheritDoc}
    	 */
        @Override
        public void onError(Exception e)
        {
            log.error("Error putting data into Cloudwatch: " + e.getMessage(), e);
            clearCompleteRequests();
        }

        /**
    	 * ${@inheritDoc}
    	 */
        @Override
        public void onSuccess(PutMetricDataRequest request, PutMetricDataResult result)
        {
            log.info("Successfully put " + request.getMetricData().size() + " data into CloudWatch.");
            clearCompleteRequests();
        }

        /**
         * Loop through the list of pending requests and clear out any that are finished
         */
        private void clearCompleteRequests()
        {
            synchronized (pendingRequests)
            {
                pendingRequests.removeIf((f) -> f.isDone());
                pendingRequests.notify();
            }
        }
    }
}
