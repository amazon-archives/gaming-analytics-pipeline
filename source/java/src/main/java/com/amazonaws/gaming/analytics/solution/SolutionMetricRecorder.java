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
package com.amazonaws.gaming.analytics.solution;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.gaming.analytics.common.AppConfiguration;
import com.amazonaws.gaming.analytics.common.AwsUtil;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsyncClientBuilder;
import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * A helper class that queries metrics from Amazon CloudWatch metrics and reports them
 * anonymously to the AWS solution metrics backend. Will not be invoked unless user has 
 * opted in to anonymous metric transmission. 
 * 
 * @author AWS Solutions Team
 */
public class SolutionMetricRecorder
{
    public static final Logger log = LoggerFactory.getLogger(SolutionMetricRecorder.class);
    
    public static final String SOLUTION_NAME_JSON_KEY = "Solution";
    public static final String SOLUTION_UUID_JSON_KEY = "UUID";
    public static final String SOLUTION_TIMESTAMP_JSON_KEY = "TimeStamp";
    public static final String SOLUTION_DATA_JSON_KEY = "Data";
    
    private final String solution;
    private final String solutionUuid;
    private final DateTimeFormatter timeFormatter;
    
    private String[] solutionMetricsToCollect;
    private int solutionMetricReportingInterval;
    
    private final URI solutionUri;
    
    private CloseableHttpClient httpClient;
    private RequestConfig requestConfig;
    
    private AmazonCloudWatch cloudwatchClient;
    
    private static final int HTTP_MAX_ROUTES = 1;
    
    public SolutionMetricRecorder()
    {
        this.solution = AppConfiguration.INSTANCE.getString("solution_id");
        this.solutionUuid = AppConfiguration.INSTANCE.getString("solution_uuid");
        this.solutionMetricsToCollect = AppConfiguration.INSTANCE.getList("solution.metric_list");
        this.solutionMetricReportingInterval = AppConfiguration.INSTANCE.getInt("solution.reporting_interval_secs");
        
        this.timeFormatter = DateTimeFormat
                .forPattern(AppConfiguration.INSTANCE.getString("solution.time_format"))
                .withZoneUTC();

        int httpConnectionTimeout = AppConfiguration.INSTANCE.getInt("solution.http_connection_timeout_millis");
        int httpSocketTimeout = AppConfiguration.INSTANCE.getInt("solution.http_socket_timeout_millis");
        
        this.requestConfig = RequestConfig
                                .custom()
                                .setConnectionRequestTimeout(httpConnectionTimeout)
                                .setConnectTimeout(httpConnectionTimeout)
                                .setSocketTimeout(httpSocketTimeout)
                                .build();
        
        String httpHostname = AppConfiguration.INSTANCE.getString("solution.http_endpoint");
        int httpPort = AppConfiguration.INSTANCE.getInt("solution.http_port");
        String httpScheme = AppConfiguration.INSTANCE.getString("solution.http_scheme");
        String httpPath = AppConfiguration.INSTANCE.getString("solution.http_path");        
        try
        {
            this.solutionUri = new URIBuilder()
                                    .setHost(httpHostname)
                                    .setPort(httpPort)
                                    .setScheme(httpScheme)
                                    .setPath(httpPath)
                                    .build();
            log.info("Reporting anonymous metrics to " + this.solutionUri.toString());
        }
        catch (URISyntaxException e)
        {
            log.error("Could not construct solution metric endpoint URI: " + e.getMessage(), e);
            throw new IllegalStateException(e);
        }
    }
    
    public void initialize()
    {
        PoolingHttpClientConnectionManager httpConnManager = new PoolingHttpClientConnectionManager();
        httpConnManager.setMaxTotal(HTTP_MAX_ROUTES);

        this.httpClient = HttpClients
                            .custom()
                            .setConnectionManager(httpConnManager)
                            .build();
        
        ClientConfiguration cwConfig = AwsUtil
                                        .getDefaultClientConfig()
                                        .withMaxConnections(HTTP_MAX_ROUTES);

        this.cloudwatchClient = AmazonCloudWatchAsyncClientBuilder
                        .standard()
                        .withClientConfiguration(cwConfig)
                        .withCredentials(AppConfiguration.INSTANCE.getCredentialsProvider())
                        .withRegion(AppConfiguration.INSTANCE.getString("aws_region_name"))
                        .build();
    }
    
    private String getSolutionMetricConfigName(String metricConfigName, String fieldName)
    {
        StringBuilder sb = new StringBuilder(64);
        sb.append("solution.");
        sb.append(metricConfigName);
        sb.append(".");
        sb.append(fieldName);
        return sb.toString();
    }
    
    /**
     * Query CloudWatch to get all the configured metrics that will be reported to the AWS solution metrics backend.
     */
    private List<SolutionMetric> getSolutionMetrics(final DateTime windowStart, final DateTime windowEnd)
    {
        List<SolutionMetric> solutionMetricsToSend = new LinkedList<>();
        
        for(String metricConfigName : this.solutionMetricsToCollect)
        {
            log.info("Fetching solution metric \"" + metricConfigName + "\"...");
            String metricNamespace = AppConfiguration.INSTANCE.getString(getSolutionMetricConfigName(metricConfigName, "metric_namespace"));
            String metricName = AppConfiguration.INSTANCE.getString(getSolutionMetricConfigName(metricConfigName, "metric_name"));
            String[] dimensions = AppConfiguration.INSTANCE.getList(getSolutionMetricConfigName(metricConfigName, "dimensions"));
            String[] statistics = AppConfiguration.INSTANCE.getList(getSolutionMetricConfigName(metricConfigName, "statistics"));
            
            GetMetricStatisticsRequest gmsRequest = new GetMetricStatisticsRequest()
                                                       .withStartTime(windowStart.toDate())
                                                       .withEndTime(windowEnd.toDate())
                                                       .withMetricName(metricName)
                                                       .withNamespace(metricNamespace)
                                                       .withPeriod(this.solutionMetricReportingInterval)
                                                       .withStatistics(statistics);
            
            //Dimensions are expected to be a list of "name=value,name=value,..." in the config file
            List<Dimension> cloudWatchDimensions = new LinkedList<>();
            for(String dimensionKeyValue : dimensions)
            {
                String[] pieces = dimensionKeyValue.split("=");
                if(pieces.length == 0)
                {
                    log.warn("Invalid dimension value in configuration: " + dimensionKeyValue);
                    continue;
                }
                
                String dimensionName = pieces[0].trim();
                String dimensionValue = pieces[1].trim();
                
                Dimension dimension = new Dimension()
                                            .withName(dimensionName)
                                            .withValue(dimensionValue);
                
                cloudWatchDimensions.add(dimension);
            }
            gmsRequest.setDimensions(cloudWatchDimensions);
            
            log.info("Fetching datapoints from CloudWatch metrics...");
            GetMetricStatisticsResult gmsResult = this.cloudwatchClient.getMetricStatistics(gmsRequest);
            List<Datapoint> datapoints = gmsResult.getDatapoints();
            log.info("Found " + datapoints.size() + " datapoints.");
            for(Datapoint dp : datapoints)
            {
                SolutionMetric solutionMetric = new SolutionMetric();
                solutionMetric.setFromCloudWatch(metricNamespace, metricName, cloudWatchDimensions, dp);                
                solutionMetricsToSend.add(solutionMetric);
            }
        }
        
        return solutionMetricsToSend;
    }
    
    /**
     * Take a set of solution metrics and transmit them to the AWS solution metrics backend.
     */
    private void reportSolutionMetrics(List<SolutionMetric> solutionMetricsToSend)
    {
        log.info("Sending solution metrics to backend...");
        String jsonBody;
        try
        {
            jsonBody = generatePayload(solutionMetricsToSend);
        }
        catch (JsonProcessingException e)
        {
            log.error("Could not serialize solution metrics to JSON: " + e.getMessage(),e);
            return;
        }
        
        HttpPost httpRequest = new HttpPost();
        httpRequest.setURI(this.solutionUri);
        httpRequest.setConfig(this.requestConfig);
        
        log.info("JSON payload = " + jsonBody);
        try
        {
            httpRequest.setEntity(new StringEntity(jsonBody));
            CloseableHttpResponse httpResponse = this.httpClient.execute(httpRequest);
            log.info("Result = " + httpResponse.getStatusLine().toString());
        }
        catch (UnsupportedEncodingException e)
        {
            log.error("Could not encode JSON body for metric reporting: " + e.getMessage(), e);
            return;
        }
        catch (IOException e)
        {
            log.error("Failed to transmit payload to backend: " + e.getMessage(), e);
        }
    }
    
    /**
     * Collect and report solution-oriented metrics to the AWS solution metrics backend.
     */
    public void reportMetrics()
    {
        DateTime windowEnd = DateTime.now();
        DateTime windowStart = windowEnd.minusSeconds(this.solutionMetricReportingInterval);
        
        log.info("Fetching solution metrics from " + windowStart + " to " + windowEnd);
        
        List<SolutionMetric> solutionMetricsToSend = getSolutionMetrics(windowStart, windowEnd);
        
        reportSolutionMetrics(solutionMetricsToSend);
    }
    
    /**
     * Generate a JSON payload in the format expected by the solution metrics backend.
     */
    private String generatePayload(List<SolutionMetric> solutionMetrics) throws JsonProcessingException
    {
        ObjectMapper jsonSerializer = new ObjectMapper();
        
        String formattedTimestamp = this.timeFormatter.print(DateTime.now());
        
        ObjectNode jsonRoot = jsonSerializer.createObjectNode();
        
        jsonRoot.put(SOLUTION_NAME_JSON_KEY, this.solution)
                .put(SOLUTION_UUID_JSON_KEY, this.solutionUuid)
                .put(SOLUTION_TIMESTAMP_JSON_KEY, formattedTimestamp);
        
        ArrayNode dataNode = jsonSerializer.createArrayNode();
        for(SolutionMetric sm : solutionMetrics)
        {
            dataNode.add(sm.toJsonNode(jsonSerializer));
        }        
        jsonRoot.set(SOLUTION_DATA_JSON_KEY, dataNode);
        
        return jsonSerializer.writeValueAsString(jsonRoot);
    }
}
