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

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * An objectified representation of a solution metric sent to AWS solution metrics backend.
 * 
 * @author AWS Solutions Team
 */
public class SolutionMetric
{
    public static final String NAMESPACE_JSON_KEY = "Namespace";
    public static final String NAME_JSON_KEY = "Name";
    public static final String UNITS_JSON_KEY = "Units";
    public static final String AVERAGE_JSON_KEY = "Average";
    public static final String MAXIMUM_JSON_KEY = "Maximum";
    public static final String MINIMUM_JSON_KEY = "Minimum";
    public static final String SAMPLE_COUNT_JSON_KEY = "SampleCount";
    public static final String SUM_JSON_KEY = "Sum";
    
    private final Map<String,String> attributes;
    private final Map<String,Double> metrics;
    
    public SolutionMetric()
    {
        this.attributes = new TreeMap<>();
        this.metrics = new TreeMap<>();
    }
    
    public void addAttributes(final Dimension... dimensions)
    {
        for(Dimension d : dimensions)
        {
            this.attributes.put(d.getName(), d.getValue());
        }
    }
    
    public void addAttribute(final String key, final String value)
    {
        this.attributes.put(key, value);
    }
    
    public void addMetric(final String key, final Double value)
    {
        this.metrics.put(key, value);
    }
    
    public void setFromCloudWatch(String namespace, String metricName, List<Dimension> dimensions, Datapoint dp)
    {        
        addAttribute(NAMESPACE_JSON_KEY, namespace);
        addAttribute(NAME_JSON_KEY, metricName);
        addAttributes(dimensions.toArray(new Dimension[] {}));
        
        if(dp.getUnit() != null)
        {
            addAttribute(UNITS_JSON_KEY, dp.getUnit());
        }
        
        if(dp.getAverage() != null)
        {
            addMetric(AVERAGE_JSON_KEY, dp.getAverage());
        }
        
        if(dp.getMaximum() != null)
        {
            addMetric(MAXIMUM_JSON_KEY, dp.getMaximum());
        }
        
        if(dp.getMinimum() != null)
        {
            addMetric(MINIMUM_JSON_KEY, dp.getMinimum());
        }
        
        if(dp.getSampleCount() != null)
        {
            addMetric(SAMPLE_COUNT_JSON_KEY, dp.getSampleCount());
        }
        
        if(dp.getSum() != null)
        {
            addMetric(SUM_JSON_KEY, dp.getSum());
        }
    }
    
    public ObjectNode toJsonNode(final ObjectMapper jsonSerializer)
    {
        ObjectNode dataNode = jsonSerializer.createObjectNode();
        
        for(Map.Entry<String, String> entry : this.attributes.entrySet())
        {
            dataNode.put(entry.getKey(), entry.getValue());
        }
        
        for(Map.Entry<String, Double> entry : this.metrics.entrySet())
        {
            dataNode.put(entry.getKey(), entry.getValue());
        }
        
        return dataNode;
    }
    
    public String toJson(final ObjectMapper jsonSerializer) throws JsonProcessingException
    {
        ObjectNode dataNode = toJsonNode(jsonSerializer);        
        return jsonSerializer.writeValueAsString(dataNode);
    }
}
