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

import java.util.Map;
import java.util.TreeMap;

import org.joda.time.DateTime;

import com.amazonaws.services.cloudwatch.model.StandardUnit;

/**
 * An objectified representation of a single CloudWatch metric.
 * 
 * @author AWS Solutions Team
 * 
 * @see http://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_MetricDatum.html
 */
public class Metric
{
    private Map<String, String> dimensions;
    private String name;
    private DateTime timestamp;
    private StandardUnit units;
    private double value;

    public Metric()
    {
        this.dimensions = new TreeMap<>();
    }

    public Map<String, String> getDimensions()
    {
        return dimensions;
    }

    public void addDimension(String key, String value)
    {
        if (this.dimensions != null)
        {
            this.dimensions.put(key, value);
        }
    }

    public Metric withDimension(String key, String value)
    {
        addDimension(key, value);
        return this;
    }

    public void setDimensions(Map<String, String> dimensions)
    {
        this.dimensions = dimensions;
    }

    public Metric withDimensions(Map<String, String> dimensions)
    {
        setDimensions(dimensions);
        return this;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public Metric withName(String name)
    {
        setName(name);
        return this;
    }

    public DateTime getTimestamp()
    {
        return timestamp;
    }

    public void setTimestamp(DateTime timestamp)
    {
        this.timestamp = timestamp;
    }

    public Metric withTimestamp(DateTime timestamp)
    {
        setTimestamp(timestamp);
        return this;
    }

    public StandardUnit getUnits()
    {
        return units;
    }

    public void setUnits(StandardUnit units)
    {
        this.units = units;
    }

    public Metric withUnits(StandardUnit units)
    {
        setUnits(units);
        return this;
    }

    public double getValue()
    {
        return value;
    }

    public void setValue(double value)
    {
        this.value = value;
    }

    public Metric withValue(double value)
    {
        setValue(value);
        return this;
    }
}
