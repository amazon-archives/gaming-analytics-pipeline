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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.amazonaws.gaming.analytics.proprioception.MetricRecorder;

/**
 * Dummy emitter for testing.  Drops all records and always returns an empty list.
 * 
 * @author AWS Solutions Team
 */
public class NullEmitter extends AbstractEmitter<byte[]>
{
    private static final Logger log = LogManager.getLogger(NullEmitter.class);

    public NullEmitter(final String shardId, final String componentName, final MetricRecorder metricRecorder)
    {
        super(shardId, componentName, metricRecorder);
    }

    @Override
    public List<byte[]> emit(IBuffer<byte[]> buffer) throws IOException
    {
        return Collections.emptyList();
    }

    @Override
    public void fail(List<byte[]> records)
    {
        // do nothing
    }

    @Override
    public void shutdown()
    {
        // do nothing
    }

    @Override
    protected Logger getLogger()
    {
        return log;
    }
}
