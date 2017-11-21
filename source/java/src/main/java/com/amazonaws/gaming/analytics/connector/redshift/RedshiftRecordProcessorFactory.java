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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.amazonaws.gaming.analytics.connector.AbstractRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;

/**
 * A record processor factory for generating redshift record processors.
 * 
 * @author AWS Solutions Team
 */
public class RedshiftRecordProcessorFactory extends AbstractRecordProcessorFactory
{
    private static final Logger log = LogManager.getLogger(RedshiftRecordProcessorFactory.class);

    public static final String CONNECTOR_TYPE = "redshift";
    
    @Override
    public IRecordProcessor createProcessor()
    {
        log.info("Creating new RedshiftRecordProcessor...");
        return new RedshiftRecordProcessor("RedshiftConnector", this.metricRecorder);
    }
}
