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
package com.amazonaws.gaming.analytics.connector.errorhandler;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.amazonaws.gaming.analytics.connector.AbstractRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;

/**
 * A factory for generating a standalone ErrorHandler record processor
 * that validates incoming events and writes invalid ones to S3.
 * 
 * @author AWS Solutions Team
 */
public class ErrorHandlerRecordProcessorFactory extends AbstractRecordProcessorFactory
{
    private static final Logger log = LogManager.getLogger(ErrorHandlerRecordProcessorFactory.class);
    
    public static final String CONNECTOR_TYPE = "errorhandler";

    @Override
    public IRecordProcessor createProcessor()
    {
        log.info("Creating new ErrorHandlerRecordProcessorFactory...");
        return new ErrorHandlerRecordProcessor("ErrorConnector", this.metricRecorder);
    }
}
