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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.gaming.analytics.connector.AbstractRecordProcessorFactory;
import com.amazonaws.gaming.analytics.connector.compound.CompoundRecordProcessor;
import com.amazonaws.gaming.analytics.connector.errorhandler.ErrorHandlerRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;

/**
 * A custom compound record processor factory that combines the S3RecordProcessor
 * and the ErrorHandlerRecordProcessor to parse events and emit both good event
 * batches and error event batches to S3.
 * 
 * @author AWS Solutions Team
 */
public class S3CompoundRecordProcessorFactory extends AbstractRecordProcessorFactory
{
    private static final Log log = LogFactory.getLog(S3CompoundRecordProcessorFactory.class);

    public static final String CONNECTOR_TYPE = "s3compound";
    
    @Override
    public IRecordProcessor createProcessor()
    {
        log.info("Creating new CompoundRecordProcessor (S3 + Errors)...");

        CompoundRecordProcessor crp = new CompoundRecordProcessor();
        crp.addRecordProcessor(new S3RecordProcessor("S3Connector", this.metricRecorder));
        crp.addRecordProcessor(new ErrorHandlerRecordProcessor("ErrorConnector", this.metricRecorder));
        return crp;
    }
}
