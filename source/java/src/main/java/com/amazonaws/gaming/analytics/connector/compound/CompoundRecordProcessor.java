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
package com.amazonaws.gaming.analytics.connector.compound;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;

/**
 * A record processor that can combine and run multiple record processor implementations
 * as one.  This is helpful for multi-purposing EC2 instances for saving cost (e.g. we
 * can run the errorhandler and s3 processors in one environment together).
 * 
 * @author AWS Solutions Team
 */
public class CompoundRecordProcessor implements IRecordProcessor
{
    private static final Log log = LogFactory.getLog(CompoundRecordProcessor.class);

    private List<IRecordProcessor> recordProcessors;

    public CompoundRecordProcessor()
    {
        this.recordProcessors = new LinkedList<>();
    }

    /**
     * Add a new record processor.  Record processors will be executed in the order added.
     * This is a no-op if the record processor has already been added.
     */
    public void addRecordProcessor(final IRecordProcessor recordProcessor)
    {
        if (!this.recordProcessors.contains(recordProcessor))
        {
            log.info("Adding record processor " + recordProcessor.getClass().getName());
            this.recordProcessors.add(recordProcessor);
        }
    }

    /**
     * Remove a child record processor.  This is a no-op if the record processor
     * was not previously added.
     */
    public void removeRecordProcessor(final IRecordProcessor recordProcessor)
    {
        if (this.recordProcessors.contains(recordProcessor))
        {
            log.info("Removing record processor " + recordProcessor.getClass().getName());
            this.recordProcessors.remove(recordProcessor);
        }
    }

    /**
     * Initialize all child record processors.
     * 
     * ${@inheritDoc}
     */
    @Override
    public void initialize(InitializationInput initializationInput)
    {
        for (IRecordProcessor rp : this.recordProcessors)
        {
            rp.initialize(initializationInput);
        }
    }

    /**
     * Hand the same input to all child record processors.
     * 
     * ${@inheritDoc}
     */
    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput)
    {
        for (IRecordProcessor rp : this.recordProcessors)
        {
            rp.processRecords(processRecordsInput);
        }
    }

    /**
     * Shut down all child record processors.
     * 
     * ${@inheritDoc}
     */
    @Override
    public void shutdown(ShutdownInput shutdownInput)
    {
        for (IRecordProcessor rp : this.recordProcessors)
        {
            rp.shutdown(shutdownInput);
        }
    }
}
