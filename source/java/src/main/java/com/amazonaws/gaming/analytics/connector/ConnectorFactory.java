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

import com.amazonaws.gaming.analytics.common.AppConfiguration;
import com.amazonaws.gaming.analytics.connector.errorhandler.ErrorHandlerRecordProcessorFactory;
import com.amazonaws.gaming.analytics.connector.redshift.RedshiftRecordProcessorFactory;
import com.amazonaws.gaming.analytics.connector.s3.S3CompoundRecordProcessorFactory;
import com.amazonaws.gaming.analytics.connector.s3.S3RecordProcessorFactory;

/**
 * Convenience class for creating the various record processor factories
 * based on the configured ConnectorType setting.
 * 
 * @author AWS Solutions Team
 */
public class ConnectorFactory
{
    public static AbstractRecordProcessorFactory getRecordProcessorFactory()
    {
        String connectorType = AppConfiguration.INSTANCE.getConnectorType();
        
        if (S3RecordProcessorFactory.CONNECTOR_TYPE.equalsIgnoreCase(connectorType))
        {
            return new S3RecordProcessorFactory();
        }
        else if (RedshiftRecordProcessorFactory.CONNECTOR_TYPE.equalsIgnoreCase(connectorType))
        {
            return new RedshiftRecordProcessorFactory();
        }
        else if (ErrorHandlerRecordProcessorFactory.CONNECTOR_TYPE.equalsIgnoreCase(connectorType))
        {
            return new ErrorHandlerRecordProcessorFactory();
        }
        else if (S3CompoundRecordProcessorFactory.CONNECTOR_TYPE.equalsIgnoreCase(connectorType))
        {
            return new S3CompoundRecordProcessorFactory();
        }
        else
        {
            try
            {
                @SuppressWarnings("rawtypes")
                final Class consumerClass = (Class) Class.forName(connectorType);
                final Object factoryObject = consumerClass.newInstance();
                if (factoryObject instanceof AbstractRecordProcessorFactory)
                {
                    return (AbstractRecordProcessorFactory) factoryObject;
                }
            }
            catch (final Exception e)
            {
                // ignore
            }
        }

        throw new IllegalArgumentException("Unknown Kinesis connector name or class: \"" + connectorType + "\"");
    }
}
