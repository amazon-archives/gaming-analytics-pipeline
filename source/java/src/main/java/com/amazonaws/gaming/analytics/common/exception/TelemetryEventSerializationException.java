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
package com.amazonaws.gaming.analytics.common.exception;

/**
 * Exception to throw when an there is an error serializing a telemetry event to JSON.
 * 
 * @author AWS Solutions Team
 */
@SuppressWarnings("serial")
public class TelemetryEventSerializationException extends TelemetryEventException
{
    public TelemetryEventSerializationException()
    {
        super();
    }

    public TelemetryEventSerializationException(final String message, final Throwable cause,
            final boolean enableSuppression, final boolean writableStackTrace)
    {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public TelemetryEventSerializationException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

    public TelemetryEventSerializationException(final String message)
    {
        super(message);
    }

    public TelemetryEventSerializationException(final Throwable cause)
    {
        super(cause);
    }
}
