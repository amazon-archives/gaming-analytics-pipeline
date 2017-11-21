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
 * Common superclass exception for all telemetry-related exceptions.
 * 
 * @author AWS Solutions Team
 */
@SuppressWarnings("serial")
public abstract class TelemetryEventException extends Exception
{
    public TelemetryEventException()
    {
        super();
    }

    public TelemetryEventException(final String message, final Throwable cause, final boolean enableSuppression,
            final boolean writableStackTrace)
    {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public TelemetryEventException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

    public TelemetryEventException(final String message)
    {
        super(message);
    }

    public TelemetryEventException(final Throwable cause)
    {
        super(cause);
    }
}
