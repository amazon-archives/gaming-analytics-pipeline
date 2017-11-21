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
 * Exception to throw when an incoming telemetry event fails validation.
 * 
 * Also provides access to the original JSON blob of the event.
 * 
 * @author AWS Solutions Team
 */
@SuppressWarnings("serial")
public class TelemetryEventValidationException extends TelemetryEventException
{
    private String json;

    public TelemetryEventValidationException()
    {
        super();
    }

    public TelemetryEventValidationException(final String message, final Throwable cause,
            final boolean enableSuppression, final boolean writableStackTrace)
    {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public TelemetryEventValidationException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

    public TelemetryEventValidationException(final String message, final String json, final Throwable cause)
    {
        this(message, cause);
        this.json = json;
    }

    public TelemetryEventValidationException(final String message)
    {
        super(message);
    }

    public TelemetryEventValidationException(final String message, final String json)
    {
        this(message);
        this.json = json;
    }

    public TelemetryEventValidationException(final Throwable cause)
    {
        super(cause);
    }

    public String getJson()
    {
        return this.json;
    }
}
