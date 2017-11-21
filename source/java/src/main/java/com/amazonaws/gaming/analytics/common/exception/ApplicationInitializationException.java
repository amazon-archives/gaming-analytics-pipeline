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
 * Exception to throw when the application fails to initialize properly.
 * 
 * @author AWS Solutions Team
 */
@SuppressWarnings("serial")
public class ApplicationInitializationException extends RuntimeException
{
    public ApplicationInitializationException()
    {
        super();
    }

    public ApplicationInitializationException(String arg0, Throwable arg1, boolean arg2, boolean arg3)
    {
        super(arg0, arg1, arg2, arg3);
    }

    public ApplicationInitializationException(String arg0, Throwable arg1)
    {
        super(arg0, arg1);
    }

    public ApplicationInitializationException(String arg0)
    {
        super(arg0);
    }

    public ApplicationInitializationException(Throwable arg0)
    {
        super(arg0);
    }

}
