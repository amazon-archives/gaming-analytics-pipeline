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
package com.amazonaws.gaming.analytics.common;

import java.io.Closeable;
import java.io.IOException;

import org.apache.commons.lang.Validate;

/**
 * A set of convenience methods for handling null values and 
 * ignoring exceptions on common Java operations.
 * 
 * @author AWS Solutions Team
 */
public class SafeUtil
{
    /**
     * Performs a close. If "c" is null, nothing happens.
     * 
     * @see Closeable#close()
     */
    public static void safeClose(final Closeable c)
    {
        if (c != null)
        {
            try
            {
                c.close();
            }
            catch (final IOException e)
            {
                // ignore
            }
        }
    }

    /**
     * Performs a sleep. InterruptedExceptions are ignored.
     * 
     * @see Thread#sleep(long)
     */
    public static void safeSleep(final long millis)
    {
        Validate.isTrue(millis >= 0, "Input sleep time is less than zero");
        if (millis == 0)
        {
            return;
        }

        try
        {
            Thread.sleep(millis);
        }
        catch (final InterruptedException ie)
        {
            // ignore
        }
    }

    /**
     * Performs a join. InterruptedExceptions are ignored.
     * 
     * @see Thread#join()
     */
    public static void safeJoin(final Thread thread)
    {
        safeJoin(thread, 0);
    }

    /**
     * Performs a join. InterruptedExceptions are ignored.
     * 
     * If "millis" is zero, {@link Thread#join()} is called.
     * 
     * @see Thread#join()
     * @see Thread#join(long)
     */
    public static void safeJoin(final Thread thread, final long millis)
    {
        Validate.isTrue(millis >= 0, "Input join time is less than zero");
        try
        {
            if (millis == 0)
            {
                thread.join();
            }
            else
            {
                thread.join(millis);
            }
        }
        catch (final InterruptedException ie)
        {
            // ignore
        }
    }

    /**
     * Performs a wait. InterruptedExceptions are ignored.
     * 
     * @see Object#wait()
     */
    public static void safeWait(final Object waitObject)
    {
        safeWait(waitObject, 0L);
    }

    /**
     * Performs a wait. InterruptedExceptions are ignored.
     * 
     * If "millis" is zero, {@link Object#wait()} is called.
     * 
     * @see Object#wait()
     * @see Object#wait(long)
     */
    public static void safeWait(final Object waitObject, final long millis)
    {
        Validate.isTrue(millis >= 0, "Input wait time is less than zero");
        try
        {
            if (millis == 0)
            {
                waitObject.wait();
            }
            else
            {
                waitObject.wait(millis);
            }
        }
        catch (final InterruptedException ie)
        {
            // ignore
        }
    }
}
