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

import java.io.IOException;
import java.util.List;

/**
 * Abstraction of an emitter which is essentially a thing that sends
 * buffered records to a destination.
 * 
 * @author AWS Solutions Team
 */
public interface IEmitter<T>
{
    /**
     * Send a buffer of records to a destination.
     * 
     * Returns a list of records that failed to emit.
     */
    List<T> emit(IBuffer<T> buffer) throws IOException;

    /**
     * Indicate that one or more records failed to be emitted.
     */
    void fail(List<T> records);

    /**
     * Shutdown this emitter and clean up any resources as necessary.
     */
    void shutdown();
}
