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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Convenience utilities for decoder byte arrays/buffers.
 * 
 * @author AWS Solutions Team
 */
public class DecoderUtil
{
    /**
     * Decode the input byte buffer into a UTF-8 string.
     */
    public static String getUtf8FromByteBuffer(final ByteBuffer bb)
    {
        bb.rewind();

        byte[] bytes;
        if (bb.hasArray())
        {
            bytes = bb.array();
        }
        else
        {
            bytes = new byte[bb.remaining()];
            bb.get(bytes);
        }

        return new String(bytes, StandardCharsets.UTF_8);
    }
}
