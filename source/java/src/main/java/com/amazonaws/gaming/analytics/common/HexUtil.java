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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A convenience class for converting between byte arrays and hex strings.
 * 
 * @author AWS Solutions Team
 */
public class HexUtil
{
    private final static String[] hexSymbols = { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d",
            "e", "f" };

    @SuppressWarnings("serial")
    private static final Map<Character, Byte> HEX_TO_BYTE_MAP = Collections
            .unmodifiableMap(new HashMap<Character, Byte>()
            {
                {
                    put('0', (byte) 0x00);
                    put('1', (byte) 0x01);
                    put('2', (byte) 0x02);
                    put('3', (byte) 0x03);
                    put('4', (byte) 0x04);
                    put('5', (byte) 0x05);
                    put('6', (byte) 0x06);
                    put('7', (byte) 0x07);
                    put('8', (byte) 0x08);
                    put('9', (byte) 0x09);
                    put('a', (byte) 0x0a);
                    put('b', (byte) 0x0b);
                    put('c', (byte) 0x0c);
                    put('d', (byte) 0x0d);
                    put('e', (byte) 0x0e);
                    put('f', (byte) 0x0f);
                }
            });

    public final static int BITS_PER_HEX_DIGIT = 4;

    /**
     * Convert a byte to a hex String (one byte = two hex digits).
     */
    public static String toHexFromByte(final byte b)
    {
        byte leftSymbol = (byte) ((b >>> BITS_PER_HEX_DIGIT) & 0x0f);
        byte rightSymbol = (byte) (b & 0x0f);

        return (hexSymbols[leftSymbol] + hexSymbols[rightSymbol]);
    }

    /**
     * Convert two hex characters into a byte.
     */
    public static byte toByteFromHex(char upperChar, char lowerChar)
    {
        byte upper = HEX_TO_BYTE_MAP.get(Character.toLowerCase(upperChar));
        byte lower = HEX_TO_BYTE_MAP.get(Character.toLowerCase(lowerChar));

        return (byte) ((upper << BITS_PER_HEX_DIGIT) + lower);
    }

    /**
     * Convert a buffer of bytes into a hex string.
     */
    public static String toHexFromBytes(final ByteBuffer bytes)
    {
        if (bytes == null)
        {
            return "";
        }

        StringBuilder hexBuffer = new StringBuilder();
        while (bytes.hasRemaining())
        {
            hexBuffer.append(toHexFromByte(bytes.get()));
        }

        return hexBuffer.toString();
    }

    /**
     * Convert an array of bytes into a hex string.
     */
    public static String toHexFromBytes(final byte[] bytes)
    {
        if (bytes == null || bytes.length == 0)
        {
            return "";
        }

        // there are 2 hex digits per byte
        StringBuilder hexBuffer = new StringBuilder(bytes.length * 2);

        // for each byte, convert it to hex and append it to the buffer
        for (int i = 0; i < bytes.length; i++)
        {
            hexBuffer.append(toHexFromByte(bytes[i]));
        }

        return hexBuffer.toString();
    }

    /**
     * Convert a hex string to a byte array.  If the hex string is not
     * long enough to completely fill a byte array, the last bits will
     * be zero-ed out.
     */
    public static byte[] toBytesFromHex(String hexStr)
    {
        if (hexStr == null || hexStr.length() == 0)
        {
            return new byte[] {};
        }
        
        if (hexStr.length() % 2 != 0)
        {
            hexStr += '0';
        }

        int byteIndex = 0;
        final byte[] bytes = new byte[hexStr.length() / 2];

        for (int i = 0; i < hexStr.length(); i += 2)
        {
            char upper = hexStr.charAt(i);
            char lower = hexStr.charAt(i + 1);
            bytes[byteIndex] = toByteFromHex(upper, lower);
            byteIndex++;
        }

        return bytes;
    }
}
