package com.amazonaws.gaming.analytics.common;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.Assert;
import org.junit.Test;

public class DecoderUtilTest
{
    @Test
    public void testDecode()
    {
        String testString = "asdlkfjajo3wjaf3o8ajlfija3lfijaslijfl83auj";
        byte[] encodedBytes = testString.getBytes(StandardCharsets.UTF_8);
        ByteBuffer bb = ByteBuffer.wrap(encodedBytes);
        String decodedString = DecoderUtil.getUtf8FromByteBuffer(bb);
        
        Assert.assertEquals(testString, decodedString);
    }
}
