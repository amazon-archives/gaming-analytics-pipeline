package com.amazonaws.gaming.analytics.common;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

public class HexUtilTest
{
    @Test
    public void testHexToByte()
    {
        Assert.assertEquals("00", HexUtil.toHexFromByte((byte)0x00));
        Assert.assertEquals("01", HexUtil.toHexFromByte((byte)0x01));
        Assert.assertEquals("02", HexUtil.toHexFromByte((byte)0x02));
        Assert.assertEquals("03", HexUtil.toHexFromByte((byte)0x03));
        Assert.assertEquals("04", HexUtil.toHexFromByte((byte)0x04));
        Assert.assertEquals("05", HexUtil.toHexFromByte((byte)0x05));
        Assert.assertEquals("06", HexUtil.toHexFromByte((byte)0x06));
        Assert.assertEquals("07", HexUtil.toHexFromByte((byte)0x07));
        Assert.assertEquals("08", HexUtil.toHexFromByte((byte)0x08));
        Assert.assertEquals("09", HexUtil.toHexFromByte((byte)0x09));
        Assert.assertEquals("0a", HexUtil.toHexFromByte((byte)0x0a));
        Assert.assertEquals("0b", HexUtil.toHexFromByte((byte)0x0b));
        Assert.assertEquals("0c", HexUtil.toHexFromByte((byte)0x0c));
        Assert.assertEquals("0d", HexUtil.toHexFromByte((byte)0x0d));
        Assert.assertEquals("0e", HexUtil.toHexFromByte((byte)0x0e));
        Assert.assertEquals("0f", HexUtil.toHexFromByte((byte)0x0f));
        
        Assert.assertEquals("23", HexUtil.toHexFromByte((byte)0x23));
        Assert.assertEquals("19", HexUtil.toHexFromByte((byte)0x19));
        Assert.assertEquals("75", HexUtil.toHexFromByte((byte)0x75));
        Assert.assertEquals("43", HexUtil.toHexFromByte((byte)0x43));
        Assert.assertEquals("a9", HexUtil.toHexFromByte((byte)0xa9));
        Assert.assertEquals("3b", HexUtil.toHexFromByte((byte)0x3b));
        Assert.assertEquals("c3", HexUtil.toHexFromByte((byte)0xc3));
        Assert.assertEquals("ff", HexUtil.toHexFromByte((byte)0xff));
    }
    
    @Test
    public void testByteToHex()
    {
        Assert.assertEquals((byte)0x00, HexUtil.toByteFromHex('0','0'));
        Assert.assertEquals((byte)0x01, HexUtil.toByteFromHex('0','1'));
        Assert.assertEquals((byte)0x02, HexUtil.toByteFromHex('0','2'));
        Assert.assertEquals((byte)0x03, HexUtil.toByteFromHex('0','3'));
        Assert.assertEquals((byte)0x04, HexUtil.toByteFromHex('0','4'));
        Assert.assertEquals((byte)0x05, HexUtil.toByteFromHex('0','5'));
        Assert.assertEquals((byte)0x06, HexUtil.toByteFromHex('0','6'));
        Assert.assertEquals((byte)0x07, HexUtil.toByteFromHex('0','7'));
        Assert.assertEquals((byte)0x08, HexUtil.toByteFromHex('0','8'));
        Assert.assertEquals((byte)0x09, HexUtil.toByteFromHex('0','9'));
        Assert.assertEquals((byte)0x0a, HexUtil.toByteFromHex('0','a'));
        Assert.assertEquals((byte)0x0b, HexUtil.toByteFromHex('0','b'));
        Assert.assertEquals((byte)0x0c, HexUtil.toByteFromHex('0','c'));
        Assert.assertEquals((byte)0x0d, HexUtil.toByteFromHex('0','d'));
        Assert.assertEquals((byte)0x0e, HexUtil.toByteFromHex('0','e'));
        Assert.assertEquals((byte)0x0f, HexUtil.toByteFromHex('0','f'));
        
        Assert.assertEquals((byte)0x23, HexUtil.toByteFromHex('2','3'));
        Assert.assertEquals((byte)0x19, HexUtil.toByteFromHex('1','9'));
        Assert.assertEquals((byte)0x75, HexUtil.toByteFromHex('7','5'));
        Assert.assertEquals((byte)0x43, HexUtil.toByteFromHex('4','3'));
        Assert.assertEquals((byte)0xa9, HexUtil.toByteFromHex('a','9'));
        Assert.assertEquals((byte)0x3b, HexUtil.toByteFromHex('3','b'));
        Assert.assertEquals((byte)0xc3, HexUtil.toByteFromHex('c','3'));
        Assert.assertEquals((byte)0xff, HexUtil.toByteFromHex('f','f'));
    }
    
    @Test
    public void testHexToBytes()
    {
        byte[] bytes = { (byte)0x0a, (byte)0xff, (byte)0x12, (byte)0x38, 
                         (byte)0xde, (byte)0xad, (byte)0xbe, (byte)0xef,
                         (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00,
                         (byte)0xde, (byte)0xad, (byte)0xc0, (byte)0xde };
        
        Assert.assertEquals("0aff1238deadbeef00000000deadc0de", HexUtil.toHexFromBytes(bytes));

        Assert.assertEquals("", HexUtil.toHexFromBytes(new byte[] {}));        
        Assert.assertEquals("", HexUtil.toHexFromBytes((byte[])null));        
        Assert.assertEquals("01", HexUtil.toHexFromBytes(new byte[] { (byte) 0x01 }));
        
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        
        Assert.assertEquals("0aff1238deadbeef00000000deadc0de", HexUtil.toHexFromBytes(bb));

        Assert.assertEquals("", HexUtil.toHexFromBytes(ByteBuffer.wrap(new byte[] {})));        
        Assert.assertEquals("", HexUtil.toHexFromBytes((ByteBuffer)null));        
        Assert.assertEquals("01", HexUtil.toHexFromBytes(ByteBuffer.wrap(new byte[] { (byte) 0x01 })));
    }
    
    @Test
    public void testBytesToHex()
    {
        byte[] bytes = { (byte)0x0a, (byte)0xff, (byte)0x12, (byte)0x38, 
                (byte)0xde, (byte)0xad, (byte)0xbe, (byte)0xef,
                (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00,
                (byte)0xde, (byte)0xad, (byte)0xc0, (byte)0xde };
        String hex = "0aff1238deadbeef00000000deadc0de";
        
        Assert.assertTrue(Arrays.equals(bytes, HexUtil.toBytesFromHex(hex)));
        
        hex = "54321";
        
        Assert.assertTrue(Arrays.equals(new byte[] { (byte)0x54, (byte)0x32, (byte)0x10 }, HexUtil.toBytesFromHex(hex)));
        
        hex = "a";
        
        Assert.assertTrue(Arrays.equals(new byte[] { (byte)0xa0 }, HexUtil.toBytesFromHex(hex)));
        
        Assert.assertTrue(Arrays.equals(new byte[] { }, HexUtil.toBytesFromHex("")));
        Assert.assertTrue(Arrays.equals(new byte[] { }, HexUtil.toBytesFromHex(null)));
    }
}
