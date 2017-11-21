package com.amazonaws.gaming.analytics.common;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AppConfigurationTest
{
	@Before
    public void setup()
    {
        System.setProperty("test_system_property", "test_sysprop_value");
        AppConfiguration.INSTANCE.initialize(true, "unittest", "analytics");
        AppConfiguration.INSTANCE.loadConfigurationFile("test.properties");
    }
    
    @Test
    public void testOverrides() 
    {
    	Assert.assertEquals("value1", AppConfiguration.INSTANCE.getString("property_a"));
    	Assert.assertEquals("value2", AppConfiguration.INSTANCE.getString("property_b"));
    	Assert.assertEquals("value3", AppConfiguration.INSTANCE.getString("property_c"));
    	Assert.assertEquals("value4", AppConfiguration.INSTANCE.getString("property_d"));
    	Assert.assertEquals("value5", AppConfiguration.INSTANCE.getString("property_e"));
    	Assert.assertEquals("value6", AppConfiguration.INSTANCE.getString("property_f"));
    }
    
    @Test
    public void testSystemProperty() 
    {
    	Assert.assertTrue(AppConfiguration.INSTANCE.hasProperty("test_system_property"));
    	Assert.assertEquals("test_sysprop_value", AppConfiguration.INSTANCE.getString("test_system_property"));
    }
    
    @Test
    public void testMissingProperty() 
    {
    	Assert.assertFalse(AppConfiguration.INSTANCE.hasProperty("non_existent_property"));
    	Assert.assertNull(AppConfiguration.INSTANCE.getString("non_existent_property"));
    }
    
    @Test
    public void testBadUsage() 
    {
    	try
    	{
    		AppConfiguration.INSTANCE.getInt("string_property");
    		Assert.fail("Failed to throw exception on badly formatted property");
    	}
    	catch(NumberFormatException nfe)
    	{
    		//success
    	}
    	
    	try
    	{
    		AppConfiguration.INSTANCE.getLong("string_property");
    		Assert.fail("Failed to throw exception on badly formatted property");
    	}
    	catch(NumberFormatException nfe)
    	{
    		//success
    	}
    	
    	try
    	{
    		AppConfiguration.INSTANCE.getFloat("string_property");
    		Assert.fail("Failed to throw exception on badly formatted property");
    	}
    	catch(NumberFormatException nfe)
    	{
    		//success
    	}
    	
    	try
    	{
    		AppConfiguration.INSTANCE.getDouble("string_property");
    		Assert.fail("Failed to throw exception on badly formatted property");
    	}
    	catch(NumberFormatException nfe)
    	{
    		//success
    	}
    }
    
    @Test
    public void testStringProperties() 
    {
    	Assert.assertTrue(AppConfiguration.INSTANCE.hasProperty("string_property"));
    	Assert.assertEquals("test_value", AppConfiguration.INSTANCE.getString("string_property"));
    	Assert.assertEquals("default_value", AppConfiguration.INSTANCE.getString("non_existent_property", "default_value"));
    }
    
    @Test
    public void testIntProperties() 
    {
    	Assert.assertTrue(AppConfiguration.INSTANCE.hasProperty("int_property"));
    	Assert.assertEquals(1024, AppConfiguration.INSTANCE.getInt("int_property"));
    	Assert.assertEquals(123, AppConfiguration.INSTANCE.getInt("non_existent_property", 123));
    }
    
    @Test
    public void testBooleanProperties()
    {
    	Assert.assertTrue(AppConfiguration.INSTANCE.hasProperty("boolean_property"));
    	Assert.assertEquals(true, AppConfiguration.INSTANCE.getBoolean("boolean_property"));
    	Assert.assertEquals(true, AppConfiguration.INSTANCE.getBoolean("non_existent_property", true));
    }
    
    @Test
    public void testDoubleProperties() 
    {
    	Assert.assertTrue(AppConfiguration.INSTANCE.hasProperty("double_property"));
    	Assert.assertEquals(55555.44444d, AppConfiguration.INSTANCE.getDouble("double_property"), 0.00001d);
    	Assert.assertEquals(1111.2222d, AppConfiguration.INSTANCE.getDouble("non_existent_property", 1111.2222), 0.00001d);
    }
    
    @Test
    public void testFloatProperties() 
    {
    	Assert.assertTrue(AppConfiguration.INSTANCE.hasProperty("float_property"));
    	Assert.assertEquals(123.456f, AppConfiguration.INSTANCE.getFloat("float_property"), 0.00001f);
    	Assert.assertEquals(12.13f, AppConfiguration.INSTANCE.getFloat("non_existent_property", 12.13f), 0.00001f);
    }
    
    @Test
    public void testLongProperties() 
    {
    	Assert.assertTrue(AppConfiguration.INSTANCE.hasProperty("long_property"));
    	Assert.assertEquals(9999999999L, AppConfiguration.INSTANCE.getLong("long_property"));
    	Assert.assertEquals(123456L, AppConfiguration.INSTANCE.getLong("non_existent_property", 123456L));
    }
    
    @Test
    public void testListProperties() 
    {
        Assert.assertTrue(AppConfiguration.INSTANCE.hasProperty("list_property"));
        String[] values = AppConfiguration.INSTANCE.getList("list_property");
        Assert.assertEquals(5, values.length);
        Assert.assertEquals("a", values[0]);
        Assert.assertEquals("b", values[1]);
        Assert.assertEquals("c", values[2]);
        Assert.assertEquals("def", values[3]);
        Assert.assertEquals("ghi", values[4]);
        
        String[] emptyValues = AppConfiguration.INSTANCE.getList("non_existent_property", new String[] {});
        Assert.assertEquals(0, emptyValues.length);
    }
    
    @After
    public void tearDown()
    {
        AppConfiguration.INSTANCE.clear();
    }
}
