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
import java.util.Random;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.gaming.analytics.common.AppConfiguration;
import com.amazonaws.gaming.analytics.common.exception.TelemetryEventValidationException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
 
/**
 * @author AWS Solutions Team
 */
public class KinesisEventTest
{  
    @Before
    public void setup()
    {
        AppConfiguration.INSTANCE.initialize(true, "unittest", "analytics");
    }
    
    private static Random random = new Random();
    private static String[] ALPHABET = new String[] { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k",
                                                      "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v",
                                                      "w", "x", "y", "z"}; 
    private String generateRandomAlphaString(int length)
    {
        StringBuilder sb = new StringBuilder(length);
        
        for(int i=0; i < length; i++)
        {
            sb.append(ALPHABET[random.nextInt(ALPHABET.length)]);
        }
        
        return sb.toString();
    }
    
    private void parseEventEnforceFailure(String eventJson)
    {
        KinesisEvent event = new KinesisEvent();
        try
        {
            event.parseFromJson(new ObjectMapper(), eventJson);
            Assert.fail("No exception thrown when there should've been for event " + eventJson);
        }
        catch (TelemetryEventValidationException teve)
        {
            //success
        }
        catch(Exception e)
        {
            e.printStackTrace();
            Assert.fail("Caught unexpected exception: " + e.getMessage());
        }
    }
    
    private KinesisEvent parseEventEnforceSuccess(String eventJson)
    {
        KinesisEvent event = new KinesisEvent();
        event.setServerTimestamp(System.currentTimeMillis());
        try
        {
            event.parseFromJson(new ObjectMapper(), eventJson);
        }
        catch(Exception e)
        {
            e.printStackTrace();
            Assert.fail("Caught unexpected exception: " + e.getMessage());
        }
        
        return event;
    }
    
    private JsonNode eventProcessedJsonToJsonNode(KinesisEvent event)
    {
        JsonNode jsonRoot = null;
        try
        {
            jsonRoot = new ObjectMapper().readTree(event.getProcessedJson());
        }
        catch (final IOException e)
        {
            e.printStackTrace();
            Assert.fail("Could not parse processed JSON: " + e.getMessage());
        }
        
        return jsonRoot;
    }
    
    @Test
    public void testValidation()
    {
        String missingEventVersion = "{\"position_x\":556,\"app_name\":\"SampleGame\",\"client_id\":\"d57faa2b-9bfd-4502-a7b7-a43cb365f8f2\",\"position_y\":521,\"event_id\":\"91650ce5-825a-4e90-ab22-174a4fb2da79\",\"level_id\":\"test_level\",\"app_version\":\"1.0.0\",\"event_timestamp\":1508872163135,\"event_type\":\"test_event\"}";
        String missingAppName = "{\"event_version\":\"1.0\",\"position_x\":556,\"client_id\":\"d57faa2b-9bfd-4502-a7b7-a43cb365f8f2\",\"position_y\":521,\"event_id\":\"91650ce5-825a-4e90-ab22-174a4fb2da79\",\"level_id\":\"test_level\",\"app_version\":\"1.0.0\",\"event_timestamp\":1508872163135,\"event_type\":\"test_event\"}";
        String missingClientId = "{\"event_version\":\"1.0\",\"position_x\":556,\"app_name\":\"SampleGame\",\"position_y\":521,\"event_id\":\"91650ce5-825a-4e90-ab22-174a4fb2da79\",\"level_id\":\"test_level\",\"app_version\":\"1.0.0\",\"event_timestamp\":1508872163135,\"event_type\":\"test_event\"}";
        String missingEventTimestamp = "{\"event_version\":\"1.0\",\"position_x\":556,\"app_name\":\"SampleGame\",\"client_id\":\"d57faa2b-9bfd-4502-a7b7-a43cb365f8f2\",\"position_y\":521,\"event_id\":\"91650ce5-825a-4e90-ab22-174a4fb2da79\",\"level_id\":\"test_level\",\"app_version\":\"1.0.0\",\"event_type\":\"test_event\"}";
        String missingEventType = "{\"event_version\":\"1.0\",\"position_x\":556,\"app_name\":\"SampleGame\",\"client_id\":\"d57faa2b-9bfd-4502-a7b7-a43cb365f8f2\",\"position_y\":521,\"event_id\":\"91650ce5-825a-4e90-ab22-174a4fb2da79\",\"level_id\":\"test_level\",\"app_version\":\"1.0.0\",\"event_timestamp\":1508872163135}";
        
        parseEventEnforceFailure(missingEventVersion);
        parseEventEnforceFailure(missingAppName);
        parseEventEnforceFailure(missingClientId);
        parseEventEnforceFailure(missingEventTimestamp);
        parseEventEnforceFailure(missingEventType);
        
        String missingAppVersion = "{\"event_version\":\"1.0\",\"position_x\":556,\"app_name\":\"SampleGame\",\"client_id\":\"d57faa2b-9bfd-4502-a7b7-a43cb365f8f2\",\"position_y\":521,\"event_id\":\"91650ce5-825a-4e90-ab22-174a4fb2da79\",\"level_id\":\"test_level\",\"event_timestamp\":1508872163135,\"event_type\":\"test_event\"}";
        String missingLevelId = "{\"event_version\":\"1.0\",\"position_x\":556,\"app_name\":\"SampleGame\",\"client_id\":\"d57faa2b-9bfd-4502-a7b7-a43cb365f8f2\",\"position_y\":521,\"event_id\":\"91650ce5-825a-4e90-ab22-174a4fb2da79\",\"app_version\":\"1.0.0\",\"event_timestamp\":1508872163135,\"event_type\":\"test_event\"}";
        String missingPositionX = "{\"event_version\":\"1.0\",\"app_name\":\"SampleGame\",\"client_id\":\"d57faa2b-9bfd-4502-a7b7-a43cb365f8f2\",\"position_y\":521,\"event_id\":\"91650ce5-825a-4e90-ab22-174a4fb2da79\",\"level_id\":\"test_level\",\"app_version\":\"1.0.0\",\"event_timestamp\":1508872163135,\"event_type\":\"test_event\"}";
        String missingPositionY = "{\"event_version\":\"1.0\",\"position_x\":556,\"app_name\":\"SampleGame\",\"client_id\":\"d57faa2b-9bfd-4502-a7b7-a43cb365f8f2\",\"event_id\":\"91650ce5-825a-4e90-ab22-174a4fb2da79\",\"level_id\":\"test_level\",\"app_version\":\"1.0.0\",\"event_timestamp\":1508872163135,\"event_type\":\"test_event\"}";
        
        parseEventEnforceSuccess(missingAppVersion);
        parseEventEnforceSuccess(missingLevelId);
        parseEventEnforceSuccess(missingPositionX);
        parseEventEnforceSuccess(missingPositionY);
    }
    
    @Test
    public void testSanitization()
    {
        String tooLongEventVersion = generateRandomAlphaString(KinesisEvent.EVENT_VERSION_MAX_LENGTH + 15);
        String tooLongAppName = generateRandomAlphaString(KinesisEvent.APP_NAME_MAX_LENGTH + 7);
        String tooLongAppVersion = generateRandomAlphaString(KinesisEvent.APP_VERSION_MAX_LENGTH + 8);
        String tooLongLevelId = generateRandomAlphaString(KinesisEvent.LEVEL_ID_MAX_LENGTH + 5);
        String tooLongClientId = generateRandomAlphaString(KinesisEvent.CLIENT_ID_MAX_LENGTH + 1);
        String tooLongEventId = generateRandomAlphaString(KinesisEvent.EVENT_ID_MAX_LENGTH + 11);
        String tooLongEventType = generateRandomAlphaString(KinesisEvent.EVENT_TYPE_MAX_LENGTH + 3);
        long negativeEventTimestamp = -100;
        String invalidPositionValue = "asdf";
        
        String sampleEvent = "{\"event_version\":\"" + tooLongEventVersion + "\","
                + "\"position_x\":\"" + invalidPositionValue + "\","
                + "\"app_name\":\"" + tooLongAppName + "\","
                + "\"client_id\":\"" + tooLongClientId + "\","
                + "\"position_y\":\"" + invalidPositionValue + "\","
                + "\"event_id\":\"" + tooLongEventId + "\","
                + "\"level_id\":\"" + tooLongLevelId + "\","
                + "\"app_version\":\"" + tooLongAppVersion + "\","
                + "\"event_timestamp\":" + negativeEventTimestamp + ","
                + "\"event_type\":\"" + tooLongEventType + "\"}";
        
        KinesisEvent event = parseEventEnforceSuccess(sampleEvent);
        
        Assert.assertTrue(event.isRequiredSanitization());
        Assert.assertEquals(tooLongEventVersion.substring(0, KinesisEvent.EVENT_VERSION_MAX_LENGTH), event.getEventVersion());
        Assert.assertEquals(tooLongAppName.substring(0, KinesisEvent.APP_NAME_MAX_LENGTH), event.getAppName());
        Assert.assertEquals(tooLongAppVersion.substring(0, KinesisEvent.APP_VERSION_MAX_LENGTH), event.getAppVersion());
        Assert.assertEquals(tooLongLevelId.substring(0, KinesisEvent.LEVEL_ID_MAX_LENGTH), event.getLevelId());
        Assert.assertEquals(tooLongClientId.substring(0, KinesisEvent.CLIENT_ID_MAX_LENGTH), event.getClientId());
        Assert.assertEquals(tooLongEventId.substring(0, KinesisEvent.EVENT_ID_MAX_LENGTH), event.getEventId());
        Assert.assertEquals(tooLongEventType.substring(0, KinesisEvent.EVENT_TYPE_MAX_LENGTH), event.getEventType());
        Assert.assertEquals(0L, event.getEventTimestamp().longValue());
        Assert.assertEquals(0.0d, event.getPositionX(), 0.00001d);
        Assert.assertEquals(0.0d, event.getPositionY(), 0.00001d);
    }
    
    @Test
    public void testEnrichment()
    {
        String sampleEvent = "{\"event_version\":\"1.0\","
                + "\"position_x\":556,"
                + "\"app_name\":\"SampleGame\","
                + "\"client_id\":\"d57faa2b-9bfd-4502-a7b7-a43cb365f8f2\","
                + "\"position_y\":521,"
                + "\"event_id\":\"91650ce5-825a-4e90-ab22-174a4fb2da79\","
                + "\"level_id\":\"test_level\","
                + "\"app_version\":\"1.0.0\","
                + "\"event_timestamp\":1508872163135,"
                + "\"event_type\":\"test_event\"}";
        
        //test that JSON is parsed properly
        KinesisEvent event = parseEventEnforceSuccess(sampleEvent);        
        JsonNode jsonRoot = eventProcessedJsonToJsonNode(event);
        
        //test that serialization appended server timestamp field
        Assert.assertNotNull(jsonRoot.get(KinesisEvent.SERVER_TIMESTAMP_JSON_KEY));
        Assert.assertEquals(event.getServerTimestamp().longValue(), jsonRoot.get(KinesisEvent.SERVER_TIMESTAMP_JSON_KEY).asLong());
    }
    
    @Test
    public void testSerializeDeserialize() 
    {
    	String sampleEvent = "{\"event_version\":\"1.0\","
    	        + "\"position_x\":556,"
    	        + "\"app_name\":\"SampleGame\","
    	        + "\"client_id\":\"d57faa2b-9bfd-4502-a7b7-a43cb365f8f2\","
    	        + "\"position_y\":521,"
    	        + "\"event_id\":\"91650ce5-825a-4e90-ab22-174a4fb2da79\","
    	        + "\"level_id\":\"test_level\","
    	        + "\"app_version\":\"1.0.0\","
    	        + "\"event_timestamp\":1508872163135,"
    	        + "\"event_type\":\"test_event\"}";
    	
    	//test that JSON is parsed properly
        KinesisEvent event = parseEventEnforceSuccess(sampleEvent);        
        
        //test that existing fields are read in properly
        Assert.assertEquals("SampleGame", event.getAppName());
        Assert.assertEquals("1.0.0", event.getAppVersion());
        Assert.assertEquals("d57faa2b-9bfd-4502-a7b7-a43cb365f8f2", event.getClientId());
        Assert.assertEquals("91650ce5-825a-4e90-ab22-174a4fb2da79", event.getEventId());
        Assert.assertEquals(1508872163135L, event.getEventTimestamp().longValue());
        Assert.assertEquals("test_event", event.getEventType());
        Assert.assertEquals("1.0", event.getEventVersion());
        Assert.assertEquals("test_level", event.getLevelId());
        Assert.assertEquals(556.0d, event.getPositionX(), 0.00001);
        Assert.assertEquals(521.0d, event.getPositionY(), 0.00001);        
        
        eventProcessedJsonToJsonNode(event);
    }
    
    @After
    public void tearDown()
    {
        AppConfiguration.INSTANCE.clear();
    }
}