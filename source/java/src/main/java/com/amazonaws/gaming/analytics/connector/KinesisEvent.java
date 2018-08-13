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
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.gaming.analytics.common.AppConfiguration;
import com.amazonaws.gaming.analytics.common.exception.TelemetryEventParseException;
import com.amazonaws.gaming.analytics.common.exception.TelemetryEventSerializationException;
import com.amazonaws.gaming.analytics.common.exception.TelemetryEventValidationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * A representation of an incoming JSON-based Kinesis stream record.
 * 
 * This is the representation of a single event as transmitted by a data producer
 * such as the game client or game server.
 * 
 * @author AWS Solutions Team
 */
public class KinesisEvent
{
    public static final String APP_NAME_JSON_KEY = "app_name";
    public static final String APP_VERSION_JSON_KEY = "app_version";
    public static final String EVENT_VERSION_JSON_KEY = "event_version";
    public static final String EVENT_ID_JSON_KEY = "event_id";
    public static final String EVENT_TYPE_JSON_KEY = "event_type";
    public static final String EVENT_TIMESTAMP_JSON_KEY = "event_timestamp";
    public static final String CLIENT_ID_JSON_KEY = "client_id";
    public static final String LEVEL_ID_JSON_KEY = "level_id";
    public static final String POSITION_X_JSON_KEY = "position_x";
    public static final String POSITION_Y_JSON_KEY = "position_y";
    public static final String SERVER_TIMESTAMP_JSON_KEY = "server_timestamp";

    public static final int APP_NAME_MAX_LENGTH;
    public static final int APP_VERSION_MAX_LENGTH;
    public static final int EVENT_VERSION_MAX_LENGTH;
    public static final int EVENT_ID_MAX_LENGTH;
    public static final int EVENT_TYPE_MAX_LENGTH;
    public static final int CLIENT_ID_MAX_LENGTH;
    public static final int LEVEL_ID_MAX_LENGTH;
    static
    {
        APP_NAME_MAX_LENGTH = AppConfiguration.INSTANCE.getInt("event.app_name_max_length", 64);
        APP_VERSION_MAX_LENGTH = AppConfiguration.INSTANCE.getInt("event.app_version_max_length", 64);
        EVENT_VERSION_MAX_LENGTH = AppConfiguration.INSTANCE.getInt("event.event_version_max_length", 64);
        EVENT_ID_MAX_LENGTH = AppConfiguration.INSTANCE.getInt("event.event_id_max_length", 36);
        EVENT_TYPE_MAX_LENGTH = AppConfiguration.INSTANCE.getInt("event.event_type_max_length", 256);
        CLIENT_ID_MAX_LENGTH = AppConfiguration.INSTANCE.getInt("event.client_id_max_length", 36);
        LEVEL_ID_MAX_LENGTH = AppConfiguration.INSTANCE.getInt("event.level_id_max_length", 64);
    }
    
    //Per https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html, Pattern is thread-safe
    public static final String RESTRICTED_CHARSET_REGEX = "[-a-zA-z0-9_. ]*";
    public static final Pattern RESTRICTED_CHARSET = Pattern.compile(RESTRICTED_CHARSET_REGEX);

    private String appName;
    private String appVersion;
    private String eventVersion;
    private String eventId;
    private String eventType;
    private Long eventTimestamp;
    private String clientId;
    private String levelId;
    private Double positionX;
    private Double positionY;
    
    private Long serverTimestamp;

    private String shardId;
    private String sequenceNumber;
    private String partitionKey;

    private String rawJson;
    private String processedJson;

    private boolean requiredSanitization;
    private final List<String> sanitizedFields;

    public KinesisEvent()
    {
        this.sanitizedFields = new LinkedList<String>();
    }

    /**
     * Parse the incoming event from JSON and use it to set all the fields in this object.
     * All expected attributes will be validated and sanitized.  Any unexpected or unknown attributes
     * will be ignored.
     */
    public void parseFromJson(final ObjectMapper jsonParser, final String rawJson)
            throws TelemetryEventValidationException, TelemetryEventSerializationException, TelemetryEventParseException
    {
        this.rawJson = rawJson;

        JsonNode jsonRoot = parseRawJson(jsonParser);

        String json;
        try
        {
            json = jsonParser.writeValueAsString(jsonRoot);
        }
        catch (final JsonProcessingException e)
        {
            throw new TelemetryEventSerializationException(
                    "Failed to generate enriched version of JSON string: " + e.getMessage(), e);
        }

        if (!json.endsWith("\n"))
        {
            json += "\n";
        }

        this.processedJson = json;
    }

    private JsonNode parseRawJson(final ObjectMapper jsonParser)
            throws TelemetryEventParseException, TelemetryEventValidationException
    {
        JsonNode jsonRoot = null;
        try
        {
            jsonRoot = jsonParser.readTree(this.rawJson);
        }
        catch (final IOException e)
        {
            throw new TelemetryEventParseException("Could not parse incoming event (seq num= " + this.sequenceNumber
                    + ") as JSON: " + e.getMessage() + " (Raw JSON = " + this.rawJson + ")");
        }

        //validate and sanitize all the incoming values
        this.appName = sanitizeStringNode(jsonRoot, APP_NAME_JSON_KEY, APP_NAME_MAX_LENGTH, true);
        validateStringNode(this.appName, RESTRICTED_CHARSET);
        this.appVersion = sanitizeStringNode(jsonRoot, APP_VERSION_JSON_KEY, APP_VERSION_MAX_LENGTH, false);
        validateStringNode(this.appVersion, RESTRICTED_CHARSET);
        this.eventVersion = sanitizeStringNode(jsonRoot, EVENT_VERSION_JSON_KEY, EVENT_VERSION_MAX_LENGTH, true);
        validateStringNode(this.eventVersion, RESTRICTED_CHARSET);
        this.eventId = sanitizeStringNode(jsonRoot, EVENT_ID_JSON_KEY, EVENT_ID_MAX_LENGTH, true);
        validateStringNode(this.eventId, RESTRICTED_CHARSET);
        this.eventType = sanitizeStringNode(jsonRoot, EVENT_TYPE_JSON_KEY, EVENT_TYPE_MAX_LENGTH, true);
        validateStringNode(this.eventType, RESTRICTED_CHARSET);
        this.eventTimestamp = sanitizeUnsignedIntegerNode(jsonRoot, EVENT_TIMESTAMP_JSON_KEY, true);
        this.clientId = sanitizeStringNode(jsonRoot, CLIENT_ID_JSON_KEY, CLIENT_ID_MAX_LENGTH, true);
        validateStringNode(this.clientId, RESTRICTED_CHARSET);
        this.levelId = sanitizeStringNode(jsonRoot, LEVEL_ID_JSON_KEY, LEVEL_ID_MAX_LENGTH, false);
        validateStringNode(this.levelId, RESTRICTED_CHARSET);
        this.positionX = sanitizeDoubleNode(jsonRoot, POSITION_X_JSON_KEY, false);
        this.positionY = sanitizeDoubleNode(jsonRoot, POSITION_Y_JSON_KEY, false);
        
        //enrich the event by adding server arrival timestamp
        if(this.serverTimestamp != null)
        {
            ((ObjectNode)jsonRoot).put(SERVER_TIMESTAMP_JSON_KEY, this.serverTimestamp.longValue());
        }

        return jsonRoot;
    }

    /**
     * Validate a node by ensuring the key is present if the attribute is required
     */
    private JsonNode validateRequiredNode(final JsonNode parent, final String key, final boolean required)
            throws TelemetryEventValidationException
    {
        final JsonNode node = parent.get(key);
        if (node == null && required)
        {
            throw new TelemetryEventValidationException(
                    "Could not find required attribute " + key + " in incoming event.", this.rawJson);
        }

        return node;
    }

    /**
     * Sanitize a string node by truncating the string to fit within the given length specification.
     */
    private String sanitizeStringNode(final JsonNode parent, final String key, final int maxLength, boolean required)
            throws TelemetryEventValidationException
    {
        final JsonNode node = validateRequiredNode(parent, key, required);
        if (node == null)
        {
            return "";
        }

        String value = node.asText();
        if (value.length() > maxLength)
        {
            value = value.substring(0, maxLength);
            ((ObjectNode) parent).set(key, JsonNodeFactory.instance.textNode(value));
            this.sanitizedFields.add(key);
            this.requiredSanitization = true;
        }

        return value;
    }
    
    private void validateStringNode(String value, Pattern validPattern) throws TelemetryEventValidationException
    {
    	Matcher matcher = validPattern.matcher(value);
    	if(!matcher.matches())
    	{
    		throw new TelemetryEventValidationException("The supplied value \"" + value + "\" does not match the required " +
    				"validation regular expression \"" + RESTRICTED_CHARSET_REGEX + "\"");
    			
    	}
    }

    /**
     * Sanitize an unsigned int node by by ensuring it parses as a number and is greater than zero and replacing
     * it with zero if not.
     */
    private Long sanitizeUnsignedIntegerNode(final JsonNode parent, final String key, boolean required)
            throws TelemetryEventValidationException
    {
        final JsonNode node = validateRequiredNode(parent, key, required);
        if (node == null)
        {
            return 0L;
        }

        Long value;
        try
        {
            value = Long.parseLong(node.asText());
        }
        catch (final Exception e)
        {
            value = null;
        }

        if (value == null || value < 0)
        {
            value = 0L;
            ((ObjectNode) parent).set(key, JsonNodeFactory.instance.numberNode(value));
            this.sanitizedFields.add(key);
            this.requiredSanitization = true;
        }

        return value;
    }

    /**
     * Sanitize a double node by by ensuring it parses as a number and replacing it with 0.0 if it doesn't.
     */
    private Double sanitizeDoubleNode(final JsonNode parent, final String key, boolean required)
            throws TelemetryEventValidationException
    {
        final JsonNode node = validateRequiredNode(parent, key, required);
        if (node == null)
        {
            return 0.0d;
        }

        Double value;
        try
        {
            value = Double.parseDouble(node.asText());
        }
        catch (final Exception e)
        {
            value = null;
        }

        if (value == null)
        {
            value = 0.0d;
            ((ObjectNode) parent).set(key, JsonNodeFactory.instance.numberNode(value));
            this.sanitizedFields.add(key);
            this.requiredSanitization = true;
        }

        return value;
    }

    public String getRawJson()
    {
        return this.rawJson;
    }

    public String getProcessedJson()
    {
        return this.processedJson;
    }

    @Override
    public String toString()
    {
        return getRawJson();
    }

    public boolean isRequiredSanitization()
    {
        return this.requiredSanitization;
    }

    public List<String> getSanitizedFields()
    {
        return this.sanitizedFields;
    }

    public String getAppName()
    {
        return this.appName;
    }

    public void setAppName(String appName)
    {
        this.appName = appName;
    }

    public String getAppVersion()
    {
        return this.appVersion;
    }

    public void setAppVersion(String appVersion)
    {
        this.appVersion = appVersion;
    }

    public String getEventVersion()
    {
        return eventVersion;
    }

    public void setEventVersion(String eventVersion)
    {
        this.eventVersion = eventVersion;
    }

    public String getEventId()
    {
        return this.eventId;
    }

    public void setEventId(String eventId)
    {
        this.eventId = eventId;
    }

    public String getEventType()
    {
        return this.eventType;
    }

    public void setEventType(String eventType)
    {
        this.eventType = eventType;
    }

    public Long getEventTimestamp()
    {
        return this.eventTimestamp;
    }

    public void setEventTimestamp(Long eventTimestamp)
    {
        this.eventTimestamp = eventTimestamp;
    }

    public String getClientId()
    {
        return this.clientId;
    }

    public void setClientId(String clientId)
    {
        this.clientId = clientId;
    }

    public String getLevelId()
    {
        return this.levelId;
    }

    public void setLevelId(String levelId)
    {
        this.levelId = levelId;
    }

    public Double getPositionX()
    {
        return this.positionX;
    }

    public void setPositionX(Double positionX)
    {
        this.positionX = positionX;
    }

    public Double getPositionY()
    {
        return this.positionY;
    }

    public void setPositionY(Double positionY)
    {
        this.positionY = positionY;
    }

    public String getShardId()
    {
        return this.shardId;
    }

    public void setShardId(String shardId)
    {
        this.shardId = shardId;
    }

    public String getPartitionKey()
    {
        return this.partitionKey;
    }

    public void setPartitionKey(String partitionKey)
    {
        this.partitionKey = partitionKey;
    }

    public String getSequenceNumber()
    {
        return this.sequenceNumber;
    }

    public void setSequenceNumber(String sequenceNumber)
    {
        this.sequenceNumber = sequenceNumber;
    }

    public Long getServerTimestamp()
    {
        return serverTimestamp;
    }

    public void setServerTimestamp(Long serverArrivalTimestamp)
    {
        this.serverTimestamp = serverArrivalTimestamp;
    }
}
