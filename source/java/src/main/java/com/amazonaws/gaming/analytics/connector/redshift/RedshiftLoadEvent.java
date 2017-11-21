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
package com.amazonaws.gaming.analytics.connector.redshift;

import java.io.IOException;

import com.amazonaws.gaming.analytics.common.exception.TelemetryEventParseException;
import com.amazonaws.gaming.analytics.common.exception.TelemetryEventSerializationException;
import com.amazonaws.gaming.analytics.common.exception.TelemetryEventValidationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * A representation of a Kinesis event from the Kinesis file stream
 * for the application which essentially contains an S3 file pointer.
 * 
 * @author AWS Solutions Team
 */
public class RedshiftLoadEvent
{
    public static final String FILENAME_JSON_KEY = "filename";

    private Long serverTimestamp; 
    private String filename;
    private String sequenceNumber;

    public RedshiftLoadEvent()
    {
        this.filename = "";
    }

    public RedshiftLoadEvent(String filename)
    {
        this.filename = filename;
    }

    public String getFilename()
    {
        return this.filename;
    }

    public Long getServerTimestamp()
    {
        return this.serverTimestamp;
    }

    public String getSequenceNumber()
    {
        return this.sequenceNumber;
    }

    public void setSequenceNumber(String sequenceNumber)
    {
        this.sequenceNumber = sequenceNumber;
    }

    public void setServerTimestamp(Long arrivalTimestamp)
    {
        this.serverTimestamp = arrivalTimestamp;
    }

    public void setFilename(String filename)
    {
        this.filename = filename;
    }

    @Override
    public String toString()
    {
        return getFilename();
    }

    /**
     * Serialize this object to JSON.
     */
    public String toJson(final ObjectMapper mapper) throws TelemetryEventSerializationException
    {
        ObjectNode jsonRoot = mapper.createObjectNode();

        jsonRoot.put(FILENAME_JSON_KEY, this.filename);

        String json;
        try
        {
            json = mapper.writeValueAsString(jsonRoot);
        }
        catch (final JsonProcessingException e)
        {
            throw new TelemetryEventSerializationException(
                    "Failed to generate Redshift load JSON string: " + e.getMessage(), e);
        }

        return json;
    }
    
    /**
     * Deserialize values into this object from JSON.
     */
    public void parseFromJson(final ObjectMapper jsonParser, final String rawJson)
            throws TelemetryEventParseException, TelemetryEventValidationException
    {
        JsonNode jsonRoot = null;
        try
        {
            jsonRoot = jsonParser.readTree(rawJson);
        }
        catch (final IOException e)
        {
            throw new TelemetryEventParseException("Could not parse incoming Redshift load event as JSON: "
                    + e.getMessage() + " (Raw JSON = " + rawJson + ")");
        }

        JsonNode filenameNode = jsonRoot.get(FILENAME_JSON_KEY);
        if (filenameNode == null)
        {
            throw new TelemetryEventParseException("Missing required node \"" + FILENAME_JSON_KEY + "\".");
        }

        this.filename = filenameNode.asText().trim();
        if (this.filename.isEmpty())
        {
            throw new TelemetryEventValidationException("Input \"" + FILENAME_JSON_KEY + "\" is empty.", rawJson);
        }
    }
}
