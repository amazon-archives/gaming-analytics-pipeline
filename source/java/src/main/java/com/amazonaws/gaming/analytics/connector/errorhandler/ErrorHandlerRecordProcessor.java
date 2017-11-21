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
package com.amazonaws.gaming.analytics.connector.errorhandler;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.amazonaws.gaming.analytics.common.AppConfiguration;
import com.amazonaws.gaming.analytics.common.exception.TelemetryEventParseException;
import com.amazonaws.gaming.analytics.common.exception.TelemetryEventSerializationException;
import com.amazonaws.gaming.analytics.common.exception.TelemetryEventValidationException;
import com.amazonaws.gaming.analytics.connector.AbstractRecordProcessor;
import com.amazonaws.gaming.analytics.connector.KinesisEvent;
import com.amazonaws.gaming.analytics.connector.MemoryBuffer;
import com.amazonaws.gaming.analytics.connector.s3.S3Emitter;
import com.amazonaws.gaming.analytics.proprioception.MetricRecorder;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * An ErrorHandler record processor that validates incoming events and emits 
 * events that fail validation or need to be sanitized.
 * 
 * @author AWS Solutions Team
 */
public class ErrorHandlerRecordProcessor extends AbstractRecordProcessor<byte[]>
{
    private static final Logger log = LogManager.getLogger(ErrorHandlerRecordProcessor.class);

    public ErrorHandlerRecordProcessor(final String componentName, MetricRecorder metricRecorder)
    {
        super(componentName, metricRecorder);
    }

    /**
     * ${@inheritDoc}
     */
    @Override
    public void initialize(InitializationInput input)
    {
        super.initialize(input);

        this.buffer = new MemoryBuffer<>();

        String bucketName = AppConfiguration.INSTANCE.getString("s3_error_bucket");
        this.emitter = new S3Emitter(bucketName, input.getShardId(), getComponentName(), this.metricRecorder);
    }

    /**
     * ${@inheritDoc}
     */
    @Override
    protected Logger getLogger()
    {
        return log;
    }

    /**
     * Given a record that failed validation or sanitization, format it as an error record JSON
     * that can be emitted.
     * 
     * @param reason An explanation why the event failed validation or sanitization
     * @param fields The list of fields that were invalid or sanitized
     * @param json The raw request json
     * @param hex The raw request binary as hex
     * 
     * @return The error string to be emitted
     */
    private String createErrorJson(String reason, List<String> fields, String json, String hex)
    {
        ObjectNode jsonRoot = this.jsonMapper.createObjectNode();
        jsonRoot.put("reason", reason);

        if (json != null && !json.isEmpty())
        {
            jsonRoot.put("json", json);
        }

        if (fields != null && !fields.isEmpty())
        {
            ArrayNode fieldNode = jsonRoot.putArray("fields");
            for (String field : fields)
            {
                fieldNode.add(field);
            }
        }

        if (hex != null && !hex.isEmpty())
        {
            jsonRoot.put("hex", hex);
        }

        String outputJson = "";
        try
        {
            outputJson = this.jsonMapper.writeValueAsString(jsonRoot);
        }
        catch (JsonProcessingException e)
        {
            return "";
        }

        if (!outputJson.endsWith("\n"))
        {
            outputJson += "\n";
        }

        return outputJson;
    }

    /**
     * This processor is the inverse of the normal S3 record processor. It only cares about
     * events that fail validation or need sanitization and emits those.
     * 
     * ${@inheritDoc}
     */
    @Override
    public void processRecord(final Record record)
    {
        String errorJson = "";
        try
        {
            KinesisEvent event = getKinesisEventFromRecord(record);
            if (event.isRequiredSanitization())
            {
                info("Event failed sanitization!");
                errorJson = createErrorJson("SanitizationException", event.getSanitizedFields(), event.getRawJson(),
                        null);
                submitMetric("NumSanitizationErrors", StandardUnit.Count, 1);
                submitMetric("TotalErrors", StandardUnit.Count, 1);
            }
        }
        catch (TelemetryEventParseException | TelemetryEventValidationException
                | TelemetryEventSerializationException e)
        {
            if (e instanceof TelemetryEventValidationException)
            {
                submitMetric("NumValidationErrors", StandardUnit.Count, 1);
            }
            else if (e instanceof TelemetryEventSerializationException)
            {
                submitMetric("NumSerializationErrors", StandardUnit.Count, 1);
            }
            else
            {
                submitMetric("NumParseErrors", StandardUnit.Count, 1);
            }

            info("Event caused exception " + e.getClass().getSimpleName() + "!");

            String json = "";
            try
            {
                json = getJsonFromRecord(record);
            }
            catch (Exception e1)
            {
                // ignore
            }

            String hex = "";
            try
            {
                hex = this.getRawHexFromRecord(record);
            }
            catch (Exception e1)
            {
                // ignore
            }

            errorJson = createErrorJson(e.getClass().getSimpleName(), Collections.emptyList(), json, hex);
            submitMetric("TotalErrors", StandardUnit.Count, 1);
        }

        if (!errorJson.isEmpty())
        {
            byte[] errorJsonBytes = errorJson.getBytes(StandardCharsets.UTF_8);
            this.buffer.consumeRecord(errorJsonBytes, errorJsonBytes.length, record.getSequenceNumber(), 
                    DateTime.now().withZone(DateTimeZone.UTC),
                    DateTime.now().withZone(DateTimeZone.UTC));
        }
    }
}
