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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.javatuples.Pair;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.gaming.analytics.common.AppConfiguration;
import com.amazonaws.gaming.analytics.common.AwsUtil;
import com.amazonaws.gaming.analytics.connector.AbstractEmitter;
import com.amazonaws.gaming.analytics.connector.IBuffer;
import com.amazonaws.gaming.analytics.proprioception.MetricRecorder;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;

/**
 * An emitter implementation that works on buffers full of S3 file pointers
 * and emits them to Redshift.  It does this by collecting S3 file pointers, 
 * generating a Manifest file, writing it to S3 and then initiating a COPY
 * to Amazon Redshift.
 * 
 * @author AWS Solutions Team
 */
public class RedshiftManifestEmitter extends AbstractEmitter<String>
{
    private static final Logger log = LogManager.getLogger(RedshiftManifestEmitter.class);

    private final String loadStagingTableName;
    private final String dedupeStagingTablePrefix;
    private final int redshiftDataLifetimeMonths;
    private final boolean copyMandatory;
    private final String manifestPathPrefix;
    
    private final String sourceS3Bucket;
    private final AWSCredentialsProvider credentialsProvider;
    private final AmazonS3 s3Client;
    
    private final ObjectMapper jsonWriter;

    public RedshiftManifestEmitter(final String shardId, final String componentName,
            final MetricRecorder metricRecorder)
    {
        super(shardId, componentName, metricRecorder);

        this.loadStagingTableName = AppConfiguration.INSTANCE.getString("load_staging_table");
        this.dedupeStagingTablePrefix = AppConfiguration.INSTANCE.getString("dedupe_staging_table_prefix");
        this.copyMandatory = AppConfiguration.INSTANCE.getBoolean("copy_mandatory");
        this.manifestPathPrefix = AppConfiguration.INSTANCE.getString("s3_manifest_path_prefix");
        this.redshiftDataLifetimeMonths = AppConfiguration.INSTANCE.getInt("warm_data_lifetime_months");

        this.sourceS3Bucket = AppConfiguration.INSTANCE.getString("s3_telemetry_bucket");
        this.credentialsProvider = AppConfiguration.INSTANCE.getCredentialsProvider();
        this.s3Client = AmazonS3ClientBuilder.standard().withClientConfiguration(AwsUtil.getDefaultClientConfig())
                .withRegion(AppConfiguration.INSTANCE.getString("aws_region_name"))
                .withCredentials(this.credentialsProvider).build();

        this.jsonWriter = new ObjectMapper();
    }

    /**
     * Given a buffer full of S3 file pointers, emits them to Redshift via a sequence of SQL
     * commands to create TEMP tables, COPY from S3 and perform deduplicating upserts.
     * 
     * ${@inheritDoc}
     */
    @Override
    public List<String> emit(final IBuffer<String> buffer) throws IOException
    {
        final List<String> records = buffer.getRecords();

        submitMetric("NumRecordsReceived", StandardUnit.Count, records.size());

        boolean emitSuccess = true;
        try(RedshiftConnector redshiftConn = new RedshiftConnector(true))
        {
            //open a redshift connection
            Stopwatch timer = Stopwatch.createStarted();
            redshiftConn.open();
            long redshiftConnectTime = timer.elapsed(TimeUnit.MILLISECONDS);
            submitMetric("RedshiftConnectTime", StandardUnit.Milliseconds, redshiftConnectTime);

            //write a manifest file to S3
            final String manifestFileName = createManifestInS3(records);

            //use the manifest to insert data insert redshift 
            final int numRecordsInserted = writeDataToRedshift(redshiftConn, manifestFileName, records);

            timer.reset().start();

            //emit metrics
            final int numRecordsCopied = redshiftConn.getNumberOfCopiedRecords();
            long getCopiedRecordsTime = timer.stop().elapsed(TimeUnit.MILLISECONDS);
            final int numDuplicatesIgnored = numRecordsCopied - numRecordsInserted;
            submitMetric("NumRecordsCopied", StandardUnit.Count, numRecordsCopied);
            submitMetric("GetCopiedRecordsTime", StandardUnit.Milliseconds, getCopiedRecordsTime);
            submitMetric("NumDuplicateRecordsIgnored", StandardUnit.Count, numDuplicatesIgnored);

            info("Successful Amazon Redshift manifest copy of " + numRecordsCopied + " records from " + records.size()
                    + " files using manifest s3://" + this.sourceS3Bucket + "/" + manifestFileName);

            return Collections.emptyList();
        }
        catch (final Exception e)
        {
            emitSuccess = false;
            error("Error copying data from manifest file into Amazon Redshift. Failing this emit attempt. Reason:"
                    + e.getClass().getName() + ":" + e.getMessage(), e);
            return buffer.getRecords();
        }
        finally
        {
            submitMetric("EmitAvailability", StandardUnit.Count, emitSuccess ? 1 : 0);
        }
    }

    /**
     * Create a manifest file and write it to S3.
     */
    private String createManifestInS3(final List<String> records) throws IOException
    {
        final String manifestFileName = getManifestFileS3Path(records);
        writeManifestToS3(manifestFileName, records);
        return manifestFileName;
    }

    /**
     * Perform the series of SQL commands to COPY events to Redshift, deduplicate them
     * and insert them into their destination tables.
     */
    private int writeDataToRedshift(final RedshiftConnector redshiftConn, final String manifestFileName, final List<String> records) 
            throws IOException, SQLException
    {
        Stopwatch totalTimer = Stopwatch.createStarted();

        //Create a staging table to copy data into
        Stopwatch timer = Stopwatch.createStarted();
        redshiftConn.createStagingTable(this.loadStagingTableName);
        long createLoadStagingTableTime = timer.elapsed(TimeUnit.MILLISECONDS);

        //Copy data from S3 into staging table
        timer.reset().start();
        log.info("Initiating Amazon Redshift manifest copy of \"" + manifestFileName + "\" with " + records.size()
                + " files to staging table...");
        redshiftConn.executeCopyFromS3(manifestFileName);
        long copyFromS3Time = timer.elapsed(TimeUnit.MILLISECONDS);

        //check for errors in loading
        timer.reset().start();
        int loadErrorsCount = redshiftConn.getNumberOfLoadErrorsFromLastLoad();
        long getNumLoadErrorsTime = timer.elapsed(TimeUnit.MILLISECONDS);

        //dedupe and insert data from staging table into main table(s)
        timer.reset().start();
        int numRecordsInserted = executeInserts(redshiftConn);
        long executeInsertTime = timer.elapsed(TimeUnit.MILLISECONDS);

        //drop staging tables
        timer.reset().start();
        redshiftConn.dropTable(this.loadStagingTableName);
        long dropTableTime = timer.elapsed(TimeUnit.MILLISECONDS);

        long totalTime = totalTimer.elapsed(TimeUnit.MILLISECONDS);

        //emit metrics
        submitMetric("CreateLoadStagingTableTime", StandardUnit.Milliseconds, createLoadStagingTableTime);
        submitMetric("CopyFromS3Time", StandardUnit.Milliseconds, copyFromS3Time);
        submitMetric("UpsertTime", StandardUnit.Milliseconds, executeInsertTime);
        submitMetric("DropTableTime", StandardUnit.Milliseconds, dropTableTime);
        submitMetric("GetLoadErrorsTime", StandardUnit.Milliseconds, getNumLoadErrorsTime);
        submitMetric("LoadErrorsCount", StandardUnit.Count, loadErrorsCount);
        submitMetric("TotalLoadTime", StandardUnit.Milliseconds, totalTime);

        return numRecordsInserted;
    }

    /**
     * Once data has been copied into a staging table in S3, use this function will take
     * data in the staging table, deduplicate and insert it into the main tables.
     */
    private int executeInserts(final RedshiftConnector redshiftConn) throws SQLException
    {
        int numRecordsInserted = 0;

        // Normally we'll only be inserting data from the current month, but to
        // handle backfilling data or the times when we roll over from one month 
        // to the next, we need to consider what months our input data covers so
        // we find all the year/month combos that exist in the staging table and
        // that also fall within the bounds of our data retention window

        //find the upper bound of data we'll accept for insertion
        DateTime today = DateTime.now().withZone(DateTimeZone.UTC);
        DateTime upperBound = new DateTime(today.getYear(), today.getMonthOfYear(),
                today.dayOfMonth().getMaximumValue(), today.hourOfDay().getMaximumValue(),
                today.minuteOfHour().getMaximumValue(), today.secondOfMinute().getMaximumValue(),
                today.millisOfSecond().getMaximumValue(), DateTimeZone.UTC);

        //find the lower bound of data we'll accept for insertion
        DateTime cutoffDate = today.minusMonths(this.redshiftDataLifetimeMonths);
        DateTime lowerBound = new DateTime(cutoffDate.getYear(), cutoffDate.getMonthOfYear(),
                cutoffDate.dayOfMonth().getMinimumValue(), cutoffDate.hourOfDay().getMinimumValue(),
                cutoffDate.minuteOfHour().getMinimumValue(), cutoffDate.secondOfMinute().getMinimumValue(),
                cutoffDate.millisOfSecond().getMinimumValue(), DateTimeZone.UTC);

        Stopwatch timer = Stopwatch.createStarted();
        
        //find all the distinct year/month pairs in the staging table
        List<Pair<Integer, Integer>> yearMonthPairs = redshiftConn.executeGetUniqueYearsMonths(this.loadStagingTableName);
        long findYearMonthTime = timer.stop().elapsed(TimeUnit.MILLISECONDS);

        //since we're dealing with time-series tables (one per month) we loop through
        //and do upserts one month at a time
        for (Pair<Integer, Integer> pairs : yearMonthPairs)
        {
            int year = pairs.getValue0();
            int month = pairs.getValue1();

            DateTime dt = new DateTime(year, month, 1, 0, 0, 0, 0, DateTimeZone.UTC);
            if (dt.isBefore(lowerBound) || dt.isAfter(upperBound))
            {
                warn("Ignoring old expired data or far future data upsert for " + year + "/" + month + "...");
                continue;
            }

            info("Inserting data from " + year + "/" + month + "...");

            // create one dedupe_staging_table_yyyy_mm per year/month
            String dedupeStagingTableName = RedshiftConnector.constructTimeSeriesTableName(this.dedupeStagingTablePrefix, year, month);
            redshiftConn.createStagingTable(dedupeStagingTableName);

            // upsert from load_staging_table to dedupe_staging_table_yyyy_mm
            String eventTableName = redshiftConn.constructEventTableName(year, month);
            redshiftConn.executeDedupeTableInsert(dedupeStagingTableName, eventTableName, year, month);

            // upsert from dedupe_staging_table_yyyy_mm to events_yyyy_mm
            redshiftConn.executeEventTableInsert(dedupeStagingTableName, eventTableName, year, month);

            numRecordsInserted += redshiftConn.getNumberOfInsertedRecords();

            // drop dedupe_staging_table_yyyy_mm
            redshiftConn.dropTable(dedupeStagingTableName);
        }

        //emit metrics
        submitMetric("UpsertFindMonthsTime", StandardUnit.Milliseconds, findYearMonthTime);
        submitMetric("UpsertNumMonths", StandardUnit.Count, yearMonthPairs.size());
        submitMetric("UpsertNumRecordsInserted", StandardUnit.Count, numRecordsInserted);

        return numRecordsInserted;
    }

    /**
     * ${@inheritDoc}
     */
    @Override
    public void fail(final List<String> records)
    {
        for (final String record : records)
        {
            error("Record FAILED: " + record);
        }
    }

    /**
     * Generate a manifest file and write it to S3.
     */
    private void writeManifestToS3(final String fileName, final List<String> records) throws IOException
    {
        //generate a manifest file and serialize it to an in-memory byte array
        final String fileContents = generateManifestFile(records);
        byte[] fileContentBytes = fileContents.getBytes(this.utfDecoder.charset());

        final ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(fileContentBytes.length);
        metadata.addUserMetadata("NumFilesInManifest", Integer.toString(records.size()));

        //upload manifest file to S3
        final ByteArrayInputStream fileBytes = new ByteArrayInputStream(fileContentBytes);
        final PutObjectRequest putObjectRequest = new PutObjectRequest(this.sourceS3Bucket, fileName, fileBytes, metadata);

        Stopwatch timer = Stopwatch.createStarted();
        this.s3Client.putObject(putObjectRequest);
        long s3PutTime = timer.stop().elapsed(TimeUnit.MILLISECONDS);

        //emit metrics
        submitMetric("NumFilesPerManifest", StandardUnit.Count, records.size());
        submitMetric("ManifestPutTime", StandardUnit.Milliseconds, s3PutTime);
        
        info("Successfully wrote manifest file to: s3://" + this.sourceS3Bucket + "/" + fileName);
    }

    /**
     * Generate a unique path to write the manifest file to S3.
     */
    private String getManifestFileS3Path(final List<String> records)
    {
        // Manifest file is named in the format manifests/{firstFileName}-{lastFileName}
        //(leave Year/Month/Day/Hour on first record)
        final String first = records.get(0);
        final String last = records.get(records.size() - 1) .substring(records.get(records.size() - 1).lastIndexOf('/') + 1);

        return this.manifestPathPrefix + "/" + first + "-" + last + ".manifest";
    }

    /**
     * Create a Redshift manifest file from a list of S3 file pointerse.
     * 
     * Format for Redshift Manifest File:
     *
     * { "entries": [ {"url":"s3://s3Bucket/file1","mandatory":true},
     * {"url":"s3://s3Bucket/file2","mandatory":true},
     * {"url":"s3://s3Bucket/file3","mandatory":true} ] }
     * 
     * @see https://docs.aws.amazon.com/redshift/latest/dg/loading-data-files-using-manifest.html
     */
    private String generateManifestFile(final List<String> files) throws JsonProcessingException
    {
        final ObjectNode root = this.jsonWriter.createObjectNode();
        final ArrayNode entriesNode = root.putArray("entries");

        for (final String file : files)
        {
            final ObjectNode entry = entriesNode.objectNode();
            entry.put("url", "s3://" + this.sourceS3Bucket + "/" + file);
            entry.put("mandatory", this.copyMandatory);
            entriesNode.add(entry);
        }

        return this.jsonWriter.writeValueAsString(root);
    }

    /**
     * ${@inheritDoc}
     */
    @Override
    public void shutdown()
    {
        this.s3Client.shutdown();
    }

    /**
     * ${@inheritDoc}
     */
    @Override
    protected Logger getLogger()
    {
        return log;
    }
}
