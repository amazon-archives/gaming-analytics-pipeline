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

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.javatuples.Pair;
import org.joda.time.DateTime;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.gaming.analytics.common.AppConfiguration;
import com.amazonaws.gaming.analytics.common.AwsUtil;
import com.amazonaws.services.redshift.AmazonRedshift;
import com.amazonaws.services.redshift.AmazonRedshiftClientBuilder;
import com.amazonaws.services.redshift.model.GetClusterCredentialsRequest;
import com.amazonaws.services.redshift.model.GetClusterCredentialsResult;

/**
 * An adapter class for interacting with Amazon Redshift using standard SQL
 * over JDBC.
 * 
 * @author AWS Solutions Team
 */
public class RedshiftConnector implements Closeable, AutoCloseable
{
    private static final Logger log = LogManager.getLogger(RedshiftConnector.class);

    private final AWSCredentialsProvider credentialsProvider;
    private final AmazonRedshift redshiftClient;

    private final String databaseSchema;
    private final String databaseName;
    private final String databaseUser; 

    private final String clusterIdentifier;
    private final String jdbcUrl;
    private final String jdbcDriverName;
    private Connection dbConn;
    private boolean autoCommit;

    private final String eventFinalInsertFormat;
    private final String eventDedupeInsertFormat;
    private final String stagingTableCreateFormat;
    private final String eventTableCreateFormat;
    private final String copyPrefixFormat;
    private final String getUniqueYearsMonthsFormat;
    private final String analyzeTableFormat;
    private final String vacuumTableFormat;
    private final String vacuumTableReindexFormat;
    private final String dropTableFormat;

    private final String getCopyCountSql;
    private final String getInsertCountSql;
    private final String getAllTablesSql;
    private final String createViewPrefixSql;
    private final String createViewSuffixSql;
    private final String eventsTablePrefix;
    private final String getCopyErrorCountSql;

    public RedshiftConnector()
    {
        this(false);
    }

    public RedshiftConnector(boolean autoCommit)
    {
        this.databaseSchema = AppConfiguration.INSTANCE.getString("redshift_schema");
        this.databaseName = AppConfiguration.INSTANCE.getString("redshift_database");
        this.databaseUser = AppConfiguration.INSTANCE.getString("redshift_worker_username");
        this.clusterIdentifier = AppConfiguration.INSTANCE.getString("redshift_cluster_identifier");
        this.jdbcUrl = AppConfiguration.INSTANCE.getString("redshift_jdbc");

        this.credentialsProvider = AppConfiguration.INSTANCE.getCredentialsProvider();

        this.redshiftClient = AmazonRedshiftClientBuilder.standard().withCredentials(this.credentialsProvider)
                .withRegion(AppConfiguration.INSTANCE.getString("aws_region_name"))
                .withClientConfiguration(AwsUtil.getDefaultClientConfig()).build();

        this.jdbcDriverName = AppConfiguration.INSTANCE.getString("jdbc_driver_name");
        try
        {
            Class.forName(this.jdbcDriverName);
        }
        catch (ClassNotFoundException e)
        {
            throw new IllegalStateException(e);
        }

        this.eventDedupeInsertFormat = AppConfiguration.INSTANCE.getString("sql.event_dedupe_insert");
        this.eventFinalInsertFormat = AppConfiguration.INSTANCE.getString("sql.event_final_insert");
        this.stagingTableCreateFormat = AppConfiguration.INSTANCE.getString("sql.create_temp_table");
        this.eventTableCreateFormat = AppConfiguration.INSTANCE.getString("sql.create_event_table");
        this.copyPrefixFormat = AppConfiguration.INSTANCE.getString("sql.s3_copy_prefix");
        this.getUniqueYearsMonthsFormat = AppConfiguration.INSTANCE.getString("sql.get_unique_years_months");
        this.analyzeTableFormat = AppConfiguration.INSTANCE.getString("sql.analyze_table");
        this.vacuumTableFormat = AppConfiguration.INSTANCE.getString("sql.vacuum_table");
        this.vacuumTableReindexFormat = AppConfiguration.INSTANCE.getString("sql.vacuum_reindex_table");
        this.dropTableFormat = AppConfiguration.INSTANCE.getString("sql.drop_table");
        this.getCopyCountSql = AppConfiguration.INSTANCE.getString("sql.get_copy_count");
        this.getInsertCountSql = AppConfiguration.INSTANCE.getString("sql.get_insert_count");
        this.getAllTablesSql = AppConfiguration.INSTANCE.getString("sql.get_all_tables");
        this.createViewPrefixSql = AppConfiguration.INSTANCE.getString("sql.create_view_prefix");
        this.createViewSuffixSql = AppConfiguration.INSTANCE.getString("sql.create_view_suffix");
        this.eventsTablePrefix = AppConfiguration.INSTANCE.getString("events_table_prefix");
        this.getCopyErrorCountSql = AppConfiguration.INSTANCE.getString("sql.get_last_load_error_count");

        this.autoCommit = autoCommit;
    }

    public void open() throws SQLException
    {
        //Use the Redshift GetClusterCredentials API to generate a temporary DB username and password
        GetClusterCredentialsRequest gccRequest = new GetClusterCredentialsRequest()
                                                  .withAutoCreate(false)
                                                  .withClusterIdentifier(this.clusterIdentifier)
                                                  .withDbName(this.databaseName)
                                                  .withDbUser(this.databaseUser)
                                                  .withDurationSeconds(3600); //1 hour
        GetClusterCredentialsResult gccResult = redshiftClient.getClusterCredentials(gccRequest);

        //JDBC connection properties
        Properties connProps = new Properties();
        connProps.put("UID", gccResult.getDbUser());
        connProps.put("PWD", gccResult.getDbPassword());
        connProps.put("ssl", "true");

        log.info("Connecting to " + this.jdbcUrl + " as user \"" + gccResult.getDbUser() + "\".");

        this.dbConn = DriverManager.getConnection(this.jdbcUrl, connProps);
        this.dbConn.setAutoCommit(this.autoCommit);
    }

    /**
     * Given a year and month, get the name of the time-series event table for that month.
     */
    public String constructEventTableName(int year, int month)
    {
        return constructTimeSeriesTableName(this.eventsTablePrefix, year, month);
    }

    /**
     * Given a year and month, get the name of the time-series event table for that month.
     */
    public static String constructTimeSeriesTableName(String prefix, int year, int month)
    {
        String yearString = StringUtils.leftPad(Integer.toString(year), 4, '0');
        String monthString = StringUtils.leftPad(Integer.toString(month), 2, '0');
        return String.format("%s_%s_%s", prefix, yearString, monthString);
    }

    /**
     * Given a timestamp, get the name of the time-series event table for that month.
     */
    public String constructEventTableName(DateTime dt)
    {
        return constructEventTableName(dt.getYear(), dt.getMonthOfYear());
    }

    public boolean isAutoCommit(boolean autoCommit)
    {
        return this.autoCommit;
    }

    public void setAutoCommit(boolean autoCommit)
    {
        this.autoCommit = autoCommit;
    }

    /**
     * ${@inheritDoc}
     */
    @Override
    public void close() throws IOException
    {
        if (this.dbConn != null)
        {
            try
            {
                log.info("Closing connection.");
                this.dbConn.close();
            }
            catch (final SQLException e)
            {
                log.warn("Error closing connection.", e);
            }
        }
        this.dbConn = null;
    }

    /**
     * Roll back the current database transaction.
     */
    public void rollback()
    {
        try
        {
            if (this.dbConn != null && !this.dbConn.isClosed())
            {
                log.warn("Rolling back transaction.");
                this.dbConn.rollback();
            }
        }
        catch (Exception e)
        {
            log.error("Unable to rollback Amazon Redshift transaction to savepoint.", e);
        }
    }

    /**
     * Commit the current database transaction.
     */
    public void commit() throws SQLException
    {
        if (this.dbConn != null)
        {
            log.info("Committing transaction.");
            this.dbConn.commit();
        }
    }

    /**
     * Execute an SQL statement against the database via JDBC.
     */
    public void executeStatement(final String sql, final String queryName) throws SQLException
    {
        log.info("SQL = " + sql);

        try (final Statement stmt = this.dbConn.createStatement())
        {
            stmt.execute(sql);
        }
        catch (SQLException ex)
        {
            throw new SQLException("SQLException on " + queryName + " - " + ex.getMessage(), ex);
        }
    }

    /**
     * Get the total number of records copied by a COPY command.
     */
    public int getNumberOfCopiedRecords() throws SQLException
    {
        return querySingleIntValue(this.getCopyCountSql, "Count copy records");
    }

    /**
     * Get the total number of record errors during a COPY command.
     */
    public int getNumberOfLoadErrorsFromLastLoad() throws SQLException
    {
        return querySingleIntValue(this.getCopyErrorCountSql, "Count load errors");
    }

    /**
     * Get the total number of records inserted by an INSERT command.
     */
    public int getNumberOfInsertedRecords() throws SQLException
    {
        return querySingleIntValue(this.getInsertCountSql, "Count inserted records");
    }

    /**
     * Special purpose function to execute a query and return a result when it is known that
     * the result is a single integer value. Returns -1 on failure.
     */
    protected int querySingleIntValue(final String sql, final String operation) throws SQLException
    {
        log.info("SQL = " + sql);

        try (final Statement stmt = this.dbConn.createStatement())
        {
            try (final ResultSet resultSet = stmt.executeQuery(sql))
            {
                resultSet.next();
                return resultSet.getInt(1);
            }
        }
        catch (final Exception e)
        {
            log.warn("Query operation failed: " + operation, e);
        }

        return -1;
    }

    /**
     * Get the list of all the table names in the given schema.
     */
    public List<String> getTables() throws SQLException
    {
        final String sql = String.format(this.getAllTablesSql);

        log.info("SQL = " + sql);

        List<String> tables = new LinkedList<>();

        // The query itself handles deduping and ordering
        try (final Statement stmt = this.dbConn.createStatement())
        {
            try (final ResultSet resultSet = stmt.executeQuery(sql))
            {
                while (resultSet.next())
                {
                    String tableName = resultSet.getString(1);
                    tables.add(this.databaseSchema + "." + tableName);
                }
            }
        }
        catch (final Exception e)
        {
            log.warn("Query operation failed: " + e.getMessage(), e);
        }

        return tables;
    }

    /**
     * Create a UNION ALL view from a list of tables.
     */
    public void createUnionedView(List<String> tables) throws SQLException
    {
        if (tables.isEmpty())
        {
            return;
        }

        log.info("Creating UNIONed view with " + tables.size() + " tables.");

        StringBuilder sql = new StringBuilder(256);

        sql.append(this.createViewPrefixSql);

        sql.append("\nSELECT * FROM ");
        sql.append(tables.get(0));
        sql.append("\n");

        for (int i = 1; i < tables.size(); i++)
        {
            sql.append("UNION ALL\nSELECT * FROM ");
            sql.append(tables.get(i));
            sql.append("\n");
        }
        sql.append(";");
        sql.append(this.createViewSuffixSql);

        executeStatement(sql.toString(), "create union view");
    }

    /**
     * Create a new time-series event table.
     */
    public void createEventTable(int year, int month) throws SQLException
    {
        String eventTableName = constructEventTableName(year, month);
        createEventTable(eventTableName);
    }

    /**
     * Create a new time-series event table.
     */
    public void createEventTable(String eventTableName) throws SQLException
    {
        log.info("Creating table \"" + eventTableName + "\"...");

        String createEventTableSql = String.format(this.eventTableCreateFormat, eventTableName, eventTableName,
                eventTableName, eventTableName, eventTableName);
        executeStatement(createEventTableSql, "create event table");
    }

    /**
     * Create a new temporary staging table.
     */
    public void createStagingTable(String stagingTableName) throws SQLException
    {
        log.info("Creating staging table \"" + stagingTableName + "\"...");

        String createStagingTableSql = String.format(this.stagingTableCreateFormat, stagingTableName);
        executeStatement(createStagingTableSql, "create staging table");
    }

    /**
     * DROP a database table.
     */
    public void dropTable(String tableName) throws SQLException
    {
        log.info("Dropping table \"" + tableName + "\"...");

        String dropSql = String.format(this.dropTableFormat, tableName);
        executeStatement(dropSql, "drop table:" + tableName);
    }

    /**
     * VACUUM the specified table.
     */
    public void vacuumTable(String tableName, boolean reindex) throws SQLException
    {
        log.info("Vacuuming table \"" + tableName + "\" (reindex=" + reindex + ")...");

        String vacuumSql = String.format(reindex ? this.vacuumTableReindexFormat : this.vacuumTableFormat, tableName);
        executeStatement(vacuumSql, "vacuum table:" + tableName + " Reindex:" + reindex);
    }

    /**
     * ANALYZE the specified table.
     */
    public void analyzeTable(String tableName) throws SQLException
    {
        log.info("Analyzing table \"" + tableName + "\"...");

        String analyzeSql = String.format(this.analyzeTableFormat, tableName);
        executeStatement(analyzeSql, "analyze table");
    }

    /**
     * Get the unique year/month pairs for all the data in a given table.
     */
    public List<Pair<Integer, Integer>> executeGetUniqueYearsMonths(String tableName) throws SQLException
    {
        String sql = String.format(this.getUniqueYearsMonthsFormat, tableName);
        List<Pair<Integer, Integer>> yearMonthPairs = new LinkedList<>();

        // The query itself handles deduping and ordering, so what we should get
        // back is a list of [year,month] pairs ordered from oldest to newest
        try (final Statement stmt = this.dbConn.createStatement())
        {
            try (final ResultSet resultSet = stmt.executeQuery(sql))
            {
                while (resultSet.next())
                {
                    int year = resultSet.getInt(1);
                    int month = resultSet.getInt(2);
                    yearMonthPairs.add(new Pair<>(year, month));
                }
            }
        }
        catch (final Exception e)
        {
            log.warn("Query operation failed: " + e.getMessage(), e);
        }

        return yearMonthPairs;
    }

    /**
     * Take data from staging table, dedupe it and load it to a secondary staging table.
     */
    public void executeDedupeTableInsert(String dedupeStagingTableName, String eventTableName, int year, int month)
            throws SQLException
    {
        log.info("Inserting data from load staging table to dedupe staging table \"" + dedupeStagingTableName + "\"...");

        String mergeSql = String.format(this.eventDedupeInsertFormat, dedupeStagingTableName, eventTableName, year, month);
        executeStatement(mergeSql, "dedupe staging table insert");
    }

    /**
     * Take data from deduped staging table and insert into main time-series event table.
     */
    public void executeEventTableInsert(String dedupeStagingTableName, String eventTableName, int year, int month)
            throws SQLException
    {
        log.info("Inserting data from dedupe staging table \"" + dedupeStagingTableName + "\" to main table \"" + eventTableName + "\"...");

        String insertSql = String.format(this.eventFinalInsertFormat, eventTableName, dedupeStagingTableName, year, month);
        executeStatement(insertSql, "main event table insert");
    }

    /**
     * COPY data from S3 into Redshift using the specified manifest file.
     */
    public void executeCopyFromS3(String manifestFile) throws SQLException
    {
        String copyPrefixSql = String.format(this.copyPrefixFormat, manifestFile);

        final StringBuilder copyCmd = new StringBuilder(256);
        copyCmd.append(copyPrefixSql);
        copyCmd.append(" ");
        copyCmd.append(getRedshiftCredentialString());
        copyCmd.append(";");

        executeStatement(copyCmd.toString(), "copy from s3");
    }

    /**
     * Generate the CREDENTIAL string for a Redshift COPY command.
     */
    public String getRedshiftCredentialString()
    {
        final AWSCredentials credentials = this.credentialsProvider.getCredentials();

        final StringBuilder creds = new StringBuilder(128);

        creds.append("CREDENTIALS 'aws_access_key_id=");
        creds.append(credentials.getAWSAccessKeyId());
        creds.append(";aws_secret_access_key=");
        creds.append(credentials.getAWSSecretKey());
        if (credentials instanceof AWSSessionCredentials)
        {
            creds.append(";token=" + ((AWSSessionCredentials) credentials).getSessionToken());
        }
        creds.append("'");

        return creds.toString();
    }
}
