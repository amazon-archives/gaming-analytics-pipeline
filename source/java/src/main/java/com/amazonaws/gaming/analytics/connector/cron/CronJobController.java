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
package com.amazonaws.gaming.analytics.connector.cron;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.Validate;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.amazonaws.gaming.analytics.common.AppConfiguration;
import com.amazonaws.gaming.analytics.connector.health.HealthCheckController;
import com.amazonaws.gaming.analytics.connector.redshift.RedshiftConnector;
import com.amazonaws.gaming.analytics.proprioception.Metric;
import com.amazonaws.gaming.analytics.proprioception.MetricRecorder;
import com.amazonaws.gaming.analytics.solution.SolutionMetricRecorder;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.google.common.base.Stopwatch;

/**
 * An Elastic Beanstalk worker tier application that is set up to listen on a set
 * of endpoints to perform periodic maintenance operations such as keeping up time-series
 * tables and running VACUUM/ANALYZE.  This app is also configured to set up the initial set
 * of tables in Redshift used by the rest of the pipeline.
 * 
 * @author AWS Solutions Team
 */
@RestController
public class CronJobController
{
    public static final Logger log = LoggerFactory.getLogger(CronJobController.class);

    private int redshiftDataLifetimeMonths;
    private MetricRecorder metricRecorder;
    private SolutionMetricRecorder solutionMetricRecorder;
    private boolean isMainApp;
    private final boolean sendAnonymousData;
    
    @Autowired
    private HealthCheckController healthCheckController;

    public CronJobController()
    {
        this.redshiftDataLifetimeMonths = AppConfiguration.INSTANCE.getInt("warm_data_lifetime_months");
        this.sendAnonymousData = AppConfiguration.INSTANCE.getBoolean("send_anonymous_data", false);
        this.isMainApp = AppConfiguration.INSTANCE.isCronConnector();
    }

    @PostConstruct
    protected void initialize() throws IOException, SQLException
    {
        if(this.isMainApp)
        {
            log.info("Initializing " + getClass().getName() + "...");
            
            this.metricRecorder = new MetricRecorder(getClass().getSimpleName());
            
            //At bootup, create all the time-series tables that we need in Redshift
            try (RedshiftConnector redshiftConn = new RedshiftConnector())
            {
                redshiftConn.open();
                
                //Starting at next month, create all monthly tables until the retention horizon
                //(in case it's not clear, passing in -1 will create next month)
                DateTime today = DateTime.now().withZone(DateTimeZone.UTC);
                for (int i = -1; i < this.redshiftDataLifetimeMonths; i++)
                {
                    DateTime month = today.minusMonths(i);
                    redshiftConn.createEventTable(month.getYear(), month.getMonthOfYear());
                }

                //Union all the tables together into a view
                List<String> eventTables = redshiftConn.getTables();
                redshiftConn.createUnionedView(eventTables);

                redshiftConn.commit();
            }
            
            if(this.sendAnonymousData)
            {
                this.solutionMetricRecorder = new SolutionMetricRecorder();
                this.solutionMetricRecorder.initialize();
            }
        }
    }

    /**
     * Submit a metric for publishing to CloudWatch Metrics. 
     * 
     * @param operation The name of the Operation dimension
     * @param name The name of the metric to publish
     * @param units The units of the metric
     * @param value The numeric value of the metric
     */
    protected void submitMetric(String operation, String name, StandardUnit units, double value)
    {
        Metric numRecordsMetric = this.metricRecorder.createMetric(name, units).withValue(value)
                .withDimension("Operation", operation);
        this.metricRecorder.putMetric(numRecordsMetric);
    }

    /**
     * Handle updating of time-series tables.  This means deleting any time-series table that
     * has been expired (outside the data lifetime window) and ensuring the table for the coming
     * month is created as well.  As a result, the UNION VIEW is also recreated.  
     * 
     * This gets called by an Elastic Beanstalk
     * worker tier application on a cron schedule (see src/main/webapp/cron.yaml).
     */
    @RequestMapping(value = "/redshift-time-series-table-create", method = RequestMethod.POST)
    public void updateTimeSeriesTables()
    {
        Validate.isTrue(this.isMainApp);

        log.info("updateTimeSeriesTables()");

        final String OPERATION = "CreateTimeSeriesTables";

        boolean updateTimeSeriesTablesAvailability = true;
        try(RedshiftConnector redshiftConn = new RedshiftConnector())
        {
            redshiftConn.setAutoCommit(false);
            
            DateTime today = DateTime.now().withZone(DateTimeZone.UTC);
            DateTime nextMonth = today.plusMonths(1);
            DateTime expiredMonth = today.minusMonths(this.redshiftDataLifetimeMonths);

            Stopwatch timer = Stopwatch.createStarted();
            redshiftConn.open();
            long redshiftConnectTime = timer.stop().elapsed(TimeUnit.MILLISECONDS);

            log.info("Creating time-series table for next month (" + nextMonth.getYear() + "-"
                    + nextMonth.getMonthOfYear() + ")...");

            // (re)create new table for next month
            String tableToRecreate = redshiftConn.constructEventTableName(nextMonth);

            timer.reset().start();
            redshiftConn.dropTable(tableToRecreate);
            long dropNextTableTime = timer.stop().elapsed(TimeUnit.MILLISECONDS);

            timer.reset().start();
            redshiftConn.createEventTable(nextMonth.getYear(), nextMonth.getMonthOfYear());
            long eventTableCreateTime = timer.stop().elapsed(TimeUnit.MILLISECONDS);

            // drop old table for previous month
            log.info("Dropping time-series table for expired month (" + expiredMonth.getYear() + "-"
                    + expiredMonth.getMonthOfYear() + ")...");
            String tableToDrop = redshiftConn.constructEventTableName(expiredMonth);
            timer.reset().start();
            redshiftConn.dropTable(tableToDrop);
            long dropPreviousTableTime = timer.stop().elapsed(TimeUnit.MILLISECONDS);

            // update view
            log.info("Updating time-series views with new tables...");
            List<String> eventTables = redshiftConn.getTables();
            timer.reset().start();

            redshiftConn.createUnionedView(eventTables);
            long createViewTime = timer.stop().elapsed(TimeUnit.MILLISECONDS);

            timer.reset().start();
            redshiftConn.commit();
            long commitTransactionTime = timer.stop().elapsed(TimeUnit.MILLISECONDS);

            submitMetric(OPERATION, "RedshiftConnectTime", StandardUnit.Milliseconds, redshiftConnectTime);
            submitMetric(OPERATION, "DropNextTableTime", StandardUnit.Milliseconds, dropNextTableTime);
            submitMetric(OPERATION, "CreateEventTableTime", StandardUnit.Milliseconds, eventTableCreateTime);
            submitMetric(OPERATION, "DropPreviousTableTime", StandardUnit.Milliseconds, dropPreviousTableTime);
            submitMetric(OPERATION, "CreateUnionedViewTime", StandardUnit.Milliseconds, createViewTime);
            submitMetric(OPERATION, "CommitTransactionTime", StandardUnit.Milliseconds, commitTransactionTime);
            
            this.healthCheckController.setHealthy(true);
        }
        catch (final Exception e)
        {
            updateTimeSeriesTablesAvailability = false;
            log.error("Error updating time series tables.", e);
            this.healthCheckController.setHealthy(false);
            throw new IllegalStateException("Operation Failed.");
        }
        finally
        {
            submitMetric(OPERATION, "Availability", StandardUnit.Count, updateTimeSeriesTablesAvailability ? 1 : 0);
            this.metricRecorder.attemptFlush(true);
        }

        log.info("Successfully updated time-series tables.");
    }

    /**
     * Handle running VACUUM and ANALYZE against all the available event tables.
     * 
     * This gets called by an Elastic Beanstalk
     * worker tier application on a cron schedule (see src/main/webapp/cron.yaml).
     */
    @RequestMapping(value = "/redshift-analyze-vacuum-tables", method = RequestMethod.POST)
    public void vacuumAndAnalyzeTables()
    {
        Validate.isTrue(this.isMainApp);

        log.info("analyzeVacuumTables()");

        final String OPERATION = "AnalyzeVacuumTables";

        boolean analyzeVacuumAvailability = true;
        try(RedshiftConnector redshiftConn = new RedshiftConnector())
        {
            redshiftConn.setAutoCommit(true);
            
            Stopwatch timer = Stopwatch.createStarted();
            redshiftConn.open();
            long redshiftConnectTime = timer.stop().elapsed(TimeUnit.MILLISECONDS);

            //Find list of event tables
            log.info("Getting list of tables from DB...");
            timer.reset().start();
            List<String> tables = redshiftConn.getTables();
            long getTablesTime = timer.stop().elapsed(TimeUnit.MILLISECONDS);

            //VACUUM all event tables
            log.info("Vacuuming " + tables.size() + " tables...");
            timer.reset().start();
            for (String table : tables)
            {
                try
                {
                    redshiftConn.vacuumTable(table, false);
                }
                catch (final Exception e)
                {
                    log.error("Could not vacuum table \"" + table + "\".", e);
                }
            }
            long vacuumAllTablesTime = timer.stop().elapsed(TimeUnit.MILLISECONDS);

            //ANALYZE all event tables (NOTE: this should come after VACUUM so stats
            //get updated for the tables in Redshift)
            log.info("Analyzing " + tables.size() + " tables...");
            timer.reset().start();
            for (String table : tables)
            {
                try
                {
                    redshiftConn.analyzeTable(table);
                }
                catch (final Exception e)
                {
                    log.warn("Could not analyze table \"" + table + "\".", e);
                }
            }
            long analyzeAllTablesTime = timer.stop().elapsed(TimeUnit.MILLISECONDS);

            submitMetric(OPERATION, "RedshiftConnectTime", StandardUnit.Milliseconds, redshiftConnectTime);
            submitMetric(OPERATION, "GetTablesTime", StandardUnit.Milliseconds, getTablesTime);
            submitMetric(OPERATION, "NumTables", StandardUnit.Count, tables.size());
            submitMetric(OPERATION, "VacuumTablesTime", StandardUnit.Milliseconds, vacuumAllTablesTime);
            submitMetric(OPERATION, "AnalyzeTablesTime", StandardUnit.Milliseconds, analyzeAllTablesTime);
            
            this.healthCheckController.setHealthy(true);
        }
        catch (final Exception e)
        {
            analyzeVacuumAvailability = false;
            log.error("Error fetching list of known tables.", e);
            this.healthCheckController.setHealthy(false);
            throw new IllegalStateException("Operation Failed.");
        }
        finally
        {
            submitMetric(OPERATION, "Availability", StandardUnit.Count, analyzeVacuumAvailability ? 1 : 0);
            this.metricRecorder.attemptFlush(true);
        }

        log.info("Successfully analyzed and vacuumed tables.");
    }

    /**
     * Handle reporting anonymous solution statistics.
     * 
     * This gets called by an Elastic Beanstalk
     * worker tier application on a cron schedule (see src/main/webapp/cron.yaml).
     */
    @RequestMapping(value = "/report-solution-statistics", method = RequestMethod.POST)
    public void reportSolutionStatistics()
    {
        Validate.isTrue(this.isMainApp);

        log.info("reportSolutionStatistics()");

        //if users opt out, make sure to not report anything
    	if(!this.sendAnonymousData || this.solutionMetricRecorder == null)
    	{
    		log.info("Reporting DISABLED. Ignoring request for solution metrics.");
    		return;
    	}
    	
    	log.info("Reporting ENABLED.  Starting report of solution metrics...");
    	
    	this.solutionMetricRecorder.reportMetrics();
    }
}
