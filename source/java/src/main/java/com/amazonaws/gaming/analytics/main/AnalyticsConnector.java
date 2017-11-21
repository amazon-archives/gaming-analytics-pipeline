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
package com.amazonaws.gaming.analytics.main;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.http.HttpStatus;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.web.SpringBootServletInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import com.amazonaws.gaming.analytics.common.AppConfiguration;
import com.amazonaws.gaming.analytics.common.exception.ApplicationInitializationException;
import com.amazonaws.gaming.analytics.connector.AbstractRecordProcessorFactory;
import com.amazonaws.gaming.analytics.connector.ConnectorFactory;
import com.amazonaws.gaming.analytics.connector.health.HealthCheckController;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

/**
 * Main entry point for all connector applications.  
 * 
 * Applications run from the command line will enter via "main".
 * 
 * Applications run via Tomcat will boot via Spring.
 * 
 * @author AWS Solutions Team
 */
@Configuration
@EnableAutoConfiguration
@ComponentScan("com.amazonaws.gaming.analytics.connector")
@EnableWebMvc
public class AnalyticsConnector extends SpringBootServletInitializer implements ServletContextListener
{
    private static final Logger log = LogManager.getLogger(AnalyticsConnector.class);

    public static final String ERROR_URL_MAPPING = "/error";    
    public static final String CONNECTOR_TYPE_PARAM_NAME = "ConnectorType";
    public static final String PROJECT_NAME_PARAM_NAME = "ProjectName";
    public static boolean IS_RUNNING_CMD_LINE = false;
    
    private static boolean configurationLoaded = false;
    
    private Thread runnerThread;

    @Autowired
    private HealthCheckController healthCheckController;

    public static void main(final String[] args)
    {
        IS_RUNNING_CMD_LINE = true;

        loadConfiguration();

        SpringApplication.run(AnalyticsConnector.class, args);
    }

    /**
     * Initialize and load the application configuration.  It's fine if this is called
     * multiple times.  It is not thread-safe, but application boot-up is single-threaded.
     */
    private static void loadConfiguration()
    {
        //don't load the config again if we did it already
        if (configurationLoaded)
        {
            return;
        }

        String connectorName = System.getProperty(CONNECTOR_TYPE_PARAM_NAME);
        if(connectorName == null || connectorName.trim().isEmpty())
        {
            throw new ApplicationInitializationException("Missing required parameter: " + CONNECTOR_TYPE_PARAM_NAME);
        }
        
        String projectName = System.getProperty(PROJECT_NAME_PARAM_NAME);
        if(projectName == null || projectName.trim().isEmpty())
        {
            throw new ApplicationInitializationException("Missing required parameter: " + PROJECT_NAME_PARAM_NAME);
        }
        
        log.info("Initializing configuration for project \"" + projectName + "\" connector \"" + connectorName + "\" (local=\"" + IS_RUNNING_CMD_LINE + "\")");

        AppConfiguration.INSTANCE.initialize(IS_RUNNING_CMD_LINE, connectorName, projectName);

        configurationLoaded = true;
    }

    private void runConsumer()
    {
        if (AppConfiguration.INSTANCE.isCronConnector())
        {
            return;
        }

        //create a thread to instantiate the necessary KCL worker that will
        //create the record processors
        this.runnerThread = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                AbstractRecordProcessorFactory recordProcessorFactory;
                try
                {
                    recordProcessorFactory = ConnectorFactory.getRecordProcessorFactory();
                }
                catch (final Exception e)
                {
                    e.printStackTrace();
                    log.warn("Could not create a RecordProcessorFactory. Killing background thread.");
                    return;
                }

                final Worker worker = new Worker.Builder().recordProcessorFactory(recordProcessorFactory)
                        .config(recordProcessorFactory.getKinesisClientLibConfiguration()).build();

                log.info("Starting worker for " + getClass().getName());
                try
                {
                    worker.run();
                }
                catch (final Exception e)
                {
                    log.error("Caught exception while processing data: " + e.getMessage(), e);
                    return;
                }
            }
        });

        this.runnerThread.start();
    }

    /**
     * ${@inheritDoc}
     */
    @Override
    public void contextInitialized(final ServletContextEvent arg0)
    {
        log.info(getClass().getSimpleName() + ".contextInitialized(...)");
        loadConfiguration();
        AppConfiguration.INSTANCE.setHealthCheckController(this.healthCheckController);
        runConsumer();
    }

    /**
     * ${@inheritDoc}
     */
    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application)
    {
        log.info(getClass().getSimpleName() + ".configure(...)");
        loadConfiguration();
        AppConfiguration.INSTANCE.setHealthCheckController(this.healthCheckController);
        return application.sources(AnalyticsConnector.class);
    }

    /**
     * ${@inheritDoc}
     */
    @Override
    public void contextDestroyed(final ServletContextEvent arg0)
    {
        log.info(getClass().getSimpleName() + ".contextDestroyed(...)");
    }

    /**
     * Generate a servlet interceptor to check incoming HTTP requests.
     */
    @Bean
    public RequestMappingHandlerMapping requestMappingHandlerMapping()
    {
        RequestMappingHandlerMapping mapping = new RequestMappingHandlerMapping();
        mapping.setInterceptors(new Object[] { new ServletRequestInterceptor() });
        return mapping;
    }
    
    /**
     * Simple interceptor to limit the HTTP requests that are allowed to come in based on connector type.
     */
    private class ServletRequestInterceptor extends HandlerInterceptorAdapter
    {
        @Override
        public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
                throws Exception
        {
            if (HealthCheckController.HEALTH_CHECK_URL_MAPPING.equals(request.getServletPath()) || 
                AnalyticsConnector.ERROR_URL_MAPPING.equals(request.getServletPath()))
            {
                return true;
            }
            else if (AppConfiguration.INSTANCE.isCronConnector())
            {
                return super.preHandle(request, response, handler);
            }

            log.warn("Blocking access to path " + request.getServletPath() + " for unsupported app.");
            response.setStatus(HttpStatus.SC_FORBIDDEN);
            return false;
        }
    }
}
