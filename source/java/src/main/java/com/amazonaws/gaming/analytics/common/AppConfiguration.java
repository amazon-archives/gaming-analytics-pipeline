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
package com.amazonaws.gaming.analytics.common;

import java.net.URL;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.gaming.analytics.connector.health.HealthCheckController;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.google.common.io.Resources;

/**
 * Custom global static configuration singleton to provide a set of configuration overrides.
 *
 * <project> is the value of the "projectName" Java System property (e.g. "analytics")
 * <connector_type> is the value of the "ConnectorType" Java System property (e.g. "redshift", "s3compound", "cron", etc.)
 *
 * Order of precendence:
 * 
 * 1. Java System Property (System.getProperty) (Elastic Beanstalk environment variables come in as
 * Java System Property values as well)
 * 2. test.<project>.<connector_type>.<property_name>
 * 3. test.common.<connector_type>.<property_name>
 * 4. <project>.<connector_type>.<property_name>
 * 5. common.<connector_type>.<property_name>
 * 6. <project>.<property_name>
 * 7. common.<property_name>
 * 
 * NOTE: "test" is auto-prepended to property names when the app is run locally 
 * (e.g. in Eclipse or via command line when "main" is invoked)
 * 
 * @author AWS Solutions Team
 * 
 */
public class AppConfiguration
{
    private static final Logger log = LogManager.getLogger(AppConfiguration.class);

    private static final String[] PROPERTIES_FILES = { 
            "connector.properties", 
            "sql.properties",
            "proprioception.properties"
    };

    private static final String AWS_ROLE_ARN_PROPERTY = "aws.roleArn";

    private static final String LOCAL_PROPERTY_PREFIX = "test.";
    private static final String COMMON_PROPERTY_PREFIX = "common.";

    /** The single global static instance (declaring this way avoids the double-checked locking unsafe debate) */
    public static final AppConfiguration INSTANCE = new AppConfiguration();

    private AWSCredentialsProvider provider;
    
    private boolean isRunningCmdLine;
    
    private String connectorType;
    private String projectName;

    private SystemConfiguration javaSystemConfig;
    private CompositeConfiguration serviceConfig;

    private HealthCheckController healthCheckController;

    private AppConfiguration()
    {
        this.isRunningCmdLine = false;
    }

    public boolean isCronConnector()
    {
        return "cron".equalsIgnoreCase(this.connectorType);
    }

    /**
     * Load all of the available configuration into "serviceConfig".
     * 
     * Java System properties are loaded first, then .properties files are loaded in the order
     * they are listed in the PROPERTIES_FILES array.
     * 
     * @param isRunningCmdLine true if running from "main", false if running from Tomcat (e.g. on EB)
     * @param connectorType The type of app being run (e.g. "redshift", "s3compound", etc.)
     * @param projectName The project name being run (e.g. "analytics")
     */
    public void initialize(final boolean isRunningCmdLine, final String connectorType, final String projectName)
    {
        this.isRunningCmdLine = isRunningCmdLine;
        this.connectorType = connectorType.toLowerCase();
        this.projectName = projectName.toLowerCase();

        this.serviceConfig = new CompositeConfiguration();
        this.serviceConfig.setThrowExceptionOnMissing(false);
        this.javaSystemConfig = new SystemConfiguration();
        this.serviceConfig.addConfiguration(this.javaSystemConfig);

        for (String filename : PROPERTIES_FILES)
        {
        	if(!loadConfigurationFile(filename))
        	{
        	    log.info("Loaded configuration from file: " + filename);
        	}
        	else
        	{
        		log.error("Failed to load configuration file: " + filename);
        	}
        }
    }
    
    protected boolean loadConfigurationFile(String filename)
    {
    	try
        {
            this.serviceConfig.addConfiguration(new PropertiesConfiguration(filename));
        }
        catch (final ConfigurationException ce)
        {
            log.warn("Failed to load properties file " + filename + " via standard mechanism.", ce);
            try
            {
                URL url = Resources.getResource(filename);
                this.serviceConfig.addConfiguration(new PropertiesConfiguration(url));
            }
            catch (Exception e)
            {
                log.error("Could not parse local file \"" + filename + "\": " + e.getMessage(), e);
                return false;
            }
        }
    	
    	return true;
    }
    
    /**
     * Clear all existing configuration. After calling this function, "hasProperty" will return false
     * for any value not set in Java system properties.
     */
    public void clear()
    {
        this.serviceConfig.clear();
    }

    /**
     * Test if a property exists.  Follows the same rules for property names as the getString() method.
     * 
     * @param propertyName The name of the property to fetch with any prefix (connector, project, etc.) stripped off
     * 
     * @return true if the property exists, false otherwise.
     * 
     * @see #getString(String)
     */
    public boolean hasProperty(final String propertyName)
    {
        if(this.javaSystemConfig.containsKey(propertyName))
        {
            return true;
        }
        
        List<String> propertiesToAttempt = getOrderedPropertyList(propertyName);
        for (String propertyToAttempt : propertiesToAttempt)
        {
            if (this.serviceConfig.containsKey(propertyToAttempt))
            {
                return true;
            }
        }
        
        return false;
    }

    /**
     * Fetch a property by name as a string.
     * 
     * It's important to note that any project/connector prefixes are expected to already be
     * stripped off of the input "propertyName" when it is passed to this method.
     *  
     * For instance, to retrive the value of "analytics.s3compound.buffer_byte_size_limit" from connector.properties,
     * you would simply call getString("buffer_byte_size_limit")
     * 
     * @param propertyName The name of the property to fetch with any prefix (connector, project, etc.) stripped off
     * 
     * @return The value of the property if it exists or null otherwise.
     */
    public String getString(final String propertyName)
    {
        if(this.javaSystemConfig.containsKey(propertyName))
        {
            return this.javaSystemConfig.getString(propertyName);
        }
        
        List<String> propertiesToAttempt = getOrderedPropertyList(propertyName);
        for (String propertyToAttempt : propertiesToAttempt)
        {
            if (this.serviceConfig.containsKey(propertyToAttempt))
            {
                return this.serviceConfig.getString(propertyToAttempt);
            }
        }

        log.warn("No value found for property named \"" + propertyName + "\"");
        return null;
    }
    
    /**
     * Fetch a property by name as a string or return the default value if the property is not found.
     * 
     * @param propertyName The name of the property to fetch with any prefix (connector, project, etc.) stripped off.
     * @param defaultValue The default value to return if the specified propertyName is not found.
     * @return The value of the property if it exists or defaultValue otherwise.
     * @see #getString(String)
     */
    public String getString(final String propertyName, final String defaultValue)
    {
        String propertyValue = getString(propertyName);
        if (propertyValue == null)
        {
            return defaultValue;
        }

        return propertyValue;
    }

    /**
     * Fetch a property by name as a list.
     * 
     * It's important to note that any project/connector prefixes are expected to already be
     * stripped off of the input "propertyName" when it is passed to this method.
     *  
     * For instance, to retrive the value of "analytics.s3compound.buffer_byte_size_limit" from connector.properties,
     * you would simply call getString("buffer_byte_size_limit")
     * 
     * @param propertyName The name of the property to fetch with any prefix (connector, project, etc.) stripped off
     * 
     * @return The value of the property if it exists or null otherwise.
     */
    public String[] getList(final String propertyName)
    {
        if(this.javaSystemConfig.containsKey(propertyName))
        {
            return this.javaSystemConfig.getList(propertyName).toArray(new String[] {});
        }
        
        List<String> propertiesToAttempt = getOrderedPropertyList(propertyName);
        for (String propertyToAttempt : propertiesToAttempt)
        {
            if(this.serviceConfig.containsKey(propertyToAttempt))
            {
                return this.serviceConfig.getList(propertyToAttempt).toArray(new String[] {});
            }
        }

        log.warn("No value found for property named \"" + propertyName + "\"");
        return null;
    }
    
    /**
     * Fetch a property by name as a list or return the default value if the property is not found.
     * 
     * @param propertyName The name of the property to fetch with any prefix (connector, project, etc.) stripped off.
     * @param defaultValue The default value to return if the specified propertyName is not found.
     * @return The value of the property if it exists or defaultValue otherwise.
     * @see #getString(String)
     */
    public String[] getList(final String propertyName, final String[] defaultValue)
    {
        String[] propertyValue = getList(propertyName);
        if (propertyValue == null)
        {
            return defaultValue;
        }

        return propertyValue;
    }

    /**
     * Fetch a property by name as a long.
     * 
     * @param propertyName The name of the property to fetch with any prefix (connector, project, etc.) stripped off.
     * @param defaultValue The default value to return if the specified propertyName is not found.
     * @return The value of the property if it exists, otherwise an exception will be thrown.
     * @see #getString(String)
     */    
    public long getLong(final String propertyName)
    {
        return Long.parseLong(getString(propertyName));
    }

    /**
     * Fetch a property by name as a long or return the default value if the property is not found.
     * 
     * @param propertyName The name of the property to fetch with any prefix (connector, project, etc.) stripped off.
     * @param defaultValue The default value to return if the specified propertyName is not found.
     * @return The value of the property if it exists or defaultValue otherwise.
     * @see #getString(String)
     */    
    public long getLong(final String propertyName, final long defaultValue)
    {
        String propertyValue = getString(propertyName);
        if (propertyValue == null)
        {
            return defaultValue;
        }

        return Long.parseLong(propertyValue);
    }

    /**
     * Fetch a property by name as an int.
     * 
     * @param propertyName The name of the property to fetch with any prefix (connector, project, etc.) stripped off.
     * @param defaultValue The default value to return if the specified propertyName is not found.
     * @return The value of the property if it exists, otherwise an exception will be thrown.
     * @see #getString(String)
     */    
    public int getInt(final String propertyName)
    {
        return Integer.parseInt(getString(propertyName));
    }

    /**
     * Fetch a property by name as an int or return the default value if the property is not found.
     * 
     * @param propertyName The name of the property to fetch with any prefix (connector, project, etc.) stripped off.
     * @param defaultValue The default value to return if the specified propertyName is not found.
     * @return The value of the property if it exists or defaultValue otherwise.
     * @see #getString(String)
     */    
    public int getInt(final String propertyName, final int defaultValue)
    {
        String propertyValue = getString(propertyName);
        if (propertyValue == null)
        {
            return defaultValue;
        }

        return Integer.parseInt(propertyValue);
    }

    /**
     * Fetch a property by name as a float.
     * 
     * @param propertyName The name of the property to fetch with any prefix (connector, project, etc.) stripped off.
     * @param defaultValue The default value to return if the specified propertyName is not found.
     * @return The value of the property if it exists, otherwise an exception will be thrown.
     * @see #getString(String)
     */
    public float getFloat(final String propertyName)
    {
        return Float.parseFloat(getString(propertyName));
    }

    /**
     * Fetch a property by name as a float or return the default value if the property is not found.
     * 
     * @param propertyName The name of the property to fetch with any prefix (connector, project, etc.) stripped off.
     * @param defaultValue The default value to return if the specified propertyName is not found.
     * @return The value of the property if it exists or defaultValue otherwise.
     * @see #getString(String)
     */    
    public float getFloat(final String propertyName, final float defaultValue)
    {
        String propertyValue = getString(propertyName);
        if (propertyValue == null)
        {
            return defaultValue;
        }

        return Float.parseFloat(propertyValue);
    }

    /**
     * Fetch a property by name as a double.
     * 
     * @param propertyName The name of the property to fetch with any prefix (connector, project, etc.) stripped off.
     * @param defaultValue The default value to return if the specified propertyName is not found.
     * @return The value of the property if it exists, otherwise an exception will be thrown.
     * @see #getString(String)
     */
    public double getDouble(final String propertyName)
    {
        return Double.parseDouble(getString(propertyName));
    }

    /**
     * Fetch a property by name as a double or return the default value if the property is not found.
     * 
     * @param propertyName The name of the property to fetch with any prefix (connector, project, etc.) stripped off.
     * @param defaultValue The default value to return if the specified propertyName is not found.
     * @return The value of the property if it exists or defaultValue otherwise.
     * @see #getString(String)
     */    
    public double getDouble(final String propertyName, final double defaultValue)
    {
        String propertyValue = getString(propertyName);
        if (propertyValue == null)
        {
            return defaultValue;
        }

        return Double.parseDouble(propertyValue);
    }

    /**
     * Fetch a property by name as a boolean.
     * 
     * @param propertyName The name of the property to fetch with any prefix (connector, project, etc.) stripped off.
     * @param defaultValue The default value to return if the specified propertyName is not found.
     * @return The value of the property if it exists, otherwise an exception will be thrown.
     * @see #getString(String)
     */    
    public boolean getBoolean(final String propertyName)
    {
        return Boolean.parseBoolean(getString(propertyName));
    }

    /**
     * Fetch a property by name as a boolean or return the default value if the property is not found.
     * 
     * @param propertyName The name of the property to fetch with any prefix (connector, project, etc.) stripped off.
     * @param defaultValue The default value to return if the specified propertyName is not found.
     * @return The value of the property if it exists or defaultValue otherwise.
     * @see #getString(String)
     */    
    public boolean getBoolean(final String propertyName, final boolean defaultValue)
    {
        String propertyValue = getString(propertyName);
        if (propertyValue == null)
        {
            return defaultValue;
        }

        return Boolean.parseBoolean(propertyValue);
    }

    public AWSCredentialsProvider getCredentialsProvider()
    {
        if (this.provider == null)
        {
            findCredentials();
        }
        
        return this.provider;
    }

    public boolean isRunningCmdLine()
    {
        return this.isRunningCmdLine;
    }

    public String getConnectorType()
    {
        return this.connectorType;
    }

    public String getProjectName()
    {
        return this.projectName;
    }

    public HealthCheckController getHealthCheckController()
    {
        return this.healthCheckController;
    }

    public void setHealthCheckController(final HealthCheckController healthCheckController)
    {
        this.healthCheckController = healthCheckController;
    }

    /**
     * Convenience method for finding AWS Credentials in both local and on AWS scenarios.
     */
    private void findCredentials()
    {
        final String roleArn = System.getProperty(AWS_ROLE_ARN_PROPERTY);
        if (isRunningCmdLine() && roleArn != null)
        {
            log.info("Assuming IAM role " + roleArn);

            // STS defaults to a global, non-regional endpoint
            ClientConfiguration clientConfig = AwsUtil.getDefaultClientConfig();

            AWSSecurityTokenService sts = AWSSecurityTokenServiceClientBuilder.standard()
                    .withCredentials(new DefaultAWSCredentialsProviderChain()).withClientConfiguration(clientConfig)
                    .build();

            this.provider = new STSAssumeRoleSessionCredentialsProvider.Builder(roleArn, "CommandLineBuild")
                    .withStsClient(sts).build();
        }
        else
        {
            this.provider = new DefaultAWSCredentialsProviderChain();
        }
    }
    
    private List<String> getOrderedPropertyList(final String propertyName)
    {
        List<String> propertiesToAttempt = new LinkedList<>();

        if (this.isRunningCmdLine)
        {
            propertiesToAttempt.add(LOCAL_PROPERTY_PREFIX + this.projectName + "." + this.connectorType + "." + propertyName);
            propertiesToAttempt.add(LOCAL_PROPERTY_PREFIX + COMMON_PROPERTY_PREFIX + this.connectorType + "." + propertyName);
        }
        propertiesToAttempt.add(this.projectName + "." + this.connectorType + "." + propertyName);
        propertiesToAttempt.add(COMMON_PROPERTY_PREFIX + this.connectorType + "." + propertyName);

        propertiesToAttempt.add(this.projectName + "." + propertyName);
        propertiesToAttempt.add(COMMON_PROPERTY_PREFIX + propertyName);
        
        return propertiesToAttempt;
    }
}
