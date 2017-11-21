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
package com.amazonaws.gaming.analytics.connector.health;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * A bean to respond to health check requests from Elastic Beanstalk.
 * 
 * @author AWS Solutions Team
 */
@RestController
public class HealthCheckController
{
    public static final String HEALTH_CHECK_URL_MAPPING = "/health";
    
    private boolean healthy;

    public HealthCheckController()
    {
        this.healthy = true;
    }

    @RequestMapping(value = HEALTH_CHECK_URL_MAPPING, method = RequestMethod.GET)
    public ResponseEntity<Boolean> queryHealth()
    {
        return new ResponseEntity<>(isHealthy(), HttpStatus.OK);
    }

    public boolean isHealthy()
    {
        return this.healthy;
    }

    public void setHealthy(final boolean healthy)
    {
        this.healthy = healthy;
    }
}
