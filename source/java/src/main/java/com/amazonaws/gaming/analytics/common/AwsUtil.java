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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;

/**
 * Convenience methods for creating AWS client objects.
 * 
 * @author AWS Solutions Team
 */
public class AwsUtil
{
    public static final int DEFAULT_CONNECTION_TIMEOUT = 5000;
    public static final int DEFAULT_SOCKET_TIMEOUT = 5000;
    public static final int DEFAULT_MAX_CONNECTIONS = 1;
    public static final Protocol DEFAULT_PROTOCOL = Protocol.HTTPS;

    public static ClientConfiguration getDefaultClientConfig()
    {
        return getClientConfig(DEFAULT_PROTOCOL, DEFAULT_CONNECTION_TIMEOUT, DEFAULT_SOCKET_TIMEOUT,
                DEFAULT_MAX_CONNECTIONS);
    }

    public static ClientConfiguration getClientConfig(int maxConnections)
    {
        return getClientConfig(DEFAULT_PROTOCOL, DEFAULT_CONNECTION_TIMEOUT, DEFAULT_SOCKET_TIMEOUT, maxConnections);
    }

    public static ClientConfiguration getClientConfig(int connectionTimeout, int socketTimeout)
    {
        return getClientConfig(DEFAULT_PROTOCOL, connectionTimeout, socketTimeout, DEFAULT_MAX_CONNECTIONS);
    }

    public static ClientConfiguration getClientConfig(Protocol protocol, int connectionTimeout, int socketTimeout,
            int maxConnections)
    {
        return new ClientConfiguration().withProtocol(protocol).withConnectionTimeout(connectionTimeout)
                .withSocketTimeout(socketTimeout).withMaxConnections(maxConnections);
    }
}
