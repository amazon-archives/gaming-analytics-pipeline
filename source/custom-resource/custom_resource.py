#!/usr/bin/python
# -*- coding: utf-8 -*-

##############################################################################
#  Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.   #
#                                                                            #
#  Licensed under the Amazon Software License (the 'License'). You may not   #
#  use this file except in compliance with the License. A copy of the        #
#  License is located at                                                     #
#                                                                            #
#      http://aws.amazon.com/asl/                                            #
#                                                                            #
#  or in the 'license' file accompanying this file. This file is distributed #
#  on an 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,        #
#  express or implied. See the License for the specific language governing   #
#  permissions and limitations under the License.                            #
##############################################################################

import json
import logging
import uuid

import urllib2

from botocore.vendored import requests
from custom_resource import redshift_setup

log = logging.getLogger()
log.setLevel(logging.INFO)

def lambda_handler(event, context):

  if event['RequestType'] == 'Create':
    create(event, context)

  else:
    returnSuccess(event, context)


def sendResponse(event, context, responseStatus, resourceId):
  responseBody = {
    'Status': responseStatus,
    'PhysicalResourceId': resourceId,
    'StackId': event['StackId'],
    'RequestId': event['RequestId'],
    'LogicalResourceId': event['LogicalResourceId']
  }

  if responseStatus == 'FAILED':
    responseBody['Reason'] = context.log_stream_name

  log.info('RESPONSE BODY:n' + json.dumps(responseBody))

  try:
    if event['ResponseURL'] == "http://pre-signed-S3-url-for-response":
      return "SUCCESS"
    else:
      requests.put(event['ResponseURL'], data=json.dumps(responseBody))
      return

  except Exception as e:
    responseStatus = 'FAILED'
    if event['ResponseURL'] == "http://pre-signed-S3-url-for-response":
      return "FAILED"
    else:
      sendResponse(event, context, responseStatus, resourceId)
      log.error(e)
      raise

def create(event, context):
  try:

    if redshift_setup.init(event) == True:
      responseStatus = 'SUCCESS'
    else:
      responseStatus = 'FAILED'

    resourceId = str(uuid.uuid4())
    sendResponse(event, context, responseStatus, resourceId)

  except Exception as e:
    log.error(e)
    responseStatus = 'FAILED'
    resourceId = context.log_stream_name
    sendResponse(event, context, responseStatus, resourceId)

def returnSuccess(event, context):
  try:
    responseStatus = 'SUCCESS'
    sendResponse(event, context, responseStatus, event['LogicalResourceId'])

  except Exception as e:
    log.error(e)
    responseStatus = 'FAILED'
    sendResponse(event, context, responseStatus, event['LogicalResourceId'])
