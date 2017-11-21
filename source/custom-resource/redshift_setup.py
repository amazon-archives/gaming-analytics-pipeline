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

import logging
import pg8000

from timeout import timeout
from time import time

log = logging.getLogger()
log.setLevel(logging.INFO)

@timeout(20)
def init(event):
  print 'redshift_setup init'  

  try:
    username = event['ResourceProperties']['Username']
    password = event['ResourceProperties']['Password']

    database = event['ResourceProperties']['DatabaseName']
    host = event['ResourceProperties']['Host']
    port = int(event['ResourceProperties']['Port'])

    analytics_worker = event['ResourceProperties']['WorkerUsername']
    worker_password = event['ResourceProperties']['WorkerPassword']
    analytics_readonly = event['ResourceProperties']['ReadOnlyUsername']
    readonly_password = event['ResourceProperties']['ReadOnlyPassword']
    schema = event['ResourceProperties']['SchemaName']

    print 'Attempting to connect to Redshift'

    # Connect to Redshift
    conn = pg8000.connect(user=username, host=host, port=port, database=database, password=password, ssl=True)
    
    print 'Connection Successful'
    cursor = conn.cursor()

    log.info('revoking PRIVILEGES')
    cursor.execute('REVOKE TEMP ON DATABASE %s FROM PUBLIC' % database)
    cursor.execute('REVOKE ALL PRIVILEGES ON SCHEMA public FROM PUBLIC')
    cursor.execute('REVOKE ALL PRIVILEGES ON SCHEMA information_schema FROM PUBLIC')
    cursor.execute('REVOKE ALL PRIVILEGES ON SCHEMA pg_catalog FROM PUBLIC')

    # Create db users
    log.info('create db users ' + analytics_worker + ', ' + analytics_readonly)
    cursor.execute('CREATE USER %s WITH NOCREATEDB NOCREATEUSER PASSWORD \'%s\'' % (analytics_worker , worker_password))
    cursor.execute('CREATE USER %s WITH NOCREATEDB NOCREATEUSER PASSWORD \'%s\'' % (analytics_readonly, readonly_password))
 
    # Create new non-public schema
    log.info('create new non-public schema')
    cursor.execute('CREATE SCHEMA IF NOT EXISTS %s AUTHORIZATION %s' % (schema, analytics_worker))
    cursor.execute('REVOKE ALL PRIVILEGES ON SCHEMA %s FROM PUBLIC' % schema)

    log.info('set search path')
    # Set the Redshift search path
    # https://docs.aws.amazon.com/redshift/latest/dg/r_search_path.html
    cursor.execute('SET search_path to %s, \'$user\', public' % schema)

    # Set worker user permissions
    log.info('set worker user permissions')
    cursor.execute('GRANT USAGE ON SCHEMA %s TO %s' % (schema, analytics_worker))
    cursor.execute('GRANT USAGE ON SCHEMA information_schema TO %s' % analytics_worker)
    cursor.execute('GRANT USAGE ON SCHEMA pg_catalog TO %s' % analytics_worker)
    cursor.execute('GRANT TEMP ON DATABASE %s TO %s' % (database, analytics_worker))
    cursor.execute('GRANT SELECT ON stv_tbl_perm TO %s' % analytics_worker)

    # Set read only user permissions
    log.info('set readonly permissions')
    cursor.execute('GRANT USAGE ON SCHEMA %s TO %s' % (schema, analytics_readonly))
    cursor.execute('GRANT USAGE ON SCHEMA information_schema TO %s' % analytics_readonly)
    cursor.execute('GRANT USAGE ON SCHEMA pg_catalog TO %s' % analytics_readonly)
    cursor.execute('GRANT SELECT ON stv_tbl_perm TO %s' % analytics_readonly)
    cursor.execute('GRANT USAGE ON LANGUAGE plpythonu TO %s' % analytics_readonly)

    conn.commit()

    log.info('SQL executed successfully.')
    return True
  except Exception as e:
    log.error('Error executing SQL.')
    log.error(e)
    return False