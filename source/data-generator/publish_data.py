######################################################################################################################
# Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved. *
# 
# Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance *
# with the License. A copy of the License is located at *
# 
# http://aws.amazon.com/asl/ *
# 
# or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES *
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions *
# and limitations under the License. *
######################################################################################################################

import argparse
import boto3.session
import json
import random
import time
import uuid

DEFAULT_APP_NAME = 'SampleGame'
DEFAULT_APP_VERSION = '1.0.0'
DEFAULT_EVENT_VERSION = '1.0'
DEFAULT_EVENT_TYPE = 'test_event'
DEFAULT_LEVEL_ID = 'test_level'
DEFAULT_BATCH_SIZE = 100


def parse_cmd_line():
    """Parse the command line and extract the necessary values."""

    parser = argparse.ArgumentParser(description='Send data to a Kinesis stream for analytics. By default, the script '
                                                 'will send events infinitely. If an input file is specified, the '
                                                 'script will instead read and transmit all of the events contained '
                                                 'in the file and then terminate.')

    # REQUIRED arguments
    kinesis_regions = boto3.session.Session().get_available_regions('kinesis')
    parser.add_argument('-r', '--region_name', required=True, choices=kinesis_regions, type=str,
                        dest='region_name', metavar='kinesis_aws_region',
                        help='The AWS region where the Kinesis stream is located.')
    parser.add_argument('-s', '--stream_name', required=True, type=str, dest='stream_name',
                        help='The name of the Kinesis stream to publish to. Must exist in the specified region.')

    # OPTIONAL arguments
    parser.add_argument('-b', '--batch_size', type=int, dest='batch_size', default=DEFAULT_BATCH_SIZE,
                        help='The number of events to send at once using the Kinesis PutRecords API.')
    parser.add_argument('-i', '--input_filename', type=str, dest='input_filename',
                        help='Send events from a file rather than randomly generate them. The format of the file'
                             ' should be one JSON-formatted event per line.')

    return parser.parse_args()


def generate_event():
    """Generate a random analytics event as a dictionary."""

    return {
        'app_name': DEFAULT_APP_NAME,
        'app_version': DEFAULT_APP_VERSION,
        'event_version': DEFAULT_EVENT_VERSION,
        'event_id': str(uuid.uuid4()),
        'event_type': DEFAULT_EVENT_TYPE,
        'event_timestamp': int(time.time() * 1000),
        'client_id': str(uuid.uuid4()),
        'level_id': DEFAULT_LEVEL_ID,
        'position_x': random.randrange(start=300, stop=700),
        'position_y': random.randrange(start=550, stop=950)
    }


def send_record_batch(kinesis_client, stream_name, raw_records, pause_time=5.0):
    """Send a batch of records to Amazon Kinesis."""

    # Translate input records into the format needed by the boto3 SDK
    formatted_records = []
    for rec in raw_records:
        formatted_records.append({'PartitionKey': rec['event_id'], 'Data': json.dumps(rec)})

    kinesis_client.put_records(StreamName=stream_name, Records=formatted_records)
    print 'Sent %d records to stream %s.' % (len(formatted_records), stream_name)

    # Sleep between sending batches
    if pause_time > 0:
        print 'Sleeping for %f seconds...' % pause_time
        time.sleep(pause_time)


def send_event_file(kinesis_client, input_filename, stream_name, batch_size):
    """Send the contents of a file full of JSON-formatted events."""

    records = []
    with open(input_filename, 'r') as infile:
        for line in infile:
            event_dict = json.loads(line)
            records.append(event_dict)

            # Check if we have enough records to create a new batch
            if len(records) >= batch_size:
                send_record_batch(kinesis_client, stream_name, records)
                records = []

        # There's likely to be one last set of records that aren't the
        # size of a defined batch_size
        if records:
            send_record_batch(kinesis_client, stream_name, records)


def send_events_infinite(kinesis_client, stream_name, batch_size):
    """Send a batches of randomly generated events to Amazon Kinesis."""

    while True:
        records = []

        # Create a batch of random events to send
        for i in range(0, batch_size):
            event_dict = generate_event()
            records.append(event_dict)

        send_record_batch(kinesis_client, stream_name, records)


if __name__ == '__main__':

    args = parse_cmd_line()

    client = boto3.client('kinesis', region_name=args.region_name)

    if args.input_filename:
        send_event_file(client, args.input_filename, args.stream_name, args.batch_size)
    else:
        send_events_infinite(client, args.stream_name, args.batch_size)
