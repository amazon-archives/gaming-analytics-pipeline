# #####################################################################################################################
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
# #####################################################################################################################

import argparse
import boto3
import matplotlib.cm
import matplotlib.pyplot
import numpy
import os.path
import pandas
import pg8000
import sys

# The SQL statement used to bin events by (x,y) coordinate in Redshift
HEATMAP_BIN_SQL_FORMAT = '''SELECT FLOOR(position_x), FLOOR(position_y), COUNT(*) 
                            FROM %s 
                            WHERE level_id='%s' AND event_type='%s'
                            GROUP BY 1, 2'''

# Defaults for drawing heatmaps
DEFAULT_OVERLAY_ALPHA = 0.75
DEFAULT_HEX_BIN_SIZE = 25
DEFAULT_WORLD_CENTER_X = 503.245239
DEFAULT_WORLD_CENTER_Y = 748.971008
DEFAULT_LEVEL_ID = 'test_level'
DEFAULT_EVENT_TYPE = 'player_death'
DEFAULT_MAP_WIDTH = 512
DEFAULT_MAP_HEIGHT = 512

# Defaults for database connections
DEFAULT_DB_CLUSTER_ID = 'analytics'
DEFAULT_DB_NAME = 'analytics'
DEFAULT_DB_PORT = 5439
DEFAULT_DB_USER = 'analytics_ro'
DEFAULT_DB_TABLE_NAME = 'game.events'
DEFAULT_ASSUME_ROLE_DURATION = 900

# Assume that the image we want lives in the same directory as this script
DEFAULT_LEVEL_SCREENSHOT_IMAGE_FILE = os.path.join(os.path.split(__file__)[0], 'level_screenshot_512x512.png')


def parse_cmd_line():
    """Parse the command line and extract the necessary values."""

    parser = argparse.ArgumentParser(description='Generate a heatmap based on analytics data in a Redshift database. '
                                                 'By default, a heatmap image will be displayed on-screen, but a user '
                                                 'can also specify the --output_file option to write it to a file '
                                                 'instead.')

    # REQUIRED arguments
    parser.add_argument('--db_host', required=True, type=str, dest='db_host',
                        help='The DNS hostname of the database to connect to [REQUIRED].')

    # OPTIONAL database-related arguments
    parser.add_argument('-u', '--db_username', default=DEFAULT_DB_USER, type=str, dest='db_user',
                        help='The database username used to connect to the database. '
                             'Defaults to "%s".' % DEFAULT_DB_USER)
    parser.add_argument('-c', '--db_cluster_id', default=DEFAULT_DB_CLUSTER_ID, type=str, dest='db_cluster_id',
                        help='The Redshift database cluster ID used to connect to the database. Only necessary when '
                             'a database password is not specified on the command line.'
                             'Defaults to "%s".' % DEFAULT_DB_CLUSTER_ID)
    parser.add_argument('--db_port', default=DEFAULT_DB_PORT, type=int, dest='db_port',
                        help='The DNS hostname of the database to connect to [REQUIRED]. '
                             'Defaults to "%s".' % DEFAULT_DB_PORT)
    parser.add_argument('-p', '--db_password', default=None, type=str, dest='db_pass',
                        help='The database password used to connect to the database. If specified, a standard '
                             'username/password style connection will be made. If not specified, the tool will instead '
                             'use the Redshift GetClusterCredentials API to generate one (appropriate AWS credentials '
                             'and an AWS region name will be required in that scenario.)')

    redshift_regions = boto3.session.Session().get_available_regions('redshift')
    parser.add_argument('-r', '--region_name', required=True, choices=redshift_regions, type=str,
                        dest='db_region_name', default=None, metavar='db_region_name',
                        help='The AWS region where the Redshift cluster is located. Only necessary when a database '
                             'password is not specified on the command line.')
    parser.add_argument('-n', '--db_name', default=DEFAULT_DB_NAME, type=str, dest='db_name',
                        help='The name of the database to connect to (inside the database cluster). '
                             'Defaults to "%s".' % DEFAULT_DB_NAME)
    parser.add_argument('-t', '--table_name', default=DEFAULT_DB_TABLE_NAME, type=str, dest='table_name',
                        help='The name of the analytics table or view in the database to run queries against. '
                             'Defaults to "%s".' % DEFAULT_DB_TABLE_NAME)
    parser.add_argument('-l', '--level_id', default=DEFAULT_LEVEL_ID, type=str, dest='level_id',
                        help='The name of the level to generate a heatmap against. Used for database query. '
                             'Defaults to "%s".' % DEFAULT_LEVEL_ID)
    parser.add_argument('-e', '--event_type', default=DEFAULT_EVENT_TYPE, type=str, dest='event_type',
                        help='The name of the event_type to generate a heatmap against. Used for database query. '
                             'Defaults to "%s".' % DEFAULT_EVENT_TYPE)

    # OPTIONAL heatmap-related arguments
    parser.add_argument('-i', '--image_file', default=DEFAULT_LEVEL_SCREENSHOT_IMAGE_FILE, type=str, dest='image_file',
                        help='The image file to use for the background image of the heatmap. '
                             'Defaults to "%s".' % DEFAULT_LEVEL_SCREENSHOT_IMAGE_FILE)
    parser.add_argument('-a', '--overlay_alpha', default=DEFAULT_OVERLAY_ALPHA, type=float, dest='overlay_alpha',
                        help='The alpha value of the heatmap overlay. Values range from 0-1. A value of 0 will cause '
                             'the heatmap overlay to be invisible and a value of 1 will cause the heatmap to be '
                             'completely opaque. '
                             'Defaults to "%s".' % DEFAULT_OVERLAY_ALPHA)
    parser.add_argument('-b', '--bin_size', default=DEFAULT_HEX_BIN_SIZE, type=int, dest='bin_size',
                        help='The size of the hexagons in the heatmap overlay. Bigger values = smaller hexagons. '
                             'Defaults to "%s".' % DEFAULT_HEX_BIN_SIZE)
    parser.add_argument('-x', '--world_center_x', default=DEFAULT_WORLD_CENTER_X, type=float, dest='world_center_x',
                        help='The X coordinate of the level/map center. '
                             'Defaults to "%s".' % DEFAULT_WORLD_CENTER_X)
    parser.add_argument('-y', '--world_center_y', default=DEFAULT_WORLD_CENTER_Y, type=float, dest='world_center_y',
                        help='The Y coordinate of the level/map center. '
                             'Defaults to "%s".' % DEFAULT_WORLD_CENTER_Y)
    parser.add_argument('--map_width', default=DEFAULT_MAP_WIDTH, type=int, dest='map_width',
                        help='The width of the level/map. '
                             'Defaults to "%s".' % DEFAULT_MAP_WIDTH)
    parser.add_argument('--map_height', default=DEFAULT_MAP_HEIGHT, type=int, dest='map_height',
                        help='The height of the level/map. '
                             'Defaults to "%s".' % DEFAULT_MAP_HEIGHT)

    # OPTIONAL other arguments
    parser.add_argument('-o', '--output_file', type=str, dest='output_file',
                        help='By default, the heatmap will pop up on-screen. Specifying this argument will write '
                             'the heatmap to an image file instead.')

    args = parser.parse_args()

    if not args.db_pass and not args.db_region_name:
        print>> sys.stderr, 'If a database password is not specified, database region name must be specified.'
        sys.exit(1)
    if not args.db_pass and not args.db_cluster_id:
        print>> sys.stderr, 'If a database password is not specified, database cluster ID must be specified.'
        sys.exit(1)

    return args


def connect_to_db(args):
    """Connect to the Redshift database.  If args contains a valid username and password, they will
    be used directly to create the connection.  Otherwise this method will attempt to use available
    AWS credentials to call the Redshift GetClusterCredentials API to generate a temporary username
    and password to use instead."""

    db_user = args.db_user
    db_pass = args.db_pass

    # No password available; generating a temporary username/password pair
    # (will error out if no AWS credentials are available)
    if not args.db_pass:

        redshift_client = boto3.client('redshift', region_name=args.db_region_name)
        result = redshift_client.get_cluster_credentials(DbUser=args.db_user,
                                                         DbName=args.db_name,
                                                         ClusterIdentifier=args.db_cluster_id,
                                                         DurationSeconds=DEFAULT_ASSUME_ROLE_DURATION,
                                                         AutoCreate=False)
        db_user = result['DbUser']
        db_pass = result['DbPassword']

    print 'Connecting to DB at %s:%s/%s as user "%s"' % (args.db_host, args.db_port, args.db_name, db_user)
    return pg8000.connect(user=db_user, password=db_pass,
                          host=args.db_host, port=args.db_port,
                          database=args.db_name, ssl=True)


def get_xy_event_bins(db_connection, args):
    """Given a database connection run an SQL query to take occurrences of the given event type
    and bin them by (x,y) coordinate."""

    cursor = db_connection.cursor()

    print 'Running SQL query to bin events...'
    sql = HEATMAP_BIN_SQL_FORMAT % (args.table_name, args.level_id, args.event_type)
    cursor.execute(sql)

    map_upper_left = (args.world_center_x - args.map_width / 2.0, args.world_center_y - args.map_height / 2.0)
    map_offset_x = map_upper_left[0]
    map_offset_y = map_upper_left[1]

    xy_bins = []
    for row in cursor.fetchall():

        # The format of this line is dependent on the SELECT part of the binning SQL query
        x, y, event_count = row

        # ####################################################################################
        '''IMPORTANT: this is the logic for the custom world space to pixel space
        mapping for our example. You will likely need to change this for your game
        to get things to draw the heatmap properly.
        '''

        x = int(round(x - map_offset_x))
        y = int(round(y - map_offset_y))
        # ####################################################################################

        xy_bins.append((x, y, event_count))

    print 'Finished fetching data from Redshift...'

    return xy_bins


def generate_heatmap(xy_bins, args):
    """Generate a heatmap based on the (x,y) bin histogram-style values and the associated
    level screenshot image.
    
    See: https://pandas-docs.github.io/pandas-docs-travis/visualization.html#hexagonal-bin-plot"""

    print 'Plotting heatmap...'

    # Construct a data frame containing all the (x,y) event bins
    df = pandas.DataFrame.from_records(xy_bins, columns=['pos_x', 'pos_y', 'events'])

    # Generate a hexbin plot
    df.plot.hexbin(x='pos_x', y='pos_y', C='events', linewidth=0,
                   reduce_C_function=numpy.max, gridsize=args.bin_size,
                   alpha=args.overlay_alpha, cmap=matplotlib.cm.hot)

    # Show the level screenshot image as a background to the plot
    matplotlib.pyplot.imshow(X=matplotlib.pyplot.imread(args.image_file), zorder=0)

    # Render the heatmap (either to a file or the screen)
    if args.output_file:
        matplotlib.pyplot.savefig(args.output_file, format='png')
        print 'Heatmap has been saved to "%s"' % args.output_file
    else:
        print 'Displaying heatmap to screen...'
        matplotlib.pyplot.show()


if __name__ == '__main__':

    cli_args = parse_cmd_line()

    db_conn = connect_to_db(cli_args)

    event_bins = get_xy_event_bins(db_conn, cli_args)

    if not event_bins:
        print>>sys.stderr, 'No data was found in the provided Redshift cluster!'
        print>>sys.stderr, '\t1. Make sure you are using the command line args to point to the proper Redshift cluster.'
        print>>sys.stderr, '\t2. Make sure you have data in your tables in Redshift (it takes a little while to get there).'
        print>>sys.stderr, '''\t3. Make sure you have the correct "level_id" and "event_type" specified on the command line. 
\t   This script defaults to looking for the data in the heatmap-sample-data.txt script. 
\t   The data generator publishes data with a different event_type by default.'''
        print>>sys.stderr, 'Verify the relevant data and then try again.'
        sys.exit(1)

    generate_heatmap(event_bins, cli_args)
