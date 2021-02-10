# This file is part of tcollector.
# Copyright (C) 2010  The tcollector Authors.
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.  This program is distributed in the hope that it
# will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
# General Public License for more details.  You should have received a copy
# of the GNU Lesser General Public License along with this program.  If not,
# see <http://www.gnu.org/licenses/>.

from optparse import OptionParser


def parse_cmdline(argv):
    """Parses the command-line."""

    defaults = {
        'cdir': 'collectors',
        'pcollector_addr': '127.0.0.1:8080',
        'fea_addr': '127.0.0.1:8080',
        'wai_addr': '127.0.0.1:8080',
        'wai_token': '',

        'evictinterval': 6000,
        'dedupinterval': 0,
        'deduponlyzero': False,
        'allowed_inactivity_time': 600,
        'dryrun': True,
        'reconnectinterval': 0,
        'remove_inactive_collectors': False,

        'sender_thread_number': 1,
        'send_package_wait': 5000,
        'not_work_threshold': 3600,

        'api_gw_url': '',
        'api_gw_key': '',
        'iam_access_key': '',
        'iam_secret_key': ''
    }

    parser = OptionParser(description='Manages collectors which gather '
                                      'data and report back.')

    parser.add_option('-b', '--base-dir', dest='basedir', type='str',
                      help='Agent base directory.')
    parser.add_option('-c', '--collector-dir', dest='cdir', metavar='DIR',
                      default=defaults['cdir'],
                      help='Directory where the collectors are located.')
    parser.add_option('--perf-collector-addr', dest='pcollector_addr', type='str',
                      default=defaults['pcollector_addr'],
                      help='performance collector address.')
    parser.add_option('--fea-addr', dest='fea_addr', type='str',
                      default=defaults['fea_addr'], metavar='FEAADDR',
                      help='Fea server address.')
    parser.add_option('--wai-addr', dest='wai_addr', type='str',
                      default=defaults['wai_addr'],
                      help='WAI server address.')
    parser.add_option('--wai-token', dest='wai_token', type='str',
                      default=defaults['wai_token'],
                      help='WAI register token.')

    # deduplicate
    parser.add_option('--dedup-interval', dest='dedupinterval', type='int',
                      default=defaults['dedupinterval'], metavar='DEDUPINTERVAL',
                      help='Number of seconds in which successive duplicate '
                           'datapoints are suppressed before sending to the TSD. '
                           'Use zero to disable. '
                           'default=%default')
    parser.add_option('--dedup-only-zero', dest='deduponlyzero', action='store_true',
                      default=defaults['deduponlyzero'],
                      help='Only dedup 0 values.')
    parser.add_option('--evict-interval', dest='evictinterval', type='int',
                      default=defaults['evictinterval'], metavar='EVICTINTERVAL',
                      help='Number of seconds after which to remove cached '
                           'values of old data points to save memory. '
                           'default=%default')
    parser.add_option('--allowed-inactivity-time', dest='allowed_inactivity_time', type='int',
                      default=600, metavar='ALLOWEDINACTIVITYTIME',
                      help='How long to wait for datapoints before assuming '
                           'a collector is dead and restart it. '
                           'default=%default')
    parser.add_option('--remove-inactive-collectors', dest='remove_inactive_collectors', action='store_true',
                      default=defaults['remove_inactive_collectors'], help='Remove collectors not sending data '
                                                                           'in the max allowed inactivity interval')
    parser.add_option('--reconnect-interval', dest='reconnectinterval', type='int',
                      default=defaults['reconnectinterval'], metavar='RECONNECTINTERVAL',
                      help='Number of seconds after which the connection to'
                           'the TSD hostname reconnects itself. This is useful'
                           'when the hostname is a multiple A record (RRDNS).')

    # sender
    parser.add_option('--sender-thread-number', dest='sender_thread_number', type='int',
                      default=defaults['sender_thread_number'], help='Thread number of sender.')
    parser.add_option('--send-package-wait', dest='send_package_wait', type='int',
                      default=defaults['send_package_wait'], help='waiting time for sender package')
    parser.add_option('--not-work-threshold', dest='not_work_threshold', type='int',
                      default=defaults['not_work_threshold'], help='threshold to determine whether agent is working')

    # CW add
    parser.add_option('-U', '--api-gw-url', dest='api_gw_url', type='str',
                      default=defaults['api_gw_url'], help='Sender destination url of API Gateway.')
    parser.add_option('-K', '--api-gw-key', dest='api_gw_key', type='str',
                      default=defaults['api_gw_key'], help='API Gateway key.')
    parser.add_option('-A', '--iam-access-key', dest='iam_access_key', type='str',
                      default=defaults['iam_access_key'], help='IAM access key.')
    parser.add_option('-S', '--iam-secret-key', dest='iam_secret_key', type='str',
                      default=defaults['iam_secret_key'], help='IAM secret key.')
    parser.add_option('-M', '--member-number', dest='member_no', type='int', default=-1, help='Member number')
    parser.add_option('--cw-key', dest='cw_key', type='str', default='', help='CloudWatch product key')

    (options, args) = parser.parse_args(argv)

    if options.dedupinterval < 0:
        parser.error('--dedup-interval must be at least 0 seconds')
    if options.evictinterval <= options.dedupinterval:
        parser.error('--evict-interval must be strictly greater than '
                     '--dedup-interval')

    return options, args
