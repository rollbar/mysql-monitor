#!/usr/bin/env python
"""
Processes mysql slow query log data and notifies ratchet.io of slow queries.
"""

import optparse
import sys

import ratchet

VERSION = 0.1

TIME_PATTERN = r'^# Time: (?P<date>[0-9]{4}) (?P<time>[0-9]{2}:[0-9]{2}:[0-9]{2}$'
USER_HOST_PATTERN = r'^# User@Host: (?P<user_host>.* @ .*)$'
QUERY_STATS_PATTERN = r'^# Query_time: (?P<query_seconds>[0-9]\.[0-9]+)\s+' \
                      r'Lock_time: (?P<lock_time>[0-9]\.[0-9]+)\s+' \
                      r'Rows_sent: (?P<rows_sent>[0-9]+)\s+' \
                      r'Rows_examined: (?P<rows_examined>[0-9]+)$'

# replication adds in the SET timestamp= line which we'll just ignore
QUERY_PATTERN = r'^(SET timestamp=[0-9]+;\n)?(?P<query>[^;]+;)$'

# e.g. 
#
# # Time: 121228 15:24:25
# # User@Host: user[db] @ host [10.10.10.10]
# # Query_time: 0.000255  Lock_time: 0.000044 Rows_sent: 590  Rows_examined: 590
# SET timestamp=1356737065;
# SELECT foo FROM bar
# WHERE x = 2;
#
QUERY_EVENT_REGEX = re.compile(TIME_PATTERN + '\n' +
                               USER_HOST_PATTERN + '\n' +
                               QUERY_STATS_PATTERN + '\n'
                               QUERY_PATTERN,
                               flags=re.MULTILINE)

heuristics = None

def notify_ratchet(heuristic_name, event):
    ratchet.report_message(

def process_event(event):
    """
    Notify ratchet.io about this query if the event passes the heuristics.
    """
    for name, heuristic in heuristics.iteritems():
        if heuristic and heuristic(event):
            notify_ratchet(name, event)


def extract_events(lines):
    """
    Looks for slow query events in '\n'.join(lines) and
    returns a list of the ones found.
    """
    matches = QUERY_EVENT_REGEX.findall(lines)
    events = []
    for match in matches:
        event = match.group_dict()
        event['raw'] = match.group(0)
        events.append(event)
        lines.replace(match.group(0), '')

    return events, lines


def process_input():
    while True:
        line = sys.stdin.readline()
        if line:
            lines += line + '\n'

            events, lines = extract_events(lines)
            for event in events:
                process_event(event)


def build_heuristics(opts):
    return {
        'slow query': SlowQuery(0.000001, 0.00001, 0.0001, 0.001, 0.01),
        'too many rows returned': TooManyRowsReturned(100, 1000, 10000, 100000, 100000),
        'too many rows examined': TooManyRowsExamined(100, 1000, 10000, 100000, 100000),
        'ratio of examined to return is too high': RatioOfExaminedRowsTooHigh(10, 100, 1000, 10000, 100000),
        'long lock time': LongLockTime(0.000001, 0.00001, 0.0001, 0.001, 0.01)
    }


def build_option_parser():
    usage = 'usage: %prog [options] access_token'
    parser = optparse.OptionParser(usage=usage, version='%%prog %f' % VERSION)

    parser.add_option('-e',
                      '--environment',
                      dest='environment',
                      action='store',
                      type='string',
                      default='production',
                      help='The environment in which the mysql instance is running.')

    parser.add_option('-m',
                      '--max_buffer_lines',
                      dest='max_buffer_lines',
                      type='int',
                      default=500,
                      help='The maximum number of lines to buffer the most recent ' \
                           'slow query. This should be slightly larger than the maximum ' \
                           'number of lines used by your SQL queries.')
    return parser


def main():
    global heuristics

    parser = build_option_parser()
    (options, args) = parser.parse_args(sys.argv)

    if len(args) != 1:
        parser.error('incorrect number of arguments')
        sys.exit(1)

    access_token = args[0]
    environment = options.environment

    ratchet.init(access_token, environment)

    heuristics = build_heuristics(options)

    return process_input()


if __name__ == '__main__':
    main()


## Heuristics

class Heuristic(object):
    def __init__(self, min_val, max_debug_val, max_info_val, max_warning_val, max_error_val):
        self.ranges = [('debug', min_val, max_debug_val),
                       ('info', max_debug_val, max_info_val),
                       ('warning', max_info_val, max_warning_val),
                       ('error', max_warning_val, max_error_val),
                       ('critical', max_error_val, None)]

    def check(self, val):
        for name, min, max in self.ranges:
            if val >= min and (val < max if max is not None else True):
                return name

        return None

    def calculate_val(self, event):
        raise NotImplementedError()


class SlowQuery(Heuristic):
    def calculate_val(self, event):
        return int(event.query_seconds)


class TooManyRowsReturned(Heuristic):
    def calculate_val(self, event):
        return int(event.rows_sent)


class TooManyRowsExamined(Heuristic):
    def calculate_val(self, event):
        return int(event.rows_examined)


class RatioOfExaminedRowsTooHigh(Heuristic):
    def calculate_val(self, event):
        return int(event.rows_examined) / float(event.rows_sent)


class LongLockTime(Heuristic):
    def calculate_val(self, event):
        return float(event.lock_time)

