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

import json
import threading
import time
from Queue import Queue, Full

import agent_globals
import logger

LOG = logger.get_logger('reader')
MAX_REASONABLE_TIMESTAMP = 1600000000


class ReaderThread(threading.Thread):
    """The main ReaderThread is responsible for reading from the collectors
       and assuring that we always read from the input no matter what.
       All data read is put into the self.readerq Queue, which is
       consumed by the SenderThread."""

    # def __init__(self, collector_holder, dedupinterval, evictinterval, deduponlyzero):
    def __init__(self, collector_holder, options):
        """Constructor.
            Args:
              dedupinterval: If a metric sends the same value over successive
                intervals, suppress sending the same value to the TSD until
                this many seconds have elapsed.  This helps graphs over narrow
                time ranges still see timeseries with suppressed datapoints.
              evictinterval: In order to implement the behavior above, the
                code needs to keep track of the last value seen for each
                combination of (metric, tags).  Values older than
                evictinterval will be removed from the cache to save RAM.
                Invariant: evictinterval > dedupinterval
              deduponlyzero: do the above only for 0 values.
        """
        assert options.evictinterval > options.dedupinterval, "%r <= %r" % (options.evictinterval,
                                                                            options.dedupinterval)
        super(ReaderThread, self).__init__()

        self.options = options
        self.readerq = ReaderQueue(agent_globals.MAX_READQ_SIZE)
        self.lines_collected = 0
        self.lines_dropped = 0
        self.collector_holder = collector_holder
        self.dedupinterval = options.dedupinterval
        self.evictinterval = options.evictinterval
        self.deduponlyzero = options.deduponlyzero

    def run(self):
        """Main loop for this thread.  Just reads from collectors,
           does our input processing and de-duping, and puts the data
           into the queue."""

        LOG.debug("ReaderThread up and running")

        lastevict_time = 0
        # we loop every second for now.  ideally we'll setup some
        # select or other thing to wait for input on our children,
        # while breaking out every once in a while to setup selects
        # on new children.
        while agent_globals.RUN:
            try:
                print('*** RUN = True')
                alc = self.collector_holder.all_living_collectors()
                for col in alc:
                    for line in col.collect():
                        # LOG.debug('... collect line of collector[%s]: %s' % (col.name, line))
                        self.process_line(col, line)

                if self.dedupinterval != 0:  # if 0 we do not use dedup
                    now = int(time.time())
                    if now - lastevict_time > self.evictinterval:
                        lastevict_time = now
                        now -= self.evictinterval
                        for col in self.collector_holder.collectors_dict():
                            col.evict_old_keys(now)

                # and here is the loop that we really should get rid of, this
                # just prevents us from spinning right now
                time.sleep(1)
            except Exception:
                LOG.error('reading error', exc_info=True)

    def process_line(self, col, line):
        """Parses the given line and appends the result to the reader queue."""

        self.lines_collected += 1
        # If the line contains more than a whitespace between
        # parameters, it won't be interpeted.
        while '  ' in line:
            line = line.replace('  ', ' ')

        # LOG.debug('... read line: %s' % line)

        col.lines_received += 1
        # if len(line) >= 1024:  # Limit in net.opentsdb.tsd.PipelineFactory
        #     LOG.warning('%s line too long: %s', col.name, line)
        #     col.lines_invalid += 1
        #     return

        try:
            data_json = json.loads(line)
        except ValueError as e:
            LOG.warning('%s sent invalid data: %s', col.name, line)
            col.lines_invalid += 1
            return

        # parsed = re.match('^([-_./a-zA-Z0-9]+)\s+'  # Metric name.
        #                   '(\d+\.?\d+)\s+'  # Timestamp.
        #                   '(\S+?)\s+'  # Value (int or float).
        #                   '(\[(?:\s?[-_./a-zA-Z0-9]+=[-_.:\\\\/a-zA-Z0-9]+)*\])$',  # Tags
        #                   line)
        # if parsed is None:
        #     LOG.warning('%s sent invalid data: %s', col.name, line)
        #     col.lines_invalid += 1
        #     return
        # metric, timestamp, value, tags = parsed.groups()
        # timestamp = int(timestamp)
        #
        # # If there are more than 11 digits we're dealing with a timestamp
        # # with millisecond precision
        # if len(str(timestamp)) > 11:
        #     global MAX_REASONABLE_TIMESTAMP
        #     MAX_REASONABLE_TIMESTAMP = MAX_REASONABLE_TIMESTAMP * 1000

        # De-dupe detection...  To reduce the number of points we send to the
        # TSD, we suppress sending values of metrics that don't change to
        # only once every 10 minutes (which is also when TSD changes rows
        # and how much extra time the scanner adds to the beginning/end of a
        # graph interval in order to correctly calculate aggregated values).
        # When the values do change, we want to first send the previous value
        # with what the timestamp was when it first became that value (to keep
        # slopes of graphs correct).

        if self.dedupinterval != 0:  # if 0 we do not use dedup
            if self.dedup(col, data_json):
                return
            # key = (metric, tags)
            # if key in col.values:
            #     # if the timestamp isn't > than the previous one, ignore this value
            #     if timestamp <= col.values[key][3]:
            #         LOG.error("Timestamp out of order: metric=%s%s,"
            #                   " old_ts=%d >= new_ts=%d - ignoring data point"
            #                   " (value=%r, collector=%s)", metric, tags,
            #                   col.values[key][3], timestamp, value, col.name)
            #         col.lines_invalid += 1
            #         return
            #     elif timestamp >= MAX_REASONABLE_TIMESTAMP:
            #         LOG.error("Timestamp is too far out in the future: metric=%s%s"
            #                   " old_ts=%d, new_ts=%d - ignoring data point"
            #                   " (value=%r, collector=%s)", metric, tags,
            #                   col.values[key][3], timestamp, value, col.name)
            #         return
            #
            #     # if this data point is repeated, store it but don't send.
            #     # store the previous timestamp, so when/if this value changes
            #     # we send the timestamp when this metric first became the current
            #     # value instead of the last.  Fall through if we reach
            #     # the dedup interval so we can print the value.
            #     if ((not self.deduponlyzero or (self.deduponlyzero and float(value) == 0.0)) and
            #             col.values[key][0] == value and
            #             (timestamp - col.values[key][3] < self.dedupinterval)):
            #         col.values[key] = (value, True, line, col.values[key][3])
            #         return
            #
            #     # we might have to append two lines if the value has been the same
            #     # for a while and we've skipped one or more values.  we need to
            #     # replay the last value we skipped (if changed) so the jumps in
            #     # our graph are accurate,
            #     if ((col.values[key][1] or
            #          (timestamp - col.values[key][3] >= self.dedupinterval))
            #             and col.values[key][0] != value):
            #         col.lines_sent += 1
            #         if not self.readerq.nput(
            #                 MetricLine(col.values[key][2], col.name[0:col.name.index('.')], col.version, col.config_version, wai.get_host_id())):
            #             self.lines_dropped += 1
            #
            # # now we can reset for the next pass and send the line we actually
            # # want to send
            # # col.values is a dict of tuples, with the key being the metric and
            # # tags (essentially the same as wthat TSD uses for the row key).
            # # The array consists of:
            # # [ the metric's value, if this value was repeated, the line of data,
            # #   the value's timestamp that it last changed ]
            # col.values[key] = (value, False, line, timestamp)

        col.lines_sent += 1
        # print('read line: %s' % line)
        if not self.readerq.nput(MetricLine(data_json, col.name[0:col.name.index('.')], col.version, col.config_version)):
            self.lines_dropped += 1

    def dedup(self, collector, data_json):
        return False


class ReaderQueue(Queue):
    """A Queue for the reader thread"""

    def nput(self, value):
        """A nonblocking put, that simply logs and discards the value when the
           queue is full, and returns false if we dropped."""
        try:
            self.put(value, False)
        except Full:
            LOG.error("DROPPED LINE: %s", value)
            return False
        return True


class MetricLine:

    def __init__(self, data, script_type, script_version, config_version):
        self.data = data
        self.script_type = script_type
        self.script_version = script_version
        self.config_version = config_version
