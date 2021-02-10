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

        if self.dedupinterval != 0:  # if 0 we do not use dedup
            if self.dedup(col, data_json):
                return

        script_type = col.name[0:col.name.index('.')]
        if script_type == 'custom':
            for data_item in data_json:
                data = data_item['data']
                source_key = data_item['source_key']
                col.lines_sent += 1
                if not self.readerq.nput(MetricLine(data, '', col.version, col.config_version, source_key)):
                    self.lines_dropped += 1
        else:
            col.lines_sent += 1
            # print('read line: %s' % line)
            if not self.readerq.nput(MetricLine(data_json, script_type, col.version, col.config_version)):
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

    def __init__(self, data, script_type, script_version, config_version, source_key=None):
        self.data = data
        self.script_type = script_type
        self.script_version = script_version
        self.config_version = config_version
        self.source_key = source_key
