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

import os
import signal
import subprocess
import sys
import time

import fcntl

import logger

# import fcntl
import wai

LOG = logger.get_logger('collector')


class Collector(object):

    def __init__(self, options, colname, interval, script, version, recycle_queue, mtime=0, last_spawn=0):
        self.options = options
        self.name = colname
        self.interval = interval
        self.script = script
        self.version = version
        self.config_version = None
        self.recycle_queue = recycle_queue
        self.generation = 0
        self.process = None
        self.finish = False
        self.last_spawn = last_spawn
        # self.nextkill = 0
        # self.killstate = 0
        self.mtime = mtime
        self.buffer = ""
        self.datalines = []
        self.values = {}
        self.last_datapoint = int(time.time())
        self.lines_sent = 0
        self.lines_received = 0
        self.lines_invalid = 0
        self.disable = False
        self.modified = False
        self.need_stop = False
        self.need_start = False
        self.period_count = 0

    def read(self):
        try:
            errbytes = self.process.stderr.read()
            if errbytes is not None:
                out = errbytes.decode(encoding='ascii')
                if out:
                    LOG.debug('reading %s got %d bytes on stderr',
                              self.name, len(out))
                    for line in out.splitlines():
                        if len(line.strip()):
                            LOG.warning('%s: %s', self.name, line)
        except IOError as e:
            pass
            # LOG.debug('read stderr errno: %d' % e)
            # if e != errno.EAGAIN:
            #     raise
        except Exception as e:
            LOG.exception('uncaught exception in stderr read: %s', e)

        try:
            readbytes = self.process.stdout.read()
            if readbytes is not None:
                self.buffer += readbytes.decode(encoding='ascii')
                if len(self.buffer):
                    LOG.debug('reading %s, buffer now %d bytes',
                              self.name, len(self.buffer))
        except IOError as e:
            pass
            # LOG.debug('read stdout errno: %d' % e)
            # if e.errno != errno.EAGAIN:
            #     raise
        except AttributeError:
            # sometimes the process goes away in another thread and we don't
            # have it anymore, so log an error and bail
            LOG.exception('caught exception, collector process went away while reading stdout')
        except Exception as e:
            LOG.exception('uncaught exception in stdout read: %s', e)
            return

        while self.buffer:
            idx = self.buffer.find('\n')
            if idx == -1:
                break

            line = self.buffer[0:idx].strip()
            if line:
                self.datalines.append(line)
                self.last_datapoint = int(time.time())
            self.buffer = self.buffer[idx + 1:]

    def collect(self):
        while self.process is not None:
            self.read()
            if not len(self.datalines):
                return
            while len(self.datalines):
                yield self.datalines.pop(0)

    def startup(self, config_version=None):
        LOG.debug('starting up %s (interval=%d)', self.name, self.interval)

        try:
            py = os.path.join(self.options.basedir, '.venv', 'bin', 'python')
            if config_version is not None:
                host_id = wai.get_host_id(self.options)
                self.process = subprocess.Popen([py, self.script, sys.path[0], str(config_version), host_id],
                                                stdout=subprocess.PIPE,
                                                stderr=subprocess.PIPE,
                                                close_fds=True,
                                                preexec_fn=os.setsid)
            else:
                self.process = subprocess.Popen([py, self.script, sys.path[0]],
                                                stdout=subprocess.PIPE,
                                                stderr=subprocess.PIPE,
                                                close_fds=True,
                                                preexec_fn=os.setsid)
            # print("--- script:%s" % self.script)
            # self.process = subprocess.Popen('python %s' % self.script, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if self.process.pid <= 0:
                LOG.error('failed to startup collector: %s', self.name, exc_info=True)
                return
            self.last_spawn = int(time.time())
            self.last_datapoint = self.last_spawn
            set_nonblocking(self.process.stdout.fileno())
            set_nonblocking(self.process.stderr.fileno())
            LOG.debug('%s (interval=%d) started up', self.name, self.interval)
        except OSError as e:
            LOG.error('Failed to startup collector %s: %s', self.name, e, exc_info=True)

    def shutdown(self):
        """Cleanly shut down the collector"""

        if not self.process:
            return
        try:
            if self.process.poll() is None:
                kill(self.process)
                # for attempt in range(5):
                #     if self.process.poll() is not None:
                #         return
                #     LOG.info('Waiting %ds for PID %d (%s) to exit...'
                #              % (5 - attempt, self.proc.pid, self.name))
                #     time.sleep(1)
                # kill(self.process, signal.SIGKILL)
                # self.process.wait()
        except:
            # we really don't want to die as we're trying to exit gracefully
            LOG.exception('ignoring uncaught exception while shutting down')

        self.recycle_queue.put(self.process)
        self.process = None

    def evict_old_keys(self, cut_off):
        """Remove old entries from the cache used to detect duplicate values.

        Args:
          cut_off: A UNIX timestamp.  Any value that's older than this will be
            removed from the cache.
        """
        for key in self.values.keys():
            timestamp = self.values[key][3]
            if timestamp < cut_off:
                del self.values[key]


def set_nonblocking(fd):
    """Sets the given file descriptor to non-blocking mode."""
    fl = fcntl.fcntl(fd, fcntl.F_GETFL) | os.O_NONBLOCK
    fcntl.fcntl(fd, fcntl.F_SETFL, fl)


def kill(process, signum=signal.SIGTERM):
    os.killpg(process.pid, signum)


class CollectorHolder:

    def __init__(self):
        self.collectors = {}

    def collectors_dict(self):
        return self.collectors

    def all_collectors(self):
        """Generator to return all collectors."""
        return self.collectors.values()

    def all_valid_collectors(self):
        """Generator to return all defined collectors that haven't been marked
           dead in the past hour, allowing temporarily broken collectors a
           chance at redemption."""

        # now = int(time.time())
        for col in self.all_collectors():
            # if not col.disable or (now - col.lastspawn > 3600):
            if not col.disable:
                yield col

    def all_living_collectors(self):
        for col in self.all_collectors():
            if col.process:
                yield col

    def collector(self, name):
        return self.collectors[name]

    def register_collector(self, collector):
        assert isinstance(collector, Collector), "collector=%r" % (collector,)

        if collector.name in self.collectors:
            col = self.collectors[collector.name]
            if col.process is not None:
                LOG.error('%s still has a process (pid=%d) and is being reset,'
                          ' terminating', col.name, col.process.pid)
                col.shutdown()

        self.collectors[collector.name] = collector

    def del_collector(self, name):
        if name in self.collectors:
            col = self.collectors[name]
            if col.process is not None:
                LOG.error('%s still has a process (pid=%d) and is being deleted,'
                          ' terminating', col.name, col.process.pid)
                col.shutdown()
            del self.collectors[name]
