import Queue
import os
import signal
import threading
import time

from apscheduler.schedulers.blocking import BlockingScheduler

import agent_globals
import logger
from collector import Collector, kill

LOG = logger.get_logger('scripts_runner')
RECYCLE_QUEUE = Queue.Queue()
PERIOD = 5
SCRIPT_CONFIG_VERSIONS = {}


class ScriptsRunner:

    def __init__(self, options, collectors_holder, version_queue):
        self.options = options
        self.collectors_holder = collectors_holder
        self.version_queue = version_queue

        self.scheduler = BlockingScheduler()
        self.GENERATION = 0

        recycle_thread = RecycleThread(RECYCLE_QUEUE)
        recycle_thread.start()

    def start(self):
        self.scheduler.add_job(self.prepare, 'cron', minute='*/1', second='30', max_instances=1)
        self.scheduler.add_job(self.spawn_children, 'cron', minute='*/1', second='0', max_instances=1)
        self.scheduler.add_job(self.check_finish_task, 'cron', minute='*/1', second='0', max_instances=1)
        self.scheduler.start()

    def stop(self):
        self.scheduler.shutdown()

    def prepare(self):
        if agent_globals.RUN:
            self.update_config_version()
            self.load_scripts(self.options.cdir)
            LOG.debug('populated collectors: %s' % str(self.collectors_holder.collectors_dict()))
            self.check_stop_start()
            self.stop_children()

    def load_scripts(self, coldir):
        self.GENERATION += 1

        for interval in os.listdir(coldir):
            if interval.isdigit():
                interval = int(interval)
                for colname in os.listdir('%s/%s' % (coldir, interval)):
                    if colname.startswith('.') or colname == '__init__.py':
                        continue

                    script = '%s/%d/%s' % (coldir, interval, colname)

                    if os.path.isdir(script):
                        continue

                    if os.path.isfile(script) and os.access(script, os.X_OK):
                        version = parse_version(script)
                        mtime = os.path.getmtime(script)

                        if colname in self.collectors_holder.collectors_dict():
                            LOG.debug('************** populating collector %s refresh' % colname)
                            collector = self.collectors_holder.collector(colname)

                            collector.modified = False
                            collector.need_stop = False
                            collector.need_start = False

                            collector.generation = self.GENERATION

                            if collector.version != version:
                                LOG.info('%s has been updated, from version[%s] to version[%s]' %
                                         (collector.name, collector.version, version))
                                collector.version = version
                                collector.modified = True
                                collector.disable = False

                            if collector.interval != interval:
                                LOG.info('collector %s changed interval %d to %d' %
                                         (colname, interval, collector.interval))
                                collector.interval = interval
                                collector.modified = True
                                collector.disable = False

                            if collector.mtime < mtime:
                                LOG.info('%s has been updated on disk', collector.name)
                                collector.mtime = mtime
                                collector.modified = True
                                collector.disable = False

                        else:
                            LOG.debug('************** populating collector %s register' % colname)
                            collector = Collector(self.options,
                                                  colname,
                                                  interval,
                                                  script,
                                                  version,
                                                  RECYCLE_QUEUE,
                                                  mtime)
                            collector.generation = self.GENERATION
                            self.collectors_holder.register_collector(collector)

        to_delete = []
        for col in self.collectors_holder.all_collectors():
            if col.generation < self.GENERATION:
                LOG.info('collector %s removed from the filesystem, forgetting',
                         col.name)
                col.shutdown()
                to_delete.append(col.name)
        for name in to_delete:
            self.collectors_holder.del_collector(name)

    def update_config_version(self):
        while not self.version_queue.empty():
            version_info = self.version_queue.get()
            for key in version_info:
                version = version_info[key]
                if key not in SCRIPT_CONFIG_VERSIONS or SCRIPT_CONFIG_VERSIONS[key] != version:
                    SCRIPT_CONFIG_VERSIONS[key] = version

    def check_stop_start(self):
        LOG.debug('check_stop_start')
        now = int(time.time())

        # self.check_finish()

        for col in self.collectors_holder.all_valid_collectors():
            # if col.interval > 0:
            #     col.need_stop = True
            #     if col.period_count <= 0:
            #         col.need_start = True
            #         col.period_count = col.interval / (PERIOD * 4)
            #     col.period_count = col.period_count - 1
            if col.process is None:
                col.need_start = True
            elif col.modified:
                col.need_stop = True
                col.need_start = True
            elif (now - col.last_datapoint) > self.options.allowed_inactivity_time:
                col.need_stop = True
                if not self.options.remove_inactive_collectors:
                    col.need_start = True

    def check_finish_task(self):
        count = 19
        while count > 0:
            count -= 1
            self.check_finish()
            time.sleep(1)
        for col in self.collectors_holder.all_collectors():
            if col.finish:
                col.finish = False
                col.process = None

    def check_finish(self):

        for col in self.collectors_holder.all_living_collectors():

            if col.finish:
                continue

            status = col.process.poll()
            if status is None:
                continue
            # col.process = None
            col.finish = True

            if status == 13:
                LOG.info('removing %s from the list of collectors (by request)',
                         col.name)
                # col.disable = True
            elif status != 0:
                now = int(time.time())
                LOG.warning('collector %s terminated after %d seconds with '
                            'status code %d',
                            col.name, now - col.last_spawn, status)
                # col.disable = True

    def stop_children(self):
        LOG.debug('stop_children')
        for col in self.collectors_holder.all_living_collectors():
            if col.need_stop:
                col.shutdown()

    def spawn_children(self):
        """Iterates over our defined collectors and performs the logic to
               determine if we need to spawn, kill, or otherwise take some
               action on them."""

        LOG.debug('spawn_children')
        if not agent_globals.RUN:
            return

        for col in self.collectors_holder.all_valid_collectors():
            if col.process is None and col.need_start:
                key = col.name[0:col.name.index('.')]
                if key in SCRIPT_CONFIG_VERSIONS:
                    col.startup(SCRIPT_CONFIG_VERSIONS[key])
                else:
                    col.startup()

        # for col in all_valid_collectors():
        #     now = int(time.time())
        #     if col.interval == 0:
        #         if col.proc is None:
        #             spawn_collector(col)
        #     elif col.interval <= now - col.lastspawn:
        #         if col.proc is None:
        #             spawn_collector(col)
        #             continue
        #
        #         # I'm not very satisfied with this path.  It seems fragile and
        #         # overly complex, maybe we should just reply on the asyncproc
        #         # terminate method, but that would make the main tcollector
        #         # block until it dies... :|
        #         if col.nextkill > now:
        #             continue
        #         if col.killstate == 0:
        #             LOG.warning('warning: %s (interval=%d, pid=%d) overstayed '
        #                         'its welcome, SIGTERM sent',
        #                         col.name, col.interval, col.proc.pid)
        #             kill(col.proc)
        #             col.nextkill = now + 5
        #             col.killstate = 1
        #         elif col.killstate == 1:
        #             LOG.error('error: %s (interval=%d, pid=%d) still not dead, '
        #                       'SIGKILL sent',
        #                       col.name, col.interval, col.proc.pid)
        #             kill(col.proc, signal.SIGKILL)
        #             col.nextkill = now + 5
        #             col.killstate = 2
        #         else:
        #             LOG.error('error: %s (interval=%d, pid=%d) needs manual '
        #                       'intervention to kill it',
        #                       col.name, col.interval, col.proc.pid)
        #             col.nextkill = now + 300


def parse_version(script):
    # f = open(script, "r")
    # line = f.readline().replace('\n', ''.replace('\r', ''))
    # f.close()
    # return line[1:]
    return '-1'


# def parse_config_version(config):
#     # f = open(config, "r")
#     # line = f.readline().replace('\n', ''.replace('\r', ''))
#     # f.close()
#     # return line[1:]
#     return '-1'


class RecycleThread(threading.Thread):

    def __init__(self, queue):
        super(RecycleThread, self).__init__()
        self.queue = queue

    def run(self):
        while True:
            if self.queue.empty():
                if agent_globals.RUN:
                    time.sleep(1)
                else:
                    break
            else:
                process = self.queue.get()
                if process is None:
                    continue
                try:
                    terminated = False
                    for attempt in range(5):
                        if process.poll() is not None:
                            terminated = True
                            break
                        LOG.info('Waiting %ds for PID %d to exit...'
                                 % (5 - attempt, process.pid))
                        time.sleep(1)
                    if not terminated:
                        kill(process, signal.SIGKILL)
                        self.queue.put(process)
                        # process.wait()
                except Exception as e:
                    LOG.exception('ignoring uncaught exception while shutting down: %s', e)
                    self.queue.put(process)
