import atexit
import Queue
import os

import agent_globals
import logger
import option_parser
from collector import CollectorHolder
from configs import agent_config
from error_report_thread import ErrReportThread
from reader import ReaderThread
from scripts_runner import ScriptsRunner
from sender import SenderThread

LOG = logger.get_logger('tcollector')
SENDERS = []
CONFIG_VERSION_QUEUE = Queue.Queue(128)


def start(options):
    atexit.register(stop)

    collector_holder = CollectorHolder()

    reader = ReaderThread(collector_holder, options)
    reader.start()

    for i in range(options.sender_thread_number):
        sender = SenderThread(reader.readerq, CONFIG_VERSION_QUEUE, options)
        sender.start()
        SENDERS.append(sender)

    err_report_thread = ErrReportThread(options)
    err_report_thread.start()

    main_loop = ScriptsRunner(options, collector_holder, CONFIG_VERSION_QUEUE)
    try:
        main_loop.start()
    except KeyboardInterrupt:
        LOG.info('main_loop KeyboardInterrupt')

    agent_globals.RUN = False
    LOG.info("********** agent is exiting")

    for col in collector_holder.all_living_collectors():
        LOG.info('shutting down collector %s' % col.name)
        col.shutdown()

    # LOG.info('Shutting down -- joining the reader thread.')
    # reader.join()
    # for sender in SENDERS:
    #     LOG.info('Shutting down -- joining the sender thread.')
    #     sender.join()


def stop():
    LOG.info("********** agent stop")
    # agent_globals.RUN = False


def add_path():
    dirs_to_add = ['/usr/sbin', '/usr/bin', '/sbin', '/bin']
    path = os.environ["PATH"]
    dirs = path.split(os.pathsep)

    for dta in dirs_to_add:
        if dta in dirs:
            pass
        else:
            os.environ["PATH"] += os.pathsep + dta


if __name__ == '__main__':
    agent_argv = []
    agent_configs = agent_config.get_configs()
    for key in agent_configs:
        agent_argv.append(key)
        agent_argv.append(agent_configs[key])
    options, args = option_parser.parse_cmdline(agent_argv)
    agent_globals.RUN = True
    add_path()
    start(options)
