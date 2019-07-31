import atexit
import Queue

import agent_globals
import logger
import option_parser
from collector import CollectorHolder
from configs import agent_config
from reader import ReaderThread
from scripts_runner import ScriptsRunner
from sender import SenderThread

LOG = logger.get_logger('tcollector')
SENDERS = []
MSG_QUEUE = Queue.Queue()


def start(options):
    atexit.register(stop)

    collector_holder = CollectorHolder()

    reader = ReaderThread(collector_holder, options)
    reader.start()

    for i in range(options.sender_thread_number):
        sender = SenderThread(reader.readerq, MSG_QUEUE, options)
        sender.start()
        SENDERS.append(sender)

    main_loop = ScriptsRunner(options, collector_holder, MSG_QUEUE)
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


if __name__ == '__main__':
    agent_argv = []
    agent_configs = agent_config.get_configs()
    for key in agent_configs:
        agent_argv.append(key)
        agent_argv.append(agent_configs[key])
    options, args = option_parser.parse_cmdline(agent_argv)
    agent_globals.RUN = True
    start(options)
