import json
import socket
import threading
import time
import traceback
import urllib2

import agent_globals
import logger
from error_report import ErrorReport

LOG = logger.get_logger('ErrReportThread')


class ErrReportThread(threading.Thread):

    def __init__(self, options):
        super(ErrReportThread, self).__init__()
        self.options = options
        self.err_report = ErrorReport()

    def run(self):
        cycle = self.options.err_report_cycle
        while agent_globals.RUN:
            try:
                LOG.debug('--------ErrReportThread running')
                self.report_err()
            except Exception:
                print traceback.format_exc()
            finally:
                time.sleep(cycle)

    def report_err(self):

        reader_err = self.err_report.pop_reader_err()
        sender_err = self.err_report.pop_sender_err()
        other_err = self.err_report.pop_other_err()

        report_list = []

        if reader_err is not None:
            report_list.append(reader_err)
        if sender_err is not None:
            report_list.append(sender_err)

        if len(report_list) == 0:
            self.err_report.set_report_enabled(False)
            LOG.debug("<ErrReportThread> no error report")
            return
        else:
            self.err_report.set_report_enabled(True)

        if other_err is not None:
            report_list.append(other_err)

        report = {'report_type': 'error_report',
                  'report_id': self.err_report.get_report_id(),
                  'hostname': socket.gethostname(),
                  'errors': report_list}

        try:
            url = self.options.pcollector_addr
            headers = {}
            req = urllib2.Request(url, headers=headers)

            report_str = json.dumps(report)

            LOG.debug(report_str)

            response = urllib2.urlopen(req, report_str, timeout=3)
            LOG.debug('<ErrReportThread> response code:%s, data:%s' % (response.getcode(), response.read().rstrip('\n')))
        except urllib2.URLError as e:
            LOG.error("<ErrReportThread> Got URLError error %s", e)
