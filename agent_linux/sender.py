import json
import threading
import time
import urllib2
from Queue import Empty

import agent_globals
import logger
import wai

LOG = logger.get_logger('SenderThread')


class SenderThread(threading.Thread):

    def __init__(self, reader_queue, msg_queue, options):

        super(SenderThread, self).__init__()

        self.reader_queue = reader_queue
        self.msg_queue = msg_queue
        self.options = options

        self.apigw_url = options.api_gw_url
        self.apigw_key = options.api_gw_key
        self.access_key = options.iam_access_key
        self.secret_key = options.iam_secret_key

        self.pack_start_time = int(time.time() * 1000)
        self.package = {}
        self.package_wait = options.send_package_wait

    def run(self):

        while agent_globals.RUN:
            LOG.debug('... Sender Running')
            try:
                try:
                    metric_line = self.reader_queue.get(True, 5)
                    self.pack_data(metric_line)
                except Empty:
                    if (int(time.time() * 1000) - self.pack_start_time) >= self.package_wait:
                        self.send_package()
                    time.sleep(0.1)
            except Exception as e:
                LOG.exception('Exception in SenderThread: %s' % e)
                time.sleep(1)

    def pack_data(self, metric_line):

        json_data = []

        data_array = metric_line.data
        for data in data_array:

            json_obj = {}
            metrics = data['metrics']
            for metric in metrics:
                json_obj[metric] = metrics[metric]

            dimensions = data['dimensions']
            for dimension in dimensions:
                json_obj[dimension] = dimensions[dimension]

            json_data.append(json_obj)

        json_out = {'data': json_data,
                    'script.type': metric_line.script_type,
                    # 'script.version': metric_line.script_version,
                    # 'config.version': metric_line.config_version,
                    'script.version': '-1',
                    'config.version': '-1',
                    # 'host.id': metric_line.host_id,
                    'time': int(time.time() * 1000)}
        # json_object = json.dumps(json_out)
        # LOG.debug(json_object)

        now = int(time.time() * 1000)
        if not self.package:
            self.package['host.id'] = wai.get_host_id(self.options)
            self.package['package'] = []
            self.pack_start_time = now

        self.package['package'].append(json_out)
        if (now - self.pack_start_time) >= self.package_wait:
            self.send_package()

    def send_package(self):

        if self.package:

            if 'package' in self.package and len(self.package['package']) > 0:
                package_json = json.dumps(self.package)
                LOG.debug(package_json)
                try:
                    headers = {}
                    req = urllib2.Request(self.options.pcollector_addr, headers=headers)

                    response = urllib2.urlopen(req, package_json, timeout=3)
                    LOG.debug('response code:%s, data:%s' % (response.getcode(), response.read().rstrip('\n')))
                except urllib2.URLError as e:
                    LOG.error("Got URLError error %s", e)
                    for err_line in e:
                        print err_line

            self.package = {}
