import json
import threading
import time
import traceback
import urllib2
from Queue import Empty, Full

import agent_globals
import logger
import wai
from error_report import ErrorReport

LOG = logger.get_logger('SenderThread')


class SenderThread(threading.Thread):

    def __init__(self, reader_queue, version_queue, options):

        super(SenderThread, self).__init__()

        self.reader_queue = reader_queue
        self.version_queue = version_queue
        self.options = options

        self.apigw_url = options.api_gw_url
        self.apigw_key = options.api_gw_key
        self.access_key = options.iam_access_key
        self.secret_key = options.iam_secret_key

        self.pack_start_time = long(time.time() * 1000)
        self.package = {}
        self.package_wait = options.send_package_wait

        self.send_record = {}
        self.redundant_record = {}

        self.err_report = ErrorReport()

        self.last_data_time = int(time.time())

    def run(self):

        while agent_globals.RUN:
            if (int(time.time()) - self.last_data_time) < self.options.not_work_threshold:
                LOG.debug('... Sender Running')
            try:
                try:
                    metric_line = self.reader_queue.get(True, 5)
                    self.pack_data(metric_line)
                    self.last_data_time = int(time.time())
                except Empty:
                    if (long(time.time() * 1000) - self.pack_start_time) >= self.package_wait:
                        self.send_package()
                    time.sleep(0.1)
            except Exception as e:
                self.package = {}
                LOG.exception('Exception in SenderThread: %s' % e)
                self.err_report.record_sender_err(traceback.format_exc())
                time.sleep(1)

    def pack_data(self, metric_line):

        now = long(time.time() * 1000)

        # redundant check
        script_type = metric_line.script_type
        if script_type:
            if script_type in self.send_record:
                last_time = self.send_record[script_type]
                if now - last_time < 40000:
                    LOG.debug('------ discard %s metric_line !!! interval: %d', script_type, now - last_time)
                    if script_type in self.redundant_record:
                        count = self.redundant_record[script_type]
                        self.redundant_record[script_type] = count + 1
                    else:
                        self.redundant_record[script_type] = 1
                    # discard redundant data
                    return

            self.send_record[script_type] = now

        # actual job
        json_data = []
        timestamp = now

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

            timestamp = data['timestamp']

            # ntp_checked = True
            # if 'ntp_checked' in data:
            #     ntp_checked = data['ntp_checked']

            ntp_checked = data['ntp_checked']

        json_out = {'data': json_data,
                    'script.type': metric_line.script_type,
                    # 'script.version': metric_line.script_version,
                    # 'config.version': metric_line.config_version,
                    'script.version': '-1',
                    'config.version': '-1',
                    # 'host.id': metric_line.host_id,
                    'time': timestamp,
                    'ntp_checked': ntp_checked}
        if metric_line.source_key:
            json_out['source.key'] = metric_line.source_key
        # json_object = json.dumps(json_out)
        # LOG.debug(json_object)

        if not self.package:
            self.package['host.id'] = wai.get_host_id(self.options)
            self.package['package'] = []
            self.pack_start_time = now

        self.package['package'].append(json_out)
        if (now - self.pack_start_time) >= self.package_wait:
            self.send_package()

    def generate_redundant_report(self):
        data = {'error_code': 201}

        need_report = False
        for k in self.redundant_record:
            count = self.redundant_record[k]
            if count > 0:
                need_report = True
                data[k] = count

        if not need_report:
            return None

        report = {'data': data,
                  'script.type': 'error_report',
                  'script.version': '-1',
                  'config.version': '-1',
                  'time': long(time.time() * 1000)}

        self.redundant_record = {}

        return report

    def send_package(self):

        if self.package:
            # add error report if it exist
            report = self.generate_redundant_report()
            if report:
                LOG.debug('------ error occurred, check error report !!!')
                self.package['package'].append(report)


            # actual collected data
            if 'package' in self.package and len(self.package['package']) > 0:
                package_json = json.dumps(self.package)
                LOG.debug(package_json)
                try:
                    headers = {}
                    req = urllib2.Request(self.options.pcollector_addr, headers=headers)

                    response = urllib2.urlopen(req, package_json, timeout=10)
                    response_str = response.read().rstrip('\n')
                    LOG.debug('response code:%s, data:%s' % (response.getcode(), response_str))

                    try:
                        response_dict = json.loads(response_str)
                        if 'plugin' in response_dict:
                            latest_versions = response_dict['plugin']
                            self.update_config_version(latest_versions)
                    except Exception:
                        pass
                except urllib2.URLError as e:
                    LOG.error("Got URLError error %s", e)
                    for err_line in e:
                        print err_line

            self.package = {}

    def update_config_version(self, latest_versions):
        try:
            self.version_queue.put(latest_versions, False)
        except Full:
            pass

